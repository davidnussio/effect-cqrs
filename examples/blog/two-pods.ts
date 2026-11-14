/**
 * The post "Two pods, two emails": the library scaled to two replicas that share the
 * event store. Without leases both run the outbox that emails members, so every email
 * goes out twice. With leases one replica runs it, and the other takes over when it
 * stops. `two-pods.test.ts` runs it.
 */
import { Context, Effect, Exit, Layer, Option, Ref, Schedule, Scope, SubscriptionRef } from "effect"
import { AggregateService, Checkpoints, EventStore, Leases, Outbox } from "effect-cqrs"
import { Book } from "./library.ts"

// #region mailer
export interface Email {
  readonly to: string
  readonly subject: string
}

/** The mail service both replicas send through. It keeps what it sent. */
export class Mailer extends Context.Service<Mailer, {
  send(email: Email): Effect.Effect<void>
  readonly sent: Effect.Effect<ReadonlyArray<Email>>
}>()("library/Mailer") {
  static readonly layer = Layer.effect(
    Mailer,
    Effect.gen(function*() {
      const sent = yield* Ref.make<ReadonlyArray<Email>>([])
      return Mailer.of({ send: (email) => Ref.update(sent, (all) => [...all, email]), sent: Ref.get(sent) })
    })
  )
}

// Members are emailed when a book is lent to them.
export const Notifications = Outbox.make({
  name: "notifications",
  aggregates: [Book],
  messages: {
    BookLent: ({ memberId }, { aggregateId }) => ({
      type: "email",
      payload: { to: memberId, subject: `You borrowed ${aggregateId}` }
    })
  },
  publish: (messages) =>
    Mailer.use((mailer) => Effect.forEach(messages, ({ payload }) => mailer.send(payload as Email), { discard: true }))
})
// #endregion

// #region replica
// What each replica runs: the outbox, on the shared store, checkpoints and mailer.
const replica = Layer.fresh(Outbox.layer([Notifications]))

// What the replicas share: in production, the database and the mail service.
export const Shared = Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, Mailer.layer)
// #endregion

// #region shared-leases
/**
 * Leases in memory that two replicas in one process can share: the first to ask holds a
 * lease, and when its scope closes the next one in line gets it. `Leases.layerPostgres()`
 * does the same across processes, with PostgreSQL advisory locks.
 */
export const sharedLeases = Layer.sync(Leases.Leases, () => {
  const lines = new Map<string, ReadonlyArray<SubscriptionRef.SubscriptionRef<boolean>>>()
  return Leases.Leases.of({
    acquire: (name) =>
      Effect.gen(function*() {
        const held = yield* SubscriptionRef.make(false)
        const line = lines.get(name) ?? []
        lines.set(name, [...line, held])
        if (line.length === 0) yield* SubscriptionRef.set(held, true)
        yield* Effect.addFinalizer(() =>
          Effect.gen(function*() {
            const rest = (lines.get(name) ?? []).filter((ref) => ref !== held)
            lines.set(name, rest)
            const next = rest[0]
            if (next !== undefined) yield* SubscriptionRef.set(next, true)
          })
        )
        return { held: SubscriptionRef.get(held), changes: SubscriptionRef.changes(held) }
      })
  })
})
// #endregion

/** Waits until the notifications outbox of a replica holds its lease. */
const holding = (outboxes: Outbox.Outboxes["Service"]) =>
  Effect.repeat(outboxes.status, {
    until: (all) => all.some(({ holdsLease, name }) => name === "notifications" && holdsLease),
    schedule: Schedule.spaced("10 millis")
  })

/** Two replicas, a loan, then the first replica stops and there is another loan. */
export const twoReplicas = Effect.gen(function*() {
  const books = yield* AggregateService.make(Book)
  const mailer = yield* Mailer

  // #region two-replicas
  const first = yield* Scope.make()
  const second = yield* Scope.make()
  const replicaA = Context.get(yield* Layer.buildWithScope(replica, first), Outbox.Outboxes)
  const replicaB = Context.get(yield* Layer.buildWithScope(replica, second), Outbox.Outboxes)

  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  const lent = yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })
  yield* replicaA.published(lent.position)
  yield* replicaB.published(lent.position)

  const afterOneLoan = yield* mailer.sent
  // #endregion

  // #region failover
  // Replica A is stopped: a deploy, a crash, the node it ran on is drained.
  yield* Scope.close(first, Exit.void)

  yield* books.commands.add("book-2", { title: "Emma", author: "Jane Austen" })
  const lentToBob = yield* books.commands.lend("book-2", { loanId: "loan-2", memberId: "bob" })
  yield* holding(replicaB)
  yield* replicaB.published(lentToBob.position)

  const afterFailover = yield* mailer.sent
  // #endregion

  const leaseB = (yield* replicaB.status).find(({ name }) => name === "notifications")
  yield* Scope.close(second, Exit.void)
  return { afterOneLoan, afterFailover, holdsLease: Option.fromNullishOr(leaseB?.holdsLease) }
})
