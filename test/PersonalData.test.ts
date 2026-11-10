import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { Cause, Config, Effect, Exit, Layer, Option, Redacted, Ref, Schema, Stream } from "effect"
import { KeyValueStore } from "effect/persistence"
import { SqlClient } from "effect/sql"
import { TestClock } from "effect/testing"
import {
  Aggregate,
  AggregateService,
  Checkpoints,
  EventStore,
  EventVersions,
  Fixture,
  PersonalData,
  ViewModel
} from "effect-cqrs"
import * as PersonalDataNode from "effect-cqrs/PersonalDataNode"
import { postgresClient, postgresUrl } from "../bench/common.ts"

const AccountEvent = Schema.TaggedUnion({
  AccountOpened: {
    owner: PersonalData.field(Schema.String, { redacted: "[forgotten]" }),
    email: Schema.optional(PersonalData.field(Schema.String, { redacted: "forgotten@example.invalid" })),
    currency: Schema.Literals(["CHF", "EUR"])
  },
  Deposited: { amount: Schema.Number },
  TransferSent: {
    to: Schema.String,
    // Describes the receiver, so it belongs to them rather than to the account holder.
    beneficiary: PersonalData.field(Schema.String, { redacted: "[forgotten]", subject: "to" }),
    amount: Schema.Number
  }
})

interface State {
  readonly owner: string
  readonly balance: number
  readonly beneficiaries: ReadonlyArray<string>
}

const Account = Aggregate.make<State>()({
  name: "account",
  events: AccountEvent,
  initial: { owner: "", balance: 0, beneficiaries: [] },
  projection: {
    AccountOpened: (state, { owner }) => ({ ...state, owner }),
    Deposited: (state, { amount }) => ({ ...state, balance: state.balance + amount }),
    TransferSent: (state, { amount, beneficiary }) => ({
      ...state,
      balance: state.balance - amount,
      beneficiaries: [...state.beneficiaries, beneficiary]
    })
  }
}).command("open", {
  payload: Schema.Struct({ owner: Schema.String, email: Schema.optional(Schema.String) }),
  handler: (_, { email, owner }) =>
    Effect.succeed(
      AccountEvent.cases.AccountOpened.make({ owner, currency: "CHF", ...(email === undefined ? {} : { email }) })
    )
}).command("deposit", {
  payload: Schema.Struct({ amount: Schema.Number }),
  handler: (_, { amount }) => Effect.succeed(AccountEvent.cases.Deposited.make({ amount }))
}).command("transfer", {
  payload: Schema.Struct({ to: Schema.String, beneficiary: Schema.String, amount: Schema.Number }),
  handler: (_, payload) => Effect.succeed(AccountEvent.cases.TransferSent.make(payload))
})

const Keys = PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
const Storage = Layer.merge(EventStore.layerMemory, Keys)

const storedPayloads = Effect.gen(function*() {
  const store = yield* EventStore.EventStore
  return Array.from(yield* Stream.runCollect(store.read({ aggregateNames: ["account"] }))).map(({ payload }) => payload)
})

/** The ephemeral master key, counting its calls as a KMS would bill them. */
const countingMasterKey = Effect.gen(function*() {
  const inner = yield* PersonalData.MasterKey
  const calls = { wrap: 0, unwrap: 0 }
  const masterKey = PersonalData.MasterKey.of({
    keyId: inner.keyId,
    wrap: (dataKey, context) => Effect.suspend(() => (calls.wrap++, inner.wrap(dataKey, context))),
    unwrap: (wrapped, context) => Effect.suspend(() => (calls.unwrap++, inner.unwrap(wrapped, context)))
  })
  return { calls, masterKey }
})

const openAlice = (accounts: AggregateService.AggregateService<typeof Account>) =>
  Effect.gen(function*() {
    yield* accounts.commands.open("alice", { owner: "Alice Rossi", email: "alice@example.ch" })
    yield* accounts.commands.deposit("alice", { amount: 100 })
    yield* accounts.commands.transfer("alice", { to: "bob", beneficiary: "Bob Bianchi", amount: 30 })
  })

describe("PersonalData", () => {
  it.effect("stores personal fields encrypted and reads them back", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* openAlice(accounts)

      const stored = JSON.stringify(yield* storedPayloads)
      for (const secret of ["Alice Rossi", "alice@example.ch", "Bob Bianchi"]) {
        assert.isFalse(stored.includes(secret), `${secret} is stored in plaintext`)
      }
      assert.isTrue(stored.includes(`"currency":"CHF"`))
      assert.isTrue(stored.includes(`"amount":30`))

      assert.deepStrictEqual(yield* accounts.readModel("alice"), {
        owner: "Alice Rossi",
        balance: 70,
        beneficiaries: ["Bob Bianchi"]
      })
      const history = yield* accounts.history("alice", { payload: true })
      assert.deepStrictEqual(
        history[0]!.payload,
        AccountEvent.cases.AccountOpened.make({ owner: "Alice Rossi", email: "alice@example.ch", currency: "CHF" })
      )
    }).pipe(Effect.provide(Storage)))

  it.effect("reads the replacements once the subject is forgotten, leaving the log and the amounts alone", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* openAlice(accounts)
      // The state is remembered with Alice's name in it.
      assert.strictEqual((yield* accounts.readModel("alice")).owner, "Alice Rossi")
      const before = yield* storedPayloads

      yield* PersonalData.forget("alice")

      assert.deepStrictEqual(yield* storedPayloads, before)
      assert.deepStrictEqual(yield* accounts.readModel("alice"), {
        owner: "[forgotten]",
        balance: 70,
        beneficiaries: ["Bob Bianchi"]
      })
      const [opened] = yield* accounts.history("alice", { payload: true })
      assert.deepStrictEqual(
        opened!.payload,
        AccountEvent.cases.AccountOpened.make({
          owner: "[forgotten]",
          email: "forgotten@example.invalid",
          currency: "CHF"
        })
      )
    }).pipe(Effect.provide(Storage)))

  it.effect("encrypts a field with the key of the subject it names", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* openAlice(accounts)

      yield* PersonalData.forget("bob")

      assert.deepStrictEqual(yield* accounts.readModel("alice"), {
        owner: "Alice Rossi",
        balance: 70,
        beneficiaries: ["[forgotten]"]
      })
    }).pipe(Effect.provide(Storage)))

  it.effect("records the request in the log once", () =>
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      yield* PersonalData.forget("alice")
      yield* PersonalData.forget("alice")

      const recorded = Array.from(yield* Stream.runCollect(store.read({ aggregateNames: [PersonalData.streamName] })))
      assert.deepStrictEqual(
        recorded.map(({ aggregateId, payload, type }) => ({ aggregateId, type, payload })),
        [{ aggregateId: "alice", type: PersonalData.subjectForgottenType, payload: {} }]
      )
    }).pipe(Effect.provide(Storage)))

  it.effect("refuses to record personal data of a forgotten subject", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* openAlice(accounts)
      yield* PersonalData.forget("bob")

      const error = yield* Effect.flip(
        accounts.commands.transfer("alice", { to: "bob", beneficiary: "Bob Bianchi", amount: 1 })
      )
      assert.deepStrictEqual(error, new PersonalData.SubjectForgotten({ subject: "bob" }))
      // Events without personal data of the subject still go through.
      yield* accounts.commands.deposit("alice", { amount: 1 })
    }).pipe(Effect.provide(Storage)))

  it.effect("fails to read a field whose ciphertext was altered", () =>
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const accounts = yield* AggregateService.make(Account)
      yield* accounts.commands.open("alice", { owner: "Alice Rossi" })
      const [stored] = Array.from(yield* Stream.runCollect(store.read()))
      const payload = stored!.payload as any
      // Moves Alice's encrypted name into another field, where it must not decrypt.
      const decode = AggregateService.makeEventDecoder(Account, yield* PersonalData.Keys)
      const error = yield* Effect.flip(decode({ ...stored!, payload: { ...payload, email: payload.owner } }))
      assert.strictEqual(error._tag, "PersonalDataError")
    }).pipe(Effect.provide(Storage)))

  it.effect("fails to read a field copied into another stream", () =>
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const accounts = yield* AggregateService.make(Account)
      yield* accounts.commands.open("alice", { owner: "Alice Rossi" })
      yield* accounts.commands.open("mallory", { owner: "Mallory" })
      const [alice, mallory] = Array.from(yield* Stream.runCollect(store.read()))
      const decode = AggregateService.makeEventDecoder(Account, yield* PersonalData.Keys)
      const copied = { ...mallory!, payload: { ...(mallory!.payload as object), owner: (alice!.payload as any).owner } }
      const error = yield* Effect.flip(decode(copied))
      assert.include(error.message, "encrypted for another subject")
    }).pipe(Effect.provide(Storage)))

  it.effect("fails rather than redacts when the key store has lost a key", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* accounts.commands.open("alice", { owner: "Alice Rossi" })

      // Another process whose key store is empty.
      const empty = yield* PersonalData.make(PersonalData.makeMemoryBackend()).pipe(
        Effect.provide(PersonalData.layerMasterKeyEphemeral)
      )
      const store = yield* EventStore.EventStore
      const [stored] = Array.from(yield* Stream.runCollect(store.read()))
      const error = yield* Effect.flip(AggregateService.makeEventDecoder(Account, empty)(stored!))
      assert.strictEqual(error._tag, "PersonalDataError")
    }).pipe(Effect.provide(Storage)))

  it.effect("leaves fields without a value alone", () =>
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      yield* accounts.commands.open("alice", { owner: "Alice Rossi" })
      const [payload] = yield* storedPayloads
      assert.isFalse(Object.hasOwn(payload as object, "email"))
    }).pipe(Effect.provide(Storage)))

  it.effect("needs keys when an aggregate has personal fields", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(AggregateService.make(Account))
      assert.isTrue(Exit.hasDies(exit))
      assert.include(String(Exit.isFailure(exit) ? exit.cause : ""), "needs PersonalData.Keys")
    }).pipe(Effect.provide(EventStore.layerMemory)))

  it.effect("seals snapshots, which forgetting destroys", () =>
    Effect.gen(function*() {
      const store = yield* KeyValueStore.KeyValueStore
      const snapshots = {
        every: 1,
        schema: Schema.Struct({
          owner: Schema.String,
          balance: Schema.Number,
          beneficiaries: Schema.Array(Schema.String)
        })
      }

      yield* Effect.scoped(Effect.flatMap(AggregateService.make(Account, { snapshots }), openAlice))
      const key = `effect-cqrs/snapshot/${JSON.stringify(["account", "1", "alice"])}`
      const saved = yield* store.get(key)
      assert.isDefined(saved)
      assert.isFalse(saved!.includes("Alice"))

      // A new process starts from the snapshot.
      const second = yield* AggregateService.make(Account, { snapshots })
      assert.strictEqual((yield* second.readModel("alice")).owner, "Alice Rossi")

      yield* PersonalData.forget("alice")

      const third = yield* AggregateService.make(Account, { snapshots })
      assert.strictEqual((yield* third.readModel("alice")).owner, "[forgotten]")
    }).pipe(Effect.provide(Layer.merge(Storage, KeyValueStore.layerMemory))))

  it.effect("let other processes use a key until their cache expires", () =>
    Effect.gen(function*() {
      const backend = PersonalData.makeMemoryBackend()
      const here = yield* PersonalData.make(backend)
      const there = yield* PersonalData.make(backend, { cacheTtl: "1 minute" })
      const { keyId } = yield* here.encryptionKey("alice")
      assert.isTrue(Option.isSome(yield* there.decryptionKey("alice", keyId)))

      yield* here.forget("alice")
      assert.isTrue(Option.isNone(yield* here.decryptionKey("alice", keyId)))
      assert.isTrue(Option.isSome(yield* there.decryptionKey("alice", keyId)))

      yield* TestClock.adjust("1 minute")
      assert.isTrue(Option.isNone(yield* there.decryptionKey("alice", keyId)))
      assert.isTrue(yield* there.isForgotten("alice"))
    }).pipe(Effect.provide(PersonalData.layerMasterKeyEphemeral)))

  it.effect("create the key of a new subject once for concurrent writers", () =>
    Effect.gen(function*() {
      const { calls, masterKey } = yield* countingMasterKey
      const backend = PersonalData.makeMemoryBackend()
      const keys = yield* PersonalData.make(backend).pipe(Effect.provideService(PersonalData.MasterKey, masterKey))
      const results = yield* Effect.forEach(
        Array.from({ length: 20 }, () => "alice"),
        (subject) => keys.encryptionKey(subject),
        { concurrency: "unbounded" }
      )
      assert.strictEqual(new Set(results.map(({ keyId }) => keyId)).size, 1)
      assert.deepStrictEqual(calls, { wrap: 1, unwrap: 0 })

      // Another process reading the key unwraps it once, however many readers it has.
      const other = yield* PersonalData.make(backend).pipe(Effect.provideService(PersonalData.MasterKey, masterKey))
      yield* Effect.forEach(
        Array.from({ length: 20 }, () => "alice"),
        (subject) => other.decryptionKey(subject, results[0]!.keyId),
        { concurrency: "unbounded" }
      )
      assert.deepStrictEqual(calls, { wrap: 1, unwrap: 1 })
    }).pipe(Effect.provide(PersonalData.layerMasterKeyEphemeral)))

  it.effect("refuse a key for a subject forgotten while it was created", () =>
    Effect.gen(function*() {
      const keys = yield* PersonalData.make(PersonalData.makeMemoryBackend())
      yield* Effect.all([Effect.exit(keys.encryptionKey("alice")), keys.forget("alice")], { concurrency: "unbounded" })
      // Either may come first, as long as nothing encrypts with the key afterwards.
      const error = yield* Effect.flip(keys.encryptionKey("alice"))
      assert.strictEqual(error._tag, "SubjectForgotten")
      assert.isTrue(yield* keys.isForgotten("alice"))
    }).pipe(Effect.provide(PersonalData.layerMasterKeyEphemeral)))

  it.effect("keep a master key from configuration out of its errors", () =>
    Effect.gen(function*() {
      const secret = "c2VjcmV0IGtleQ"
      const layer = PersonalData.layerMasterKeyConfig({
        keyId: Config.succeed("m1"),
        key: Config.succeed(Redacted.make(`${secret}!!`))
      })
      const exit = yield* Effect.exit(Effect.scoped(Layer.build(layer)))
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        // Structured loggers print the error as JSON, causes included.
        assert.notInclude(Cause.pretty(exit.cause), secret)
        assert.notInclude(JSON.stringify(exit.cause), secret)
      }
    }))

  it.effect("read keys wrapped by an earlier master key", () =>
    Effect.gen(function*() {
      const backend = PersonalData.makeMemoryBackend()
      const first = { keyId: "2025", key: Redacted.make(crypto.getRandomValues(new Uint8Array(32))) }
      const second = { keyId: "2026", key: Redacted.make(crypto.getRandomValues(new Uint8Array(32))) }

      const before = yield* PersonalData.make(backend).pipe(Effect.provide(PersonalData.layerMasterKey(first)))
      const { keyId } = yield* before.encryptionKey("alice")

      const rotated = yield* PersonalData.make(backend).pipe(
        Effect.provide(PersonalData.layerMasterKey({ ...second, previous: [first] }))
      )
      assert.isTrue(Option.isSome(yield* rotated.decryptionKey("alice", keyId)))

      const withoutPrevious = yield* PersonalData.make(backend).pipe(Effect.provide(PersonalData.layerMasterKey(second)))
      const error = yield* Effect.flip(withoutPrevious.decryptionKey("alice", keyId))
      assert.include(error.message, "Unknown master key 2025")
    }))

  it.effect("decrypt older versions of an event before upcasting them", () =>
    Effect.gen(function*() {
      const OpenedV1 = Schema.Struct({ name: PersonalData.field(Schema.String, { redacted: "[forgotten]" }) })
      const Versioned = Aggregate.make<State>()({
        name: "account",
        events: AccountEvent,
        initial: Account.initial,
        projection: Account.projection,
        versions: {
          AccountOpened: EventVersions.from(OpenedV1).next(
            AccountEvent.cases.AccountOpened,
            ({ name }) => ({ owner: name, currency: "CHF" as const })
          )
        }
      }).command("deposit", Account.commands.deposit)
      const keys = yield* PersonalData.Keys
      const { keyId } = yield* keys.encryptionKey("alice")
      const store = yield* EventStore.EventStore
      // What a V1 writer stored: the name encrypted at its V1 path.
      const v1 = AggregateService.makeEventEncoder(
        Aggregate.make<{}>()({
          name: "account",
          events: Schema.TaggedUnion({ AccountOpened: OpenedV1.fields }),
          initial: {},
          projection: { AccountOpened: (state) => state }
        }),
        keys
      )
      const event = yield* v1({ _tag: "AccountOpened", name: "Alice Rossi" }, "alice")
      yield* store.append({ aggregateName: "account", aggregateId: "alice", expectedVersion: 0, events: [event] })
      assert.isTrue(JSON.stringify(event.payload).includes(keyId))

      const accounts = yield* AggregateService.make(Versioned)
      assert.strictEqual((yield* accounts.readModel("alice")).owner, "Alice Rossi")
    }).pipe(Effect.provide(Storage)))

  it.effect("reach view models in plaintext", () =>
    Effect.gen(function*() {
      const owners = yield* Ref.make<ReadonlyArray<string>>([])
      const Owners = ViewModel.make({
        name: "owners",
        aggregates: [Account],
        dispose: Ref.set(owners, []),
        handlers: { AccountOpened: ({ owner }) => Ref.update(owners, (all) => [...all, owner]) }
      })
      yield* Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        const viewModels = yield* ViewModel.ViewModels
        yield* openAlice(accounts)
        yield* viewModels.caughtUp()
        assert.deepStrictEqual(yield* Ref.get(owners), ["Alice Rossi"])

        yield* PersonalData.forget("alice")
        yield* viewModels.replay({ viewModels: ["owners"] })
        assert.deepStrictEqual(yield* Ref.get(owners), ["[forgotten]"])
      }).pipe(Effect.provide(ViewModel.layer([Owners])))
    }).pipe(Effect.provide(Layer.mergeAll(Storage, Checkpoints.layerMemory))))

  it.effect("let view models erase what they stored about a forgotten subject", () =>
    Effect.gen(function*() {
      const owners = yield* Ref.make<ReadonlyMap<string, string>>(new Map())
      const erased = yield* Ref.make<ReadonlyArray<string>>([])
      const Owners = ViewModel.make({
        name: "owners",
        aggregates: [Account],
        dispose: Ref.set(owners, new Map()),
        handlers: {
          AccountOpened: ({ owner }, { aggregateId }) => Ref.update(owners, (all) => new Map(all).set(aggregateId, owner))
        },
        onForget: (subject) =>
          Effect.andThen(
            Ref.update(erased, (all) => [...all, subject]),
            Ref.update(owners, (all) => {
              const rest = new Map(all)
              rest.delete(subject)
              return rest
            })
          )
      })
      yield* Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        const viewModels = yield* ViewModel.ViewModels
        yield* openAlice(accounts)
        yield* accounts.commands.open("carla", { owner: "Carla Verdi" })
        yield* viewModels.caughtUp()

        yield* PersonalData.forget("alice")
        yield* viewModels.caughtUp()
        assert.deepStrictEqual(yield* Ref.get(owners), new Map([["carla", "Carla Verdi"]]))
        assert.deepStrictEqual(yield* Ref.get(erased), ["alice"])

        // A replay goes through the same steps.
        yield* viewModels.replay({ viewModels: ["owners"] })
        assert.deepStrictEqual(yield* Ref.get(owners), new Map([["carla", "Carla Verdi"]]))
      }).pipe(Effect.provide(ViewModel.layer([Owners])))
    }).pipe(Effect.provide(Layer.mergeAll(Storage, Checkpoints.layerMemory))))

  it.effect("work in fixtures without any setup", () =>
    Fixture.make(Account)
      .given([AccountEvent.cases.AccountOpened.make({ owner: "Alice Rossi", currency: "CHF" })])
      .when("deposit", { amount: 5 })
      .expectEvents([AccountEvent.cases.Deposited.make({ amount: 5 })])
      .inspectState((state) => assert.strictEqual(state.owner, "Alice Rossi")))

  describe("field", () => {
    it("rejects a replacement that isn't a value of the field", () => {
      assert.throws(
        () => PersonalData.field(Schema.String.check(Schema.isMinLength(3)), { redacted: "x" }),
        /redacted value/
      )
    })

    it("finds fields with checks, added before or after marking them", () => {
      const Refined = Schema.TaggedUnion({
        Opened: {
          owner: PersonalData.field(Schema.NonEmptyString, { redacted: "[forgotten]" }),
          nickname: PersonalData.field(Schema.String, { redacted: "x" }).check(Schema.isMinLength(1))
        }
      })
      assert.deepStrictEqual(
        PersonalData.fieldsByTag(Refined, "refined").get("Opened")?.map(({ path }) => path),
        [["owner"], ["nickname"]]
      )
    })

    it("rejects personal fields where they can't be found", () => {
      const InArray = Schema.TaggedUnion({
        Listed: { names: Schema.Array(PersonalData.field(Schema.String, { redacted: "" })) }
      })
      assert.throws(() => PersonalData.fieldsByTag(InArray, "list"), /names is a Arrays/)
    })

    it("rejects a subject that isn't a sibling", () => {
      const Missing = Schema.TaggedUnion({
        Sent: { name: PersonalData.field(Schema.String, { redacted: "", subject: "to" }) }
      })
      assert.throws(() => PersonalData.fieldsByTag(Missing, "transfer"), /not next to it/)
    })
  })
})

describe("PersonalData ciphers", () => {
  const ciphers = [["WebCrypto", PersonalData.webCrypto], ["node:crypto", PersonalDataNode.cipher]] as const
  const place = new TextEncoder().encode("place")

  for (const [writer, encrypting] of ciphers) {
    for (const [reader, decrypting] of ciphers) {
      it.effect(`read with ${reader} what ${writer} wrote`, () =>
        Effect.gen(function*() {
          const bytes = crypto.getRandomValues(new Uint8Array(32))
          const plaintext = new TextEncoder().encode(JSON.stringify("Alice Rossi"))
          const sealed = yield* encrypting.encrypt(yield* encrypting.importKey(bytes), plaintext, place)
          assert.strictEqual(sealed.byteLength, PersonalData.ivLength + plaintext.byteLength + PersonalData.tagLength)
          const key = yield* decrypting.importKey(bytes)
          assert.deepStrictEqual(yield* decrypting.decrypt(key, sealed, place), plaintext)

          const altered = new Uint8Array(sealed)
          altered[PersonalData.ivLength] = altered[PersonalData.ivLength]! ^ 1
          assert.isTrue(Exit.isFailure(yield* Effect.exit(decrypting.decrypt(key, altered, place))))
          assert.isTrue(Exit.isFailure(yield* Effect.exit(decrypting.decrypt(key, sealed, new Uint8Array([1])))))
          assert.isTrue(Exit.isFailure(yield* Effect.exit(decrypting.decrypt(key, sealed.subarray(0, 20), place))))
        }))
    }

    it.effect(`${writer} refuses keys that aren't 32 bytes`, () =>
      Effect.gen(function*() {
        assert.isTrue(Exit.isFailure(yield* Effect.exit(encrypting.importKey(new Uint8Array(16)))))
      }))
  }

  it.effect("read events written with node:crypto through WebCrypto, and the other way", () =>
    Effect.gen(function*() {
      const backend = PersonalData.makeMemoryBackend()
      const masterKey = { keyId: "m1", key: Redacted.make(crypto.getRandomValues(new Uint8Array(32))) }
      const keysWith = (cipher: Layer.Layer<never>) =>
        Layer.fresh(
          Layer.effect(PersonalData.Keys, PersonalData.make(backend)).pipe(
            Layer.provide(PersonalData.layerMasterKey(masterKey)),
            Layer.provide(cipher)
          )
        )
      const node = keysWith(PersonalDataNode.layerCipher)
      const web = keysWith(Layer.empty)

      yield* Effect.flatMap(AggregateService.make(Account), (accounts) =>
        accounts.commands.open("alice", { owner: "Alice Rossi" })).pipe(Effect.provide(node))
      yield* Effect.flatMap(AggregateService.make(Account), (accounts) =>
        accounts.commands.open("bob", { owner: "Bob Bianchi" })).pipe(Effect.provide(web))

      for (const keys of [node, web]) {
        const owners = yield* Effect.gen(function*() {
          const accounts = yield* AggregateService.make(Account, { stateCacheSize: 0 })
          assert.strictEqual((yield* PersonalData.Keys).cipher === PersonalDataNode.cipher, keys === node)
          return [(yield* accounts.readModel("alice")).owner, (yield* accounts.readModel("bob")).owner]
        }).pipe(Effect.provide(keys))
        assert.deepStrictEqual(owners, ["Alice Rossi", "Bob Bianchi"])
      }
    }).pipe(Effect.provide(EventStore.layerMemory)))
})

/** A key store backend on a fresh database, dropped with the scope. */
const backends: ReadonlyArray<readonly [string, Layer.Layer<SqlClient.SqlClient> | undefined]> = [
  ["memory", undefined],
  ["SQLite", SqliteClient.layer({ filename: ":memory:" }) as Layer.Layer<SqlClient.SqlClient>],
  ...(postgresUrl === undefined ? [] : [["Postgres", Layer.fresh(postgresClient()) as unknown as Layer.Layer<SqlClient.SqlClient>] as const])
]

const makeBackend = (client: Layer.Layer<SqlClient.SqlClient> | undefined) =>
  Effect.gen(function*() {
    if (client === undefined) return PersonalData.makeMemoryBackend()
    const table = `keys_${crypto.randomUUID().replaceAll("-", "")}`
    const sql = yield* SqlClient.SqlClient
    yield* Effect.addFinalizer(() => sql.unsafe(`DROP TABLE IF EXISTS ${table}`).pipe(Effect.ignore))
    return yield* PersonalData.makeSqlBackend({ table })
  })

describe.each(backends)("PersonalData key store on %s", (_, client) => {
  const run = <A, E>(effect: (backend: PersonalData.Backend) => Effect.Effect<A, E, PersonalData.MasterKey>) =>
    Effect.scoped(Effect.flatMap(makeBackend(client), effect)).pipe(
      Effect.provide(PersonalData.layerMasterKeyEphemeral),
      Effect.provide(client ?? (Layer.empty as unknown as Layer.Layer<SqlClient.SqlClient>))
    )

  const key = (keyId: string) => ({
    _tag: "Active" as const,
    keyId,
    masterKeyId: "m1",
    wrapped: new Uint8Array([1, 2, 3])
  })

  it.live("keeps the first key a subject gets", () =>
    run((backend) =>
      Effect.gen(function*() {
        assert.isTrue(Option.isNone(yield* backend.get("alice")))
        assert.deepStrictEqual(yield* backend.create("alice", key("k1")), key("k1"))
        assert.deepStrictEqual(yield* backend.create("alice", key("k2")), key("k1"))
        assert.deepStrictEqual(yield* backend.get("alice"), Option.some(key("k1")))
      })
    ))

  it.live("forgets a subject for good, keeping when it first happened", () =>
    run((backend) =>
      Effect.gen(function*() {
        yield* backend.create("alice", key("k1"))
        yield* backend.forget("alice", 1000)
        yield* backend.forget("alice", 2000)
        const forgotten = { _tag: "Forgotten" as const, forgottenAt: 1000 }
        assert.deepStrictEqual(yield* backend.get("alice"), Option.some(forgotten))
        assert.deepStrictEqual(yield* backend.create("alice", key("k2")), forgotten)
        // A subject can be forgotten before it ever had a key.
        yield* backend.forget("bob", 3000)
        assert.deepStrictEqual(yield* backend.get("bob"), Option.some({ _tag: "Forgotten" as const, forgottenAt: 3000 }))
      })
    ))

  it.live("gives every racing writer the same key", () =>
    run((backend) =>
      Effect.gen(function*() {
        // Two processes, each with writers racing for a new subject.
        const { calls, masterKey } = yield* countingMasterKey
        const keys = yield* Effect.all([PersonalData.make(backend), PersonalData.make(backend)]).pipe(
          Effect.provideService(PersonalData.MasterKey, masterKey)
        )
        const results = yield* Effect.forEach(
          Array.from({ length: 20 }, (_, index) => keys[index % 2]!),
          (each) => each.encryptionKey("alice"),
          { concurrency: "unbounded" }
        )
        assert.strictEqual(new Set(results.map(({ keyId }) => keyId)).size, 1)
        assert.isAtMost(calls.wrap, 2)
      })
    ))
})

describe("PersonalData on SQLite", () => {
  it.effect("leaves the table to migrations of your own", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const table = "keys_migrated"
      const backend = yield* PersonalData.makeSqlBackend({ table, migrate: false })
      assert.isTrue(Exit.isFailure(yield* Effect.exit(backend.get("alice"))))

      for (const statement of PersonalData.migrations("sqlite", { table })) yield* sql.unsafe(statement)
      const keys = yield* PersonalData.make(backend)
      const { keyId } = yield* keys.encryptionKey("alice")
      assert.isTrue(Option.isSome(yield* keys.decryptionKey("alice", keyId)))
      // Running them again changes nothing.
      for (const statement of PersonalData.migrations("sqlite", { table })) yield* sql.unsafe(statement)
      assert.isTrue(Option.isSome(yield* backend.get("alice")))
    }).pipe(
      Effect.provide(Layer.merge(PersonalData.layerMasterKeyEphemeral, SqliteClient.layer({ filename: ":memory:" })))
    ))

  it.effect("keeps keys across restarts", () =>
    Effect.gen(function*() {
      const masterKey = PersonalData.layerMasterKey({
        keyId: "m1",
        key: Redacted.make(crypto.getRandomValues(new Uint8Array(32)))
      })
      const keys = Layer.fresh(PersonalData.layerSql().pipe(Layer.provide(masterKey)))
      const firstRun = Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        yield* openAlice(accounts)
      }).pipe(Effect.provide(keys))
      const nextRun = Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        return yield* accounts.readModel("alice")
      }).pipe(Effect.provide(keys))

      yield* firstRun
      assert.strictEqual((yield* nextRun).owner, "Alice Rossi")
      yield* PersonalData.forget("alice").pipe(Effect.provide(keys))
      assert.strictEqual((yield* nextRun).owner, "[forgotten]")
    }).pipe(Effect.provide(Layer.merge(EventStore.layerMemory, SqliteClient.layer({ filename: ":memory:" })))))
})
