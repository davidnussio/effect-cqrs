import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Exit, Layer, Option, Schema } from "effect"
import { Aggregate, AggregateService, PersonalData } from "effect-cqrs"
import { KeyValueStore } from "effect/persistence"
import { configurations, head, on, readAll, storageOf, verifies } from "./support.ts"

const erased = "[erased]"

const PatientEvent = Schema.TaggedUnion({
  Registered: {
    name: PersonalData.field(Schema.String, { redacted: erased }),
    birthDate: PersonalData.field(Schema.String, { redacted: "0000-00-00" }),
    ward: Schema.String
  },
  // Describes the contact, so it belongs to them rather than to the patient.
  ContactNamed: {
    contactId: Schema.String,
    contactName: PersonalData.field(Schema.String, { redacted: erased, subject: "contactId" })
  },
  Weighed: { kilograms: Schema.Number }
})

const PatientState = Schema.Struct({
  name: Schema.String,
  birthDate: Schema.String,
  ward: Schema.String,
  contacts: Schema.Array(Schema.String),
  weights: Schema.Array(Schema.Number)
})

const Patient = Aggregate.make<typeof PatientState.Type>()({
  name: "patient",
  events: PatientEvent,
  initial: { name: "", birthDate: "", ward: "", contacts: [], weights: [] },
  projection: {
    Registered: (state, { birthDate, name, ward }) => ({ ...state, name, birthDate, ward }),
    ContactNamed: (state, { contactName }) => ({ ...state, contacts: [...state.contacts, contactName] }),
    Weighed: (state, { kilograms }) => ({ ...state, weights: [...state.weights, kilograms] })
  }
})
  .command("register", {
    payload: Schema.Struct({ name: Schema.String, birthDate: Schema.String, ward: Schema.String }),
    handler: (_, payload) => Effect.succeed(PatientEvent.cases.Registered.make(payload))
  })
  .command("nameContact", {
    payload: Schema.Struct({ contactId: Schema.String, contactName: Schema.String }),
    handler: (_, payload) => Effect.succeed(PatientEvent.cases.ContactNamed.make(payload))
  })
  .command("weigh", {
    payload: Schema.Struct({ kilograms: Schema.Number }),
    handler: (_, payload) => Effect.succeed(PatientEvent.cases.Weighed.make(payload))
  })

const makePatients = AggregateService.make(Patient)
type Patients = AggregateService.AggregateService<typeof Patient>

const snapshots = { every: 1, schema: PatientState }
const snapshotKey = (aggregateId: string) => `effect-cqrs/snapshot/${JSON.stringify([Patient.name, "1", aggregateId])}`

const secrets = ["Anna Bernasconi", "1961-04-23", "Marco Bernasconi", "Luca Ferrari", "1987-11-02"]

const admit = (patients: Patients) =>
  Effect.gen(function*() {
    yield* patients.commands.register("p1", { name: "Anna Bernasconi", birthDate: "1961-04-23", ward: "ward-3" })
    yield* patients.commands.nameContact("p1", { contactId: "c9", contactName: "Marco Bernasconi" })
    yield* patients.commands.weigh("p1", { kilograms: 71.5 })
    yield* patients.commands.register("p2", { name: "Luca Ferrari", birthDate: "1987-11-02", ward: "ward-1" })
  })

const anna = {
  name: "Anna Bernasconi",
  birthDate: "1961-04-23",
  ward: "ward-3",
  contacts: ["Marco Bernasconi"],
  weights: [71.5]
}
const luca = { name: "Luca Ferrari", birthDate: "1987-11-02", ward: "ward-1", contacts: [], weights: [] }

const patientEvents = readAll({ aggregateNames: [Patient.name] })

class KeyRecords extends Context.Service<KeyRecords, PersonalData.Backend>()("qualification/KeyRecords") {}

/** A key store whose records the test can look at, as an auditor would the table. */
const KeyStore = Layer.unwrap(Effect.sync(() => {
  const backend = PersonalData.makeMemoryBackend()
  return Layer.merge(
    Layer.effect(PersonalData.Keys, PersonalData.make(backend)),
    Layer.succeed(KeyRecords, backend)
  )
})).pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))

/** The ciphertext of a field as it is stored. */
const sealedOf = (payload: unknown, field: string) =>
  (payload as Record<string, { readonly $personal: { readonly data: string } }>)[field]!.$personal

const withData = (payload: unknown, field: string, data: string) => ({
  ...(payload as object),
  [field]: { $personal: { ...sealedOf(payload, field), data } }
})

for (const configuration of configurations) {
  const storage = Layer.mergeAll(storageOf(configuration), KeyStore, KeyValueStore.layerMemory)

  describe.skipIf(!configuration.available)(`personal data ${on(configuration)}`, () => {
    it.effect(
      verifies(["PD-001", "PD-002"], "stores personal fields encrypted and reads them back"),
      () =>
        Effect.gen(function*() {
          const saved = yield* KeyValueStore.KeyValueStore
          const patients = yield* Effect.scoped(Effect.tap(AggregateService.make(Patient, { snapshots }), admit))

          const stored = JSON.stringify(yield* readAll())
          const snapshot = `${yield* saved.get(snapshotKey("p1"))}${yield* saved.get(snapshotKey("p2"))}`
          assert.isAbove(snapshot.length, 100)
          for (const secret of secrets) {
            assert.isFalse(stored.includes(secret), `${secret} is in the events`)
            assert.isFalse(snapshot.includes(secret), `${secret} is in the snapshots`)
          }
          // Fields that aren't personal stay readable, for queries and for support.
          assert.isTrue(stored.includes("ward-3") && stored.includes("71.5") && stored.includes("\"c9\""))

          for (const service of [patients, yield* makePatients, yield* AggregateService.make(Patient, { snapshots })]) {
            assert.deepStrictEqual(yield* service.readModel("p1"), anna)
            assert.deepStrictEqual(yield* service.readModel("p2"), luca)
          }
          const history = yield* patients.history("p1", { payload: true })
          assert.deepStrictEqual(history.map((entry) => entry.payload), [
            PatientEvent.cases.Registered.make({ name: "Anna Bernasconi", birthDate: "1961-04-23", ward: "ward-3" }),
            PatientEvent.cases.ContactNamed.make({ contactId: "c9", contactName: "Marco Bernasconi" }),
            PatientEvent.cases.Weighed.make({ kilograms: 71.5 })
          ])
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies(["PD-003", "PD-004"], "erases a forgotten subject everywhere, and nobody else"),
      () =>
        Effect.gen(function*() {
          const keys = yield* PersonalData.Keys
          const records = yield* KeyRecords
          const patients = yield* Effect.scoped(Effect.tap(AggregateService.make(Patient, { snapshots }), admit))
          // The state is remembered with the name in it.
          assert.strictEqual((yield* patients.readModel("p1")).name, "Anna Bernasconi")
          const before = yield* patientEvents
          assert.strictEqual(Option.getOrThrow(yield* records.get("p1"))._tag, "Active")

          yield* PersonalData.forget("p1")
          yield* PersonalData.forget("p1")

          // The key is gone, so what the log holds can't be read by anyone.
          assert.strictEqual(Option.getOrThrow(yield* records.get("p1"))._tag, "Forgotten")
          assert.isTrue(yield* keys.isForgotten("p1"))
          assert.deepStrictEqual(yield* patientEvents, before)
          // The patient's own fields read as their replacements. The contact is another subject.
          const forgotten = { ...anna, name: erased, birthDate: "0000-00-00" }
          for (const service of [patients, yield* makePatients, yield* AggregateService.make(Patient, { snapshots })]) {
            assert.deepStrictEqual(yield* service.readModel("p1"), forgotten)
            assert.deepStrictEqual(yield* service.readModel("p2"), luca)
          }
          const [registered] = yield* patients.history("p1", { payload: true })
          assert.deepStrictEqual(
            registered!.payload,
            PatientEvent.cases.Registered.make({ name: erased, birthDate: "0000-00-00", ward: "ward-3" })
          )
          // The log says that it happened, once.
          const erasures = yield* readAll({ aggregateNames: [PersonalData.streamName] })
          assert.deepStrictEqual(
            erasures.map((event) => [event.aggregateId, event.type, event.version]),
            [["p1", PersonalData.subjectForgottenType, 1]]
          )
          assert.isString(erasures[0]!.correlationId)

          // Forgetting the contact erases their name from the patient's record, and nothing else.
          yield* PersonalData.forget("c9")
          assert.deepStrictEqual(yield* patients.readModel("p1"), { ...forgotten, contacts: [erased] })
          assert.deepStrictEqual(yield* patients.readModel("p2"), luca)
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("PD-005", "refuses to store personal data of a forgotten subject"),
      () =>
        Effect.gen(function*() {
          const patients = yield* makePatients
          yield* admit(patients)
          yield* PersonalData.forget("c9")
          yield* PersonalData.forget("p1")
          const before = yield* head

          const contact = yield* Effect.flip(
            patients.commands.nameContact("p2", { contactId: "c9", contactName: "Marco B." })
          )
          const patient = yield* Effect.flip(
            patients.commands.register("p1", { name: "Anna Bernasconi", birthDate: "1961-04-23", ward: "ward-3" })
          )

          assert.deepStrictEqual(contact, new PersonalData.SubjectForgotten({ subject: "c9" }))
          assert.deepStrictEqual(patient, new PersonalData.SubjectForgotten({ subject: "p1" }))
          assert.strictEqual(yield* head, before)
          // What isn't personal data of the subject still goes through.
          yield* patients.commands.weigh("p1", { kilograms: 70 })
          yield* patients.commands.nameContact("p2", { contactId: "c4", contactName: "Sara Ferrari" })
          assert.strictEqual(yield* head, before + 2)
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("PD-006", "fails to read an encrypted field that was altered or moved"),
      () =>
        Effect.gen(function*() {
          const patients = yield* makePatients
          yield* admit(patients)
          const decode = AggregateService.makeEventDecoder(Patient, yield* PersonalData.Keys)
          const [first, , , second] = yield* patientEvents
          assert.deepStrictEqual(
            yield* decode(first!),
            PatientEvent.cases.Registered.make({ name: "Anna Bernasconi", birthDate: "1961-04-23", ward: "ward-3" })
          )
          const { data } = sealedOf(first!.payload, "name")
          const middle = Math.floor(data.length / 2)
          const flipped = `${data.slice(0, middle)}${data[middle] === "A" ? "B" : "A"}${data.slice(middle + 1)}`

          const damaged = [
            // One character of the ciphertext changed, the end cut off, and nothing left.
            { ...first!, payload: withData(first!.payload, "name", flipped) },
            { ...first!, payload: withData(first!.payload, "name", data.slice(0, -8)) },
            { ...first!, payload: withData(first!.payload, "name", "") },
            // The birth date put where the name goes, in the same event.
            { ...first!, payload: { ...(first!.payload as object), name: (first!.payload as any).birthDate } },
            // Another patient's name put in this one's event, and this event put in another stream.
            { ...first!, payload: { ...(first!.payload as object), name: (second!.payload as any).name } },
            { ...first!, aggregateId: "p2" },
            { ...first!, aggregateId: "p3" }
          ]
          for (const [index, event] of damaged.entries()) {
            const exit = yield* Effect.exit(decode(event))
            assert.isTrue(Exit.isFailure(exit), `case ${index} was read`)
            const error = yield* Effect.flip(decode(event))
            assert.strictEqual(error._tag, "PersonalDataError", `case ${index}`)
            for (const secret of secrets) assert.isFalse(JSON.stringify(error).includes(secret))
          }
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("PD-007", "fails on a missing key, rather than reading the replacement"),
      () =>
        Effect.gen(function*() {
          const patients = yield* makePatients
          yield* admit(patients)
          // A process whose key store lost its records, as after restoring the wrong backup.
          const empty = yield* PersonalData.make(PersonalData.makeMemoryBackend()).pipe(
            Effect.provide(PersonalData.layerMasterKeyEphemeral)
          )
          const lost = yield* makePatients.pipe(Effect.provideService(PersonalData.Keys, empty))

          const errors = yield* Effect.all([
            Effect.flip(lost.readModel("p1")),
            Effect.flip(lost.history("p1", { payload: true })),
            Effect.flip(lost.commands.weigh("p1", { kilograms: 70 }))
          ])

          assert.deepStrictEqual(errors.map((error) => error._tag), [
            "PersonalDataError",
            "PersonalDataError",
            "PersonalDataError"
          ])
          assert.strictEqual(yield* head, 4)
          // Without the payloads nothing needs a key.
          assert.strictEqual((yield* lost.history("p1")).length, 3)
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("PD-008", "can't build a service for personal fields without a key store"),
      () =>
        Effect.gen(function*() {
          const exit = yield* Effect.exit(makePatients)

          assert.isTrue(Exit.hasDies(exit))
          assert.include(String(Exit.isFailure(exit) ? exit.cause : ""), "needs PersonalData.Keys")
        }).pipe(Effect.provide(storageOf(configuration)))
    )
  })
}
