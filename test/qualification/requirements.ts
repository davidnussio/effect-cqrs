/**
 * What the qualification suite verifies, as data. Each requirement says what the
 * package does and which harm follows if it doesn't, so a manufacturer can link it
 * to the hazards of their own risk analysis.
 *
 * A test claims a requirement by naming its id in the title, through `verifies`
 * in `support.ts`. `report.ts` builds the traceability matrix from the two.
 */

/** What goes wrong for the device when a requirement doesn't hold. */
export type Harm =
  | "data loss"
  | "data corruption"
  | "wrong state"
  | "untraceable change"
  | "disclosure"
  | "undetected failure"
  | "uncontrolled change"
  | "unavailability"

export interface Requirement {
  readonly area: string
  readonly statement: string
  readonly harm: Harm
}

const define = <const R extends Record<string, Requirement>>(requirements: R): R => requirements

export const requirements = define({
  // -------------------------------------------------------------------------
  // The event log, verified on every storage backend
  // -------------------------------------------------------------------------
  "ES-001": {
    area: "Event log",
    statement: "An append stores all of its events or none of them, and a failed append uses up no position.",
    harm: "data corruption"
  },
  "ES-002": {
    area: "Event log",
    statement:
      "Versions number the events of a stream from 1 without gaps, and positions number the events of the store from 1 without gaps, also when writers run at once.",
    harm: "data loss"
  },
  "ES-003": {
    area: "Event log",
    statement:
      "Of several appends based on the same stream version, exactly one is stored. The others fail with ConcurrencyConflict and store nothing.",
    harm: "data corruption"
  },
  "ES-004": {
    area: "Event log",
    statement:
      "An event is read back exactly as it was appended: payload, metadata, type, schema version, timestamp and causation. A value the backend can't hold fails the append rather than being stored altered.",
    harm: "data corruption"
  },
  "ES-005": {
    area: "Event log",
    statement:
      "The log is append-only: the store has no operation that changes or removes an event, and stored events stay the same as the log grows.",
    harm: "untraceable change"
  },
  "ES-006": {
    area: "Event log",
    statement:
      "A read returns every selected event once, in position order, however many pages it takes. Within a stream that is version order.",
    harm: "data loss"
  },
  "ES-007": {
    area: "Event log",
    statement:
      "Filters select exactly the events of the named streams, types and time window. The window includes its start and excludes its end.",
    harm: "wrong state"
  },
  "ES-008": {
    area: "Event log",
    statement:
      "Streams are told apart by aggregate name and id, compared exactly: ids that differ in case, spacing, Unicode form or punctuation never share events.",
    harm: "data corruption"
  },
  "ES-009": {
    area: "Event log",
    statement:
      "A subscriber receives every selected event once and in order, across the step from history to live events.",
    harm: "data loss"
  },
  "ES-010": {
    area: "Event log",
    statement:
      "A failure of the storage backend reaches the caller as an EventStoreError. It is never reported as success.",
    harm: "undetected failure"
  },
  "ES-011": {
    area: "Event log",
    statement:
      "On a durable backend, acknowledged events are still there after the store is closed and opened again, and numbering continues where it stopped.",
    harm: "data loss"
  },
  "ES-012": {
    area: "Event log",
    statement:
      "On a durable backend, a writer process killed without warning loses no acknowledged command and leaves no command half written.",
    harm: "data corruption"
  },
  "ES-013": {
    area: "Event log",
    statement:
      "Every event records the time of its append, taken from the clock of the application. The events of one append share it.",
    harm: "untraceable change"
  },
  "ES-014": {
    area: "Event log",
    statement:
      "A process can start on a durable store that other processes are writing to: its start doesn't fail, and neither do their appends.",
    harm: "unavailability"
  },

  // -------------------------------------------------------------------------
  // Commands and aggregate state
  // -------------------------------------------------------------------------
  "CMD-001": {
    area: "Commands",
    statement:
      "A command whose payload doesn't match its schema, an unknown command and a malformed envelope are rejected before anything is decided, and store nothing.",
    harm: "data corruption"
  },
  "CMD-002": {
    area: "Commands",
    statement:
      "A command its handler rejects fails with the handler's typed error, stores nothing and leaves the state as it was.",
    harm: "wrong state"
  },
  "CMD-003": {
    area: "Commands",
    statement:
      "The state of an aggregate is the fold of all events of its stream, in order. It is the same from memory, in a new service that replays the stream, and after any sequence of commands.",
    harm: "wrong state"
  },
  "CMD-004": {
    area: "Commands",
    statement:
      "Commands on one aggregate run one at a time within a service: each decides on a state that includes every earlier one, so an invariant the handler checks holds under concurrent calls.",
    harm: "wrong state"
  },
  "CMD-005": {
    area: "Commands",
    statement:
      "The same holds across services that share a store, as separate processes do: no decision is stored that was based on a state another writer changed.",
    harm: "wrong state"
  },
  "CMD-006": {
    area: "Commands",
    statement:
      "A command that loses a race decides again on the new state. When its retries run out it fails with ConcurrencyConflict and stores nothing.",
    harm: "undetected failure"
  },
  "CMD-007": {
    area: "Commands",
    statement:
      "A command repeated with its commandId returns the first result and stores nothing more, also in a new service and for concurrent duplicates. The id of another command is refused with CommandIdReused.",
    harm: "data corruption"
  },
  "CMD-008": {
    area: "Commands",
    statement:
      "A command given an expectedVersion other than the aggregate's version fails with WrongExpectedVersion and stores nothing.",
    harm: "wrong state"
  },
  "CMD-009": {
    area: "Commands",
    statement:
      "The events of one command are stored together, at consecutive versions, with one timestamp, one correlation and one cause.",
    harm: "data corruption"
  },
  "CMD-010": {
    area: "Commands",
    statement:
      "A stored event that doesn't match its schema, or has a schema version the aggregate doesn't know, fails the read with an EventDecodeError naming the event. It is never skipped.",
    harm: "wrong state"
  },
  "CMD-011": {
    area: "Commands",
    statement: "The state as of a past time is the fold of the events recorded before it.",
    harm: "wrong state"
  },
  "CMD-012": {
    area: "Commands",
    statement:
      "An event stored in an older version of its schema is read in the current one through the declared upcasts, and the stored event is left as it was.",
    harm: "wrong state"
  },
  "SNAP-001": {
    area: "Snapshots",
    statement: "A state restored from a snapshot is the state a replay of the whole stream gives.",
    harm: "wrong state"
  },
  "SNAP-002": {
    area: "Snapshots",
    statement: "A snapshot that can't be read is ignored and the state is rebuilt from the log.",
    harm: "wrong state"
  },

  // -------------------------------------------------------------------------
  // Audit trail
  // -------------------------------------------------------------------------
  "AUD-001": {
    area: "Audit trail",
    statement:
      "Every event a command stores records when it happened, the correlation it belongs to and what caused it. History lists the events of an aggregate in order, whole or in pages.",
    harm: "untraceable change"
  },
  "AUD-002": {
    area: "Audit trail",
    statement:
      "Metadata that says who ran a command and with which software is recorded on every event the command stores, from the application, the request and the envelope. The keys the framework reserves can't be set from outside.",
    harm: "untraceable change"
  },
  "AUD-003": {
    area: "Audit trail",
    statement:
      "A command run in reaction to an event records that event as its cause and keeps its correlation, so a chain of reactions leads back to the request that started it.",
    harm: "untraceable change"
  },

  // -------------------------------------------------------------------------
  // View models
  // -------------------------------------------------------------------------
  "VM-001": {
    area: "View models",
    statement:
      "A view model handles every event it subscribes to, in store order. Once caughtUp returns for a command's position, the view model has handled that command's events.",
    harm: "wrong state"
  },
  "VM-002": {
    area: "View models",
    statement:
      "A handler that fails stalls its view model on that event: nothing later is handled until it succeeds, and status reports the failure.",
    harm: "undetected failure"
  },
  "VM-003": {
    area: "View models",
    statement:
      "A view model that starts again continues from its checkpoint and handles the events stored while it was down.",
    harm: "data loss"
  },
  "VM-004": {
    area: "View models",
    statement: "Replaying a view model disposes it and rebuilds the content that handling the events live gave.",
    harm: "wrong state"
  },
  "VM-005": {
    area: "View models",
    statement:
      "With the park policy, an event that keeps failing is recorded with its position and its error, counted in status and can be retried. It is never dropped silently.",
    harm: "undetected failure"
  },

  // -------------------------------------------------------------------------
  // Personal data
  // -------------------------------------------------------------------------
  "PD-001": {
    area: "Personal data",
    statement: "A field marked as personal data is never stored in plaintext, in events or in snapshots.",
    harm: "disclosure"
  },
  "PD-002": {
    area: "Personal data",
    statement: "Personal fields read back with their original values, in state and in history.",
    harm: "data corruption"
  },
  "PD-003": {
    area: "Personal data",
    statement:
      "Forgetting a subject destroys its key: its personal fields read as their replacements everywhere, also in a new service and from snapshots, and the log records the erasure once.",
    harm: "disclosure"
  },
  "PD-004": {
    area: "Personal data",
    statement: "Forgetting a subject leaves the data of other subjects and all other fields readable.",
    harm: "data loss"
  },
  "PD-005": {
    area: "Personal data",
    statement:
      "A command that would store personal data of a forgotten subject fails with SubjectForgotten and stores nothing.",
    harm: "disclosure"
  },
  "PD-006": {
    area: "Personal data",
    statement:
      "An encrypted field that was altered, or copied to another field or stream, fails to read with a PersonalDataError. It never reads as other data.",
    harm: "data corruption"
  },
  "PD-007": {
    area: "Personal data",
    statement: "A missing key fails the read. Only a subject recorded as forgotten reads as the replacement.",
    harm: "undetected failure"
  },
  "PD-008": {
    area: "Personal data",
    statement: "A service for an aggregate with personal fields can't be built without a key store.",
    harm: "disclosure"
  },

  // -------------------------------------------------------------------------
  // The package
  // -------------------------------------------------------------------------
  "PKG-001": {
    area: "Package",
    statement:
      "The modules and names the package exports are the recorded ones, so a release that changes them is noticed.",
    harm: "uncontrolled change"
  },
  "PKG-002": {
    area: "Package",
    statement: "The package has no runtime dependency of its own. Its only peer dependency is effect.",
    harm: "uncontrolled change"
  }
})

export type RequirementId = keyof typeof requirements

export const requirementIds = Object.keys(requirements) as ReadonlyArray<RequirementId>

/**
 * A requirement the package is known not to meet in some configuration. The test
 * that shows it is written with `it.fails` and named with `knownAnomaly`, so the
 * suite stays green while the report says the requirement isn't met, and turns
 * red once a release fixes it: the entry then has to go.
 */
export interface Anomaly {
  readonly requirement: RequirementId
  /** Where it shows. */
  readonly configuration: string
  readonly summary: string
  /** How an application avoids it meanwhile. */
  readonly workaround: string
}

/** None is recorded for this version. An entry is keyed by its id, as in `"ANOM-001"`. */
export const anomalies: Readonly<Record<string, Anomaly>> = {}

export type AnomalyId = string
