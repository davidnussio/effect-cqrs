/**
 * A tracer for the browser demos: every span reports to `record` when it starts,
 * when an attribute is added and when it ends, so a devtools panel can show spans
 * while they run. In production an exporter such as `OtlpTracer` takes its place.
 */
import { Cause, Exit, Option, Predicate, Tracer } from "effect"

/** A span the tracer saw, while it runs and after it ends. */
export interface SpanEntry {
  readonly spanId: string
  readonly traceId: string
  readonly parentId: string | undefined
  readonly name: string
  /** Milliseconds, on the clock the spans were timed with. */
  readonly start: number
  readonly end: number | undefined
  readonly attributes: Readonly<Record<string, unknown>>
  readonly status: "running" | "ok" | "error"
  /** The tag of the error, `defect` or `interrupted`, when the span failed. */
  readonly error?: string
}

const millis = (nanos: bigint) => Number(nanos / 1000n) / 1000

const failureOf = (exit: Exit.Exit<unknown, unknown>): string => {
  const error = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none()
  if (Option.isSome(error)) {
    const value = error.value
    return Predicate.hasProperty(value, "_tag") && Predicate.isString(value._tag) ? value._tag : "error"
  }
  return Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause) ? "interrupted" : "defect"
}

export const makeRecordingTracer = (record: (span: SpanEntry) => void): Tracer.Tracer => {
  class RecordedSpan extends Tracer.NativeSpan {
    private endedAt: number | undefined = undefined
    private failure: string | undefined = undefined

    constructor(options: ConstructorParameters<typeof Tracer.NativeSpan>[0]) {
      super(options)
      this.report()
    }

    private report() {
      record({
        spanId: this.spanId,
        traceId: this.traceId,
        parentId: Option.getOrUndefined(this.parent)?.spanId,
        name: this.name,
        start: millis(this.startTime),
        end: this.endedAt,
        attributes: Object.fromEntries(this.attributes),
        status: this.endedAt === undefined ? "running" : this.failure === undefined ? "ok" : "error",
        ...(this.failure === undefined ? {} : { error: this.failure })
      })
    }

    override attribute(key: string, value: unknown) {
      super.attribute(key, value)
      // Fields aren't set yet while the parent constructor runs.
      if (this.spanId !== undefined) this.report()
    }

    override end(endTime: bigint, exit: Exit.Exit<unknown, unknown>) {
      super.end(endTime, exit)
      this.endedAt = millis(endTime)
      this.failure = Exit.isSuccess(exit) ? undefined : failureOf(exit)
      this.report()
    }
  }
  return Tracer.make({ span: (options) => new RecordedSpan(options) })
}

/** Replaces a span already recorded, or adds it, keeping at most `max`. */
export const upsertSpan = (
  spans: ReadonlyArray<SpanEntry>,
  span: SpanEntry,
  max: number
): ReadonlyArray<SpanEntry> => {
  const index = spans.findIndex((entry) => entry.spanId === span.spanId)
  if (index === -1) return [...spans, span].slice(-max)
  const next = spans.slice()
  next[index] = span
  return next
}

export const ms = (value: number) =>
  value < 1 ? `${Math.round(value * 1000)}µs` : value < 1000 ? `${value.toFixed(1)}ms` : `${(value / 1000).toFixed(2)}s`
