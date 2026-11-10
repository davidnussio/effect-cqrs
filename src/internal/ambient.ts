/**
 * Contexts that services capture when they are built.
 *
 * @internal
 * @module
 */
import { Context, References, Tracer } from "effect"

/**
 * Removes from a context what describes where an effect runs rather than what it
 * needs: the span and the log annotations. A service provides the context it was
 * built with to the handlers it runs, which would otherwise put their spans under
 * the one that built the service and hide the caller's annotations.
 */
export const withoutAmbient = <R>(context: Context.Context<R>): Context.Context<R> =>
  Context.omit(
    Tracer.ParentSpan,
    References.CurrentLogAnnotations,
    References.CurrentLogSpans,
    References.CurrentStackFrame,
    References.TracerSpanAnnotations,
    References.TracerSpanLinks
  )(context) as Context.Context<R>
