import { TraceView } from "../../../devtools/TraceView.tsx"
import { useSnapshot } from "../hooks.ts"
import { Code } from "./ui.tsx"

export const Trace = () => (
  <TraceView
    spans={useSnapshot((s) => s.spans)}
    intro={
      <>
        Every span Effect opens, from a <Code>Tracer</Code>{" "}
        of the demo's own: a root span per command on the bus, a child per stage it waits in, then the aggregate's{" "}
        <Code>account.*</Code> span and <Code>EventStore.append</Code>. A transfer adds the saga's{" "}
        <Code>Transfer.execute</Code> and its <Code>credit</Code> or <Code>refund</Code>{" "}
        activity. In production an OTLP exporter sends the same spans on.
      </>
    }
  />
)
