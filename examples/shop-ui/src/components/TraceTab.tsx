import { Code } from "../../../bank-ui/src/components/ui.tsx"
import { TraceView } from "../../../devtools/TraceView.tsx"
import { useSnapshot } from "../hooks.ts"

export const TraceTab = () => (
  <TraceView
    spans={useSnapshot((s) => s.spans)}
    intro={
      <>
        Every span Effect opens, from <Code>Tracer.make</Code>: the commands' own spans, <Code>EventStore.append</Code>,
        the workflow's <Code>Fulfil.execute</Code> and one span per activity. In production an OTLP exporter sends the
        same spans on. Grey bars are the demo's latency.
      </>
    }
  />
)
