import { MotionConfig } from "motion/react"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { App } from "./components/App.tsx"
import "./engine.ts"
import "./styles.css"

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MotionConfig reducedMotion="user">
      <App />
    </MotionConfig>
  </StrictMode>
)
