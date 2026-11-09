// @ts-check
import starlight from "@astrojs/starlight"
import { defineConfig } from "astro/config"

// GitHub Pages serves the site under /<repository>/. The workflow passes that path.
const base = process.env.DOCS_BASE ?? "/effect-cqrs"

// The fonts of the bank demo, so the site and the demo read as one thing.
const fonts =
  "https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap"

export default defineConfig({
  site: "https://davidnussio.github.io",
  base,
  integrations: [
    starlight({
      title: "effect-cqrs",
      description: "CQRS and event sourcing for Effect v4, a trial port of moleculer-cqrs.",
      logo: { src: "./src/assets/logo.svg" },
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/davidnussio/effect-cqrs" }],
      editLink: { baseUrl: "https://github.com/davidnussio/effect-cqrs/edit/main/docs/" },
      head: [
        { tag: "link", attrs: { rel: "preconnect", href: "https://fonts.googleapis.com" } },
        { tag: "link", attrs: { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: true } },
        { tag: "link", attrs: { rel: "stylesheet", href: fonts } }
      ],
      customCss: ["./src/styles/custom.css"],
      components: {
        Hero: "./src/components/Hero.astro",
        // Dark only, like the demo.
        ThemeProvider: "./src/components/ThemeProvider.astro",
        ThemeSelect: "./src/components/Nothing.astro"
      },
      expressiveCode: {
        themes: ["starlight-dark"],
        styleOverrides: {
          borderRadius: "0.75rem",
          borderColor: "rgba(255, 255, 255, 0.08)",
          codeBackground: "#0b0b14",
          codeFontFamily: "'JetBrains Mono', ui-monospace, monospace",
          uiFontFamily: "'Space Grotesk', ui-sans-serif, system-ui, sans-serif",
          frames: {
            editorTabBarBackground: "#11111d",
            editorActiveTabBackground: "#0b0b14",
            editorActiveTabIndicatorTopColor: "#8b6dff",
            editorActiveTabIndicatorBottomColor: "transparent",
            editorTabBarBorderBottomColor: "rgba(255, 255, 255, 0.06)",
            terminalTitlebarBackground: "#11111d",
            terminalTitlebarBorderBottomColor: "rgba(255, 255, 255, 0.06)",
            terminalBackground: "#0b0b14",
            frameBoxShadowCssValue: "0 24px 60px -36px rgba(124, 92, 255, 0.55)"
          }
        }
      },
      sidebar: [
        { label: "Start here", items: ["getting-started", "tutorial"] },
        {
          label: "Guides",
          items: ["guides/aggregates", "guides/commands", "guides/view-models", "guides/sagas", "guides/testing", "guides/storage"]
        },
        { label: "Example", items: ["example/bank", { label: "Live demo", link: "/demo/" }] },
        {
          label: "Reference",
          items: ["reference/modules", "reference/moleculer-cqrs", "reference/performance", "reference/limitations"]
        }
      ]
    })
  ]
})
