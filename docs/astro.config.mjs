// @ts-check
import starlight from "@astrojs/starlight"
import { defineConfig } from "astro/config"

// GitHub Pages serves the site under /<repository>/. The workflow passes that path.
const base = process.env.DOCS_BASE ?? "/effect-cqrs"

export default defineConfig({
  site: "https://davidnussio.github.io",
  base,
  integrations: [
    starlight({
      title: "effect-cqrs",
      description: "CQRS and event sourcing for Effect v4, a trial port of moleculer-cqrs.",
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/davidnussio/effect-cqrs" }],
      editLink: { baseUrl: "https://github.com/davidnussio/effect-cqrs/edit/main/docs/" },
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        { label: "Start here", items: ["getting-started"] },
        {
          label: "Guides",
          items: ["guides/aggregates", "guides/commands", "guides/view-models", "guides/testing", "guides/storage"]
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
