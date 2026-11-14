// @ts-check
import { satteri } from "@astrojs/markdown-satteri"
import starlight from "@astrojs/starlight"
import { defineConfig } from "astro/config"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import starlightLlmsTxt from "starlight-llms-txt"
import starlightTypeDoc from "starlight-typedoc"
import { baseLinks } from "./src/links.ts"

const root = fileURLToPath(new URL("..", import.meta.url))

// The function starlight-llms-txt turns a page into Markdown with, which its package
// doesn't export. The Markdown copy of each page uses it too, so the two agree.
const llmsMarkdown = join(dirname(fileURLToPath(import.meta.resolve("starlight-llms-txt"))), "entryToSimpleMarkdown.ts")

// GitHub Pages serves the site under /<repository>/. The workflow passes that path.
const base = process.env.DOCS_BASE ?? "/effect-cqrs"

// The fonts of the bank demo, so the site and the demo read as one thing.
const fonts =
  "https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap"

export default defineConfig({
  site: "https://davidnussio.github.io",
  base,
  // Pages that moved, so old links still land. Astro adds the base to where a redirect
  // starts but not to where it goes.
  redirects: {
    "/tutorial": `${base.replace(/\/$/, "")}/tutorial/shop/`,
    "/example/bank": `${base.replace(/\/$/, "")}/tutorial/bank/`,
    "/example/workshop": `${base.replace(/\/$/, "")}/tutorial/workshop/`
  },
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
      plugins: [
        // The API reference, from the TSDoc comments of every module in src/.
        starlightTypeDoc({
          entryPoints: [join(root, "src/*.ts")],
          tsconfig: join(root, "tsconfig.build.json"),
          output: "reference/api",
          typeDoc: {
            name: "API reference",
            // The list of modules is the index of the directory, so the sidebar puts it first.
            entryFileName: "index",
            // index.ts only gathers the modules into namespaces.
            exclude: ["**/index.ts"],
            // One page per module.
            outputFileStrategy: "modules",
            // Leaves out what classes inherit from Effect, such as the members of Schema.TaggedError.
            excludeExternals: true,
            // TypeDoc compiles with TypeScript 6 and the project with TypeScript 7.
            skipErrorChecking: true,
            // Source links point to the commit the site was built from. TypeDoc asks git which
            // files it tracks, so a build from a git worktree has no links.
            sourceLinkTemplate: "https://github.com/davidnussio/effect-cqrs/blob/{gitRevision}/{path}#L{line}"
          }
        }),
        starlightLlmsTxt({
          details: [
            "Commands decide on events, events are the only source of truth, and read models are projections",
            "you can throw away and rebuild.",
            "",
            "- It is built on Effect 4 (`effect@^4`), whose APIs differ from Effect 3's.",
            "- Everything is exported from `effect-cqrs` as a namespace, as in `import { Aggregate } from \"effect-cqrs\"`.",
            "- Each page is also served as Markdown: replace the trailing slash of its URL with `.md`."
          ].join("\n"),
          optionalLinks: [
            {
              label: "LLMS.md",
              url: "https://github.com/davidnussio/effect-cqrs/blob/main/LLMS.md",
              description: "a short guide for coding agents that use effect-cqrs, also in the npm package"
            }
          ],
          promote: ["index*", "getting-started", "tutorial/**", "using-with-ai", "guides/**"],
          demote: ["reference/api/**"],
          // The generated API reference has its own set, so the abridged file stays short.
          exclude: ["reference/api/**"],
          // The "Section titled" links next to each heading.
          customSelectors: { all: [".sl-anchor-link"] },
          customSets: [
            {
              label: "API reference",
              paths: ["reference/api/**"],
              description: "the exports of every module, generated from the TSDoc comments"
            }
          ]
        })
      ],
      components: {
        Hero: "./src/components/Hero.astro",
        SocialIcons: "./src/components/HeaderLinks.astro",
        Search: "./src/components/HeaderSearch.astro",
        // Dark only, like the demo.
        ThemeProvider: "./src/components/ThemeProvider.astro",
        // The title, with buttons to copy the page as Markdown or ask a chat about it.
        PageTitle: "./src/components/PageTitle.astro",
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
        { label: "Start here", items: ["getting-started", "using-with-ai"] },
        // The three examples, each with how it's built and its demo: the shop built step by
        // step, then the bank in the browser and the workshop on a server. The demo links
        // would be the pages' previous and next links, so the pages set their own.
        {
          label: "Tutorial",
          items: [
            {
              label: "The shop",
              items: [
                { slug: "tutorial/shop", label: "How it's built" },
                { label: "Open the demo ↗", link: "/shop-demo/" }
              ]
            },
            {
              label: "The bank",
              items: [
                { slug: "tutorial/bank", label: "How it's built" },
                { label: "Open the demo ↗", link: "/demo/" }
              ]
            },
            {
              label: "The workshop",
              items: [
                { slug: "tutorial/workshop", label: "How it's built" },
                {
                  label: "Run the demo ↗",
                  link: "https://github.com/davidnussio/effect-cqrs/blob/main/examples/workshop/README.md"
                }
              ]
            }
          ]
        },
        {
          label: "Guides",
          items: [
            "guides/aggregates",
            "guides/commands",
            "guides/view-models",
            "guides/sagas",
            "guides/testing",
            "guides/storage",
            "guides/personal-data",
            "guides/integration"
          ]
        },
        {
          label: "Reference",
          items: [
            "reference/modules",
            "reference/moleculer-cqrs",
            "reference/performance",
            "reference/limitations",
            // Generated from the TSDoc comments, a page per module. The group starlight-typedoc
            // makes has a subgroup per module, which stays empty when a module is one page.
            { label: "API", collapsed: true, items: [{ autogenerate: { directory: "reference/api" } }] }
          ]
        },
        // A series of posts on the lending library of examples/blog. The index and the
        // last post set their own previous and next links, so the series stands apart
        // from the API reference before it.
        {
          label: "Blog",
          items: [
            { slug: "blog", label: "All posts" },
            "blog/why-cqrs-and-event-sourcing",
            "blog/your-first-aggregate",
            "blog/read-models",
            "blog/sagas",
            "blog/tests-schema-changes-and-production",
            "blog/the-saga-you-skipped",
            "blog/three-ways-to-read"
          ]
        }
      ]
    })
  ],
  markdown: {
    processor: satteri({ hastPlugins: [baseLinks({ base, contentDir: join(root, "docs/src/content/docs") })] })
  },
  vite: {
    resolve: { alias: { "#llms-markdown": llmsMarkdown } }
  }
})
