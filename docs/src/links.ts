/**
 * A Sätteri plugin that rewrites the relative links of a page to start from the site's
 * base, so `../commands/` on the sagas guide becomes `/effect-cqrs/guides/commands/`.
 * The pages work either way, but `llms-full.txt` and the Markdown copy of a page don't
 * know which page a relative link was relative to.
 */
import { relative, sep } from "node:path"
import { fileURLToPath } from "node:url"

interface Node {
  readonly type: string
  readonly properties?: Readonly<Record<string, unknown>>
  readonly attributes?: ReadonlyArray<{ readonly type: string; readonly name?: string; readonly value?: unknown }>
}

interface Context {
  setProperty(node: Node, key: string, value: unknown): void
}

// Not a scheme such as `https:` or `mailto:`, a path from the root, a fragment or a query.
const isRelative = (href: unknown): href is string =>
  typeof href === "string" && href !== "" && !/^(?:[a-z][a-z\d+.-]*:|\/|#|\?)/i.test(href)

export const baseLinks =
  (options: { readonly base: string; readonly contentDir: string }) =>
  ({ fileURL }: { readonly fileURL: URL | undefined }) => {
    if (!fileURL) return null
    const id = relative(options.contentDir, fileURLToPath(fileURL)).split(sep).join("/").replace(/\.mdx?$/, "")
    if (id.startsWith("../")) return null
    const path = id === "index" ? "" : `${id.replace(/\/index$/, "")}/`
    const page = new URL(`${options.base.replace(/\/$/, "")}/${path}`, "https://site.invalid")
    const resolve = (href: string) => {
      const url = new URL(href, page)
      return url.pathname + url.search + url.hash
    }

    return {
      name: "base-links",
      element: {
        filter: ["a"],
        visit(node: Node, ctx: Context) {
          const href = node.properties?.href
          if (isRelative(href)) ctx.setProperty(node, "href", resolve(href))
        }
      },
      // Components in MDX, such as <LinkCard href="../tutorial/" />.
      mdxJsxFlowElement: { filter: [], visit: visitJsx },
      mdxJsxTextElement: { filter: [], visit: visitJsx }
    }

    function visitJsx(node: Node, ctx: Context) {
      for (const attribute of node.attributes ?? []) {
        if (attribute.type === "mdxJsxAttribute" && attribute.name === "href" && isRelative(attribute.value)) {
          ctx.setProperty(node, "href", resolve(attribute.value))
        }
      }
    }
  }
