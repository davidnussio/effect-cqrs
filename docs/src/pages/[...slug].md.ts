/**
 * Every page as Markdown, at its URL with `.md` in place of the trailing slash, such as
 * `/guides/aggregates.md`. The "Copy page" button copies it, and the prompts that open
 * a page in a chat point to it. It is rendered the way `llms-full.txt` renders pages,
 * so the code a page quotes from the examples is there too.
 */
import type { APIRoute, GetStaticPaths, InferGetStaticPropsType } from "astro"
import { getCollection } from "astro:content"
// An alias in astro.config.mjs, to a module starlight-llms-txt doesn't export.
import { entryToSimpleMarkdown } from "#llms-markdown"

export const prerender = true

export const getStaticPaths = (async () => {
  const docs = await getCollection("docs", (entry) => !entry.data.draft)
  return docs.map((entry) => ({ params: { slug: entry.id }, props: { entry } }))
}) satisfies GetStaticPaths

type Props = InferGetStaticPropsType<typeof getStaticPaths>

export const GET: APIRoute<Props> = async (context) => {
  const { entry } = context.props
  const title = entry.data.hero?.title ?? entry.data.title
  const description = entry.data.hero?.tagline ?? entry.data.description
  const markdown: string = await entryToSimpleMarkdown(entry, context, false)
  // Links start from the site's base. A page read on its own needs the host too.
  const base = import.meta.env.BASE_URL.replace(/\/$/, "")
  const body = markdown.replaceAll(`](${base}/`, `](${new URL(`${base}/`, context.site)}`)
  const segments = [`# ${title}`, ...(description ? [`> ${description}`] : []), body]
  return new Response(segments.join("\n\n") + "\n", {
    headers: { "Content-Type": "text/markdown; charset=utf-8" }
  })
}
