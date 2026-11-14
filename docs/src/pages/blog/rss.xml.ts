/**
 * The blog's feed, at `/blog/rss.xml`: every post with a `date`, newest first. Readers and
 * aggregators such as daily.dev follow the blog through it.
 */
import rss from "@astrojs/rss"
import type { APIRoute } from "astro"
import { getCollection } from "astro:content"

export const prerender = true

export const GET: APIRoute = async (context) => {
  const posts = await getCollection("docs", (entry) => entry.id.startsWith("blog/") && entry.data.date !== undefined)
  const base = import.meta.env.BASE_URL.replace(/\/$/, "")
  return rss({
    title: "effect-cqrs blog",
    description: "Posts about CQRS and event sourcing, with examples that run on effect-cqrs.",
    site: context.site!,
    items: posts
      .sort((a, b) => b.data.date!.getTime() - a.data.date!.getTime())
      .map((post) => ({
        title: post.data.title,
        description: post.data.description,
        pubDate: post.data.date,
        link: `${base}/${post.id}/`
      }))
  })
}
