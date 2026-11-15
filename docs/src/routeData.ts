/**
 * Gives every page the image that stands for it when it is shared on X, LinkedIn, Bluesky
 * or Discord: a blog post its own, from `public/social/<slug>.jpg`, every other page
 * `public/social/default.jpg`. `scripts/social-images.ts` makes them.
 */
import { defineRouteMiddleware } from "@astrojs/starlight/route-data"

export const onRequest = defineRouteMiddleware((context) => {
  const { entry, head } = context.locals.starlightRoute
  const isPost = entry.id.startsWith("blog/") && entry.data.date !== undefined
  const name = isPost ? entry.id.slice("blog/".length) : "default"
  const base = import.meta.env.BASE_URL.replace(/\/$/, "")
  const image = new URL(`${base}/social/${name}.jpg`, context.site).href
  const alt = isPost ? `${entry.data.title}, on the effect-cqrs blog` : "effect-cqrs: CQRS and event sourcing for Effect v4"
  head.push(
    { tag: "meta", attrs: { property: "og:image", content: image } },
    { tag: "meta", attrs: { property: "og:image:width", content: "1200" } },
    { tag: "meta", attrs: { property: "og:image:height", content: "630" } },
    { tag: "meta", attrs: { property: "og:image:alt", content: alt } },
    { tag: "meta", attrs: { name: "twitter:image", content: image } },
    { tag: "meta", attrs: { name: "twitter:image:alt", content: alt } }
  )
})
