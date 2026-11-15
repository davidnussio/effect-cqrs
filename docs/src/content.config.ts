import { docsLoader, i18nLoader } from "@astrojs/starlight/loaders"
import { docsSchema, i18nSchema } from "@astrojs/starlight/schema"
import { z } from "astro/zod"
import { defineCollection } from "astro:content"

export const collections = {
  // A post of the blog has the day it went up on the site, which the feed in pages/blog/rss.xml.ts gives.
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({ extend: z.object({ date: z.coerce.date().optional() }) })
  }),
  // Starlight looks for translations of its interface here. `en.json` keeps the defaults.
  i18n: defineCollection({ loader: i18nLoader(), schema: i18nSchema() })
}
