import { docsLoader, i18nLoader } from "@astrojs/starlight/loaders"
import { docsSchema, i18nSchema } from "@astrojs/starlight/schema"
import { defineCollection } from "astro:content"

export const collections = {
  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
  // Starlight looks for translations of its interface here. `en.json` keeps the defaults.
  i18n: defineCollection({ loader: i18nLoader(), schema: i18nSchema() })
}
