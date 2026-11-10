interface ImportMetaEnv {
  /** Where the workshop server runs, `http://localhost:3000` by default. */
  readonly VITE_API_URL?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
