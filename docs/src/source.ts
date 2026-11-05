/**
 * The lines between `// #region name` and `// #endregion` in a source file, so a page
 * quotes the code that runs. A region that went missing fails the build.
 */
export const region = (source: string, name: string): string => {
  const lines = source.split("\n")
  const start = lines.findIndex((line) => line.trim() === `// #region ${name}`)
  const end = lines.findIndex((line, index) => index > start && line.trim() === "// #endregion")
  if (start === -1 || end === -1) throw new Error(`No region "${name}" in the quoted source`)
  const body = lines.slice(start + 1, end)
  const indent = Math.min(...body.filter((line) => line.trim() !== "").map((line) => line.search(/\S/)))
  return body.map((line) => line.slice(indent)).join("\n")
}

/** The bank demo, which the build puts next to the documentation. */
export const demoUrl = `${import.meta.env.BASE_URL.replace(/\/$/, "")}/demo/`

export const repository = "https://github.com/davidnussio/effect-cqrs"

export const sourceUrl = (path: string) => `${repository}/blob/main/${path}`
