/**
 * Renders the images that stand for a page when it is shared: the title of each blog post
 * beside its cover, in the site's colours. Two sizes, into `public/social`:
 *
 * - `<slug>.jpg`, 1200×630: the `og:image` of the post, which X, LinkedIn, Bluesky and
 *   Discord show under a link to it. `src/routeData.ts` adds it to the page.
 * - `devto/<slug>.jpg`, 1000×420: the `cover_image` of the copy on dev.to.
 *
 * `default.jpg` stands for every other page. Run it after adding a post or changing a
 * title, and commit the images:
 *
 *   node docs/scripts/social-images.ts
 *
 * It drives Google Chrome without a window, from its usual place on macOS or from `CHROME`,
 * and crops and converts with ImageMagick's `magick`. The fonts come from Google Fonts.
 */
import { execFileSync, spawn } from "node:child_process"
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { pathToFileURL } from "node:url"

const docs = join(import.meta.dirname, "..")
const posts = join(docs, "src/content/docs/blog")
const covers = join(docs, "src/assets/blog")
const out = join(docs, "public/social")
const chrome = process.env.CHROME ?? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
const logo = readFileSync(join(docs, "src/assets/logo.svg"), "utf8")
const fonts =
  "https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=JetBrains+Mono:wght@500&display=block"

interface Card {
  readonly name: string
  readonly label: string
  readonly title: string
  readonly cover: string
}

// The width and height of the page Chrome renders, and the size the image is saved at.
// dev.to's cover is rendered larger and scaled down, so the text keeps the same proportions.
const formats = [
  { dir: out, width: 1200, height: 630, saveWidth: 1200, pad: 64, title: 66 },
  { dir: join(out, "devto"), width: 1200, height: 504, saveWidth: 1000, pad: 52, title: 60 }
] as const

const escape = (text: string) =>
  text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;")

const field = (source: string, name: string) => {
  const value = source.match(new RegExp(`^${name}: (.+)$`, "m"))?.[1]
  if (value === undefined) throw new Error(`No ${name} in the frontmatter`)
  return value.startsWith("\"") ? (JSON.parse(value) as string) : value
}

const html = (card: Card, format: (typeof formats)[number]) => {
  // Long titles wrap onto three lines at the full size, so they start a little smaller.
  const size = card.title.length > 28 ? format.title - 8 : format.title
  return `<!doctype html>
<html><head><meta charset="utf-8">
<link rel="stylesheet" href="${fonts}">
<style>
  html, body { margin: 0; }
  body {
    width: ${format.width}px; height: ${format.height}px; overflow: hidden; position: relative;
    background: #07070d; font-family: "Space Grotesk", sans-serif;
  }
  .art {
    position: absolute; top: 0; right: 0; bottom: 0; width: 50%;
    background: url("${pathToFileURL(card.cover).href}") center / cover;
  }
  .art::after {
    content: ""; position: absolute; inset: 0;
    background: linear-gradient(90deg, #07070d 0%, rgba(7, 7, 13, 0.55) 22%, rgba(7, 7, 13, 0) 50%);
  }
  .text {
    position: absolute; top: ${format.pad}px; bottom: ${format.pad}px; left: ${format.pad}px; width: 58%;
    display: flex; flex-direction: column;
  }
  .brand { display: flex; align-items: center; gap: 14px; color: #ececf5; font-size: 28px; font-weight: 600; }
  .brand svg { width: 42px; height: 42px; }
  .label {
    margin-top: auto; color: #a78bfa; font-family: "JetBrains Mono", monospace; font-size: 19px;
    font-weight: 500; letter-spacing: 0.06em; text-transform: uppercase;
  }
  h1 {
    margin: 16px 0 0; color: #ffffff; font-size: ${size}px; line-height: 1.04; font-weight: 700;
    letter-spacing: -0.025em; text-wrap: balance;
  }
  .domain { margin-top: 26px; display: flex; align-items: center; gap: 12px; color: #8e8ea8; font-size: 22px; }
  .domain::before { content: ""; width: 28px; height: 3px; border-radius: 2px; background: #34d399; }
</style></head>
<body>
  <div class="art"></div>
  <div class="text">
    <div class="brand">${logo}<span>effect-cqrs</span></div>
    <div class="label">${escape(card.label)}</div>
    <h1>${escape(card.title)}</h1>
    <div class="domain">effect-cqrs.dambox.ch</div>
  </div>
</body></html>`
}

const cards: Array<Card> = [
  {
    name: "default",
    label: "CQRS and event sourcing",
    title: "Commands decide, events remember",
    cover: join(covers, "why-cqrs-and-event-sourcing.webp")
  }
]
for (const file of readdirSync(posts).filter((file) => file.endsWith(".mdx") && file !== "index.mdx").sort()) {
  const source = readFileSync(join(posts, file), "utf8")
  const slug = file.replace(/\.mdx$/, "")
  const part = field(source, "description").match(/^Part (\d) of the series/)?.[1]
  cards.push({
    name: slug,
    label: part === undefined ? "Blog" : `Part ${part} of 5 · CQRS with effect-cqrs`,
    title: field(source, "title"),
    cover: join(covers, `${slug}.webp`)
  })
}

const work = mkdtempSync(join(tmpdir(), "social-images-"))

// Headless Chrome keeps part of the window for itself, so the page gets less height than
// --window-size asks for. Measure how much once, ask for that much more, and crop it away.
const probe = join(work, "probe.html")
writeFileSync(probe, "<body><script>document.body.textContent = innerHeight</script></body>")
const measured = Number(
  execFileSync(chrome, [
    "--headless",
    "--disable-gpu",
    "--window-size=1200,1000",
    "--dump-dom",
    pathToFileURL(probe).href
  ], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"]
  }).match(/<body>(\d+)/)?.[1]
)
if (!Number.isFinite(measured)) throw new Error("Could not measure Chrome's viewport")
const chromeTakes = 1000 - measured

// Headless Chrome writes the screenshot, says so on stderr, and then sometimes never exits.
// So wait for that line and close it, and give up on a run after a minute, up to three times.
const shoot = (page: string, png: string, width: number, height: number) =>
  new Promise<void>((resolve, reject) => {
    const child = spawn(chrome, [
      "--headless",
      "--disable-gpu",
      "--hide-scrollbars",
      "--force-device-scale-factor=1",
      `--user-data-dir=${mkdtempSync(join(work, "profile-"))}`,
      `--window-size=${width},${height}`,
      "--virtual-time-budget=8000",
      `--screenshot=${png}`,
      pathToFileURL(page).href
    ], { stdio: ["ignore", "ignore", "pipe"] })
    let log = ""
    const timer = setTimeout(() => {
      child.kill("SIGKILL")
      reject(new Error(`Chrome wrote no screenshot of ${page} in a minute`))
    }, 60_000)
    child.stderr.on("data", (chunk: Buffer) => {
      log += chunk.toString()
      if (log.includes("bytes written to file")) {
        clearTimeout(timer)
        child.kill("SIGKILL")
        resolve()
      }
    })
    child.on("exit", () => {
      clearTimeout(timer)
      if (log.includes("bytes written to file")) resolve()
      else reject(new Error(`Chrome exited without a screenshot of ${page}:\n${log}`))
    })
  })

const screenshot = async (page: string, png: string, width: number, height: number) => {
  for (let attempt = 1;; attempt++) {
    try {
      return await shoot(page, png, width, height)
    } catch (error) {
      if (attempt === 3) throw error
      console.log(`  ${String(error)}, trying again`)
    }
  }
}

for (const format of formats) mkdirSync(format.dir, { recursive: true })
for (const card of cards) {
  for (const format of formats) {
    const page = join(work, `${card.name}-${format.width}x${format.height}.html`)
    const png = page.replace(/\.html$/, ".png")
    writeFileSync(page, html(card, format))
    await screenshot(page, png, format.width, format.height + chromeTakes)
    // The page is the top of the screenshot: keep it, scale it to its size, save it as JPEG.
    execFileSync("magick", [
      png,
      "-crop",
      `${format.width}x${format.height}+0+0`,
      "+repage",
      "-resize",
      `${format.saveWidth}x`,
      "-quality",
      "82",
      join(format.dir, `${card.name}.jpg`)
    ], { stdio: "ignore" })
  }
  console.log(card.name)
}
