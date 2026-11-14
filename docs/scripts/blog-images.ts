/**
 * Generates the cover image of each blog post with OpenAI's image model, through the AI SDK,
 * into `src/assets/blog/<slug>.webp`. Images that exist are kept: delete one, or pass its slug
 * with `--force`, to make it again.
 *
 *   envsec -c ai run --inject 'node docs/scripts/blog-images.ts'
 *   envsec -c ai run --inject 'node docs/scripts/blog-images.ts --force read-models'
 *
 * It reads the key from `OPENAI_API_KEY`.
 */
import { openai, type OpenAIImageModelGenerationOptions } from "@ai-sdk/openai"
import { generateImage } from "ai"
import { existsSync, mkdirSync, writeFileSync } from "node:fs"
import { join } from "node:path"

const model = openai.image("gpt-image-2.5-sunburst")

const out = join(import.meta.dirname, "../src/assets/blog")

// One style for the whole series, so the covers read as a set.
const style = [
  "Wide editorial illustration for a technical blog about software architecture.",
  "Flat vector style with soft paper grain, clean geometric shapes and gentle shadows.",
  "Limited palette: deep teal, warm cream, muted navy, with small coral accents.",
  "A small public lending library is the recurring setting.",
  "Calm, precise, slightly playful. Generous empty space on the left and right.",
  "Absolutely no text, letters, numbers, words, signs with writing, logos or watermarks anywhere."
].join(" ")

const covers: Record<string, string> = {
  "why-cqrs-and-event-sourcing":
    "A long paper ribbon of stamped loan cards unrolls from a library desk across the whole scene, each card a small event in a row. On one side a librarian writes a new card at the end of the ribbon; on the other side readers look at tidy display boards built from the same cards.",
  "your-first-aggregate":
    "A single hardcover book stands on a librarian's desk inside a softly glowing boundary. A stack of loan cards beside it, a rubber stamp, and a gate-like frame around the book that one request at a time passes through; one request card is turned back with a gentle red mark.",
  "read-models":
    "One central shelf of stamped loan cards feeds, through thin flowing lines, three different display boards: a catalog wall with books on and off the shelf, a board of members with the books they hold, and a small bar chart. One of the boards is being dismantled and rebuilt by a librarian, unbothered.",
  "sagas":
    "Two separate desks, a member's desk and a book desk, each with its own ledger. A courier with a satchel walks a dotted path between them carrying a loan card, with a small checklist of steps floating above the path, one step checked, one in progress.",
  "tests-schema-changes-and-production":
    "A workbench where an old, yellowed loan card is fed through a small brass translating machine and comes out as a newer card. Behind it, sturdy database cylinders, a test checklist board with green ticks, and a drawer from which one card is gently being erased.",
  "the-saga-you-skipped":
    "A single copy of a book on a desk with two members on either side, each holding a loan card for the same book, a subtle tangle of crossed lines between them. In the background, a calm courier with a satchel waits by a dotted path, ready to do it properly.",
  "three-ways-to-read":
    "Three readers look at the same lent book in three ways: one holds the book itself at the desk, one reads a catalog board on the wall, and one waits beside the board holding a numbered ticket, an hourglass at their feet. Light lines connect the board to a ribbon of loan cards.",
  "fix-the-code-not-the-data":
    "On one side, a librarian looks at a torn index card in a card drawer, its missing half nowhere to be found. On the other side, a second librarian calmly rebuilds a display board of loans from an intact ribbon of stamped loan cards, a small wrench resting on the desk beside a single corrected card.",
  "the-wrong-event":
    "A librarian pins a new correction card, marked with a small coral tab, right next to an older loan card on a long ribbon of stamped loan cards, leaving the old card in place. Beside the desk, a reminder letter is being folded back into its envelope.",
  "the-double-write":
    "A librarian files a loan card in a big ledger at the desk while, at the door, a courier with a satchel takes neat copies straight from the ledger's pages into the satchel. A small mail cart outside waits under a passing rain cloud.",
  "i-clicked-twice":
    "A member at the library front desk presses a brass desk bell twice, while the calm librarian holds up a single receipt. A second, identical payment slip is set aside on a desk spike, unprocessed.",
  "the-form-that-overwrites":
    "Two librarians at separate desks each fill in their own copy of the same catalog card at the same time. On the wall between them, a single large catalog card shows only one of their changes, with a faint smudge where the other one was erased.",
  "the-message-that-blocks-the-queue":
    "A short conveyor belt of books runs through a small spine-labelling machine in the library workroom. The machine is jammed on one old book, a line of books waiting behind it, while a librarian calmly lifts that one book onto a small side cart so the others can move on.",
  "what-it-looked-like-last-tuesday":
    "At the front desk, a librarian lays a transparent sheet over one section of a long ribbon of stamped loan cards, highlighting a single moment from the week before. A member stands at the desk waiting with a puzzled look, a wall calendar behind them with one past day circled.",
  "the-bug-that-wont-reproduce":
    "In a quiet library workroom, a librarian carefully cuts a short strip of stamped loan cards from a long ribbon and lays it into a small glass test box on a workbench. A magnifying glass and a little brass tray of index cards sit beside the box.",
  "two-pods-two-emails":
    "Two identical mail sorting desks side by side in the library's back office, each with a clerk putting the same letter into an envelope for the same member. Between the desks, a single brass key hangs on a hook, one clerk reaching for it while the other pauses.",
  "forget-me-from-the-backups-too":
    "In the library's archive room, a librarian drops a single small key into a paper shredder. Behind her, shelves of sealed archive boxes, each closed with a small padlock, stay neatly in place, untouched.",
  "from-a-table-to-a-log":
    "At a long library desk, a librarian copies the rows of an old leather ledger, one card per row, onto the beginning of a long ribbon of stamped loan cards that continues across the desk and out of view. The old ledger lies open beside a small stack of finished cards.",
  "change-the-rules-keep-the-history":
    "Above the library front desk, a librarian on a small step ladder swaps an old rules board for a new one, both boards shown only as simple shapes and icons with no writing. On the desk below, a ribbon of already stamped loan cards keeps its stamps unchanged, while one fresh card is being stamped with a slightly different coloured stamp."
}

const args = process.argv.slice(2)
const force = args.includes("--force")
const only = args.filter((arg) => !arg.startsWith("--"))

for (const slug of only) {
  if (!(slug in covers)) throw new Error(`No cover for "${slug}". Known: ${Object.keys(covers).join(", ")}`)
}

mkdirSync(out, { recursive: true })

const todo = Object.entries(covers).filter(([slug]) => {
  if (only.length > 0 && !only.includes(slug)) return false
  return force || !existsSync(join(out, `${slug}.webp`))
})

if (todo.length === 0) {
  console.log("Every cover exists. Pass --force and a slug to make one again.")
}

await Promise.all(todo.map(async ([slug, scene]) => {
  const { image } = await generateImage({
    model,
    prompt: `${style}\n\nScene: ${scene}`,
    size: "1536x768",
    providerOptions: {
      openai: {
        quality: "high",
        outputFormat: "webp",
        outputCompression: 85
      } satisfies OpenAIImageModelGenerationOptions
    }
  })
  const file = join(out, `${slug}.webp`)
  writeFileSync(file, image.uint8Array)
  console.log(`${slug}: ${file} (${Math.round(image.uint8Array.length / 1024)} KB)`)
}))
