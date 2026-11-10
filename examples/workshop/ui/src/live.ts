/**
 * Reads the server's Server-Sent Events. `EventSource` can't send a token, so the
 * stream is a plain request through Effect's HTTP client, parsed with the `Sse`
 * decoder, and opened again with a growing delay whenever it drops.
 */
import { Effect, Fiber, Schedule, Stream } from "effect"
import { Sse } from "effect/encoding"
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http"
import { useEffect, useRef, useSyncExternalStore } from "react"
import type { LiveEvent, Presence } from "../../api.ts"
import { apiUrl, session } from "./client.ts"

export type LiveStatus = "connecting" | "live" | "offline"

/**
 * Follows a stream for as long as the returned function isn't called. A stream
 * refused with `401` ends the session; any other failure retries.
 */
export const follow = (
  path: "/stream/events" | "/stream/control",
  handlers: {
    readonly onMessage: (event: string, data: unknown) => void
    readonly onStatus: (status: LiveStatus) => void
  }
): () => void => {
  const open = Effect.gen(function*() {
    const token = session.get()?.token
    const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk)
    const request = HttpClientRequest.get(`${apiUrl}${path}`).pipe(
      HttpClientRequest.acceptJson,
      HttpClientRequest.setHeader("accept", "text/event-stream"),
      token === undefined ? (r) => r : HttpClientRequest.bearerToken(token)
    )
    yield* HttpClientResponse.stream(client.execute(request)).pipe(
      Stream.decodeText,
      Stream.pipeThroughChannel(Sse.decode()),
      Stream.runForEach((event) =>
        Effect.sync(() => {
          handlers.onStatus("live")
          if (event._tag === "Event") handlers.onMessage(event.event, JSON.parse(event.data))
        })
      )
    )
  })

  const fiber = Effect.runFork(
    Effect.sync(() => handlers.onStatus("connecting")).pipe(
      Effect.andThen(open),
      Effect.tapError(() => Effect.sync(() => handlers.onStatus("offline"))),
      // The server closed the stream, or a free instance restarted: open it again.
      Effect.andThen(Effect.fail("closed" as const)),
      Effect.retry({
        while: (error) => {
          const unauthorized = HttpClientError.isHttpClientError(error) && error.response?.status === 401
          if (unauthorized) session.set(undefined)
          return !unauthorized
        },
        schedule: Schedule.min([Schedule.exponential("500 millis"), Schedule.spaced("10 seconds")])
      }),
      Effect.ignore,
      Effect.provide(FetchHttpClient.layer)
    )
  )
  return () => Effect.runFork(Fiber.interrupt(fiber))
}

// ---------------------------------------------------------------------------
// The room, as the participants' screens see it
// ---------------------------------------------------------------------------

export interface RoomState {
  readonly status: LiveStatus
  /** The position of the last event seen: screens read again when it moves. */
  readonly position: number | undefined
  readonly presence: Presence | undefined
  /** The last events, newest first. */
  readonly feed: ReadonlyArray<LiveEvent>
}

const initial: RoomState = { status: "connecting", position: undefined, presence: undefined, feed: [] }
let room = initial
const listeners = new Set<() => void>()
const set = (next: Partial<RoomState>) => {
  room = { ...room, ...next }
  for (const listener of listeners) listener()
}

/** Opens the participants' stream for the current session; the returned function closes it. */
export const joinRoom = () => {
  room = initial
  return follow("/stream/events", {
    onStatus: (status) => {
      if (status !== room.status) set({ status })
    },
    onMessage: (event, data) => {
      if (event === "presence") set({ presence: data as Presence })
      if (event === "event") {
        const live = data as LiveEvent
        set({ position: live.position, feed: [live, ...room.feed].slice(0, 30) })
      }
    }
  })
}

export const useRoom = () =>
  useSyncExternalStore((listener) => {
    listeners.add(listener)
    return () => listeners.delete(listener)
  }, () => room)

/**
 * Reads a screen's data again when the log moves, at most every 2 seconds: with
 * fifty phones in a room, reading on every event would multiply the server's work
 * by the size of the room. A burst of events ends with one last read. Every 15
 * seconds it reads anyway, in case the stream is down.
 */
export const useLiveRefresh = (refresh: (position?: number) => Promise<void>) => {
  const { position } = useRoom()
  const last = useRef(0)
  const pending = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  useEffect(() => {
    const wait = last.current + 2000 - Date.now()
    const run = () => {
      pending.current = undefined
      last.current = Date.now()
      void refresh(position)
    }
    if (pending.current !== undefined) clearTimeout(pending.current)
    pending.current = setTimeout(run, Math.max(150, wait))
  }, [position, refresh])
  useEffect(() => () => clearTimeout(pending.current), [])
  useEffect(() => {
    const timer = setInterval(() => void refresh(), 15_000)
    return () => clearInterval(timer)
  }, [refresh])
}
