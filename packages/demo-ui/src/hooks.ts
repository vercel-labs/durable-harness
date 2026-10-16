import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { LiveText } from './bits.tsx';
import { inFlight } from './lanes.ts';
import { LatencyTracker } from './latency.ts';
import { type ClientLoaders, type DemoClient, type DriveChange, type EventView, isSessionNotFound, type Mode, type RawClientLoaders, type SessionRow, type SessionView } from './view.ts';

export type ClientEntry =
  | { status: 'loading' }
  | { status: 'ready'; client: DemoClient }
  | { status: 'error'; error: string };

/**
 * One client per mode (and per durability: `server:off` is the raw Function
 * client), created on first use and never disposed, so a session driving in
 * this tab keeps going while another mode is on screen.
 */
export function useClients(
  loaders: ClientLoaders,
  rawLoaders: RawClientLoaders = {},
  nativeLoaders: RawClientLoaders = {},
  /** Clients under their own key (`server:pi-durable`), outside the mode/durability grid. */
  extraLoaders: Partial<Record<string, () => Promise<{ createClient(): Promise<DemoClient> }>>> = {}
) {
  const [entries, setEntries] = useState<Partial<Record<string, ClientEntry>>>({});
  const started = useRef(new Set<string>());
  const loaderFor = useCallback(
    (key: string) => {
      if (extraLoaders[key]) return extraLoaders[key];
      const [mode, durability] = key.split(':') as [Mode, string | undefined];
      return durability === 'off' ? rawLoaders[mode] : durability === 'native' ? nativeLoaders[mode] : loaders[mode];
    },
    [loaders, rawLoaders, nativeLoaders, extraLoaders]
  );
  const load = useCallback(
    (key: string) => {
      if (started.current.has(key)) return;
      const loader = loaderFor(key);
      if (!loader) return;
      started.current.add(key);
      setEntries((e) => ({ ...e, [key]: { status: 'loading' } }));
      const durability = key.split(':')[1];
      loader()
        .then((m) => m.createClient(durability === 'run-ahead' ? { durability: 'run-ahead' } : undefined))
        .then(
          (client) => setEntries((e) => ({ ...e, [key]: { status: 'ready', client } })),
          (error) => {
            console.error(`[demo] ${key} client failed`, error);
            started.current.delete(key);
            setEntries((e) => ({ ...e, [key]: { status: 'error', error: messageOf(error) } }));
          }
        );
    },
    [loaderFor]
  );
  const prefetch = useCallback((key: string) => void loaderFor(key)?.().catch(() => {}), [loaderFor]);
  return { entries, load, prefetch };
}

/** The busy poll when the client pushes updates (hints, or live marks for every commit). */
export const PUSHED_POLL_MS = 5000;

/**
 * How long the open session waits before its next read: backing off after
 * failures; otherwise the page's poll, which is only a fallback (at least
 * `PUSHED_POLL_MS`) when the client pushes updates.
 */
export function refreshDelay({ fails, pushed, pollMs }: { fails: number; pushed: boolean; pollMs: number }): number {
  if (fails >= 2) return Math.min(5000, 1000 * 2 ** (fails - 2));
  return pushed ? Math.max(PUSHED_POLL_MS, pollMs) : pollMs;
}

const trackers = new WeakMap<DemoClient, Map<string, LatencyTracker>>();
/** The latency tracker of one session, as seen from this page. */
export function trackerFor(client: DemoClient, id: string): LatencyTracker {
  let byId = trackers.get(client);
  if (!byId) trackers.set(client, (byId = new Map()));
  let t = byId.get(id);
  if (!t) byId.set(id, (t = new LatencyTracker()));
  return t;
}

/** Re-render when a session's latency tracker changes. */
export function useTracker(client: DemoClient | undefined, id: string | undefined): LatencyTracker | undefined {
  const tracker = client && id ? trackerFor(client, id) : undefined;
  const [, setN] = useState(0);
  useEffect(() => {
    if (!tracker) return;
    let raf = 0;
    return tracker.subscribe(() => {
      if (raf) return;
      raf = requestAnimationFrame(() => {
        raf = 0;
        setN((n) => n + 1);
      });
    });
  }, [tracker]);
  return tracker;
}

export const messageOf = (error: unknown) => (error instanceof Error ? error.message : String(error));

export function useVisible(): boolean {
  const [visible, setVisible] = useState(() => typeof document === 'undefined' || !document.hidden);
  useEffect(() => {
    const on = () => setVisible(!document.hidden);
    document.addEventListener('visibilitychange', on);
    return () => document.removeEventListener('visibilitychange', on);
  }, []);
  return visible;
}

export function useSessionList(client: DemoClient | undefined) {
  // Rows are kept with the client that listed them, so right after a mode
  // switch the previous mode's rows are never mistaken for this mode's.
  const [state, setState] = useState<{ client?: DemoClient; rows?: SessionRow[]; error?: string }>({});
  const visible = useVisible();
  const clientRef = useRef(client);
  clientRef.current = client;
  const refresh = useCallback(async () => {
    const c = clientRef.current;
    if (!c) return;
    try {
      const next = await c.list();
      if (clientRef.current !== c) return;
      setState((prev) =>
        prev.client === c && prev.rows && sameRows(prev.rows, next) && !prev.error ? prev : { client: c, rows: next }
      );
    } catch (e) {
      if (clientRef.current === c) setState((prev) => ({ client: c, rows: prev.client === c ? prev.rows : undefined, error: messageOf(e) }));
    }
  }, []);
  useEffect(() => {
    if (client) void refresh();
  }, [client, refresh]);
  const failing = state.client === client && !!state.error;
  useEffect(() => {
    if (!client || !visible) return;
    // While the server is unreachable, look for it every 3s.
    const id = setInterval(refresh, failing ? 3000 : 10_000);
    return () => clearInterval(id);
  }, [client, visible, refresh, failing]);
  const mine = state.client === client && client !== undefined;
  return { rows: mine ? state.rows : undefined, error: mine ? state.error : undefined, refresh };
}

const sameRows = (a: SessionRow[], b: SessionRow[]) =>
  a.length === b.length && a.every((r, i) => r.id === b[i]!.id && r.status === b[i]!.status);

/**
 * A fetch that never reached the server (it is down, or the network is). A
 * proxy in front of a dead server answers 502/503/504 instead (the Vite dev
 * server's /api proxy does), which means the same thing.
 */
export const isNetworkError = (message: string | undefined) =>
  !!message && (/failed to fetch|networkerror|load failed|network request failed|fetch failed/i.test(message) || /\s50[234]$/.test(message));

export interface SessionData {
  view?: SessionView;
  events: EventView[];
  error?: string;
  /** The session's own read said its World does not have it (a 404): nothing will ever load. */
  notFound: boolean;
  /** Consecutive failed reads (the server went away). */
  failures: number;
  /**
   * A drive of this session is working right now, by its live marks (its
   * `drive.start` came, its `idle` has not): a turn whose writes trail the
   * agent can be running before the log shows any of it.
   */
  driving: boolean;
  live: LiveText;
  /** A `sandbox()` session's mutating calls on its Drive this page saw on the live stream (`drive_files`), newest last: what changed since the Drive's last snapshot. */
  driveChanges: DriveChange[];
  refresh(): void;
}

/** What the page already read of a session: reopening it reads only what's new. */
interface SessionCache {
  log: EventView[];
  seen: Set<string>;
  cursor?: string;
  view?: SessionView;
}
const caches = new WeakMap<DemoClient, Map<string, SessionCache>>();
function cacheFor(client: DemoClient, id: string): SessionCache {
  let byId = caches.get(client);
  if (!byId) caches.set(client, (byId = new Map()));
  let c = byId.get(id);
  if (!c) byId.set(id, (c = { log: [], seen: new Set() }));
  return c;
}
const NO_EVENTS: EventView[] = [];
const NO_CHANGES: DriveChange[] = [];

interface Snapshot {
  key: string;
  view?: SessionView;
  events: EventView[];
  error?: string;
  failures: number;
  notFound?: boolean;
  driving?: boolean;
  driveChanges?: DriveChange[];
}

/** A read that says the session does not exist (as opposed to one that failed). */
const notFoundError = (e: unknown) => isSessionNotFound(e) || (!!e && typeof e === 'object' && (e as { name?: unknown }).name === 'WorkflowRunNotFoundError');

/**
 * The open session: the fold (`inspect`) plus the raw log, read
 * incrementally. Reads are event-driven (a live event triggers one, never
 * more than one in flight) with a fallback poll whose interval the page
 * decides (`pollMs`: fast while something is in flight, slow when idle),
 * none while the tab is hidden.
 *
 * State is keyed by session, so the frame after a switch never shows the
 * previous session's rows; a session read before comes back from the cache
 * at once and only its new events are fetched.
 */
export function useSession(client: DemoClient | undefined, id: string | undefined, pollMs: { current: () => number }): SessionData {
  const key = client && id ? `${client.mode}:${id}` : '';
  const [snap, setSnap] = useState<Snapshot>({ key: '', events: NO_EVENTS, failures: 0 });
  const live = useMemo(() => new LiveText(), [client, id]);
  const refreshRef = useRef<() => void>(() => {});

  const cached = client && id ? caches.get(client)?.get(id) : undefined;
  const current: Snapshot =
    snap.key === key ? snap : { key, view: cached?.view, events: cached?.log.length ? cached.log.slice() : NO_EVENTS, failures: 0 };

  useEffect(() => {
    if (!client || !id) return;
    const c = cacheFor(client, id);
    const put = (patch: Partial<Snapshot>) =>
      setSnap((prev) => ({
        ...(prev.key === key ? prev : { key, view: c.view, events: c.log.slice(), failures: 0 }),
        ...patch,
        key,
      }));
    let stopped = false;
    let inflight = false;
    let again = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let fails = 0;
    let first = true;
    let errored = false;
    let lastCount = c.view?.eventCount ?? -1;
    let lastStatus = c.view?.status ?? '';
    // Pushed updates (hints, or marks for every commit on live) drive the
    // refreshes; the poll is only the fallback.
    const pushed = !!client.hints || !!client.livePushes;

    let resetGen = 0;
    let resetDone = 0;
    const schedule = () => {
      clearTimeout(timer);
      if (stopped || document.hidden) return;
      const ms = refreshDelay({ fails, pushed, pollMs: pollMs.current() });
      timer = setTimeout(refresh, ms);
    };

    async function refresh() {
      if (stopped) return;
      if (inflight) {
        again = true;
        return;
      }
      inflight = true;
      clearTimeout(timer);
      // A turn boundary seen before this read began: once this read's view
      // lands (the finished row has the text), the live buffer can go.
      const resetBy = resetGen;
      try {
        // The raw log first: it is incremental and cheap. The fold is read
        // only when the log grew (or on the first read), since it is a pure
        // function of the log.
        // The first read after (re)opening always folds too: in a hybrid
        // session that read is also what drives or re-watches it here.
        let v: SessionView | undefined;
        let page: Awaited<ReturnType<DemoClient['events']>>;
        if (first) [v, page] = await Promise.all([client!.inspect(id!), client!.events(id!, c.cursor)]);
        else page = await client!.events(id!, c.cursor);
        if (stopped) return;
        let grew = false;
        for (const e of page.events) {
          if (c.seen.has(e.eventId)) continue;
          c.seen.add(e.eventId);
          grew = true;
          c.log.push({ ...e, createdAt: String(e.createdAt), index: c.log.length + 1 });
        }
        if (page.cursor) c.cursor = page.cursor;
        if (!v && (grew || c.log.length > lastCount)) v = await client!.inspect(id!);
        if (stopped) return;
        const patch: Partial<Snapshot> = {};
        if (grew) patch.events = c.log.slice();
        if (v && (first || v.eventCount !== lastCount || v.status !== lastStatus)) {
          lastCount = v.eventCount;
          lastStatus = v.status;
          c.view = v;
          patch.view = v;
        } else if (v) lastCount = v.eventCount;
        first = false;
        if (fails) patch.failures = 0;
        fails = 0;
        patch.error = undefined;
        if (grew || patch.view || errored) put(patch);
        // The view shows no model call streaming any more: the finished row has the text.
        if (resetBy > resetDone && resetBy === resetGen && !c.view?.effects.some((e) => e.name === 'model' && inFlight(e))) {
          resetDone = resetBy;
          live.reset();
        }
        errored = false;
      } catch (e) {
        if (stopped) return;
        if (notFoundError(e)) {
          // Nothing to retry: the session is not on this World.
          stopped = true;
          put({ notFound: true, error: undefined });
          return;
        }
        fails++;
        errored = true;
        put({ failures: fails, error: messageOf(e) });
      } finally {
        inflight = false;
        if (again && !stopped) {
          again = false;
          void refresh();
        } else schedule();
      }
    }
    // Live events arrive in bursts (a reconnect replays the stream): one read per burst.
    let soon: ReturnType<typeof setTimeout> | undefined;
    const refreshSoon = () => {
      if (soon) return;
      soon = setTimeout(() => {
        soon = undefined;
        void refresh();
      }, 120);
    };
    refreshRef.current = () => void refresh();

    const tracker = trackerFor(client, id);
    // The latest drive that started and has not said idle (a killed drive never does; the next one replaces it).
    let driving: string | undefined;
    /** The Drive's changes this page saw (stream-only: a reload starts from the last snapshot). */
    let changes: DriveChange[] = [];
    const unlisten = client.live(id, (event) => {
      tracker.event(event);
      if (event?.type === 'drive_files' && typeof event.tool === 'string' && typeof event.callId === 'string') {
        const paths = Array.isArray(event.paths) ? event.paths.filter((p: unknown): p is string => typeof p === 'string') : undefined;
        changes = [...changes, { tool: event.tool, callId: event.callId, ...(paths?.length ? { paths } : {}), t: typeof event.t === 'number' ? event.t : Date.now() }].slice(-50);
        put({ driveChanges: changes });
      }
      if (event?.type === 'mark' && typeof event.drv === 'string') {
        if (event.name === 'drive.start' && driving !== event.drv) {
          driving = event.drv;
          put({ driving: true });
        } else if (event.name === 'idle' && event.drv === driving) {
          driving = undefined;
          put({ driving: false });
        }
      }
      if (event?.type === 'text' && typeof event.delta === 'string') {
        // New output after a boundary whose view has not landed yet: start fresh.
        if (resetGen > resetDone) {
          resetDone = resetGen;
          live.reset();
        }
        const first = !live.hasText;
        live.append(event.delta);
        if (first) refreshSoon();
        return;
      }
      // A model call ended (a tool starts, the turn is idle): keep its text
      // on screen until the next read shows the finished row, so the
      // streamed text does not blink out in between.
      if (event?.type === 'tool_start' || event?.type === 'idle') {
        const gen = ++resetGen;
        // However the reads go, the buffer does not outlive the next few seconds.
        setTimeout(() => {
          if (!stopped && resetGen === gen && resetDone < gen) {
            resetDone = gen;
            live.reset();
          }
        }, 4000);
      }
      if (event?.type === 'chaos') live.reset();
      // Run-ahead: speculative output the log does not have is retracted
      // (a rejected write rewinds the drive; it streams the turn again).
      if (event?.type === 'rewind' || event?.type === 'submit-failed') live.reset();
      // A turn a crash cut off resumed and its model answers again: the cut-off text never reached the log, so it goes; what
      // the log kept shows from the next read, and the resumed reply streams after it.
      if (event?.type === 'restart') live.reset();
      if (event?.type === 'submit-failed') {
        put({ error: `A prompt did not become durable yet (${String(event.error ?? 'write failed')}); the drive re-issues it.` });
      }
      refreshSoon();
    });
    const unhint = client.hints?.(id, refreshSoon);
    const onVisible = () => {
      if (!document.hidden) void refresh();
    };
    document.addEventListener('visibilitychange', onVisible);
    void refresh();
    return () => {
      stopped = true;
      clearTimeout(timer);
      clearTimeout(soon);
      unlisten();
      unhint?.();
      document.removeEventListener('visibilitychange', onVisible);
      refreshRef.current = () => {};
    };
    // biome-ignore lint/correctness/useExhaustiveDependencies: key follows client and id
  }, [client, id, live, pollMs]);

  const refresh = useCallback(() => refreshRef.current(), []);
  return {
    view: current.view,
    events: current.events,
    error: current.error,
    failures: current.failures,
    notFound: !!current.notFound,
    driving: !!current.driving,
    live,
    driveChanges: current.driveChanges ?? NO_CHANGES,
    refresh,
  };
}

export function useMedia(query: string): boolean {
  const [match, setMatch] = useState(() => typeof matchMedia !== 'undefined' && matchMedia(query).matches);
  useEffect(() => {
    const m = matchMedia(query);
    const on = () => setMatch(m.matches);
    on();
    m.addEventListener('change', on);
    return () => m.removeEventListener('change', on);
  }, [query]);
  return match;
}
