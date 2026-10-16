import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { createPortal } from 'react-dom';
import { ExternalIcon } from './bits.tsx';
import { type Breakdown, breakdown, markOffsets, OVERHEAD_STALL_MS } from './compare-breakdown.ts';
import type { GlossaryTerm } from './compare-glossary.ts';
import {
  type Arrival,
  type ColumnSpec,
  columnsFor,
  columnTitle,
  DURABILITY_HINT,
  DURABILITY_LABEL,
  type Durability,
  describeColumn,
  durabilitiesFor,
  HARNESS_LABEL,
  HARNESSES,
  harnessLabel,
  hasNative,
  LADDER,
  laneKind,
  MAX_COLUMNS,
  MIN_COLUMNS,
  NO_NATIVE,
  normalizeSpec,
  ownsDurability,
  PRESETS,
  parseColumns,
  processHarness,
  promptLabel,
  registerProcessHarnesses,
  RUNTIME_LABEL,
  type Runtime,
  runtimesFor,
  SETUPS,
  type Setup,
  serializeColumns,
  setupOf,
  type TurnView,
  turnEnd,
  turnView,
} from './compare-model.ts';
import {
  type ColumnSummary,
  cellText,
  countedOf,
  countedText,
  EXCLUDING,
  type ExportColumn,
  type FlagKey,
  median,
  METRICS,
  noiseFloor,
  p90,
  promptsOf,
  ROW_LABEL,
  type RoundRecord,
  recordOf,
  reliabilityPer100,
  resultsMarkdown,
  slope,
  summarize,
  withDifferentWork,
} from './compare-results.ts';
import { FLAG_LABEL, harnessGroup, HARNESS_GROUP, METRIC_LABEL, SOURCE_LABEL, SOURCE_URL } from './copy.ts';
import { messageOf } from './hooks.ts';
import { fmtSeconds, LatencyTracker } from './latency.ts';
import { Markdown } from './Markdown.tsx';
import { ActionMenu, Picker, type PickerOption } from './Menu.tsx';
import { Tip, useTip } from './Tip.tsx';
import { Brand } from './TopBar.tsx';
import { type ClientLoaders, type DemoClient, type HarnessOption, type Mode, type RawClientLoaders, registerHarnessLabels, type SendReceipt } from './view.ts';

/**
 * Compare (`/compare`): what durability costs a harness. The same prompt,
 * sent at the same instant, to 2–4 columns: by default one harness at
 * Native, Durable and Strict (the setup row), or any mix (Setups, Edit
 * Columns). A run is rounds of one prompt, after warm-up rounds in fresh
 * sessions; every number is measured in this page's clock, the same way
 * for every column, and compared round by round against the baseline
 * column, with the model's own time taken out (compare-results.ts).
 */

type Loader = () => Promise<{ createClient(options?: { durability?: 'run-ahead' }): Promise<DemoClient> }>;

export type Item =
  | { kind: 'user'; text: string; steer?: boolean; warmup?: boolean }
  | { kind: 'text'; text: string }
  | { kind: 'tool'; name: string; done: boolean; error?: boolean }
  | { kind: 'error'; text: string };

/** Fold one live event into a column's transcript (same array back if nothing shows). */
export function foldItems(items: Item[], e: any): Item[] {
  if (!e || typeof e !== 'object') return items;
  const last = items.at(-1);
  if (e.type === 'text' && typeof e.delta === 'string') {
    return last?.kind === 'text' ? [...items.slice(0, -1), { kind: 'text', text: last.text + e.delta }] : [...items, { kind: 'text', text: e.delta }];
  }
  if (e.type === 'tool_start') return [...items, { kind: 'tool', name: String(e.name ?? 'tool'), done: false }];
  if (e.type === 'tool_end') {
    const i = items.findLastIndex((it) => it.kind === 'tool' && !it.done && it.name === e.name);
    return i < 0 ? items : items.map((it, j) => (j === i && it.kind === 'tool' ? { ...it, done: true, error: !!e.isError } : it));
  }
  if (e.type === 'error' || e.type === 'failed') return [...items, { kind: 'error', text: String(e.message ?? 'error') }];
  // A turn a crash cut off resumed and its model answers again: a turn showing more text than the log kept (`e.parts`, one
  // per kept step) ends in the cut-off reply, which goes. The turn starts at its message: a steer joins the turn it steers.
  if (e.type === 'restart') {
    const kept = Array.isArray(e.parts) ? e.parts.length : 0;
    const start = items.findLastIndex((it) => it.kind === 'user' && !it.steer);
    const shown = items.slice(start + 1).filter((it) => it.kind === 'text').length;
    return last?.kind === 'text' && shown > kept ? items.slice(0, -1) : items;
  }
  return items;
}

/** Events a column records with their arrival time: what the timeline and the breakdown read. */
const TIMED = new Set(['text', 'tool_start', 'tool_end', 'idle', 'error', 'mark', 'hello', 'stream', 'reconnect', 'rewind', 'chaos']);

interface Send {
  /** The round's place in the run: the same in every column. */
  seq: number;
  prompt: string;
  warmup: boolean;
}

/** One column's session: its client, what streamed in and when, and its clock. */
class Pane {
  items: Item[] = [];
  readonly arrivals: Arrival[] = [];
  readonly tracker = new LatencyTracker();
  running = false;
  error?: string;
  setupMs?: number;
  /** Each prompt this session was sent, in order. */
  readonly sends: Send[] = [];
  private listeners = new Set<() => void>();
  private version = 0;
  private off?: () => void;

  constructor(
    readonly client: DemoClient,
    readonly id: string
  ) {}

  attach(): Promise<void> {
    return new Promise((ready) => {
      let done = false;
      const settle = () => {
        if (!done) (done = true), ready();
      };
      this.off = this.client.live(this.id, (e) => this.onEvent(e), settle);
      // A client without a ready signal (Server mode's EventSource) replays
      // the stream from the start anyway; give it a moment to open.
      if (!this.client.signalsReady) setTimeout(settle, 400);
    });
  }

  /** Detach from the session, and release the column's client (each column opens its own). */
  close() {
    this.off?.();
    this.client.close?.();
  }

  send(text: string, kind: 'prompt' | 'steer', at: number, round?: { seq: number; warmup: boolean }): Promise<void> {
    if (kind === 'prompt') this.sends.push({ seq: round?.seq ?? -1, prompt: text, warmup: !!round?.warmup });
    this.items = [...this.items, { kind: 'user', text, ...(kind === 'steer' ? { steer: true } : {}), ...(round?.warmup ? { warmup: true } : {}) }];
    this.tracker.sent(kind, at);
    const turn = this.tracker.turns.at(-1)!;
    if (kind === 'prompt') this.running = true;
    this.changed();
    return this.client
      .send(this.id, text, kind)
      .then((receipt) => {
        // The session was still starting (eve's prewarm answers before its inbox is up):
        // that is not this turn's time, as the other columns paid theirs before this send.
        const notReadyMs = (receipt as SendReceipt | undefined)?.notReadyMs;
        if (notReadyMs) this.tracker.startedLate(turn, notReadyMs);
      })
      .catch((e) => {
        this.error = messageOf(e);
        this.running = false;
        this.changed();
      });
  }

  /** Each prompt's timeline, as this page saw it, with where its time went. */
  views(t: number): TurnView[] {
    const offsets = markOffsets(this.arrivals);
    return this.tracker.turns
      .filter((x) => x.kind === 'prompt')
      .map((x, i) => {
        // The same end for every lane: when the column is done (turnEnd).
        const { endAt, harnessIdle } = turnEnd(x);
        const v = turnView(x.sentAt, this.arrivals, endAt, t);
        return {
          ...v,
          ...(harnessIdle !== undefined ? { harnessIdle } : {}),
          ...(x.notReadyMs ? { notReadyMs: x.notReadyMs } : {}),
          ...(this.sends[i]?.warmup ? { warmup: true } : {}),
          breakdown: breakdown(v, this.arrivals, { offsets, ...(x.drvs ? { drvs: x.drvs } : {}) }),
        };
      });
  }

  /** The transcript of the i-th prompt: what came after it, up to the next prompt. */
  itemsOf(i: number): Item[] {
    let n = -1;
    const out: Item[] = [];
    for (const it of this.items) {
      if (it.kind === 'user' && !it.steer) n++;
      else if (n === i) out.push(it);
      if (n > i) break;
    }
    return out;
  }

  /** Every finished round as a record. */
  records(views: TurnView[]): RoundRecord[] {
    return views.flatMap((v, i) => {
      const send = this.sends[i];
      if (!send) return [];
      const text = this.itemsOf(i).filter((it) => it.kind === 'text').at(-1);
      const reply = text?.kind === 'text' ? text.text.trim().split('\n').at(-1)?.trim().slice(0, 80) : undefined;
      const r = recordOf(v, { seq: send.seq, prompt: send.prompt, ...(reply ? { reply } : {}) });
      return r ? [r] : [];
    });
  }

  private onEvent(e: any) {
    const at = now();
    if (e && TIMED.has(e.type)) this.arrivals.push({ at, e });
    this.tracker.event(e, at);
    const next = foldItems(this.items, e);
    const open = this.tracker.turns.some((t) => t.kind === 'prompt' && t.idleAt === undefined && !t.lost);
    const idle = e?.type === 'idle' || (e?.type === 'mark' && e.name === 'idle');
    if (next === this.items && !(idle && !open && this.running)) return;
    this.items = next;
    if (idle && !open) this.running = false;
    this.changed();
  }

  subscribe = (fn: () => void) => {
    this.listeners.add(fn);
    const off = this.tracker.subscribe(() => this.changed());
    return () => {
      this.listeners.delete(fn);
      off();
    };
  };
  snapshot = () => this.version;
  changed() {
    this.version++;
    for (const fn of this.listeners) fn();
  }
}

/** Re-render when any pane changes. */
function usePanes(panes: (Pane | undefined)[]) {
  const key = panes.map((p) => p?.id ?? '-').join('|');
  const store = useState(() => ({ key: '' as string, subscribe: (_: () => void) => () => {}, snapshot: (): string => '' }))[0];
  if (store.key !== key) {
    store.key = key;
    store.subscribe = (fn) => {
      const offs = panes.map((p) => p?.subscribe(fn));
      return () => offs.forEach((o) => o?.());
    };
    store.snapshot = () => panes.map((p) => p?.snapshot() ?? 0).join(',');
  }
  useSyncExternalStore(store.subscribe, store.snapshot, store.snapshot);
}

const now = () => performance.timeOrigin + performance.now();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface CompareProps {
  loaders: ClientLoaders;
  rawLoaders: RawClientLoaders;
  /** Durability Native in a Function: plain Pi / fx (`/api/native`). */
  nativeLoader?: Loader;
  /** Durability Native in this tab: plain Pi / fx, model calls straight to AI Gateway. */
  nativeTabLoader?: Loader;
  /** eve on Vercel Workflow (`/eve/v1`, the agent in apps/demo/agent). */
  eveLoader?: Loader;
  /** fx (libfx durable): libfx's own durable sessions (`/api/fx-durable`). */
  fxDurableLoader?: Loader;
  /** Pi (pi-durable): Pi 1.0's own durable Harness (`/api/pi-durable`). */
  piDurableLoader?: Loader;
  /** The model this tab's own lanes call (This Tab columns). */
  model: string;
}

/** What `/api/bench/info` says about the deployment: the caption, the copy and the download carry it. */
interface Info {
  sha?: string;
  url?: string;
  deploymentId?: string;
  model?: string;
  region?: string;
  world?: string;
  env?: string;
  chaos?: boolean;
}

/** Where the main page keeps the server's process harnesses as last seen (Shell's `how:process-harnesses`). */
const PROCESS_HARNESSES_KEY = 'how:process-harnesses';

/** The server's process harnesses as last seen, so a link to one opens before the server answers. */
function storedProcessHarnesses(): HarnessOption[] {
  try {
    const list = JSON.parse(localStorage.getItem(PROCESS_HARNESSES_KEY) ?? '[]') as HarnessOption[];
    return Array.isArray(list) ? list.filter((h) => h && typeof h.value === 'string' && Array.isArray(h.modes)) : [];
  } catch {
    return [];
  }
}

function useProcessHarnesses() {
  const [options, setOptions] = useState<HarnessOption[]>(() => {
    const stored = storedProcessHarnesses();
    registerProcessHarnesses(stored);
    registerHarnessLabels(stored);
    return stored;
  });
  /** A server client's list: registered, remembered, and a re-render if it changed. */
  const learn = useCallback((next: HarnessOption[]) => {
    setOptions((prev) => {
      if (JSON.stringify(prev) === JSON.stringify(next)) return prev;
      registerProcessHarnesses(next);
      registerHarnessLabels(next);
      try {
        localStorage.setItem(PROCESS_HARNESSES_KEY, JSON.stringify(next));
      } catch {}
      return next;
    });
  }, []);
  return [options, learn] as const;
}

interface Slot {
  spec: ColumnSpec;
  pane?: Pane;
  error?: string;
  /** Rounds this column finished in sessions it has since left (fresh sessions every k rounds), or restored. */
  archived: RoundRecord[];
}

const RESULTS_KEY = (cols: ColumnSpec[]) => `how:compare:results:${serializeColumns(cols)}`;
interface Stored {
  at: number;
  records: RoundRecord[][];
}
function readStored(cols: ColumnSpec[]): Stored | undefined {
  try {
    const s = JSON.parse(sessionStorage.getItem(RESULTS_KEY(cols)) ?? 'null') as Stored | null;
    return s && Array.isArray(s.records) && s.records.length === cols.length ? s : undefined;
  } catch {
    return undefined;
  }
}

/** Settings a person picks once: kept across visits. */
function useStored<T>(key: string, initial: T): [T, (v: T) => void] {
  const [v, setV] = useState<T>(() => {
    try {
      const s = localStorage.getItem(key);
      return s === null ? initial : (JSON.parse(s) as T);
    } catch {
      return initial;
    }
  });
  const set = useCallback(
    (next: T) => {
      setV(next);
      try {
        localStorage.setItem(key, JSON.stringify(next));
      } catch {}
    },
    [key]
  );
  return [v, set];
}

/** The client mode behind each runtime. */
const MODE_FOR = { function: 'server', tab: 'tab', hybrid: 'hybrid' } as const satisfies Record<Runtime, Mode>;

/** Columns are A, B, C, D: what the table and the deltas call them. */
const letter = (i: number) => String.fromCharCode(65 + i);

export function Compare({ loaders, rawLoaders, nativeLoader, nativeTabLoader, eveLoader, fxDurableLoader, piDurableLoader, model }: CompareProps) {
  const params = new URLSearchParams(location.search);
  const [processOptions, learnProcessHarnesses] = useProcessHarnesses();
  const [cols, setColsState] = useState<ColumnSpec[]>(() => parseColumns(params.get('cols'), params.get('harness')));
  // The link as opened, until the columns are changed by hand: re-read once
  // the server's process harnesses are known (a link to a Codex column opens
  // before the server has said Codex exists).
  const linked = useRef<{ cols: string | null; harness: string | null } | undefined>({ cols: params.get('cols'), harness: params.get('harness') });
  // The column every delta is against; unset: the first Native column, else the first.
  const [baseAt, setBaseAt] = useState<number | undefined>(() => {
    const b = Number(params.get('base'));
    return params.has('base') && Number.isInteger(b) && b >= 0 ? b : undefined;
  });
  const [slots, setSlots] = useState<Slot[]>([]);
  const slotsRef = useRef<Slot[]>([]);
  slotsRef.current = slots;
  const [preparing, setPreparing] = useState(false);
  const [nonce, setNonce] = useState(0);
  const [promptId, setPromptId] = useStored<string>('how:compare:prompt', 'write');
  const [custom, setCustom] = useState('');
  const [rounds, setRounds] = useStored('how:compare:rounds', 20);
  const [warmup, setWarmup] = useStored('how:compare:warmup', 2);
  const [freshEvery, setFreshEvery] = useStored('how:compare:fresh', 5);
  const [exclude, setExclude] = useStored('how:compare:exclude', true);
  const [run, setRun] = useState<{ prompt: string; round: number; of: number; warming?: number }>();
  const [steer, setSteer] = useState('');
  const [editing, setEditing] = useState(false);
  const [confirm, setConfirm] = useState<{ action: () => void; rounds: number }>();
  const [restoredAt, setRestoredAt] = useState<number>();
  const [info, setInfo] = useState<Info>();
  const [shownPrompt, setShownPrompt] = useState<string>();
  const [active, setActive] = useState(0);
  const stop = useRef(false);
  const seq = useRef(0);
  const generation = useRef(0);
  const [, tick] = useState(0);

  const panes = slots.map((s) => s.pane);
  usePanes(panes);
  const running = panes.some((p) => p?.running);
  const busy = running || !!run;

  useEffect(() => {
    fetch('/api/bench/info')
      .then((r) => (r.ok ? r.json() : undefined))
      .then((j) => j && setInfo(j as Info))
      .catch(() => {});
  }, []);

  // Keep the URL shareable.
  useEffect(() => {
    const u = new URL(location.href);
    u.searchParams.set('cols', serializeColumns(cols));
    u.searchParams.delete('harness');
    if (baseAt !== undefined && baseAt < cols.length) u.searchParams.set('base', String(baseAt));
    else u.searchParams.delete('base');
    history.replaceState(null, '', u);
  }, [cols, baseAt]);

  // The server's process harnesses changed: re-read the link, else repair the columns to what they offer now.
  // biome-ignore lint/correctness/useExhaustiveDependencies: processOptions is what re-runs this (through the registry)
  useEffect(() => {
    setColsState((cs) => {
      const next = linked.current ? parseColumns(linked.current.cols, linked.current.harness) : cs.map(normalizeSpec);
      return serializeColumns(next) === serializeColumns(cs) ? cs : next;
    });
  }, [processOptions]);

  // Timelines grow while a turn runs.
  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => tick((n) => n + 1), 200);
    return () => clearInterval(id);
  }, [running]);

  /** Open a session in every column, idle and subscribed before the first prompt, so every clock starts at send. */
  const openAll = useCallback(
    async (specs: ColumnSpec[], made: Pane[]): Promise<{ spec: ColumnSpec; pane?: Pane; error?: string }[]> => {
      const open = async (spec: ColumnSpec): Promise<Pane> => {
        const kind = laneKind(spec);
        const mode = MODE_FOR[spec.runtime];
        const loader: Loader | undefined =
          kind === 'eve'
            ? eveLoader
            : kind === 'fx-durable'
              ? fxDurableLoader
              : kind === 'pi-durable'
                ? piDurableLoader
              : kind === 'native'
                ? { server: nativeLoader, tab: nativeTabLoader, hybrid: undefined }[mode]
                : kind === 'off'
                  ? rawLoaders[mode]
                  : loaders[mode];
        if (!loader) throw new Error(`${columnTitle(spec)} is not available on this page.`);
        const client = await (await loader()).createClient(kind === 'run-ahead' ? { durability: 'run-ahead' } : undefined);
        if (client.harnesses) learnProcessHarnesses(client.harnesses);
        if (!HARNESSES.includes(spec.harness as never)) {
          // A process harness: the server's own list says whether it runs here.
          const option = (client.harnesses ?? []).find((h) => h.value === spec.harness);
          if (!option) throw new Error(`${harnessLabel(spec.harness)} is not registered on this server.`);
          if (option.disabledReason) throw new Error(option.disabledReason);
        }
        if (spec.harness === 'fx' && !client.fxAvailable) throw new Error('fx needs WebAssembly JSPI in this browser (try Chrome).');
        const t0 = now();
        // eve's client ignores the name; Hybrid has its own harness names.
        const h = spec.runtime === 'hybrid' && kind !== 'native' ? (spec.harness === 'fx' ? 'hybrid-fx' : 'hybrid') : spec.harness;
        const id = await client.start(h);
        const pane = new Pane(client, id);
        made.push(pane);
        await pane.attach();
        pane.setupMs = Math.round(now() - t0);
        return pane;
      };
      const results = await Promise.allSettled(specs.map(open));
      return specs.map((spec, i) => {
        const r = results[i]!;
        return r.status === 'fulfilled' ? { spec, pane: r.value } : { spec, error: messageOf(r.reason) };
      });
    },
    [loaders, rawLoaders, nativeLoader, nativeTabLoader, eveLoader, fxDurableLoader, piDurableLoader, learnProcessHarnesses]
  );

  // Fresh sessions in every column whenever the columns change (or Restart Sessions); results stored for these columns come back.
  const lastCols = useRef<string>('');
  // biome-ignore lint/correctness/useExhaustiveDependencies: nonce re-opens the sessions
  useEffect(() => {
    const gen = ++generation.current;
    const made: Pane[] = [];
    const key = serializeColumns(cols);
    const restarted = key === lastCols.current;
    lastCols.current = key;
    const stored = restarted ? undefined : readStored(cols);
    if (restarted) sessionStorage.removeItem(RESULTS_KEY(cols));
    setRestoredAt(stored?.at);
    seq.current = Math.max(0, ...(stored?.records.flat().map((r) => r.seq) ?? [0]));
    setSlots(cols.map((spec, i) => ({ spec, archived: stored?.records[i] ?? [] })));
    setPreparing(true);
    openAll(cols, made).then((opened) => {
      if (gen !== generation.current) return made.forEach((p) => p.close());
      setSlots((prev) => opened.map((o, i) => ({ ...o, archived: prev[i]?.archived ?? [] })));
      setPreparing(false);
    });
    return () => made.forEach((p) => p.close());
  }, [cols, openAll, nonce]);

  const t = now();
  const views = slots.map((s) => s.pane?.views(t) ?? []);
  const autoBase = Math.max(0, cols.findIndex((c) => c.durability === 'none' && !ownsDurability(c.harness)));
  const baseIndex = baseAt !== undefined && baseAt < slots.length ? baseAt : autoBase;
  const raw = slots.map((s, i) => [...s.archived, ...(s.pane?.records(views[i]!) ?? [])]);
  const columns = withDifferentWork(raw, baseIndex);
  const finishedRounds = Math.max(0, ...columns.map((rs) => rs.filter((r) => !r.warmup).length));
  const prompts = promptsOf(columns);
  const prompt = shownPrompt && prompts.includes(shownPrompt) ? shownPrompt : (run?.prompt ?? prompts.at(-1));

  // Results survive a reload (sessionStorage, keyed by the columns).
  const recordsKey = raw.map((rs) => rs.length).join(',');
  // biome-ignore lint/correctness/useExhaustiveDependencies: written when a round finishes
  useEffect(() => {
    if (!raw.some((rs) => rs.length)) return;
    try {
      sessionStorage.setItem(RESULTS_KEY(cols), JSON.stringify({ at: Date.now(), records: raw } satisfies Stored));
    } catch {}
  }, [recordsKey]);

  /** One round: the prompt to every column at the same instant, then wait until every column is done with it. */
  const round = async (text: string, warm: boolean) => {
    const ready = slotsRef.current.filter((s) => s.pane);
    if (!ready.length) return;
    const at = now();
    const s = ++seq.current;
    for (const slot of ready) void slot.pane!.send(text, 'prompt', at, { seq: s, warmup: warm });
    const t0 = now();
    await sleep(50);
    while (slotsRef.current.some((x) => x.pane?.running) && now() - t0 < 180_000) await sleep(100);
    await sleep(400);
  };

  /** Leave the current sessions for fresh ones, keeping what they finished. */
  const refresh = async () => {
    const current = slotsRef.current;
    const tNow = now();
    const kept = current.map((s) => [...s.archived, ...(s.pane ? s.pane.records(s.pane.views(tNow)) : [])]);
    for (const s of current) s.pane?.close();
    setPreparing(true);
    const opened = await openAll(
      current.map((s) => s.spec),
      []
    );
    const next = opened.map((o, i) => ({ ...o, archived: kept[i] ?? [] }));
    slotsRef.current = next;
    setSlots(next);
    setPreparing(false);
  };

  const fresh = () => slotsRef.current.every((s) => !s.pane || s.pane.sends.length === 0);

  /** Run `n` rounds of `text` (Send Once: one), warming fresh sessions up first, and starting fresh ones every k rounds. */
  const runRounds = async (text: string, n: number) => {
    stop.current = false;
    let since = 0;
    for (let r = 0; r < n && !stop.current; r++) {
      if (freshEvery > 0 && since >= freshEvery) {
        await refresh();
        since = 0;
      }
      if (fresh())
        for (let w = 0; w < warmup && !stop.current; w++) {
          setRun({ prompt: text, round: r, of: n, warming: w + 1 });
          await round(text, true);
        }
      if (stop.current) break;
      setRun({ prompt: text, round: r + 1, of: n });
      setShownPrompt(text);
      await round(text, false);
      since++;
    }
    setRun(undefined);
  };

  const promptText = promptId === 'custom' ? custom.trim() : (PRESETS.find((p) => p.id === promptId)?.text ?? PRESETS[0].text);
  const canRun = slots.some((s) => s.pane) && !preparing && !busy && !!promptText;

  /** A change that starts fresh sessions: asks first when it would clear finished rounds. */
  const guard = (action: () => void) => {
    if (finishedRounds > 0) setConfirm({ action, rounds: finishedRounds });
    else action();
  };
  const changeCols = (next: ColumnSpec[]) =>
    guard(() => {
      linked.current = undefined;
      setBaseAt(undefined);
      setColsState(next);
    });
  const restart = () => guard(() => setNonce((n) => n + 1));

  const setup = setupOf(cols);
  const local = info ? info.env !== 'production' && info.env !== 'preview' : undefined;
  const modelOf = (spec: ColumnSpec): string | undefined =>
    processHarness(spec.harness) ? (processOptions.find((o) => o.value === spec.harness)?.model ?? info?.model) : spec.runtime === 'tab' ? model : info?.model;
  const exportCols: ExportColumn[] = slots.map((s, i) => ({
    letter: letter(i),
    title: columnTitle(s.spec),
    short: setup ? DURABILITY_LABEL[s.spec.durability] : columnTitle(s.spec),
    harness: harnessLabel(s.spec.harness),
    ...(modelOf(s.spec) ? { model: modelOf(s.spec)! } : {}),
  }));
  const summary = prompt ? summarize(columns, baseIndex, prompt, exclude) : undefined;
  const heading = setup ? `${harnessLabel(setup.harness)} · ${prompt ? promptLabel(prompt) : ''} · ${RUNTIME_LABEL[setup.runtime]}` : `Custom · ${prompt ? promptLabel(prompt) : ''}`;
  const meta = [
    info?.model ?? model,
    info?.url ? `${info.url}${info.sha ? ` @ ${info.sha.slice(0, 7)}` : ''}` : 'this server',
    info?.world ? `world-${info.world}` : undefined,
    info?.region,
    `chaos ${info?.chaos ? 'on' : 'off'}`,
  ]
    .filter(Boolean)
    .join(', ');
  const markdown = () => (summary && prompt ? resultsMarkdown({ heading, prompt: promptLabel(prompt), columns: exportCols, summary, baseIndex, meta }) : '');
  const json = () => ({
    meta: info ?? {},
    columns: slots.map((s, i) => ({ ...exportCols[i], spec: s.spec, setupMs: s.pane?.setupMs })),
    baseline: letter(baseIndex),
    rules: { rounds, warmup, freshEvery, exclude, excluding: EXCLUDING },
    rounds: columns,
    summary: prompts.map((p) => ({ prompt: p, columns: summarize(columns, baseIndex, p, exclude) })),
  });

  // Scripted runs (like /bench's window.__bench): window.__compare.run({ prompt, rounds }), then .results().
  // The exports read the latest render (a run outlives the render that started it).
  const latest = useRef({ json, markdown });
  latest.current = { json, markdown };
  useEffect(() => {
    (window as unknown as { __compare: unknown }).__compare = {
      columns: serializeColumns(cols),
      ready: !preparing && slots.some((s) => s.pane),
      running: busy,
      results: () => latest.current.json(),
      markdown: () => latest.current.markdown(),
      run: async (o: { prompt?: string; rounds?: number } = {}) => {
        await runRounds(o.prompt ?? promptText, o.rounds ?? rounds);
        // One more render after the last round, so its records are in.
        await sleep(300);
        return latest.current.json();
      },
    };
  });

  const title = setup
    ? hasNative(setup.harness) || !setup.levels.includes('strict') || !setup.levels.includes('async')
      ? `What durability costs ${harnessLabel(setup.harness)}`
      : `What Strict costs ${harnessLabel(setup.harness)}, over Durable (no native baseline yet)`
    : 'Custom comparison';
  const roundsOf = (i: number) => columns[i] ?? [];
  // One scale per round across columns, so bars compare side by side.
  const scale = new Map<number, number>();
  for (const rs of columns) for (const r of rs) scale.set(r.seq, Math.max(scale.get(r.seq) ?? 1, r.turn ?? 0));

  return (
    <div className="cmp">
      <header className="topbar cmp-top">
        <span className="cmp-top-left">
          <Brand />
          <a className="source" href="/">
            ← Demo
          </a>
        </span>
        <nav className="topbar-links" aria-label="Pages">
          <a className="source" href={SOURCE_URL} target="_blank" rel="noreferrer">
            {SOURCE_LABEL} <ExternalIcon />
          </a>
        </nav>
      </header>
      <main className="cmp-main">
        <section className="cmp-head">
          <h1>{title}</h1>
          <p className="cmp-caption num">
            Model <span className="mono">{info?.model ?? model}</span>
            {' · '}
            {info?.url ? (
              <>
                {info.url}
                {info.sha && (
                  <>
                    {' @ '}
                    <span className="mono">{info.sha.slice(0, 7)}</span>
                  </>
                )}
              </>
            ) : (
              'this server'
            )}
            {info?.world && <> · world-{info.world}</>}
            {info?.region && <> · {info.region}</>}
          </p>
        </section>
        <SetupRow
          cols={cols}
          setup={setup}
          editing={editing}
          locked={busy}
          processOptions={processOptions}
          onColumns={changeCols}
          onEdit={() => setEditing((e) => !e)}
        />
        {info?.chaos && <p className="cmp-banner">Crash drills are on for this deployment. A drill can stall a turn; such turns are flagged and excluded.</p>}
        <RunRow
          promptId={promptId}
          custom={custom}
          rounds={rounds}
          warmup={warmup}
          freshEvery={freshEvery}
          canRun={canRun}
          run={run}
          running={running}
          preparing={preparing}
          steer={steer}
          onPrompt={setPromptId}
          onCustom={setCustom}
          onRounds={setRounds}
          onWarmup={setWarmup}
          onFresh={setFreshEvery}
          onRun={() => void runRounds(promptText, rounds)}
          onOnce={() => void runRounds(promptText, 1)}
          onStop={() => (stop.current = true)}
          onSteer={setSteer}
          onSendSteer={() => {
            const body = steer.trim();
            if (!body) return;
            const at = now();
            for (const s of slotsRef.current) if (s.pane?.running) void s.pane.send(body, 'steer', at);
            setSteer('');
          }}
          onRestart={restart}
        />
        <Results
          slots={slots}
          columns={columns}
          baseIndex={baseIndex}
          prompts={prompts}
          prompt={prompt}
          onPrompt={setShownPrompt}
          summary={summary}
          exclude={exclude}
          onExclude={setExclude}
          warmup={warmup}
          rounds={rounds}
          setup={setup}
          restoredAt={restoredAt}
          onClear={() => guard(() => setNonce((n) => n + 1))}
          markdown={markdown}
          json={json}
          modelOf={modelOf}
        />
        <div className="cmp-switch" role="tablist" aria-label="Columns">
          {slots.map((s, i) => (
            <button key={i} type="button" role="tab" aria-selected={active === i} onClick={() => setActive(i)}>
              {letter(i)}
            </button>
          ))}
        </div>
        <div className="cmp-cols" style={{ gridTemplateColumns: `repeat(${slots.length}, minmax(0, 1fr))` }}>
          {slots.map((s, i) => (
            <ColumnCard
              key={`${i}:${serializeColumns([s.spec])}`}
              index={i}
              slot={s}
              shown={active === i}
              isBase={i === baseIndex}
              onBase={() => setBaseAt(i)}
              views={views[i]!}
              records={roundsOf(i)}
              scale={scale}
              preparing={preparing}
              locked={busy}
              editing={editing}
              local={local}
              processOptions={processOptions}
              onSpec={(next) => changeCols(cols.map((c, j) => (j === i ? normalizeSpec({ ...c, ...next }) : c)))}
              {...(cols.length > MIN_COLUMNS ? { onRemove: () => changeCols(cols.filter((_, j) => j !== i)) } : {})}
            />
          ))}
        </div>
        {editing && (
          <div className="cmp-edit-foot">
            <button
              type="button"
              className="btn"
              disabled={busy || cols.length >= MAX_COLUMNS}
              onClick={() => changeCols([...cols, cols.at(-1) ?? { harness: 'pi', durability: 'strict', runtime: 'function' }])}
            >
              Add Column
            </button>
          </div>
        )}
      </main>
      <footer className="cmp-foot">
        <span className="cmp-triangle" role="img" aria-label="Vercel" />
        <span>Vercel Labs</span>
      </footer>
      {confirm && (
        <ConfirmDialog
          rounds={confirm.rounds}
          onCopy={() => {
            void navigator.clipboard?.writeText(latest.current.markdown());
            confirm.action();
            setConfirm(undefined);
          }}
          onClear={() => {
            confirm.action();
            setConfirm(undefined);
          }}
          onCancel={() => setConfirm(undefined)}
        />
      )}
    </div>
  );
}

/** The harnesses the setup row offers: one with more than one level (eve and fx (libfx durable) have one each). */
function setupHarnesses(processOptions: HarnessOption[]): PickerOption<string>[] {
  const builtins = (['pi', 'fx'] as const).map((h) => ({ value: h as string, label: HARNESS_LABEL[h], group: HARNESS_GROUP[harnessGroup(h)] }));
  const process = processOptions
    .filter((o) => processHarness(o.value) && durabilitiesFor(o.value).length > 1)
    .map((o) => ({
      value: o.value,
      label: o.label,
      group: HARNESS_GROUP[harnessGroup(o.value)],
      ...(o.disabledReason ? { disabled: true, detail: o.disabledReason } : {}),
    }));
  return [...builtins, ...process];
}

function SetupRow(props: {
  cols: ColumnSpec[];
  setup?: Setup;
  editing: boolean;
  locked: boolean;
  processOptions: HarnessOption[];
  onColumns(next: ColumnSpec[]): void;
  onEdit(): void;
}) {
  const { setup, locked } = props;
  // A custom comparison: the row shows the first column's harness, and changing it starts a ladder.
  const harness = setup?.harness ?? (ownsDurability(props.cols[0]!.harness) ? 'pi' : props.cols[0]!.harness);
  const levels = setup?.levels ?? [];
  const runtime = setup?.runtime ?? 'function';
  const have = durabilitiesFor(harness);
  const apply = (next: Partial<Setup>) => {
    const s: Setup = { harness, levels: setup ? levels : have.filter((d) => d !== 'wrapped'), runtime, ...next };
    // A new harness keeps the levels it has; at least two columns.
    const valid = s.levels.filter((d) => durabilitiesFor(s.harness).includes(d));
    const cols = columnsFor({ ...s, levels: valid.length >= MIN_COLUMNS ? valid : durabilitiesFor(s.harness).filter((d) => d !== 'wrapped') });
    if (cols.length >= MIN_COLUMNS) props.onColumns(cols);
  };
  const runtimes: PickerOption<Runtime>[] = (['function', 'tab', 'hybrid'] as const).map((r) => {
    const ok = (levels.length ? levels : have).some((d) => runtimesFor(harness, d).includes(r));
    return { value: r, label: RUNTIME_LABEL[r], ...(ok ? {} : { disabled: true, detail: `${harnessLabel(harness)} at these levels does not run here` }) };
  });
  return (
    <div className="cmp-row cmp-setup">
      <span className="group">
        <span className="group-label">Harness</span>
        <Picker label="Harness" value={harness} options={setupHarnesses(props.processOptions)} onChange={(h) => apply({ harness: h })} disabled={locked} />
      </span>
      <fieldset className="cmp-levels" disabled={locked}>
        <legend className="group-label">Columns</legend>
        {LADDER.map((d) => {
          const offered = have.includes(d);
          const checked = !!setup && levels.includes(d);
          const last = checked && levels.length <= MIN_COLUMNS;
          return (
            <LevelBox
              key={d}
              label={DURABILITY_LABEL[d]}
              hint={!offered ? (d === 'none' ? NO_NATIVE : `${harnessLabel(harness)} has no ${DURABILITY_LABEL[d]}`) : last ? 'Two columns at least' : DURABILITY_HINT[d]}
              checked={checked}
              disabled={!offered || last}
              onChange={(on) => apply({ levels: on ? [...levels, d] : levels.filter((x) => x !== d) })}
            />
          );
        })}
      </fieldset>
      <span className="group">
        <span className="group-label">Runs in</span>
        <Picker label="Runs in" value={runtime} options={runtimes} onChange={(r) => apply({ runtime: r })} disabled={locked} />
      </span>
      <span className="cmp-acts">
        <ActionMenu
          label="Setups"
          disabled={locked}
          items={SETUPS.map((s) => ({ key: s.id, label: s.label, detail: s.detail, run: () => props.onColumns(parseColumns(s.cols)) }))}
        />
        <button type="button" className="btn" aria-pressed={props.editing} onClick={props.onEdit}>
          Edit Columns
        </button>
      </span>
    </div>
  );
}

function LevelBox(props: { label: string; hint: string; checked: boolean; disabled: boolean; onChange(on: boolean): void }) {
  const tip = useTip(props.hint);
  return (
    <label className="check" data-disabled={props.disabled || undefined} {...tip.props}>
      <input type="checkbox" checked={props.checked} disabled={props.disabled} onChange={(e) => props.onChange(e.target.checked)} />
      <span>{props.label}</span>
      {tip.pop}
    </label>
  );
}

function NumberField(props: { label: string; value: number; min: number; max: number; onChange(v: number): void; disabled?: boolean; suffix?: string }) {
  return (
    <label className="cmp-num">
      <span className="group-label">{props.label}</span>
      <input
        type="number"
        className="num"
        min={props.min}
        max={props.max}
        value={props.value}
        disabled={props.disabled}
        onChange={(e) => props.onChange(Math.max(props.min, Math.min(props.max, Math.round(Number(e.target.value) || 0))))}
      />
      {props.suffix && <span className="group-label">{props.suffix}</span>}
    </label>
  );
}

function RunRow(props: {
  promptId: string;
  custom: string;
  rounds: number;
  warmup: number;
  freshEvery: number;
  canRun: boolean;
  run?: { prompt: string; round: number; of: number; warming?: number };
  running: boolean;
  preparing: boolean;
  steer: string;
  onPrompt(id: string): void;
  onCustom(t: string): void;
  onRounds(n: number): void;
  onWarmup(n: number): void;
  onFresh(n: number): void;
  onRun(): void;
  onOnce(): void;
  onStop(): void;
  onSteer(t: string): void;
  onSendSteer(): void;
  onRestart(): void;
}) {
  const { run } = props;
  const locked = !!run || props.running;
  const options: PickerOption<string>[] = [...PRESETS.map((p) => ({ value: p.id as string, label: p.label, detail: p.text })), { value: 'custom', label: 'Custom…', detail: 'Your own prompt' }];
  return (
    <div className="cmp-run">
      <div className="cmp-row">
        <span className="group">
          <span className="group-label">Prompt</span>
          <Picker label="Prompt" value={props.promptId} options={options} onChange={props.onPrompt} disabled={locked} />
        </span>
        {props.promptId === 'custom' && (
          <input className="cmp-custom" value={props.custom} onChange={(e) => props.onCustom(e.target.value)} placeholder="The same message to every column" disabled={locked} />
        )}
        <NumberField label="Rounds" value={props.rounds} min={1} max={100} onChange={props.onRounds} disabled={locked} />
        <NumberField label="Warm-up" value={props.warmup} min={0} max={5} onChange={props.onWarmup} disabled={locked} />
        <NumberField label="Fresh sessions every" value={props.freshEvery} min={0} max={100} onChange={props.onFresh} disabled={locked} suffix="rounds" />
        <span className="cmp-acts">
          {run ? (
            <button type="button" className="btn" onClick={props.onStop}>
              Stop
            </button>
          ) : (
            <>
              <button type="button" className="btn btn-primary" disabled={!props.canRun} onClick={props.onRun}>
                Run {props.rounds} {props.rounds === 1 ? 'Round' : 'Rounds'}
              </button>
              <button type="button" className="btn" disabled={!props.canRun} onClick={props.onOnce}>
                Send Once
              </button>
            </>
          )}
          <ActionMenu label="More" disabled={locked || props.preparing} items={[{ key: 'restart', label: 'Restart Sessions', detail: 'Fresh sessions in every column', run: props.onRestart }]} />
        </span>
      </div>
      <p className="cmp-progress num" aria-live="polite">
        {props.preparing
          ? 'Opening sessions…'
          : run
            ? run.warming
              ? `${promptLabel(run.prompt)}: warming up ${run.warming}, not counted`
              : `${promptLabel(run.prompt)}: round ${run.round} of ${run.of}${run.round === run.of ? ', finishing' : ''}`
            : ''}
      </p>
      {props.running && (
        <form
          className="cmp-row cmp-steer"
          onSubmit={(e) => {
            e.preventDefault();
            props.onSendSteer();
          }}
        >
          <input value={props.steer} onChange={(e) => props.onSteer(e.target.value)} placeholder="Steer every running column" />
          <button type="submit" className="btn" disabled={!props.steer.trim()}>
            Steer
          </button>
        </form>
      )}
    </div>
  );
}

const FLAG_ORDER: [FlagKey, string, GlossaryTerm][] = [
  ['newInstance', FLAG_LABEL.newInstance, 'newInstance'],
  ['reconnect', FLAG_LABEL.reconnect, 'reconnect'],
  ['deliveryStall', FLAG_LABEL.deliveryStall, 'deliveryStall'],
  ['slowWrite', FLAG_LABEL.slowWrite, 'slowWrite'],
  ['rewind', FLAG_LABEL.rewind, 'rewind'],
  ['doubleDrive', FLAG_LABEL.doubleDrive, 'doubleDrive'],
  ['recovered', FLAG_LABEL.recovered, 'recovered'],
  ['differentWork', FLAG_LABEL.differentWork, 'differentWork'],
  ['modelTail', FLAG_LABEL.modelTail, 'modelTail'],
  ['firstOutputTrails', FLAG_LABEL.firstOutputTrails, 'firstOutputTrails'],
  ['slow', FLAG_LABEL.slow, 'slow'],
];

const listOf = (xs: string[]) => (xs.length <= 1 ? (xs[0] ?? '') : `${xs.slice(0, -1).join(', ')} and ${xs.at(-1)}`);

function Results(props: {
  slots: Slot[];
  columns: RoundRecord[][];
  baseIndex: number;
  prompts: string[];
  prompt?: string;
  onPrompt(p: string): void;
  summary?: ColumnSummary[];
  exclude: boolean;
  onExclude(on: boolean): void;
  warmup: number;
  rounds: number;
  setup?: Setup;
  restoredAt?: number;
  onClear(): void;
  markdown(): string;
  json(): unknown;
  modelOf(spec: ColumnSpec): string | undefined;
}) {
  const { slots, columns, baseIndex, summary, prompt, setup } = props;
  const [copied, setCopied] = useState(false);
  const [open, setOpen] = useStored('how:compare:details', false);
  const head = (i: number) => `${letter(i)} ${setup ? DURABILITY_LABEL[slots[i]!.spec.durability] : columnTitle(slots[i]!.spec)}`;
  const floor = prompt ? noiseFloor(columns, prompt, props.exclude) : { columns: [] };
  const excludedCount = prompt ? columns.reduce((n, rs) => n + rs.filter((r) => r.prompt === prompt && !r.warmup).length - countedOf(rs, prompt, props.exclude).length, 0) : 0;
  const models = [...new Set(slots.map((s) => props.modelOf(s.spec)).filter(Boolean))];
  const promptOptions: PickerOption<string>[] = props.prompts.map((p) => ({ value: p, label: promptLabel(p) }));
  const download = () => {
    const blob = new Blob([JSON.stringify(props.json(), null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `compare-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.json`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  };
  const empty = !summary;
  return (
    <section className="cmp-results" aria-label="Results">
      <div className="cmp-results-head">
        {props.prompts.length > 1 && prompt ? (
          <span className="group">
            <span className="group-label">Results for</span>
            <Picker label="Results for" value={prompt} options={promptOptions} onChange={props.onPrompt} />
          </span>
        ) : (
          <h2>Results{prompt ? ` for ${promptLabel(prompt)}` : ''}</h2>
        )}
        {props.restoredAt !== undefined && (
          <span className="muted">
            Restored results from {new Date(props.restoredAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} ·{' '}
            <button type="button" className="link-btn" onClick={props.onClear}>
              Clear
            </button>
          </span>
        )}
        <span className="spacer" />
        <button
          type="button"
          className="btn"
          disabled={empty}
          onClick={() => {
            void navigator.clipboard?.writeText(props.markdown());
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
          }}
        >
          {copied ? 'Copied' : 'Copy Results'}
        </button>
        <button type="button" className="btn" disabled={empty} onClick={download}>
          Download JSON
        </button>
      </div>
      <div className="cmp-table-wrap">
        <table className="cmp-table">
          <thead>
            <tr>
              <th scope="col">
                <span className="sr-only">Metric</span>
              </th>
              {slots.map((_, i) => (
                <th key={i} scope="col" className="num">
                  {head(i)}
                  {i === baseIndex && <span className="muted"> (baseline)</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {METRICS.map((m) => (
              <tr key={m}>
                <th scope="row">
                  <Tip term={m}>{ROW_LABEL[m]}</Tip>
                </th>
                {slots.map((s, i) => {
                  const c = summary?.[i]?.cells[m];
                  const text = c ? cellText(c, harnessLabel(s.spec.harness)) : { main: '' };
                  return (
                    <td key={i} className="num" data-kind={c?.kind}>
                      {text.main}
                      {text.note && <span className="muted"> {text.note}</span>}
                    </td>
                  );
                })}
              </tr>
            ))}
            <tr>
              <th scope="row">
                <Tip term="flagged">Rounds counted</Tip>
              </th>
              {slots.map((_, i) => (
                <td key={i} className="num">
                  {summary?.[i] ? countedText(summary[i]!) : ''}
                </td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>
      <p className="cmp-note">
        {empty ? (
          `Run ${props.rounds} rounds to fill this in.`
        ) : (
          <>
            {floor.p50 !== undefined && (
              <>
                <Tip term="noise">Model time to first token</Tip> {fmtSeconds(floor.p50)} p50, pooled over {listOf(floor.columns.map(letter))}.{' '}
              </>
            )}
            <Tip term="vsBase">Δ</Tip> is the median of per-round differences against {letter(baseIndex)}; intervals are 90% bootstrap over rounds. {props.warmup}{' '}
            warm-up {props.warmup === 1 ? 'round' : 'rounds'}
            {props.exclude ? ` and flagged rounds are not counted (${excludedCount} excluded).` : ' are not counted; flagged rounds are.'}
            {models.length > 1 && <> Columns call different models: {models.join(', ')}.</>}
          </>
        )}
      </p>
      <details className="cmp-details" open={open} onToggle={(e) => setOpen((e.currentTarget as HTMLDetailsElement).open)}>
        <summary>Details</summary>
        <Details {...props} />
      </details>
    </section>
  );
}

function Details(props: { slots: Slot[]; columns: RoundRecord[][]; prompt?: string; exclude: boolean; onExclude(on: boolean): void; modelOf(spec: ColumnSpec): string | undefined }) {
  const { slots, columns, prompt } = props;
  const counted = columns.map((rs) => (prompt ? countedOf(rs, prompt, props.exclude) : []));
  const fmt = (x: number | undefined) => (x === undefined ? '' : fmtSeconds(Math.round(x)));
  const pAt = (rs: RoundRecord[], k: keyof RoundRecord) => {
    const xs = rs.map((r) => r[k]).filter((x): x is number => typeof x === 'number');
    return { p50: median(xs), p90: xs.length >= 20 ? p90(xs) : undefined, n: xs.length };
  };
  const both = (rs: RoundRecord[], k: keyof RoundRecord) => {
    const s = pAt(rs, k);
    return s.p50 === undefined ? '' : `${fmt(s.p50)}${s.p90 !== undefined ? ` · p90 ${fmt(s.p90)}` : ''}`;
  };
  const rows: { label: string; term?: GlossaryTerm; cell(i: number): string }[] = [
    { label: 'Model', cell: (i) => props.modelOf(slots[i]!.spec) ?? '' },
    { label: 'Setup time', term: 'ready', cell: (i) => fmt(slots[i]!.pane?.setupMs) },
    { label: METRIC_LABEL.model, term: 'model', cell: (i) => both(counted[i]!, 'model') },
    { label: METRIC_LABEL.firstOutput, term: 'ttft', cell: (i) => both(counted[i]!, 'firstOutput') },
    { label: METRIC_LABEL.turn, term: 'turn', cell: (i) => both(counted[i]!, 'turn') },
    { label: METRIC_LABEL.harnessIdle, term: 'harnessIdle', cell: (i) => both(counted[i]!, 'harnessIdle') },
    {
      label: 'Writes per turn · ack p50',
      cell: (i) => {
        const w = median(counted[i]!.map((r) => r.writes).filter((x): x is number => x !== undefined));
        const a = median(counted[i]!.map((r) => r.ackP50).filter((x): x is number => x !== undefined));
        return w === undefined ? '' : `${Math.round(w)}${a !== undefined ? ` · ${fmt(a)}` : ''}`;
      },
    },
    {
      label: 'Overhead per turn vs round',
      cell: (i) => {
        const s = slope(counted[i]!);
        return s === undefined ? '' : `${s >= 0 ? '+' : '−'}${Math.abs(s).toFixed(1)} ms per round`;
      },
    },
    {
      label: 'Flags',
      term: 'flagged',
      cell: (i) => {
        const rs = (columns[i] ?? []).filter((r) => !r.warmup && (!prompt || r.prompt === prompt));
        const parts = FLAG_ORDER.map(([k, name]) => [name, rs.filter((r) => !!r.flags[k]).length] as const).filter(([, n]) => n > 0);
        return parts.length ? parts.map(([name, n]) => `${name} ${n}`).join(', ') : rs.length ? 'None' : '';
      },
    },
    {
      label: 'Reliability events per 100 rounds',
      cell: (i) => {
        const r = reliabilityPer100(columns[i] ?? []);
        return r === undefined ? '' : r.toFixed(0);
      },
    },
    {
      label: 'Tools called',
      cell: (i) => [...new Set((slots[i]!.pane?.items ?? []).flatMap((it) => (it.kind === 'tool' ? [it.name] : [])))].join(', '),
    },
  ];
  return (
    <div className="cmp-details-body">
      <label className="check">
        <input type="checkbox" checked={props.exclude} onChange={(e) => props.onExclude(e.target.checked)} />
        <span>Exclude cold starts, stalls and recoveries</span>
      </label>
      <div className="cmp-table-wrap">
        <table className="cmp-table cmp-table-details">
          <thead>
            <tr>
              <th scope="col">
                <span className="sr-only">Detail</span>
              </th>
              {slots.map((s, i) => (
                <th key={i} scope="col">
                  {letter(i)} {columnTitle(s.spec)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.label}>
                <th scope="row">{r.term ? <Tip term={r.term}>{r.label}</Tip> : r.label}</th>
                {slots.map((_, i) => (
                  <td key={i} className="num">
                    {r.cell(i)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <ul className="cmp-notes">
        <li>Times are p50 over counted rounds; p90 once a column has 20.</li>
        {slots.some((s) => s.spec.harness === 'eve') && <li>eve: model time is a lower bound (taken from eve's own events), so its overhead reads low.</li>}
        {slots.some((s) => s.spec.harness === 'fx-durable' || s.spec.harness === 'pi-durable') && (
          <li>Pi (pi-durable) and fx (libfx durable) advertise their own tool lists, not the kernel lanes' nine tools; overhead takes the model's time out, so a different tool list moves their first output, not their overhead.</li>
        )}
        <li>
          <a href="/bench">Scripted Runner (/bench)</a>: the same cells, one at a time, a fresh session per sample.
        </li>
      </ul>
    </div>
  );
}

/**
 * A column's configuration while editing: harness, durability, runtime.
 * A setting shows only when there is a choice to make (eve and fx (libfx
 * durable) have one durability and one runtime).
 */
function ColumnConfig({ spec, locked, processOptions, onSpec }: { spec: ColumnSpec; locked: boolean; processOptions: HarnessOption[]; onSpec: (next: Partial<ColumnSpec>) => void }) {
  const durabilities = durabilitiesFor(spec.harness);
  const runtimes = runtimesFor(spec.harness, spec.durability);
  const harnesses: PickerOption<string>[] = [
    ...HARNESSES.map((h) => ({ value: h as string, label: HARNESS_LABEL[h], group: HARNESS_GROUP[harnessGroup(h)] })),
    ...processOptions
      .filter((o) => processHarness(o.value))
      .map((o) => ({ value: o.value, label: o.label, group: HARNESS_GROUP[harnessGroup(o.value)], ...(o.disabledReason ? { disabled: true, detail: o.disabledReason } : {}) })),
  ];
  if (!harnesses.some((h) => h.value === spec.harness)) harnesses.push({ value: spec.harness, label: harnessLabel(spec.harness), disabled: true });
  const groups: string[] = Object.values(HARNESS_GROUP);
  harnesses.sort((a, b) => groups.indexOf(a.group ?? '') - groups.indexOf(b.group ?? ''));
  return (
    <div className="cmp-config">
      <Picker label="Harness" value={spec.harness} options={harnesses} onChange={(harness) => onSpec({ harness })} disabled={locked} />
      {durabilities.length > 1 && (
        <Picker<Durability>
          label="Durability"
          value={spec.durability}
          options={durabilities.map((d) => ({ value: d, label: DURABILITY_LABEL[d], detail: DURABILITY_HINT[d] }))}
          onChange={(durability) => onSpec({ durability })}
          disabled={locked}
        />
      )}
      {runtimes.length > 1 && (
        <Picker<Runtime> label="Runtime" value={spec.runtime} options={runtimes.map((r) => ({ value: r, label: RUNTIME_LABEL[r] }))} onChange={(runtime) => onSpec({ runtime })} disabled={locked} />
      )}
    </div>
  );
}

function ColumnCard(props: {
  index: number;
  slot: Slot;
  shown: boolean;
  isBase: boolean;
  onBase(): void;
  views: TurnView[];
  records: RoundRecord[];
  scale: Map<number, number>;
  preparing: boolean;
  locked: boolean;
  editing: boolean;
  local?: boolean;
  processOptions: HarnessOption[];
  onSpec(next: Partial<ColumnSpec>): void;
  onRemove?(): void;
}) {
  const { slot, views, records } = props;
  const { spec, pane } = slot;
  const desc = describeColumn(spec, props.local);
  const live = pane?.running;
  const [open, setOpen] = useState(false);
  const counted = records.filter((r) => !r.warmup);
  const replies = new Set(counted.map((r) => r.reply ?? ''));
  const all = counted.length > 0 && replies.size === 1 && [...replies][0] ? `, all replied "${[...replies][0]}"` : '';
  // Warm-ups are numbered on their own; counted rounds from 1, across the sessions this column has had.
  const archivedCounted = slot.archived.filter((r) => !r.warmup).length;
  let n = archivedCounted;
  let w = 0;
  const lines = views.map((v, i) => {
    const warm = !!v.warmup;
    const label = warm ? `Warm-up ${++w}` : `Round ${++n}`;
    const send = pane!.sends[i];
    const rec = send ? records.find((r) => r.seq === send.seq) : undefined;
    return { v, i, label, warm, rec, seq: send?.seq };
  });
  const showList = live || open;
  return (
    <section className="cmp-col" data-shown={props.shown || undefined} aria-label={`${letter(props.index)} · ${columnTitle(spec)}`}>
      <header className="cmp-col-head">
        <h3>
          {letter(props.index)} · {columnTitle(spec)}
        </h3>
        {props.isBase ? (
          <span className="cmp-base" aria-current="true">
            ◉ Baseline
          </span>
        ) : (
          <button type="button" className="cmp-base link-btn" onClick={props.onBase}>
            ○ Use as Baseline
          </button>
        )}
        {props.onRemove && (
          <button type="button" className="icon-btn" onClick={props.onRemove} disabled={props.locked} aria-label="Remove column">
            ×
          </button>
        )}
      </header>
      <p className="cmp-col-desc">{desc}</p>
      {props.editing && <ColumnConfig spec={spec} locked={props.locked} processOptions={props.processOptions} onSpec={props.onSpec} />}
      {(slot.error || pane?.error) && <p className="cmp-error">{slot.error ?? pane?.error}</p>}
      {!pane && !slot.error && <p className="muted cmp-col-state">{props.preparing ? 'Opening a session…' : ''}</p>}
      {counted.length > 0 && !live && (
        <button type="button" className="cmp-col-toggle link-btn" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
          {counted.length} {counted.length === 1 ? 'round' : 'rounds'}
          {all} {open ? '▾' : '▸'}
        </button>
      )}
      {showList && (
        <ol className="cmp-rounds">
          {archivedCounted > 0 && <li className="muted cmp-round-earlier">Rounds 1–{archivedCounted} ran in earlier sessions.</li>}
          {lines.map(({ v, i, label, warm, rec, seq }) => (
            <RoundRow
              key={i}
              label={label}
              warm={warm}
              v={v}
              rec={rec}
              scale={seq !== undefined ? (props.scale.get(seq) ?? 1) : 1}
              items={pane!.itemsOf(i)}
              first={i === lines.findIndex((l) => !l.warm)}
              current={i === lines.length - 1 && !!live}
            />
          ))}
        </ol>
      )}
    </section>
  );
}

function RoundRow(props: { label: string; warm: boolean; v: TurnView; rec?: RoundRecord; scale: number; items: Item[]; first: boolean; current: boolean }) {
  const { v, rec } = props;
  const [open, setOpen] = useState(false);
  const b = v.breakdown;
  const done = v.turn !== undefined;
  return (
    <li className="cmp-round" data-warm={props.warm || undefined}>
      <div className="cmp-round-line num">
        <span className="cmp-round-label">{props.label}</span>
        {props.warm && <span className="muted">not counted</span>}
        <span>first output {v.ttft !== undefined ? fmtSeconds(v.ttft) : <LiveMs since={v.sentAt} />}</span>
        <span>turn {done ? fmtSeconds(v.turn!) : <LiveMs since={v.sentAt} />}</span>
        {b?.overheadMs !== undefined && <span>overhead {fmtSeconds(b.overheadMs)}</span>}
        {rec && <RoundFlags rec={rec} />}
        <button type="button" className="link-btn cmp-round-more" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
          Breakdown {open ? '▾' : '▸'}
        </button>
      </div>
      {done && b && <RoundBar v={v} b={b} scale={props.scale} labels={props.first} />}
      {(open || props.current) && (
        <div className="cmp-round-body">
          {props.items.map((it, j) =>
            it.kind === 'text' ? (
              <Markdown key={j} className="cmp-reply" text={it.text} />
            ) : it.kind === 'tool' ? (
              <div key={j} className="cmp-tool mono">
                {it.done ? (it.error ? '✕' : '✓') : '…'} {it.name}
              </div>
            ) : it.kind === 'error' ? (
              <p key={j} className="cmp-error">
                {it.text}
              </p>
            ) : it.steer ? (
              <p key={j} className="muted">
                Steer: {it.text}
              </p>
            ) : null
          )}
          {open && <TurnChips v={v} />}
        </div>
      )}
    </li>
  );
}

function RoundFlags({ rec }: { rec: RoundRecord }) {
  const shown = FLAG_ORDER.filter(([k]) => !!rec.flags[k]);
  if (!shown.length) return null;
  return (
    <>
      {shown.map(([k, name, term]) => {
        const v = rec.flags[k]!;
        const ms = k === 'deliveryStall' || k === 'slowWrite' || k === 'modelTail' || k === 'firstOutputTrails' || k === 'slow';
        return (
          <Tip key={k} term={term} className="cmp-flag" kind={EXCLUDING.includes(k) ? 'excluded' : 'shown'}>
            {name}
            {ms ? ` ${fmtSeconds(v)}` : v > 1 ? ` ×${v}` : ''}
          </Tip>
        );
      })}
    </>
  );
}

/** The round as three segments, one scale per round across columns: model, tools, harness and durability. */
function RoundBar({ v, b, scale, labels }: { v: TurnView; b: Breakdown; scale: number; labels: boolean }) {
  const turn = v.turn ?? 0;
  const model = b.turnModelMs;
  const tools = b.turnToolMs ?? 0;
  if (model === undefined) {
    // No model marks: the turn only.
    return (
      <div className="cmp-bar" aria-label={`Turn ${fmtSeconds(turn)}`}>
        <span className="cmp-seg" data-kind="turn" style={{ width: `${(turn / scale) * 100}%` }} />
      </div>
    );
  }
  const harness = Math.max(0, turn - model - tools);
  const segs = [
    { kind: 'model', ms: model, label: 'model', term: 'segModel' as const },
    { kind: 'tool', ms: tools, label: 'tools', term: 'segTool' as const },
    { kind: 'harness', ms: harness, label: 'harness and durability', term: 'segHarness' as const },
  ].filter((s) => s.ms > 0);
  return (
    <div className="cmp-bar-wrap">
      <div className="cmp-bar" role="img" aria-label={segs.map((s) => `${s.label} ${fmtSeconds(s.ms)}`).join(', ')}>
        {segs.map((s) => (
          <span key={s.kind} className="cmp-seg" data-kind={s.kind} style={{ width: `${(s.ms / scale) * 100}%` }} />
        ))}
      </div>
      {labels && (
        <div className="cmp-bar-labels">
          {segs.map((s) => (
            <Tip key={s.kind} term={s.term} className="cmp-bar-label" kind={s.kind}>
              {s.label} {fmtSeconds(s.ms)}
            </Tip>
          ))}
        </div>
      )}
    </div>
  );
}

/** A round's own numbers: the old per-turn chips, behind Breakdown. */
function TurnChips({ v }: { v: TurnView }) {
  const bd = v.breakdown;
  return (
    <span className="compare-chips">
      <span className="round-chip">
        <Tip term="overhead">{METRIC_LABEL.overhead}</Tip> {bd?.overheadMs !== undefined ? fmtSeconds(bd.overheadMs) : 'not measured'}
      </span>
      <span className="round-chip">
        <Tip term="model">{METRIC_LABEL.model}</Tip> {bd?.modelMs !== undefined ? fmtSeconds(bd.modelMs) : 'not measured'}
      </span>
      <span className="round-chip">
        <Tip term="ttft">{METRIC_LABEL.firstOutput}</Tip> {v.ttft !== undefined ? fmtSeconds(v.ttft) : '…'}
      </span>
      {v.tools.map((x, i) => {
        const t = bd?.tools[i];
        return (
          <span key={i} className="round-chip">
            <Tip term="gap">{x.name}</Tip> {x.gapMs !== undefined ? fmtSeconds(x.gapMs) : '…'}
            {t?.execMs !== undefined && <span className="muted"> · tool {fmtSeconds(t.execMs)}</span>}
            {t?.nextModelMs !== undefined && <span className="muted"> · model {fmtSeconds(t.nextModelMs)}</span>}
            {t?.oursMs !== undefined && (
              <span className="muted">
                {' '}
                · <Tip term="ours">durable-harness</Tip> {fmtSeconds(t.oursMs)}
              </span>
            )}
            {t?.overheadMs !== undefined && (
              <span>
                {' '}
                · <Tip term="toolOverhead">overhead</Tip> {fmtSeconds(t.overheadMs)}
              </span>
            )}
          </span>
        );
      })}
      <span className="round-chip">
        <Tip term="turn">{METRIC_LABEL.turn}</Tip> {v.turn !== undefined ? fmtSeconds(v.turn) : <LiveMs since={v.sentAt} />}
        {v.harnessIdle !== undefined && (
          <span className="muted">
            {' '}
            · <Tip term="harnessIdle">agent done</Tip> {fmtSeconds(v.harnessIdle)}
          </span>
        )}
      </span>
      {bd?.overheadMs !== undefined && bd.overheadMs > OVERHEAD_STALL_MS && !bd.flagged && <span className="round-chip">over the stall line</span>}
      {v.notReadyMs !== undefined && (
        <span className="round-chip muted">
          <Tip term="notReady">session starting</Tip> {fmtSeconds(v.notReadyMs)}, not counted
        </span>
      )}
    </span>
  );
}

function LiveMs({ since }: { since: number }) {
  return <span className="muted">{fmtSeconds(now() - since)}</span>;
}

function ConfirmDialog(props: { rounds: number; onCopy(): void; onClear(): void; onCancel(): void }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    ref.current?.querySelector<HTMLButtonElement>('button')?.focus();
    const key = (e: KeyboardEvent) => e.key === 'Escape' && props.onCancel();
    document.addEventListener('keydown', key);
    return () => document.removeEventListener('keydown', key);
  }, [props.onCancel]);
  return createPortal(
    <div className="cmp-modal-scrim">
      <div className="cmp-modal" role="alertdialog" aria-modal="true" aria-labelledby="cmp-confirm" ref={ref}>
        <p id="cmp-confirm">
          This starts fresh sessions in every column and clears {props.rounds} {props.rounds === 1 ? 'round' : 'rounds'} of results.
        </p>
        <div className="cmp-modal-actions">
          <button type="button" className="btn btn-primary" onClick={props.onCopy}>
            Copy and Continue
          </button>
          <button type="button" className="btn" onClick={props.onClear}>
            Clear and Continue
          </button>
          <button type="button" className="btn" onClick={props.onCancel}>
            Cancel
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}

/** The rounds a summary counts: every finished round but the warm-ups, and (`exclude`) none flagged. */
export const countedTurns = (vs: TurnView[], exclude: boolean) => vs.filter((v) => !v.warmup && v.turn !== undefined && !(exclude && v.breakdown?.flagged));

