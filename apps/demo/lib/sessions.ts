import type { World } from '@workflow/world';
import type { Hands } from 'harness-world-demo-ui';
import {
  type ChaosDriveOptions,
  type DurableSessions,
  durableSessions,
  type Harness,
  type HostTool,
  type LaneOptions,
  laneAvailable,
  type OwnerPolicy,
} from 'harness-world';
import { queueHoldConsumerGroup, vercelTriggerConsumerGroup, virtual } from 'durable-harness';
import { vercelChannel } from 'harness-world/vercel-channel';
import type { VercelSandboxProvider } from 'harness-world-hybrid/sandbox';
import { byHands, checkUpFront, demoSandbox, type UpFront, upFrontFields } from './hands';
import { wakeLane } from './hybrid';
import { getSandboxProvider, providerFrom } from './sandbox-provider';
import { Todos } from './docs';
import { processKernelHarnesses } from './harnesses';
import { gatewayKey, instructionsFor, MODEL } from './model';
import { handleWake, isPiDurableQueue, isPiWake } from './pi-durable';
import { getWorld } from './world';

/**
 * The harness packages (pi-ai, libfx, the sandbox SDK, the tools) are
 * imported on first use, not with this module. Every API route of the app
 * shares one function, and a new instance pays its first request for
 * whatever the routes import at load; here that was most of the ~2s a turn
 * on a fresh instance cost. `getSessions()` starts loading them at once, so
 * the load overlaps the session's first log read and write.
 */
async function loadHeavy() {
  const [piAi, gw, fxMod, hands, handsFx, hybridMod, sandbox, piMod, libfx] = await Promise.all([
    import('@earendil-works/pi-ai'),
    import('@earendil-works/pi-ai/providers/vercel-ai-gateway'),
    import('harness-world-fx'),
    import('harness-world-hands'),
    import('harness-world-hands/fx'),
    import('harness-world-hybrid'),
    import('harness-world-hybrid/sandbox'),
    import('durable-harness/pi'),
    import('libfx'),
  ]);
  return { piAi, gw, fxMod, hands, handsFx, hybridMod, sandbox, piMod, libfx };
}
type Heavy = Awaited<ReturnType<typeof loadHeavy>>;
const gh = globalThis as { __harnessHeavy?: Promise<Heavy> };
const heavy = (): Promise<Heavy> => (gh.__harnessHeavy ??= loadHeavy());

/** Start loading the harness packages (idempotent; for routes about to drive). */
export function preloadHarnesses(): void {
  heavy().catch(() => {});
}

/** A harness whose implementation is built on its first drive. `name` and `upFront` are known up front. */
function lazyHarness(name: string, upFront: UpFront, make: (h: Heavy) => Harness): Harness {
  let built: Promise<Harness> | undefined;
  return {
    name,
    ...upFrontFields(upFront),
    async drive(ctx) {
      const ready = (built ??= heavy().then((h) => checkUpFront(name, upFront, make(h))));
      return (await ready).drive(ctx);
    },
  };
}

export { MODEL };

/** The plain Pi and fx harnesses (not hybrid), for the durable server lane and the raw baseline. */
export function plainHarnesses() {
  return [pi(), fx()];
}

/**
 * The harness's own workspace tools for a session's hands (the demo's Hands
 * axis): none; on `virtual()` (its files in the log; with durability off,
 * the raw baseline, in memory for the turn); on `virtual({ durable: false })`;
 * on `virtual()` with `run_on_machine` next to them; or on the session's
 * `sandbox()`, which Pi's `createCodingTools` refuses until its sandbox
 * support lands (the page and the sessions route offer it to neither yet).
 */
async function handsTools(h: Heavy, hands: Hands, onWorkspace: (workspace: Parameters<Heavy['piMod']['createCodingTools']>[0]) => HostTool[]): Promise<HostTool[]> {
  switch (hands) {
    case 'none':
      return [];
    case 'ephemeral':
      return onWorkspace(virtual({ durable: false }));
    case 'machine':
      return [...onWorkspace(virtual()), h.hybridMod.runOnMachineTool({ lane: 'function', sandbox: sandboxProvider(h) })];
    case 'sandbox':
      return onWorkspace(await demoSandbox());
    default:
      return onWorkspace(virtual());
  }
}

function pi() {
  return byHands('pi', { modelCalls: 'self' }, async (hands) => {
    const h = await heavy();
    const { piAi, gw, piMod } = h;
    const models = piAi.createModels();
    models.setProvider(gw.vercelAIGatewayProvider());
    const model = models.getModel('vercel-ai-gateway', MODEL);
    if (!model) throw new Error(`pi-ai does not know ${MODEL}`);
    return piMod.piHarness({
      model,
      streamFn: models.streamSimple.bind(models),
      getApiKey: () => gatewayKey(),
      systemPrompt: instructionsFor(hands),
      // Pi's own coding tools (read, bash, edit, write) on the session's hands.
      tools: [...(await handsTools(h, hands, (ws) => piMod.createCodingTools(ws))), ...h.hands.unsafeTools(), ...h.hands.docTools([Todos])],
    });
  });
}

function fx() {
  return byHands('fx', { modelCalls: 'self' }, async (hands) => {
    const h = await heavy();
    const { fxMod, handsFx, libfx } = h;
    return fxMod.fxHarness({
      createFxEngine: libfx.createFxEngine,
      apiKey: gatewayKey,
      model: MODEL,
      instructions: instructionsFor(hands),
      // libfx's kernel tools (read_file, glob_files, grep_files, edit_file,
      // write_file, shell) on the session's hands; a sandbox() has no fx
      // kernel tools yet (the page and the sessions route offer it to neither).
      tools: [
        ...(await handsTools(h, hands, (ws) => {
          if (ws.kind !== 'virtual') throw new TypeError('fx on sandbox() is not available yet: its kernel tools run on virtual() only');
          return handsFx.fxHostTools(ws);
        })),
        ...h.hands.unsafeTools(),
        ...h.hands.docTools([Todos]),
      ],
    });
  });
}

/**
 * The server's lane of a hybrid session: this process drives `function` (a
 * backend secret; `run_on_machine` reaches the session's own VM through the
 * Sandbox API and waits for its command inside the call). The `sandbox`
 * lane (the retired brain-in-VM lane) is never available, even to an older
 * session whose log still has a host fact. Idle sessions settle to their
 * home lane. Carried on the hybrid harnesses only, so plain Pi/fx sessions
 * never move.
 */
function serverLanes(world: World): LaneOptions {
  return {
    serves: ['function'],
    settle: 'browser',
    available: (lane, state) => lane !== 'sandbox' && laneAvailable(state, lane),
    async wake(runId, to, _handoff, opts) {
      const result = await wakeLane(world, runId, to, { key: opts?.idempotencyKey });
      console.log('[harness] wake lane', { runId, to, ...result });
    },
  };
}

const chaosOn = process.env.HARNESS_CHAOS !== '0';

// The hybrid harness names (harness-world-hybrid's HYBRID_HARNESS and
// HYBRID_FX_HARNESS), spelled here so naming a harness loads nothing.
const HYBRID_HARNESS = 'hybrid';
const HYBRID_FX_HARNESS = 'hybrid-fx';

export { getSandboxProvider };

function sandboxProvider({ sandbox }: Heavy): VercelSandboxProvider {
  return providerFrom(sandbox);
}

/** The sandbox owner policy (parked commands), built on its first check. */
function sandboxOwners(): OwnerPolicy {
  let built: Promise<OwnerPolicy> | undefined;
  const policy = () => (built ??= heavy().then((h: Heavy) => h.hybridMod.sandboxOwnerPolicy({ provider: sandboxProvider(h), chaos: chaosOn })));
  return {
    kind: 'sandbox',
    check: async (c) => (await policy()).check(c),
    fireChaos: async (c) => {
      const p = await policy();
      if (!p.fireChaos) throw new Error('harness: the sandbox owner policy cannot fire crashes');
      return p.fireChaos(c);
    },
  };
}

/** The hybrid Pi session, server half: the tools for each session's hands (`config.hands`), the same list as the tab's. */
function hybrid(world: World) {
  const lanes = serverLanes(world);
  return lazyHarness(HYBRID_HARNESS, { lanes, modelCalls: 'self' }, (h) => {
    const { model, streamFn } = h.hybridMod.createGatewayModel(undefined, MODEL);
    return h.hybridMod.hybridHarness({
      lane: 'function',
      model,
      streamFn,
      getApiKey: () => gatewayKey(),
      tools: (hands) => h.hybridMod.hybridTools({ lane: 'function', sandbox: sandboxProvider(h), hands }),
      lanes,
    });
  });
}

/**
 * The hybrid fx session, server half: libfx's native addon here, its Wasm
 * build in the tab, one tape. Same model, prompt and tools as the tab's
 * (the tape digests the request body and checks the model id).
 */
function hybridFx(world: World) {
  const lanes = serverLanes(world);
  return lazyHarness(HYBRID_FX_HARNESS, { lanes, modelCalls: 'self' }, (h) =>
    h.hybridMod.hybridFxHarness({
      lane: 'function',
      createFxEngine: h.libfx.createFxEngine,
      apiKey: gatewayKey,
      model: MODEL,
      tools: (hands) => h.hybridMod.hybridTools({ lane: 'function', sandbox: sandboxProvider(h), hands }),
      lanes,
    })
  );
}

/**
 * What a tab needs to drive hybrid sessions the way this server replays
 * them: the model every lane calls, and the hash of this server's fx
 * configuration (a tab from another deployment compares it with its own
 * and hands fx turns to the server when they differ).
 */
export async function hybridConfig(): Promise<{ model: string; fxConfig: string }> {
  const { hybridMod } = await heavy();
  return { model: MODEL, fxConfig: await hybridMod.hybridFxConfigHash({ lane: 'sandbox', model: MODEL }) };
}

const g = globalThis as { __harnessSessions?: Promise<DurableSessions>; __harnessDrillSessions?: Promise<DurableSessions> };

/**
 * Whether crash drills run in a Function of their own (`/api/drill`). On
 * Vercel a Function instance serves many requests at once (Fluid compute),
 * and a drill's kill (SIGKILL) takes every one of them down: other
 * sessions' drives, their live tails. So on Vercel the turn of a session
 * with an armed crash is sent to the drill route, the only handle here that
 * fires a crash armed before its drive began; every other handle fires
 * only a crash armed while its drive runs (arming mid-turn). Locally there
 * is one process anyway: every handle fires. `HARNESS_DRILL_ROUTE=0` turns
 * the route off.
 */
export const drillRoute = () => !!process.env.VERCEL && process.env.HARNESS_DRILL_ROUTE !== '0';

/** The process's sessions, for every route but the drill route. */
export function getSessions(): Promise<DurableSessions> {
  preloadHarnesses();
  g.__harnessSessions ??= buildSessions(chaosOn ? (drillRoute() ? { fire: 'armed-in-drive' } : true) : false);
  return g.__harnessSessions;
}

/** The drill route's sessions (`/api/drill`): every armed crash fires here. */
export function getDrillSessions(): Promise<DurableSessions> {
  preloadHarnesses();
  g.__harnessDrillSessions ??= buildSessions(chaosOn);
  return g.__harnessDrillSessions;
}

async function buildSessions(chaos: boolean | ChaosDriveOptions): Promise<DurableSessions> {
  return durableSessions({
      world: await getWorld(),
      // Writes made outside a queue delivery (the drive route, direct
      // drives) share one WebSocket per session; a no-op off world-vercel.
      openChannel: vercelChannel(),
      // Process harnesses (Claude Code, Codex, ...) come from lib/harnesses.
      harness: [pi(), fx(), hybrid(await getWorld()), hybridFx(await getWorld()), ...processKernelHarnesses()],
      log: (message, fields) => console.log(`[harness] ${message}`, fields ?? ''),
      // The UI's Crash button arms a real crash: the function driving the
      // session exits mid-step and the queue redrives it. Under `next dev`
      // that exits the dev server; start it again to watch recovery. Which
      // crashes this handle fires: see `drillRoute`.
      chaos,
      // Parked sandbox commands (the detached sandbox_exec of older hybrid
      // sessions; run_on_machine never parks) are policed through the
      // Sandbox API by owner checks.
      owners: [sandboxOwners()],
      // The chat routes' wakes are held for the Function that consumes the
      // kernel's topic (send-and-hold, where the World supports it): the flow
      // route, whose trigger vercel.json gives, so its consumer group is the
      // one Vercel derives from that `functions` key.
      queueHoldConsumerGroup: queueHoldConsumerGroup() ?? vercelTriggerConsumerGroup('app/.well-known/durable-harness/v1/flow/route.ts'),
      // Nothing beats: each drive runs under its queue message (or, on the
      // drive route, a deadline with a companion message behind it), and a
      // redelivery takes over at once (harness-world's anchor.ts). A tab's
      // drives are covered by its tether (/api/hybrid/tether).
      // pi-durable sessions (lib/pi-durable.ts) share this queue and flow
      // route: their wakes and continuations go to the opener that lane
      // registered with durable-harness/pi-durable's `resumeWith`.
      // So do a restart's recovery deliveries for their runs
      // (instrumentation.ts: `__harness_wkf_workflow_<workflowName>`).
      route: (message, meta) => {
        const runId = (message as { runId?: unknown })?.runId;
        return isPiWake(message) || (typeof runId === 'string' && isPiDurableQueue(meta.queueName)) ? handleWake(message, meta) : undefined;
      },
    });
}
