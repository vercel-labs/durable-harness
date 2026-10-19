import { sandboxName } from 'durable-harness/sandbox';
import type { Harness } from 'harness-world';
import type { Brain, Hands, HarnessOption } from 'harness-world-demo-ui';
import { BUILTIN_HARNESSES, handsReason, isHands, legacyHands, pickHands, sandboxOffReason as sandboxOffCopy } from 'harness-world-demo-ui/axes';

/**
 * The demo's Hands axis on the server (docs/workspaces.md §8): where a
 * session's model commands and files go, recorded on its `run_created`
 * (`config.hands`) and read back on every drive, so each session keeps the
 * hands it started with whichever instance drives it.
 */

/** A session's hands: what its `run_created` recorded, else what its harness ran with before the hands axis. */
export function handsOfConfig(config: unknown, harness: string): Hands {
  const hands = (config as { hands?: unknown } | undefined)?.hands;
  return isHands(hands) ? hands : legacyHands(harness);
}

/**
 * What a harness built on its first drive declares before it exists: its
 * `lanes`, and how Agent Runs learns its model calls. durable-harness's
 * tracing reads `modelCalls` when a drive starts, before the build: a
 * wrapper that leaves it out gets every model call recorded twice (an empty
 * `chat` span from the `model` effect next to the harness's own).
 */
export type UpFront = Pick<Harness, 'lanes' | 'modelCalls'>;

/** The fields of `upFront` that are set, for the wrapper. */
export function upFrontFields(upFront: UpFront): UpFront {
  return { ...(upFront.lanes ? { lanes: upFront.lanes } : {}), ...(upFront.modelCalls ? { modelCalls: upFront.modelCalls } : {}) };
}

/** `built`, with a warning when its `modelCalls` is not what its wrapper declared up front. */
export function checkUpFront(name: string, upFront: UpFront, built: Harness): Harness {
  const declared = upFront.modelCalls ?? 'effects';
  const actual = built.modelCalls ?? 'effects';
  if (actual !== declared) console.warn(`[harness] ${name}: its wrapper declares modelCalls '${declared}', the harness '${actual}': Agent Runs records its model calls wrongly`);
  return built;
}

/**
 * One kernel harness that is really one per hands: each is built on the
 * first drive of a session with those hands (in this process), and each
 * drive goes to its session's. `name` and `upFront` are known up front.
 */
export function byHands(name: string, upFront: UpFront, build: (hands: Hands) => Promise<Harness>): Harness {
  const built = new Map<Hands, Promise<Harness>>();
  return {
    name,
    ...upFrontFields(upFront),
    async drive(ctx) {
      const hands = handsOfConfig(ctx.config, name);
      let harness = built.get(hands);
      if (!harness) {
        harness = build(hands).then((h) => checkUpFront(name, upFront, h));
        built.set(hands, harness);
        // A build that failed (a missing credential) is tried again on the next drive.
        harness.catch(() => built.delete(hands));
      }
      return (await harness).drive(ctx);
    },
  };
}

/**
 * The most a `sandbox()` session may spend on its model, over its life, in
 * US dollars: `HARNESS_SANDBOX_SESSION_USD`, default $2. Each session of
 * the demo's Claude Code and Codex boots a Vercel Sandbox VM and makes a
 * Drive; the cap is what keeps one visitor's session from running up the
 * model bill (the demo's routes start sessions for any visitor the
 * deployment's protection lets in).
 */
export function sandboxSessionUsd(): number {
  const usd = Number(process.env.HARNESS_SANDBOX_SESSION_USD ?? 2);
  return Number.isFinite(usd) && usd > 0 ? usd : 2;
}

/** The demo's `sandbox()`: the session's own Vercel Sandbox VM and Drive, with a finite per-session model budget. */
export async function demoSandbox() {
  const { sandbox } = await import('durable-harness');
  return sandbox({ budget: { sessionUsd: sandboxSessionUsd() } });
}

/** A production or preview deployment (`VERCEL_ENV`, not `VERCEL`: `vercel env pull` writes `VERCEL=1` locally). */
export const deployed = () => process.env.VERCEL_ENV === 'production' || process.env.VERCEL_ENV === 'preview';

/**
 * Why this deployment can't make Vercel Sandbox VMs (the `machine` and
 * `sandbox` hands; Claude Code and Codex altogether), or undefined when it
 * can. The VMs are named for the project (`sandboxName`): a deployment has
 * it in `VERCEL_PROJECT_ID` and authenticates with its OIDC token; a laptop
 * needs `vercel link` (the Vercel CLI's login gives the token).
 */
export function sandboxOffReason(): string | undefined {
  try {
    // Synchronous: durable-harness/sandbox's entry loads only the names, never the session factory or a VM.
    sandboxName('demo');
    return undefined;
  } catch {
    return sandboxOffCopy(!deployed());
  }
}

/** Why `option` can't take `hands` with its brain in `brain` (default a Function's) on this deployment, or undefined. */
export function serverHandsReason(option: Pick<HarnessOption, 'label' | 'hands' | 'handsDisabled'>, hands: Hands, brain: Brain = 'server'): string | undefined {
  const sandboxOff = sandboxOffReason();
  return handsReason(option, brain, hands, sandboxOff ? { sandboxOff } : {});
}

/**
 * The hands a session the sessions route starts gets (a Server brain, or a hybrid one's):
 * `wanted`, checked against the harness's (`option`: a process harness's
 * from lib/harnesses, Pi's and fx's from the page's own list), or its
 * default for this deployment when the page names none. A hands it can't
 * take here is an error, with why.
 */
export function startHands(option: Pick<HarnessOption, 'label' | 'hands' | 'defaultHands' | 'handsDisabled'>, wanted: unknown, brain: Brain = 'server'): { hands: Hands } | { error: string } {
  if (wanted === undefined) {
    const sandboxOff = sandboxOffReason();
    return { hands: pickHands(option, brain, undefined, sandboxOff ? { sandboxOff } : {}) };
  }
  if (!isHands(wanted)) return { error: `unknown hands ${JSON.stringify(wanted)}` };
  const reason = serverHandsReason(option, wanted, brain);
  return reason ? { error: reason } : { hands: wanted };
}

/** The option Pi's and fx's hands are checked against (the page's own list, `BUILTIN_HARNESSES`). */
export const builtinOption = (harness: string) => BUILTIN_HARNESSES.find((h) => h.value === harness);
