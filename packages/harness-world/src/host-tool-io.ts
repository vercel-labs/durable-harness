import type { DriveContext, EffectIO, HostToolIO, HostWorkspace } from './types.ts';

/**
 * A `HostTool`'s io, from the effect it runs in: every field the kernel gives
 * an effect (its step id, `park`, a settlement, an armed crash, documents),
 * so an adapter that passes a tool its io cannot forget the next one. Pass
 * `signal` (the harness's own, combined with the effect's) or `progress` to
 * replace the effect's, and `workspace` when the harness attached the
 * session's workspace.
 */
export function hostToolIO(
  io: EffectIO,
  ctx: DriveContext,
  overrides: { signal?: AbortSignal; progress?: (chunk: unknown) => void; workspace?: HostWorkspace } = {}
): HostToolIO {
  return {
    signal: overrides.signal ?? io.signal,
    progress: overrides.progress ?? io.progress,
    ctx,
    stepId: io.stepId,
    park: io.park,
    ...(io.settlement ? { settlement: io.settlement } : {}),
    ...(io.chaosArmedMs !== undefined ? { chaosArmedMs: io.chaosArmedMs } : {}),
    ...(io.docs ? { docs: io.docs } : {}),
    ...(overrides.workspace ? { workspace: overrides.workspace } : {}),
  };
}
