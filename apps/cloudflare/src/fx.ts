/**
 * fx (libfx) on the kernel: its Wasm build, the one a browser tab runs.
 * workerd has WebAssembly JSPI (`WebAssembly.Suspending`), which the Wasm
 * kernel needs, and forbids compiling Wasm at runtime, so the kernel's
 * module is compiled by wrangler at build (a `.wasm` import) and handed to
 * libfx as a `WebAssembly.Module`. `libfx/wasm` is used rather than
 * `libfx/browser`, whose default module URL needs `import.meta.url`.
 * `createFxEngine` is libfx's engine with no durability of its own (since
 * vercel-labs/fx#1180, `createFxAgent` is libfx's durable agent); the kernel
 * is what makes it durable here, as in every other fx lane. The tools are
 * libfx's kernel tools (read_file, glob_files, grep_files, edit_file,
 * write_file, shell) on a just-bash workspace, plus the two unsafe example
 * tools (`replay: 'never'`).
 */
import { fxHarness as kernelFx } from 'harness-world-fx';
import { unsafeTools } from 'harness-world-hands';
import { fxHostTools } from 'harness-world-hands/fx';
import { createFxEngine } from 'libfx/wasm';
// Not in libfx's exports map: imported by path, compiled by wrangler.
import fxCore from '../node_modules/libfx/fx-core.wasm';
import { CONFIRM_MS, gatewayKey, INSTRUCTIONS, MODEL } from './model.ts';

/** The installed libfx (package.json), recorded on every turn and checkpoint. */
export const LIBFX_VERSION = '0.0.14';

export function fxHarness() {
  return kernelFx({
    createFxEngine: ((options: Record<string, unknown>) => createFxEngine({ ...options, wasm: fxCore })) as never,
    apiKey: () => gatewayKey(),
    model: MODEL,
    instructions: INSTRUCTIONS,
    // libfx's kernel tools on a just-bash workspace whose files live in the
    // log. No worker threads in workerd: the shell runs in this isolate (its
    // hardened limits).
    tools: [...fxHostTools({ kind: 'virtual', durable: true }, { isolation: 'auto' }), ...unsafeTools({ confirmMs: CONFIRM_MS })],
    kernelVersion: LIBFX_VERSION,
    agentOptions: { backend: 'wasm' },
  });
}
