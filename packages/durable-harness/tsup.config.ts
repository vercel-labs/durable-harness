import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import { defineConfig } from 'tsup';
import { buildRelayBytes, type Esbuild } from '../harness-sandbox/src/relay/bundle.ts';
import { RELAY_MARKER } from './src/relay-bundle.ts';

/**
 * The workspace packages durable-harness is made of (the kernel, the harness
 * adapters, pi-world, the example tools, the sandbox tier and its relay link)
 * are private and ship TypeScript source, so they are bundled in, code and
 * types: an app installs durable-harness and its harness's SDK, and needs no
 * `transpilePackages`. Every npm package stays an import (a dependency, such
 * as the relay link's `ws`, or an optional peer: each harness's SDK,
 * `@vercel/sandbox`).
 */
const WORKSPACE = [
  ['harness-world', 'harness-world'],
  ['harness-world-claude-code', 'harness-claude-code'],
  ['harness-world-codex', 'harness-codex'],
  ['harness-world-opencode', 'harness-opencode'],
  ['harness-world-pi', 'harness-pi'],
  ['harness-world-process', 'harness-process'],
  ['harness-world-sandbox', 'harness-sandbox'],
  ['harness-world-remote', 'world-remote'],
  ['harness-world-hands', 'harness-hands'],
  ['pi-world', 'pi-world'],
] as const;
const inWorkspace = (id: string) => WORKSPACE.some(([name]) => id === name || id.startsWith(`${name}/`));
const packages = fileURLToPath(new URL('..', import.meta.url));
const sources = WORKSPACE.map(([, dir]) => resolve(packages, dir));
const isSource = (file: string) => sources.some((dir) => file.startsWith(`${dir}/`)) || file.startsWith(resolve(packages, 'durable-harness', 'src'));
/** This package's package.json: the kernel's run names read its `version` (harness-world's names.ts). */
const manifest = resolve(packages, 'durable-harness', 'package.json');
const { version } = JSON.parse(readFileSync(manifest, 'utf8')) as { version: string };
/** The module the build fills in with the relay bundle (src/relay-bundle.ts). */
const relayModule = resolve(packages, 'durable-harness', 'src', 'relay-bundle.ts');
/** harness-sandbox's relay build (esbuild, a devDependency there): a published build never runs it, so it ships a stub that says so. */
const relayBuilder = resolve(packages, 'harness-sandbox', 'src', 'relay', 'bundle.ts');
const RELAY_BUILDER_STUB = [
  "const shipped = () => { throw new Error('harness-sandbox: this build ships the relay bundle (durable-harness/src/relay-bundle.ts) and does not build one'); };",
  'export const relayEntry = shipped;',
  'export const buildRelayBytes = async () => shipped();',
  'export const buildRelayBundle = async () => shipped();',
  'export const bundleDir = shipped;',
  '',
].join('\n');
let relayBuilt: Promise<string> | undefined;
/** relay-bundle.ts as shipped: the relay bundle built now (once per build), gzipped and base64'd into the JavaScript. */
const shippedRelay = (esbuild: Esbuild) =>
  (relayBuilt ??= buildRelayBytes(esbuild, resolve(packages, 'harness-sandbox', 'src', 'relay', 'entry.ts')).then(({ bytes, version }) =>
    [
      '// The relay bundle, as this build built it (tsup.config.ts): src/relay-bundle.ts in the source.',
      `export const RELAY_MARKER = ${JSON.stringify(RELAY_MARKER)};`,
      `export const SHIPPED_RELAY = Object.freeze({ marker: RELAY_MARKER, version: ${JSON.stringify(version)}, gzip: ${JSON.stringify(gzipSync(bytes, { level: 9 }).toString('base64'))} });`,
      '',
    ].join('\n')
  ));

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'instrumentation/otel': 'src/instrumentation/otel.ts',
    'claude-code': 'src/claude-code.ts',
    codex: 'src/codex.ts',
    opencode: 'src/opencode.ts',
    pi: 'src/pi.ts',
    'pi-durable': 'src/pi-durable.ts',
    fx: 'src/fx.ts',
    sandbox: 'src/sandbox.ts',
    next: 'src/next.ts',
    vercel: 'src/vercel.ts',
    tools: 'src/tools.ts',
    // `durable-harness vercel-json`.
    cli: 'src/cli.ts',
  },
  format: ['esm'],
  target: 'node22',
  platform: 'node',
  removeNodeProtocol: false,
  splitting: true,
  treeshake: true,
  clean: true,
  noExternal: [/^harness-world(\/|-|$)/, /^pi-world(\/|$)/],
  tsconfig: 'tsconfig.build.json',
  dts: { entry: { 'instrumentation/otel': 'src/instrumentation/otel.ts', index: 'src/index.ts', 'claude-code': 'src/claude-code.ts', codex: 'src/codex.ts', opencode: 'src/opencode.ts', pi: 'src/pi.ts', 'pi-durable': 'src/pi-durable.ts', fx: 'src/fx.ts', sandbox: 'src/sandbox.ts', next: 'src/next.ts', vercel: 'src/vercel.ts', tools: 'src/tools.ts' } },
  esbuildOptions(options) {
    options.mainFields = ['module', 'main'];
    options.conditions = ['import', 'default'];
  },
  esbuildPlugins: [
    {
      // Sessions' runs are named `workflow//durable-harness@<version>//<harness>`:
      // the kernel imports this package's package.json for the version, and the
      // bundle carries that field alone, as it is when this build runs.
      name: 'version',
      setup(build) {
        build.onLoad({ filter: /[\\/]package\.json$/ }, (args) => (args.path === manifest ? { contents: JSON.stringify({ version }), loader: 'json' } : undefined));
      },
    },
    {
      // The relay bundle the base snapshot installs, built here and shipped in
      // the JavaScript (src/relay-bundle.ts), so a deployed Function never
      // builds it (docs/workspaces.md §5.7).
      name: 'relay',
      setup(build) {
        build.onLoad({ filter: /[\\/]relay-bundle\.ts$/ }, async (args) => (args.path === relayModule ? { contents: await shippedRelay(build.esbuild as unknown as Esbuild), loader: 'js' } : undefined));
        build.onLoad({ filter: /[\\/]relay[\\/]bundle\.ts$/ }, (args) => (args.path === relayBuilder ? { contents: RELAY_BUILDER_STUB, loader: 'js' } : undefined));
      },
    },
    {
      name: 'workspace',
      setup(build) {
        // The workspace packages have no module side effects, but do not say
        // so in their package.json (the demo bundles them too). Without it
        // every entry would pull in all of the kernel and every harness.
        build.onResolve({ filter: /.*/ }, async (args) => {
          if (args.pluginData === 'workspace') return undefined;
          const relative = args.path.startsWith('.') && isSource(args.importer);
          if (!relative && !inWorkspace(args.path)) {
            // Everything else that is bare stays an import (test/dist.test.ts checks each one is declared).
            if (/^[^./]/.test(args.path)) return { path: args.path, external: true };
            return undefined;
          }
          const result = await build.resolve(args.path, { kind: args.kind, importer: args.importer, resolveDir: args.resolveDir, pluginData: 'workspace' });
          if (result.errors.length > 0) return { errors: result.errors };
          return { ...result, sideEffects: !isSource(result.path) };
        });
      },
    },
  ],
});
