import { execFileSync } from 'node:child_process';
import { globSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { harnessFiles, VERCEL_TARGET } from '../src/native.ts';
import { applyDurableHarness } from '../src/next.ts';
import { flowTrigger, fxTrigger, QUEUE_HOLD_FILE, QUEUE_TOPIC, vercelTriggerConsumerGroup } from '../src/routing.ts';
import { queueHoldConsumerGroup } from '../src/queue-hold.ts';
import { unavailableHint } from '../src/hint.ts';
import { defaultProjectDir, harnessFunctions, harnessIncludes, vercelJson } from '../src/vercel.ts';

const CLI = fileURLToPath(new URL('../src/cli.ts', import.meta.url));

type Tree = Record<string, string | { link: string }>;

/** A project with these files (`{ link }`: a symlink, as pnpm makes them). */
function project(tree: Tree): string {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'durable-harness-vercel-')));
  writeFileSync(join(dir, 'package.json'), '{"name":"app","private":true,"type":"module"}');
  for (const [path, body] of Object.entries(tree)) {
    mkdirSync(dirname(join(dir, path)), { recursive: true });
    if (typeof body === 'string') writeFileSync(join(dir, path), body);
    else symlinkSync(body.link, join(dir, path));
  }
  return dir;
}

const pkg = (name: string, extra: object = {}) => JSON.stringify({ name, version: '1.0.0', main: 'index.js', ...extra });

/** The files the include glob matches in `dir`, sorted. */
const matched = (dir: string, glob: string) =>
  globSync(glob, { cwd: dir })
    .filter((f) => statSync(join(dir, f)).isFile())
    .sort();

const CLAUDE_SDK = { optionalDependencies: { '@anthropic-ai/claude-agent-sdk-linux-x64': '0.3.289', '@anthropic-ai/claude-agent-sdk-darwin-arm64': '0.3.289' } };
const CODEX = { optionalDependencies: { '@openai/codex-linux-x64': 'npm:@openai/codex@0.160.0-linux-x64', '@openai/codex-darwin-arm64': 'npm:@openai/codex@0.160.0-darwin-arm64' } };
const OPENCODE_CORE = { dependencies: { 'tree-sitter-bash': '0.25.0', 'tree-sitter-powershell': '0.25.10', 'web-tree-sitter': '0.25.10' } };
const BIN = 'vendor/x86_64-unknown-linux-musl/bin/codex';

/** An npm (or yarn) install on Vercel's build machine: everything hoisted, the linux packages installed. */
const NPM_LINUX: Tree = {
  'node_modules/@anthropic-ai/claude-agent-sdk/package.json': pkg('@anthropic-ai/claude-agent-sdk', CLAUDE_SDK),
  'node_modules/@anthropic-ai/claude-agent-sdk/index.js': '',
  'node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/package.json': pkg('@anthropic-ai/claude-agent-sdk-linux-x64'),
  'node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude': 'ELF',
  'node_modules/@openai/codex/package.json': pkg('@openai/codex', CODEX),
  'node_modules/@openai/codex-linux-x64/package.json': pkg('@openai/codex'),
  [`node_modules/@openai/codex-linux-x64/${BIN}`]: 'ELF',
  'node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/codex-path/rg': 'not needed',
  'node_modules/@opencode/core/package.json': pkg('@opencode/core', OPENCODE_CORE),
  'node_modules/tree-sitter-bash/package.json': pkg('tree-sitter-bash'),
  'node_modules/tree-sitter-bash/tree-sitter-bash.wasm': 'wasm',
  'node_modules/tree-sitter-bash/src/parser.c': 'not needed',
  'node_modules/tree-sitter-powershell/package.json': pkg('tree-sitter-powershell'),
  'node_modules/tree-sitter-powershell/tree-sitter-powershell.wasm': 'wasm',
  'node_modules/web-tree-sitter/package.json': pkg('web-tree-sitter'),
  'node_modules/web-tree-sitter/tree-sitter.wasm': 'wasm',
};

/** What a Function needs of those: OpenCode's grammars. Claude Code's and Codex's binaries never (their CLIs run in the session's sandbox). */
const NPM_FILES = ['node_modules/tree-sitter-bash/tree-sitter-bash.wasm', 'node_modules/tree-sitter-powershell/tree-sitter-powershell.wasm', 'node_modules/web-tree-sitter/tree-sitter.wasm'];
/** The grammars' glob in a hoisted node_modules. */
const GRAMMARS = 'node_modules/{web-tree-sitter,tree-sitter-{bash,powershell}}/*.wasm';
/** A chat function's `includeFiles` at build (`harnessFunctions`): its harnesses' files and the consumer group held wakes are claimed for. */
const withHold = (glob?: string) => (glob ? `{${glob},${QUEUE_HOLD_FILE}}` : QUEUE_HOLD_FILE);
/** Any Claude Code or Codex binary, or its platform package. */
const BINARY = /claude-agent-sdk-|codex|\/claude$/;

describe('the run-time files of each installed harness', () => {
  it('npm on the machine that builds: OpenCode\'s grammars only; the Claude Code and Codex binaries installed next to them are not taken', () => {
    const dir = project(NPM_LINUX);
    const found = harnessFiles(dir, VERCEL_TARGET);
    expect(found.map((h) => h.harness)).toEqual(['opencode']);
    expect(found.flatMap((h) => h.files).map((f) => relative(dir, f)).sort()).toEqual(NPM_FILES);
    const glob = harnessIncludes({ projectDir: dir })!;
    expect(glob).toBe(GRAMMARS);
    expect(matched(dir, glob)).toEqual(NPM_FILES);
    expect(matched(dir, glob).filter((f) => BINARY.test(f))).toEqual([]);
  });

  it('Claude Code and Codex alone: nothing to include, on Linux or a Mac', () => {
    const cli = (platform: string) =>
      project({
        'node_modules/@anthropic-ai/claude-agent-sdk/package.json': pkg('@anthropic-ai/claude-agent-sdk', CLAUDE_SDK),
        [`node_modules/@anthropic-ai/claude-agent-sdk-${platform}/package.json`]: pkg(`@anthropic-ai/claude-agent-sdk-${platform}`),
        [`node_modules/@anthropic-ai/claude-agent-sdk-${platform}/claude`]: 'binary',
        'node_modules/@openai/codex/package.json': pkg('@openai/codex', CODEX),
        [`node_modules/@openai/codex-${platform}/package.json`]: pkg('@openai/codex'),
        [`node_modules/@openai/codex-${platform}/vendor/x/bin/codex`]: 'binary',
      });
    for (const platform of ['linux-x64', 'darwin-arm64']) {
      const dir = cli(platform);
      expect(harnessFiles(dir, VERCEL_TARGET)).toEqual([]);
      expect(harnessIncludes({ projectDir: dir })).toBeUndefined();
    }
  });

  it('pnpm: the grammars by the link @opencode/core resolves them through and by the store; never a binary from the store', () => {
    const store = 'node_modules/.pnpm';
    const sdk = `${store}/@anthropic-ai+claude-agent-sdk@0.3.289_zod@4.5.4/node_modules/@anthropic-ai/claude-agent-sdk`;
    const claude = `${store}/@anthropic-ai+claude-agent-sdk-linux-x64@0.3.289/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64`;
    const launcher = `${store}/@openai+codex@0.160.0/node_modules/@openai/codex`;
    const codex = `${store}/@openai+codex@0.160.0-linux-x64/node_modules/@openai/codex`;
    const core = `${store}/@opencode+core@2.0.22_effect@4.0.0/node_modules/@opencode/core`;
    const bash = `${store}/tree-sitter-bash@0.25.0/node_modules/tree-sitter-bash`;
    const link = (from: string, to: string) => ({ link: relative(dirname(from), to) });
    const linked = (path: string, to: string) => ({ [path]: link(path, to) });
    const dir = project({
      [`${sdk}/package.json`]: pkg('@anthropic-ai/claude-agent-sdk', CLAUDE_SDK),
      [`${sdk}/index.js`]: '',
      [`${claude}/package.json`]: pkg('@anthropic-ai/claude-agent-sdk-linux-x64'),
      [`${claude}/claude`]: 'ELF',
      ...linked('node_modules/@anthropic-ai/claude-agent-sdk', sdk),
      ...linked(`${store}/@anthropic-ai+claude-agent-sdk@0.3.289_zod@4.5.4/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64`, claude),
      [`${launcher}/package.json`]: pkg('@openai/codex', CODEX),
      [`${codex}/package.json`]: pkg('@openai/codex'),
      [`${codex}/${BIN}`]: 'ELF',
      ...linked('node_modules/@openai/codex', launcher),
      ...linked(`${store}/@openai+codex@0.160.0/node_modules/@openai/codex-linux-x64`, codex),
      [`${core}/package.json`]: pkg('@opencode/core', { dependencies: { 'tree-sitter-bash': '0.25.0' } }),
      [`${bash}/package.json`]: pkg('tree-sitter-bash'),
      [`${bash}/tree-sitter-bash.wasm`]: 'wasm',
      ...linked('node_modules/@opencode/core', core),
      ...linked(`${store}/@opencode+core@2.0.22_effect@4.0.0/node_modules/tree-sitter-bash`, bash),
    });
    const found = harnessFiles(dir, VERCEL_TARGET).flatMap((h) => h.files).map((f) => relative(dir, f));
    const viaLink = `${store}/@opencode+core@2.0.22_effect@4.0.0/node_modules/tree-sitter-bash/tree-sitter-bash.wasm`;
    // What withDurableHarness traces: a grammar by both of its paths.
    expect(found.sort()).toEqual([viaLink, `${bash}/tree-sitter-bash.wasm`].sort());
    const glob = harnessIncludes({ projectDir: dir })!;
    expect(glob).toBe('node_modules/.pnpm/@opencode+core@*/node_modules/{web-tree-sitter,tree-sitter-{bash,powershell}}/*.wasm');
    // What a Function gets: each file once, by the path the harness takes.
    expect(matched(dir, glob)).toEqual([viaLink]);
  });

  it('yarn or npm with a conflict: a package nested under the one that needs it', () => {
    const dir = project({
      'node_modules/@opencode/core/package.json': pkg('@opencode/core', OPENCODE_CORE),
      'node_modules/@opencode/core/node_modules/tree-sitter-bash/package.json': pkg('tree-sitter-bash'),
      'node_modules/@opencode/core/node_modules/tree-sitter-bash/tree-sitter-bash.wasm': 'wasm',
    });
    const [opencode] = harnessFiles(dir, VERCEL_TARGET);
    expect(opencode!.files.map((f) => relative(dir, f))).toEqual(['node_modules/@opencode/core/node_modules/tree-sitter-bash/tree-sitter-bash.wasm']);
    expect(matched(dir, harnessIncludes({ projectDir: dir })!)).toEqual(['node_modules/@opencode/core/node_modules/tree-sitter-bash/tree-sitter-bash.wasm']);
  });

  it('nothing to include for Pi', () => {
    const dir = project({ 'node_modules/@earendil-works/pi-ai/package.json': pkg('@earendil-works/pi-ai'), 'node_modules/@earendil-works/pi-ai/index.js': '' });
    expect(harnessFiles(dir, VERCEL_TARGET)).toEqual([]);
    expect(harnessIncludes({ projectDir: dir })).toBeUndefined();
  });
});

describe('harnessFunctions', () => {
  const app: Tree = {
    ...NPM_LINUX,
    'lib/agent.ts': "import { opencode } from 'durable-harness/opencode'\nexport const agent = opencode({ model: 'anthropic/claude-haiku-4.5' })\n",
    // Claude Code and Codex: their CLIs run in the session's sandbox, so their chat functions need no files.
    'lib/claude.ts': "import { claudeCode } from 'durable-harness/claude-code'\nimport { codex } from 'durable-harness/codex'\nexport const agent = claudeCode({ model: 'anthropic/claude-haiku-4.5' })\nexport const coder = codex({ model: 'openai/gpt-5.4-mini' })\n",
    'api/claude.ts': "import { agent } from '../lib/claude.js'\nexport const { GET, POST } = agent.chatRoute()\n",
    'api/chat.ts': "import { agent } from '../lib/agent.js'\nexport const { GET, POST } = agent.chatRoute()\n",
    'api/support/chat.ts': "import { agent } from '../../lib/agent.js'\nexport const { GET, POST } = agent.chatRoute({ authorize })\n",
    'lib/pi.ts': "import { pi } from 'durable-harness/pi'\nexport const agent = pi({ model: 'anthropic/claude-haiku-4.5' })\n",
    'api/pi.ts': "import { agent } from '../lib/pi.js'\nexport const { GET, POST } = agent.chatRoute()\n",
    'api/aliased.ts': "import { agent } from '@/lib/agent'\nexport const { GET, POST } = agent.chatRoute()\n",
    'api/flow.ts': "import '../lib/agent.js'\nimport { flowHandler } from 'durable-harness'\nexport const POST = flowHandler()\n",
    'api/health.ts': 'export const GET = () => new Response("ok")\n',
    // Not Functions on Vercel (a `functions` entry for one fails the deploy).
    'api/_lib/routes.ts': "import { agent } from '../../lib/agent.js'\nexport const routes = agent.chatRoute()\n",
    'api/legacy.cjs': "const { agent } = require('../lib/agent.js')\nmodule.exports = agent.chatRoute()\n",
  };

  it('the flow function: max duration, the queue trigger without a consumer, the harness files; each chat function the files its harnesses need', () => {
    const dir = project(app);
    const functions = harnessFunctions({ projectDir: dir });
    expect(Object.keys(functions)).toEqual(['api/flow.ts', 'api/aliased.ts', 'api/chat.ts', 'api/claude.ts', 'api/pi.ts', 'api/support/chat.ts']);
    const include = harnessIncludes({ projectDir: dir })!;
    expect(functions['api/flow.ts']).toEqual({
      maxDuration: 'max',
      experimentalTriggers: [{ type: 'queue/v2beta', topic: '__harness_wkf_workflow_*', retryAfterSeconds: 5, initialDelaySeconds: 0 }],
      includeFiles: include,
    });
    expect(JSON.stringify(functions)).not.toContain('consumer');
    // A chat function gets the files of the harnesses its imports reach: OpenCode's here, and no binary anywhere.
    const grammars = harnessFunctions({ projectDir: dir, chat: 'api/chat.ts' })['api/chat.ts']!.includeFiles!;
    expect(functions['api/chat.ts']).toEqual({ includeFiles: grammars });
    expect(matched(dir, grammars)).toEqual([QUEUE_HOLD_FILE, ...NPM_FILES]);
    expect(functions['api/support/chat.ts']).toEqual({ includeFiles: grammars });
    expect(matched(dir, include).filter((f) => BINARY.test(f))).toEqual([]);
    // Pi, Claude Code and Codex need no harness files, only the consumer group; an import the scan cannot follow (a path alias) gets every harness's.
    expect(functions['api/pi.ts']).toEqual({ includeFiles: QUEUE_HOLD_FILE });
    expect(functions['api/claude.ts']).toEqual({ includeFiles: QUEUE_HOLD_FILE });
    expect(functions['api/aliased.ts']).toEqual({ includeFiles: withHold(include) });
  });

  it('a chat function that imports only /fx or /sandbox gets no harness files', () => {
    const dir = project({
      ...app,
      'lib/fx.ts': "import { fxTools } from 'durable-harness/fx'\nimport { sandbox } from 'durable-harness/sandbox'\nexport const tools = fxTools(sandbox())\n",
      'api/fx.ts': "import { tools } from '../lib/fx.js'\nexport const { GET, POST } = agentOf(tools).chatRoute()\n",
    });
    const functions = harnessFunctions({ projectDir: dir });
    expect(functions['api/fx.ts']).toEqual({ includeFiles: QUEUE_HOLD_FILE });
  });

  it('takes the chat functions the app names, and more files', () => {
    const dir = project({ ...app, 'api/queue.ts': app['api/flow.ts']! });
    const functions = harnessFunctions({ projectDir: dir, chat: 'api/chat.ts', flow: 'api/queue.ts', includeFiles: ['data/**'] });
    expect(Object.keys(functions)).toEqual(['api/queue.ts', 'api/chat.ts']);
    expect(functions['api/chat.ts']!.includeFiles).toMatch(/,data\/\*\*,\.durable-harness\/queue-hold\.json}$/);
  });

  it('a file it is given that is not there is an error, not a function without its files', () => {
    const dir = project(app);
    expect(() => harnessFunctions({ projectDir: dir, flow: 'api/queue.ts' })).toThrow(/no flow function api\/queue\.ts/);
    expect(() => harnessFunctions({ projectDir: dir, chat: 'api/chat.js' })).toThrow(/no chat function api\/chat\.js/);
    const js = project({ ...NPM_LINUX, 'api/flow.js': app['api/flow.ts']! });
    expect(Object.keys(harnessFunctions({ projectDir: js }))).toEqual(['api/flow.js']);
  });

  it('a chat function whose imports it cannot follow gets every harness, even next to one it knows', () => {
    const dir = project({
      ...app,
      'tsconfig.json': JSON.stringify({ compilerOptions: { paths: { '@/*': ['./*'] } } }),
      'node_modules/@acme/agents': { link: '../../packages/agents' },
      'packages/agents/package.json': pkg('@acme/agents'),
      'packages/agents/index.js': "export { codex } from 'durable-harness/codex'\n",
      // Pi here, and OpenCode through a path alias the tsconfig resolves.
      'api/mixed.ts': "import { pi } from 'durable-harness/pi'\nimport { agent } from '@/lib/agent'\nimport type { Tool } from '~/types'\nexport const { GET, POST } = agent.chatRoute()\n",
      // Pi here, and an agent from a workspace package the scan does not follow.
      'api/workspace.ts': "import { pi } from 'durable-harness/pi'\nimport { agents } from '@acme/agents'\nexport const { GET, POST } = agents.chatRoute()\n",
      // Pi here, and an agent module chosen at run time.
      'api/dynamic.ts': "import { pi } from 'durable-harness/pi'\nconst { agent } = await import(`../lib/${process.env.AGENT}.js`)\nexport const { GET, POST } = agent.chatRoute()\n",
    });
    const functions = harnessFunctions({ projectDir: dir });
    const all = harnessIncludes({ projectDir: dir });
    expect(matched(dir, functions['api/mixed.ts']!.includeFiles!)).toEqual([QUEUE_HOLD_FILE, ...NPM_FILES]);
    expect(functions['api/workspace.ts']).toEqual({ includeFiles: withHold(all) });
    expect(functions['api/dynamic.ts']).toEqual({ includeFiles: withHold(all) });
    // A tsconfig alias it resolves narrows it again.
    expect(functions['api/aliased.ts']).toEqual({ includeFiles: functions['api/chat.ts']!.includeFiles });
  });

  it('in a monorepo: the project is where the CLI loads vercel.ts from, and the glob reaches the root node_modules', () => {
    const root = project({
      'node_modules/@opencode/core/package.json': pkg('@opencode/core', OPENCODE_CORE),
      'node_modules/tree-sitter-bash/package.json': pkg('tree-sitter-bash'),
      'node_modules/tree-sitter-bash/tree-sitter-bash.wasm': 'wasm',
      'apps/web/package.json': pkg('web'),
      'apps/web/lib/agent.ts': app['lib/agent.ts']!,
      'apps/web/api/chat.ts': app['api/chat.ts']!,
      'apps/web/api/flow.ts': app['api/flow.ts']!,
    });
    const web = join(root, 'apps/web');
    // `vercel build` at the repository's root, the project its Root Directory.
    expect(defaultProjectDir(['node', join(web, '.vercel/vercel-loader.mjs'), join(web, '.vercel/vercel-temp.mjs')], root)).toBe(web);
    expect(defaultProjectDir(['node', '/usr/local/bin/durable-harness', 'vercel-json'], web)).toBe(web);
    const functions = harnessFunctions({ projectDir: web });
    expect(functions['api/chat.ts']!.includeFiles).toBe(withHold(`../../${GRAMMARS}`));
    expect(matched(web, functions['api/flow.ts']!.includeFiles!)).toEqual(['../../node_modules/tree-sitter-bash/tree-sitter-bash.wasm']);
  });

  it('records the flow function\'s consumer group, as Vercel names it after its functions key, for the chat functions\' held wakes', () => {
    const dir = project(app);
    harnessFunctions({ projectDir: dir });
    expect(JSON.parse(readFileSync(join(dir, QUEUE_HOLD_FILE), 'utf8'))).toEqual({ consumerGroup: 'api_Sflow_Dts' });
    expect(queueHoldConsumerGroup({}, dir)).toBe('api_Sflow_Dts');
    const named = project({ ...app, 'api/my_fn.ts': app['api/flow.ts']! });
    harnessFunctions({ projectDir: named, flow: 'api/my_fn.ts' });
    expect(queueHoldConsumerGroup({}, named)).toBe('api_Smy__fn_Dts');
  });

  it('a vercel.json written ahead of time (`vercel-json`) records nothing: nothing runs at build to give it, so nothing is held', () => {
    const dir = project(app);
    const { functions } = vercelJson({ projectDir: dir });
    expect(JSON.stringify(functions)).not.toContain(QUEUE_HOLD_FILE);
    expect(queueHoldConsumerGroup({}, dir)).toBeUndefined();
  });

  it("is durable-harness's own topic, apart from the Workflow SDK's", () => {
    expect(flowTrigger().topic).toBe(QUEUE_TOPIC);
    expect(QUEUE_TOPIC).toBe('__harness_wkf_workflow_*');
  });

  it('`durable-harness vercel-json` prints it, and --write merges it into vercel.json', () => {
    const dir = project({ ...app, 'vercel.json': JSON.stringify({ regions: ['iad1'], functions: { 'api/health.ts': { maxDuration: 5 }, 'api/flow.ts': { memory: 3009 } } }) });
    const printed = JSON.parse(execFileSync(process.execPath, [CLI, 'vercel-json', '--chat', 'api/chat.ts'], { cwd: dir, encoding: 'utf8' }));
    expect(printed).toEqual(vercelJson({ projectDir: dir, chat: 'api/chat.ts' }));
    execFileSync(process.execPath, [CLI, 'vercel-json', '--write'], { cwd: dir, encoding: 'utf8' });
    const written = JSON.parse(readFileSync(join(dir, 'vercel.json'), 'utf8'));
    expect(written.regions).toEqual(['iad1']);
    expect(Object.keys(written.functions)).toEqual(['api/health.ts', 'api/flow.ts', 'api/aliased.ts', 'api/chat.ts', 'api/support/chat.ts']);
    // A function's own settings stay.
    expect(written.functions['api/flow.ts']).toEqual({ memory: 3009, ...printed.functions['api/flow.ts'] });
  });
});

describe('harnessFunctions, a Next.js app', () => {
  const FLOW = 'app/.well-known/durable-harness/v1/flow/route.js';
  const FX = 'app/.well-known/durable-harness/v1/fx/route.js';
  const next: Tree = {
    'lib/agent.ts': "import { claudeCode } from 'durable-harness/claude-code'\nexport const agent = claudeCode({ model: 'anthropic/claude-haiku-4.5' })\n",
    'app/api/chat/route.ts': "import { agent } from '@/lib/agent'\nexport const { GET, POST } = agent.chatRoute()\n",
    'app/page.tsx': 'export default function Page() { return null }\n',
  };
  /** The Workflow SDK in the app (withWorkflow). */
  const workflow: Tree = { 'package.json': JSON.stringify({ name: 'app', private: true, dependencies: { workflow: '5.2.1' } }) };
  const fx: Tree = {
    'node_modules/libfx/package.json': JSON.stringify({ name: 'libfx', type: 'module', exports: { '.': './index.js' } }),
    'node_modules/libfx/index.js': 'export const createFxAgent = (o) => o\n',
    'lib/fx.ts': "import { createFxAgent } from 'libfx'\nexport const agent = createFxAgent({})\n",
  };

  it('without the Workflow SDK: nothing (the route is at the Workflow SDK\'s path, its triggers in config.json); the CLI says so and writes no vercel.json', () => {
    expect(harnessFunctions({ projectDir: project({ ...NPM_LINUX, ...next }) })).toEqual({});
    expect(harnessFunctions({ projectDir: project({ ...next, ...fx }) })).toEqual({});
    const dir = project(next);
    const out = execFileSync(process.execPath, [CLI, 'vercel-json', '--write'], { cwd: dir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
    expect(out).toBe('');
    expect(() => readFileSync(join(dir, 'vercel.json'), 'utf8')).toThrow();
  });

  it('with the Workflow SDK: the routes withDurableHarness writes at its own path, each with its queue trigger and no files (withDurableHarness traces them)', () => {
    expect(harnessFunctions({ projectDir: project({ ...NPM_LINUX, ...next, ...workflow }) })).toEqual({ [FLOW]: { maxDuration: 'max', experimentalTriggers: [flowTrigger()] } });
    expect(harnessFunctions({ projectDir: project({ ...next, ...fx, ...workflow }) })).toEqual({
      [FLOW]: { maxDuration: 'max', experimentalTriggers: [flowTrigger()] },
      [FX]: { maxDuration: 'max', experimentalTriggers: [fxTrigger()] },
    });
    // fx alone: the fx route alone.
    expect(Object.keys(harnessFunctions({ projectDir: project({ ...fx, ...workflow, 'app/page.tsx': next['app/page.tsx']! }) }))).toEqual([FX]);
    // A src/ app: the keys under src/app.
    const src = project({ ...workflow, 'src/lib/agent.ts': next['lib/agent.ts']!, 'src/app/page.tsx': next['app/page.tsx']! });
    expect(Object.keys(harnessFunctions({ projectDir: src }))).toEqual(['src/app/.well-known/durable-harness/v1/flow/route.js']);
  });

  it('keys an app’s own route there by its own file, and refuses an app with no agent', () => {
    const own = project({ ...next, ...workflow, 'app/.well-known/durable-harness/v1/flow/route.ts': "export { POST } from '@/lib/flow'\n" });
    expect(Object.keys(harnessFunctions({ projectDir: own }))).toEqual(['app/.well-known/durable-harness/v1/flow/route.ts']);
    expect(() => harnessFunctions({ projectDir: project({ ...workflow, 'app/page.tsx': next['app/page.tsx']! }) })).toThrow(/makes no agent/);
  });

  it('what it writes passes withDurableHarness’s trigger check on Vercel', () => {
    const dir = project({ ...next, ...workflow });
    execFileSync(process.execPath, [CLI, 'vercel-json', '--write'], { cwd: dir, encoding: 'utf8' });
    expect(JSON.parse(readFileSync(join(dir, 'vercel.json'), 'utf8')).functions).toEqual({ [FLOW]: { maxDuration: 'max', experimentalTriggers: [flowTrigger()] } });
    const saved = { VERCEL: process.env.VERCEL, VERCEL_DEPLOYMENT_ID: process.env.VERCEL_DEPLOYMENT_ID };
    process.env.VERCEL = '1';
    process.env.VERCEL_DEPLOYMENT_ID = 'dpl_test';
    try {
      expect(() => applyDurableHarness({}, 'phase-production-build', dir)).not.toThrow();
      expect(() => applyDurableHarness({}, 'phase-production-build', project({ ...next, ...workflow }))).toThrow(/has no queue trigger/);
    } finally {
      for (const [k, v] of Object.entries(saved)) {
        if (v === undefined) delete process.env[k];
        else process.env[k] = v;
      }
    }
  });

  it('re-running --write moves an entry from the old topic to the new one', () => {
    const dir = project({ ...next, ...workflow, 'vercel.json': JSON.stringify({ functions: { [FLOW]: { memory: 3009, maxDuration: 'max', experimentalTriggers: [{ type: 'queue/v2beta', topic: '__wkf_workflow_*' }] } } }) });
    execFileSync(process.execPath, [CLI, 'vercel-json', '--write'], { cwd: dir, encoding: 'utf8' });
    expect(JSON.parse(readFileSync(join(dir, 'vercel.json'), 'utf8')).functions[FLOW]).toEqual({ memory: 3009, maxDuration: 'max', experimentalTriggers: [flowTrigger()] });
  });
});

describe('withDurableHarness, OpenCode', () => {
  it('traces the tree-sitter grammars into the routes that drive sessions (webpack does not find them)', () => {
    const dir = project({
      ...NPM_LINUX,
      'node_modules/@opencode/server/package.json': pkg('@opencode/server'),
      'node_modules/@opencode/server/index.js': '',
      'lib/agent.ts': "import { opencode } from 'durable-harness/opencode'\nexport const agent = opencode({ model: 'anthropic/claude-haiku-4.5' })\n",
      'app/api/chat/route.ts': "import { agent } from '@/lib/agent'\nexport const { GET, POST } = agent.chatRoute()\n",
    });
    const config = applyDurableHarness({}, 'phase-production-server', dir);
    for (const route of ['/.well-known/workflow/v1/flow', '/api/chat']) {
      expect(config.outputFileTracingIncludes?.[route]).toEqual(
        expect.arrayContaining(['./node_modules/tree-sitter-bash/tree-sitter-bash.wasm', './node_modules/web-tree-sitter/tree-sitter.wasm'])
      );
    }
    expect(config.serverExternalPackages).toContain('@opencode/server');
  });
});

describe('the hint for a harness that cannot run', () => {
  it('a package that is not installed says to install it; a file a Function lacks, on Vercel, says to include it', () => {
    const vercel = process.env.VERCEL;
    process.env.VERCEL = '1';
    try {
      expect(unavailableHint({ code: 'module_missing', message: "Cannot find package '@anthropic-ai/claude-agent-sdk' imported from /var/task/x.js" }, 'claude-code')).toMatch(/^@anthropic-ai\/claude-agent-sdk is not installed/);
      expect(unavailableHint({ code: 'module_missing', message: "the OpenCode host exited: Error: Cannot find module 'tree-sitter-bash/tree-sitter-bash.wasm'" }, 'opencode')).toMatch(/does not include opencode's run-time files.*harnessFunctions\(\)/);
      // Claude Code's and Codex's binaries are the sandbox image's, never a Function's: no includeFiles.
      for (const harness of ['claude-code', 'codex']) {
        const hint = unavailableHint({ code: 'binary_missing', message: 'Native CLI binary for linux-x64 not found' }, harness)!;
        expect(hint).toMatch(/runs in the session's Vercel Sandbox.*sandbox prepare/);
        expect(hint).not.toMatch(/harnessFunctions|includeFiles/);
      }
    } finally {
      if (vercel === undefined) delete process.env.VERCEL;
      else process.env.VERCEL = vercel;
    }
  });
});

describe('vercelTriggerConsumerGroup: the consumer group Vercel gives a vercel.json / vercel.ts trigger', () => {
  it.each([
    ['app/api/libfx/route.ts', undefined, 'app_Sapi_Slibfx_Sroute_Dts'],
    ['app/.well-known/durable-harness/v1/flow/route.ts', undefined, 'app_S_Dwell-known_Sdurable-harness_Sv1_Sflow_Sroute_Dts'],
    ['api/my_fn.ts', undefined, 'api_Smy__fn_Dts'],
    ['api/flow.ts', 'web', 'web_7Eapi_Sflow_Dts'],
    ['api/**/*.ts', undefined, 'api_S_2A_2A_S_2A_Dts'],
  ])('%s (service %s) is %s', (pattern, service, group) => {
    expect(vercelTriggerConsumerGroup(pattern, service)).toBe(group);
    expect(group).toMatch(/^[A-Za-z0-9_-]+$/);
  });
});
