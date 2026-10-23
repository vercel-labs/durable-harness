import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { applyDurableHarness } from '../src/next.ts';
import { FLOW_ROUTE, flowFunctionConfig, flowTrigger, FX_ROUTE, fxTrigger, LIBFX_QUEUE_PREFIX, LIBFX_TOPIC, WORKFLOW_FLOW_ROUTE } from '../src/routing.ts';
import { fxAgentNames, routesByName, workflowSdkIn } from '../src/scan.ts';

/** The flow route at the Workflow SDK's path (an app without the Workflow SDK), and its config.json. */
const FLOW = 'app/.well-known/workflow/v1/flow/route.js';
const CONFIG = 'app/.well-known/workflow/v1/config.json';
/** durable-harness's own routes (an app that also uses the Workflow SDK). */
const OWN_FLOW = 'app/.well-known/durable-harness/v1/flow/route.js';
const OWN_FX = 'app/.well-known/durable-harness/v1/fx/route.js';
/** The own routes' `functions` entries, as `npx durable-harness vercel-json` writes them. */
const FLOW_ENTRY = { maxDuration: 'max', experimentalTriggers: [flowTrigger()] };
const FX_ENTRY = { maxDuration: 'max', experimentalTriggers: [fxTrigger()] };
const HARNESS_TRIGGER = { ...flowTrigger(), consumer: 'default' };
const LIBFX_TRIGGER = { type: 'queue/v2beta', topic: '__libfx_wkf_workflow_session', consumer: 'default' };

const ENV = ['VERCEL', 'VERCEL_DEPLOYMENT_ID', 'WORKFLOW_TARGET_WORLD', 'DURABLE_HARNESS_SKIP_TRIGGER_CHECK'];
const saved = Object.fromEntries(ENV.map((key) => [key, process.env[key]]));
beforeEach(() => {
  for (const key of ENV) delete process.env[key];
});
afterEach(() => {
  for (const key of ENV) {
    if (saved[key] === undefined) delete process.env[key];
    else process.env[key] = saved[key];
  }
});

function project(files: Record<string, string>): string {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'durable-harness-next-')));
  writeFileSync(join(dir, 'package.json'), '{"name":"app","private":true}');
  for (const [path, text] of Object.entries(files)) {
    mkdirSync(join(dir, path, '..'), { recursive: true });
    writeFileSync(join(dir, path), text);
  }
  return dir;
}

/** The Workflow SDK installed (`withWorkflow` comes from `workflow/next`). */
const WORKFLOW_SDK = {
  'node_modules/workflow/package.json': JSON.stringify({ name: 'workflow', exports: { './next': './next.js' } }),
  'node_modules/workflow/next.js': '',
};

const QUICK_START = {
  'lib/agent.ts': "import { claudeCode } from 'durable-harness/claude-code'\nexport const agent = claudeCode({ model: 'anthropic/claude-sonnet-4.5' })\n",
  'app/api/chat/route.ts': "import { agent } from '@/lib/agent'\nexport const { GET, POST } = agent.chatRoute()\n",
  'app/page.tsx': 'export default function Page() { return null }\n',
};

/** Stand-ins: libfx's createFxAgent (its wakeHandler answers which agent and topic took a delivery), and durable-harness's flowHandler. */
const STUBS = {
  'app/page.tsx': 'export default function Page() { return null }\n',
  'node_modules/libfx/package.json': JSON.stringify({ name: 'libfx', type: 'module', exports: { '.': './index.js' } }),
  'node_modules/libfx/index.js':
    "export function createFxAgent(options = {}) {\n  return { wakeHandler: () => async (request) => new Response(`fx ${options.name ?? ''}: ${request.headers.get('ce-vqsqueuename')}`) };\n}\n",
  'node_modules/durable-harness/package.json': JSON.stringify({ name: 'durable-harness', type: 'module', exports: { '.': './index.js' } }),
  'node_modules/durable-harness/index.js':
    "export const durableAgent = (options) => options;\nexport const flowHandler = (options = {}) => async (request) => new Response(`flow ${options.route}: ${request.headers.get('ce-vqsqueuename')}`);\n",
};

const FX_APP = {
  ...STUBS,
  'lib/agent.ts': "import { createFxAgent } from 'libfx'\nimport { tools } from './tools'\n\nexport const agent = createFxAgent({ name: 'main', instructions: 'Keep answers short.', tools })\n",
  'lib/tools.ts': 'export const tools = {}\n',
  'app/api/chat/route.ts': "import { agent } from '@/lib/agent'\nexport async function POST(request: Request) {\n  return new Response(agent.session().prompt(await request.text()).readable)\n}\n",
};

const MINE = { 'lib/mine.ts': "import { durableAgent } from 'durable-harness'\nexport const mine = durableAgent({ name: 'mine' })\n" };

/** A queue delivery on `topic` (Vercel Queues' v2beta push names the topic in `ce-vqsqueuename`; a World delivering over HTTP itself, in `x-vqs-queue-name`). */
const delivery = (topic?: string, header = 'ce-vqsqueuename') => new Request('http://localhost/.well-known/workflow/v1/flow', { method: 'POST', headers: topic ? { [header]: topic } : {} });

type Route = { POST: (request: Request) => Promise<Response>; GET: (request: Request) => Response; HEAD: (request: Request) => Response };

/** A generated route, loaded as Next would load it. */
async function routeOf(dir: string, file = FLOW): Promise<Route> {
  return import(/* @vite-ignore */ join(dir, file));
}

function warnings(run: () => void): string {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
  try {
    run();
    return warn.mock.calls.flat().join('\n');
  } finally {
    warn.mockRestore();
  }
}

const json = (dir: string, file: string) => JSON.parse(readFileSync(join(dir, file), 'utf8'));

describe('withDurableHarness, in an app without the Workflow SDK: zero config', () => {
  it("writes the flow route at the Workflow SDK's path, and its trigger on durable-harness's own topic in config.json", () => {
    const dir = project(QUICK_START);
    const config = applyDurableHarness({}, 'phase-production-build', dir);
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain(`import "../../../../../lib/agent";`);
    expect(route).toContain("import { flowHandler } from 'durable-harness';");
    expect(route).toContain(`const flow = flowHandler({ route: "${WORKFLOW_FLOW_ROUTE}" });`);
    expect(route).toContain('__harness_wkf_workflow_*');
    expect(json(dir, CONFIG)).toEqual(flowFunctionConfig());
    expect(json(dir, CONFIG).workflows.experimentalTriggers).toEqual([{ type: 'queue/v2beta', topic: '__harness_wkf_workflow_*', retryAfterSeconds: 5, initialDelaySeconds: 0, consumer: 'default' }]);
    expect(readFileSync(join(dir, 'app/.well-known/workflow/v1/.gitignore'), 'utf8')).toBe('*\n');
    expect(existsSync(join(dir, 'app/.well-known/durable-harness'))).toBe(false);
    expect(config.serverExternalPackages).toContain('durable-harness');
  });

  it("traces no Claude Code or Codex binary (their CLIs run in the session's sandbox): the Agent SDK stays external, @openai/codex is not looked for", () => {
    const dir = project({
      ...QUICK_START,
      'lib/codex.ts': "import { codex } from 'durable-harness/codex'\nexport const coder = codex({ model: 'openai/gpt-5.4-mini' })\n",
      'node_modules/@anthropic-ai/claude-agent-sdk/package.json': JSON.stringify({ name: '@anthropic-ai/claude-agent-sdk', main: 'index.js', optionalDependencies: { '@anthropic-ai/claude-agent-sdk-linux-x64': '0.3.296' } }),
      'node_modules/@anthropic-ai/claude-agent-sdk/index.js': '',
      'node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/package.json': JSON.stringify({ name: '@anthropic-ai/claude-agent-sdk-linux-x64' }),
      'node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude': 'ELF',
      'node_modules/@openai/codex/package.json': JSON.stringify({ name: '@openai/codex', main: 'index.js' }),
      'node_modules/@openai/codex/index.js': '',
    });
    const config = applyDurableHarness({}, 'phase-production-build', dir);
    expect(config.outputFileTracingIncludes).toBeUndefined();
    expect(config.serverExternalPackages).toContain('@anthropic-ai/claude-agent-sdk');
    expect(config.serverExternalPackages).not.toContain('@openai/codex');
  });

  it('needs no vercel.json, even building on Vercel', () => {
    process.env.VERCEL = '1';
    process.env.VERCEL_DEPLOYMENT_ID = 'dpl_test';
    const dir = project(QUICK_START);
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
    expect(existsSync(join(dir, FLOW))).toBe(true);
  });

  it("answers the Workflow tools' health probe (HEAD ?__health), as the Workflow SDK's route does", async () => {
    const dir = project({ ...STUBS, ...MINE });
    applyDurableHarness({}, 'phase-production-build', dir);
    const route = await routeOf(dir);
    const probe = new Request(`http://localhost${WORKFLOW_FLOW_ROUTE}?__health`, { method: 'HEAD' });
    expect(route.HEAD(probe).status).toBe(200);
    expect(await route.GET(new Request(`http://localhost${WORKFLOW_FLOW_ROUTE}?__health`)).json()).toMatchObject({ healthy: true, endpoint: WORKFLOW_FLOW_ROUTE });
    expect((await route.POST(new Request(`http://localhost${WORKFLOW_FLOW_ROUTE}?__health`, { method: 'POST' }))).status).toBe(200);
    expect(route.GET(new Request(`http://localhost${WORKFLOW_FLOW_ROUTE}`)).status).toBe(405);
    expect(await (await route.POST(delivery('__harness_wkf_workflow_session'))).text()).toBe(`flow ${WORKFLOW_FLOW_ROUTE}: __harness_wkf_workflow_session`);
  });

  it('writes nothing outside a dev server or a build', () => {
    const dir = project(QUICK_START);
    applyDurableHarness({}, 'phase-production-server', dir);
    expect(existsSync(join(dir, 'app/.well-known'))).toBe(false);
  });

  it('finds an agent made in a src/ app, inline in its route', () => {
    const dir = project({
      'src/app/(chat)/api/chat/route.ts': "import { pi } from 'durable-harness/pi'\nexport const { GET, POST } = pi({ model: 'anthropic/claude-sonnet-4.5' }).chatRoute()\n",
    });
    applyDurableHarness({}, 'phase-development-server', dir);
    expect(readFileSync(join(dir, 'src/app/.well-known/workflow/v1/flow/route.js'), 'utf8')).toContain(`import "../../../../(chat)/api/chat/route";`);
  });

  it('removes its route and config.json once no module makes an agent', () => {
    const dir = project(QUICK_START);
    applyDurableHarness({}, 'phase-production-build', dir);
    writeFileSync(join(dir, 'lib/agent.ts'), 'export const agent = null\n');
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(existsSync(join(dir, FLOW))).toBe(false);
    expect(existsSync(join(dir, 'app/.well-known/workflow'))).toBe(false);
  });

  it('rewrites a config.json from before durable-harness had a queue namespace of its own (`__wkf_workflow_*`)', () => {
    const OLD_CONFIG = `${JSON.stringify({ version: '0', workflows: { maxDuration: 'max', experimentalTriggers: [{ type: 'queue/v2beta', topic: '__wkf_workflow_*', retryAfterSeconds: 5, initialDelaySeconds: 0, consumer: 'default' }] } }, null, 2)}\n`;
    const OLD_ROUTE = "// generated by durable-harness/next (withDurableHarness). The World's queue delivers every\nimport { flowHandler } from 'durable-harness';\nexport const POST = flowHandler();\n";
    const dir = project({ ...QUICK_START, [FLOW]: OLD_ROUTE, [CONFIG]: OLD_CONFIG });
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(json(dir, CONFIG)).toEqual(flowFunctionConfig());
    expect(readFileSync(join(dir, FLOW), 'utf8')).toContain('flowHandler({ route:');
  });

  it("removes the routes an earlier build wrote at durable-harness's own path (the app used the Workflow SDK then)", () => {
    const dir = project({ ...FX_APP, ...MINE, 'package.json': JSON.stringify({ name: 'app', dependencies: { workflow: '5.2.1' } }) });
    warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(existsSync(join(dir, OWN_FLOW))).toBe(true);
    expect(existsSync(join(dir, OWN_FX))).toBe(true);
    // The app drops the Workflow SDK.
    writeFileSync(join(dir, 'package.json'), '{"name":"app"}');
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
    expect(existsSync(join(dir, 'app/.well-known/durable-harness'))).toBe(false);
    expect(existsSync(join(dir, FLOW))).toBe(true);
  });

  it('keeps the user’s config, and traces the harnesses’ files into the flow route at its path', () => {
    const dir = project({
      ...QUICK_START,
      'node_modules/@opencode/core/package.json': JSON.stringify({ name: '@opencode/core', version: '1.0.0', main: 'index.js' }),
      'node_modules/@opencode/core/index.js': '',
      'node_modules/tree-sitter-bash/package.json': JSON.stringify({ name: 'tree-sitter-bash', version: '1.0.0' }),
      'node_modules/tree-sitter-bash/tree-sitter-bash.wasm': 'wasm',
      'lib/opencode.ts': "import { opencode } from 'durable-harness/opencode'\nexport const coder = opencode({ model: 'anthropic/claude-haiku-4.5' })\n",
    });
    const config = applyDurableHarness({ serverExternalPackages: ['sharp'], outputFileTracingIncludes: { '/x': ['./y'] } }, 'phase-production-build', dir);
    expect(config.serverExternalPackages?.slice(0, 2)).toEqual(['sharp', 'durable-harness']);
    expect(config.outputFileTracingIncludes?.['/x']).toEqual(['./y']);
    expect(config.outputFileTracingIncludes?.[WORKFLOW_FLOW_ROUTE]).toEqual(['./node_modules/tree-sitter-bash/tree-sitter-bash.wasm']);
    expect(config.outputFileTracingIncludes?.['/api/chat']).toEqual(['./node_modules/tree-sitter-bash/tree-sitter-bash.wasm']);
  });
});

describe('withDurableHarness, in an app that also uses the Workflow SDK', () => {
  it.each([
    ['workflow in package.json', { 'package.json': JSON.stringify({ name: 'app', dependencies: { workflow: '5.2.1' } }) }, 'workflow is in package.json'],
    ['@workflow/next in package.json', { 'package.json': JSON.stringify({ name: 'app', devDependencies: { '@workflow/next': '5.0.4' } }) }, '@workflow/next is in package.json'],
    ['workflow/next resolving', WORKFLOW_SDK, 'workflow/next resolves from the project'],
    ["a route at the Workflow SDK's path that is not durable-harness's", { 'app/.well-known/workflow/v1/flow/route.js': "import { workflowEntrypoint } from 'workflow/runtime';\n" }, 'app/.well-known/workflow/v1/flow/route.js is not durable-harness'],
  ])('tells it by %s', (_, files, reason) => {
    const dir = project({ ...QUICK_START, ...files });
    expect(workflowSdkIn(dir, join(dir, 'app'))).toContain(reason);
    const without = project(QUICK_START);
    expect(workflowSdkIn(without, join(without, 'app'))).toBeUndefined();
  });

  it("writes the flow route at durable-harness's own path, and never touches the Workflow SDK's files", () => {
    const theirs = "import { workflowEntrypoint } from 'workflow/runtime';\nexport const POST = workflowEntrypoint('');\n";
    const config = '{\n  "version": "0",\n  "workflows": {},\n  "steps": {}\n}';
    const dir = project({
      ...QUICK_START,
      ...WORKFLOW_SDK,
      'app/.well-known/workflow/v1/flow/route.js': theirs,
      'app/.well-known/workflow/v1/config.json': config,
      'app/.well-known/workflow/v1/.gitignore': '*',
    });
    warnings(() => applyDurableHarness({}, 'phase-development-server', dir));
    expect(readFileSync(join(dir, 'app/.well-known/workflow/v1/flow/route.js'), 'utf8')).toBe(theirs);
    expect(readFileSync(join(dir, 'app/.well-known/workflow/v1/config.json'), 'utf8')).toBe(config);
    expect(readFileSync(join(dir, 'app/.well-known/workflow/v1/.gitignore'), 'utf8')).toBe('*');
    const route = readFileSync(join(dir, OWN_FLOW), 'utf8');
    expect(route).toContain(`const flow = flowHandler({ route: "${FLOW_ROUTE}" });`);
    expect(readFileSync(join(dir, 'app/.well-known/durable-harness/.gitignore'), 'utf8')).toBe('*\n');
    expect(existsSync(join(dir, 'app/.well-known/durable-harness/v1/config.json'))).toBe(false);
  });

  it("removes the route and config.json it wrote at the Workflow SDK's path before the app used it, and nothing of the Workflow SDK's", () => {
    const dir = project(QUICK_START);
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(existsSync(join(dir, CONFIG))).toBe(true);
    // The app adds withWorkflow: the Workflow SDK's webhook route sits next to ours.
    const theirs = "import { workflowEntrypoint } from 'workflow/runtime';\n";
    mkdirSync(join(dir, 'app/.well-known/workflow/v1/webhook/[token]'), { recursive: true });
    writeFileSync(join(dir, 'app/.well-known/workflow/v1/webhook/[token]/route.js'), theirs);
    writeFileSync(join(dir, 'package.json'), JSON.stringify({ name: 'app', dependencies: { workflow: '5.2.1' } }));
    warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(existsSync(join(dir, FLOW))).toBe(false);
    expect(readFileSync(join(dir, 'app/.well-known/workflow/v1/webhook/[token]/route.js'), 'utf8')).toBe(theirs);
    // config.json and .gitignore stay while a route of the Workflow SDK's is there (it writes its own over them).
    expect(existsSync(join(dir, CONFIG))).toBe(true);
    expect(existsSync(join(dir, OWN_FLOW))).toBe(true);

    // Alone, both go.
    const alone = project(QUICK_START);
    applyDurableHarness({}, 'phase-production-build', alone);
    writeFileSync(join(alone, 'package.json'), JSON.stringify({ name: 'app', dependencies: { workflow: '5.2.1' } }));
    warnings(() => applyDurableHarness({}, 'phase-production-build', alone));
    expect(existsSync(join(alone, 'app/.well-known/workflow'))).toBe(false);

    // A config.json that is not one we wrote stays, even alone.
    const foreign = project({ ...QUICK_START, ...WORKFLOW_SDK, [CONFIG]: '{"version":"0","workflows":{"maxDuration":60}}' });
    warnings(() => applyDurableHarness({}, 'phase-production-build', foreign));
    expect(existsSync(join(foreign, CONFIG))).toBe(true);
  });

  it('leaves a flow route the app wrote itself there alone, and says it must mount flowHandler()', () => {
    const own = "export const POST = () => new Response('mine')\n";
    const dir = project({ ...QUICK_START, ...WORKFLOW_SDK, 'app/.well-known/durable-harness/v1/flow/route.ts': own });
    const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(readFileSync(join(dir, 'app/.well-known/durable-harness/v1/flow/route.ts'), 'utf8')).toBe(own);
    expect(existsSync(join(dir, OWN_FLOW))).toBe(false);
    expect(warned).toContain('has its own route at /.well-known/durable-harness/v1/flow');
  });

  it('traces the harnesses’ files into its own flow route', () => {
    const dir = project({
      ...QUICK_START,
      ...WORKFLOW_SDK,
      'node_modules/@opencode/core/package.json': JSON.stringify({ name: '@opencode/core', version: '1.0.0', main: 'index.js' }),
      'node_modules/@opencode/core/index.js': '',
      'node_modules/tree-sitter-bash/package.json': JSON.stringify({ name: 'tree-sitter-bash', version: '1.0.0' }),
      'node_modules/tree-sitter-bash/tree-sitter-bash.wasm': 'wasm',
    });
    const config = warningsConfig(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(config.outputFileTracingIncludes?.[FLOW_ROUTE]).toEqual(['./node_modules/tree-sitter-bash/tree-sitter-bash.wasm']);
    expect(config.outputFileTracingIncludes?.[WORKFLOW_FLOW_ROUTE]).toBeUndefined();
  });
});

/** `run`'s result, with its warnings silenced. */
function warningsConfig<T>(run: () => T): T {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
  try {
    return run();
  } finally {
    warn.mockRestore();
  }
}

describe('withDurableHarness with the Workflow SDK: the queue trigger on Vercel', () => {
  const WITH = { ...QUICK_START, ...WORKFLOW_SDK };
  const onVercel = () => {
    process.env.VERCEL = '1';
    process.env.VERCEL_DEPLOYMENT_ID = 'dpl_test';
  };

  it('fails a build on Vercel with the Vercel World when vercel.json gives the flow route no trigger, with the entry to add', () => {
    onVercel();
    let thrown: unknown;
    try {
      applyDurableHarness({}, 'phase-production-build', project(WITH));
    } catch (error) {
      thrown = error;
    }
    expect(String(thrown)).toContain('This app uses the Workflow SDK (workflow/next resolves from the project)');
    expect(String(thrown)).toContain(`${OWN_FLOW} has no queue trigger`);
    expect(String(thrown)).toContain('npx durable-harness vercel-json --write');
    expect(String(thrown)).toContain(JSON.stringify({ functions: { [OWN_FLOW]: FLOW_ENTRY } }, null, 2));
  });

  it('warns once anywhere else (next dev, a build off Vercel, world-postgres on Vercel)', () => {
    const local = project(WITH);
    const warned = warnings(() => applyDurableHarness({}, 'phase-development-server', local));
    expect(warned).toContain(`${OWN_FLOW} has no queue trigger`);
    expect(warned).toContain('Locally nothing is needed');
    expect(warnings(() => applyDurableHarness({}, 'phase-development-server', local))).toBe('');
    process.env.VERCEL = '1';
    process.env.WORKFLOW_TARGET_WORLD = '@workflow/world-postgres';
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', project(WITH)))).toContain('has no queue trigger');
  });

  it.each([
    ['vercel.json, the exact key', { 'vercel.json': JSON.stringify({ functions: { [OWN_FLOW]: FLOW_ENTRY } }) }],
    ['vercel.json, a glob', { 'vercel.json': JSON.stringify({ functions: { 'app/.well-known/**/route.js': FLOW_ENTRY } }) }],
    ['vercel.ts with harnessFunctions()', { 'vercel.ts': "import { harnessFunctions } from 'durable-harness/vercel'\nexport const config = { functions: harnessFunctions() }\n" }],
    ['vercel.ts that names the topic', { 'vercel.ts': "export const config = { functions: { 'app/.well-known/durable-harness/v1/flow/route.js': { experimentalTriggers: [{ type: 'queue/v2beta', topic: '__harness_wkf_workflow_*' }] } } }\n" }],
  ])('passes with the trigger in %s', (_, files) => {
    onVercel();
    const dir = project({ ...WITH, ...files });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
  });

  it('does not count a trigger on another topic, or for another route', () => {
    onVercel();
    const old = project({ ...WITH, 'vercel.json': JSON.stringify({ functions: { [OWN_FLOW]: { experimentalTriggers: [{ type: 'queue/v2beta', topic: '__wkf_workflow_*' }] } } }) });
    expect(() => applyDurableHarness({}, 'phase-production-build', old)).toThrow(/has no queue trigger/);
    const other = project({ ...WITH, 'vercel.json': JSON.stringify({ functions: { 'app/api/flow/route.ts': FLOW_ENTRY } }) });
    expect(() => applyDurableHarness({}, 'phase-production-build', other)).toThrow(/has no queue trigger/);
  });

  it('skips the check with DURABLE_HARNESS_SKIP_TRIGGER_CHECK=1, and with flowRoute: false', () => {
    onVercel();
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', project(WITH), { flowRoute: false }))).toBe('');
    process.env.DURABLE_HARNESS_SKIP_TRIGGER_CHECK = '1';
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', project(WITH)))).toBe('');
  });
});

describe('withDurableHarness, a harness of your own', () => {
  it('finds a module that makes one with durableAgent', () => {
    const dir = project({
      'lib/agent.ts': "import { durableAgent } from 'durable-harness'\nimport { mine } from './mine'\nexport const agent = durableAgent({ harness: mine() })\n",
      'lib/mine.ts': 'export const mine = () => ({ name: "mine", drive: async () => {} })\n',
      'app/api/chat/route.ts': "import { agent } from '@/lib/agent'\nexport const { GET, POST } = agent.chatRoute()\n",
    });
    applyDurableHarness({}, 'phase-production-build', dir);
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain(`import "../../../../../lib/agent";`);
    expect(route).not.toContain('lib/mine');
  });
});

describe('withDurableHarness, automatic agent discovery', () => {
  it('imports exported agents without importing scripts or docs samples for their side effects', async () => {
    const dir = project({
      ...STUBS,
      ...MINE,
      'scripts/drill.js': `const { durableAgent } = await import('durable-harness')
async function drill() { return durableAgent({ name: 'drill' }) }
await drill()
throw new Error('the build must not run this script')
`,
      'app/how-it-works/page.tsx': `const sample = "import { durableAgent } from 'durable-harness'; durableAgent({})"
export default function Page() { return <pre>{sample}</pre> }
`,
      'lib/docs.ts': `import { durableAgent } from 'durable-harness'
export const sample = \`durableAgent({})\`
`,
      'scripts/helper.ts': `import { pi } from 'durable-harness/pi'
export async function run() { return pi({ model: 'test' }) }
`,
    });
    applyDurableHarness({}, 'phase-production-build', dir);
    const text = readFileSync(join(dir, FLOW), 'utf8');
    expect(text).toContain('lib/mine');
    for (const excluded of ['scripts/drill', 'how-it-works', 'lib/docs', 'scripts/helper']) expect(text).not.toContain(excluded);
    expect((await routeOf(dir)).POST).toBeTypeOf('function');
  });

  it.each([
    ["import { durableAgent as make } from 'durable-harness'", 'export const agent = make({})'],
    ["import * as harness from 'durable-harness'", 'export default harness.durableAgent({})'],
    ["const { durableAgent: make } = await import('durable-harness')", 'const agent = make({}); export { agent as custom }'],
    ["const harness = require('durable-harness')", 'const agent = harness.durableAgent({}); export default agent'],
    ["import { pi as make } from 'durable-harness/pi'", 'export const agent = (make({}) as Agent) satisfies Agent'],
    ["import { codex } from 'durable-harness/codex'", 'const agent = codex({}); export const agents = [agent]'],
    ["import { claudeCode } from 'durable-harness/claude-code'", 'export const agent = globalThis.agent ??= claudeCode({})'],
    ["import { opencode } from 'durable-harness/opencode'", 'const agent = opencode({}); export function getAgent() { return agent }'],
    ["import { durableAgent } from 'durable-harness'", 'const agent = durableAgent({}); const alias = agent; export { alias }'],
    ["import { pi } from 'durable-harness/pi'", 'export const { GET, POST } = pi({}).chatRoute()'],
    ["import { durableAgent } from 'durable-harness'", 'export class Agents { static agent = durableAgent({}) }'],
  ])('finds real exported values with %s and %s', (imported, made) => {
    const dir = project({ 'app/page.tsx': 'export default () => null', 'lib/agent.ts': `${imported}; ${made}` });
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(readFileSync(join(dir, FLOW), 'utf8')).toContain('lib/agent');
  });

  it('keeps explicit agents as the escape hatch for side-effect registration and custom factories', () => {
    const dir = project({
      'app/page.tsx': 'export default () => null',
      'lib/registration.ts': "import { durableAgent } from 'durable-harness'; durableAgent({})",
      'lib/custom.ts': "import { makeAgent } from './factory'; makeAgent()",
    });
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(existsSync(join(dir, FLOW))).toBe(false);
    applyDurableHarness({}, 'phase-production-build', dir, { agents: ['lib/registration.ts', 'lib/custom.ts'] });
    const text = readFileSync(join(dir, FLOW), 'utf8');
    expect(text).toContain('lib/registration');
    expect(text).toContain('lib/custom');
  });
});

describe('withDurableHarness, Pi with pi-durable: your own code on a World storage', () => {
  const PI_LIB = [
    "import { BACKGROUND_CONTEXT as ctx } from '@earendil-works/chord/context'",
    "import { createRegistry, Harness, type Storage } from '@earendil-works/pi-durable'",
    "import { gatewayModels, resumeWith } from 'durable-harness/pi-durable'",
    'const models = gatewayModels()',
    'const registry = createRegistry()',
    'export const open = resumeWith((storage: Storage) => Harness.open(storage, { models, registry }, ctx))',
    '',
  ].join('\n');
  const PI_ROUTE = [
    "import { openWorldStorage } from 'durable-harness/pi-durable'",
    "import { open } from '@/lib/pi'",
    'export async function POST(request: Request) {',
    '  const harness = await open(await openWorldStorage())',
    '  return Response.json({ ok: !!harness })',
    '}',
    '',
  ].join('\n');

  it('imports the module that calls resumeWith( in the flow route, and not a route that only opens storages', () => {
    const dir = project({ 'lib/pi.ts': PI_LIB, 'app/api/chat/route.ts': PI_ROUTE, 'app/page.tsx': 'export default function Page() { return null }\n' });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain('import "../../../../../lib/pi";');
    expect(route).not.toContain('api/chat/route');
    expect(json(dir, CONFIG)).toEqual(flowFunctionConfig());
  });

  it("warns when a module opens pi-durable sessions and nothing calls resumeWith(, and fails a build on Vercel", () => {
    const files = { 'lib/pi.ts': PI_LIB.replace('resumeWith(', '(').replace(', resumeWith', ''), 'app/api/chat/route.ts': PI_ROUTE };
    const dir = project(files);
    const warned = warnings(() => applyDurableHarness({}, 'phase-development-server', dir));
    expect(warned).toContain('app/api/chat/route.ts opens pi-durable sessions on the World, but no module of this app calls resumeWith()');
    expect(existsSync(join(dir, FLOW))).toBe(false);
    process.env.VERCEL = '1';
    process.env.VERCEL_DEPLOYMENT_ID = 'dpl_test';
    expect(() => applyDurableHarness({}, 'phase-production-build', project(files))).toThrow(/no module of this app calls resumeWith\(\)/);
    process.env.DURABLE_HARNESS_SKIP_TRIGGER_CHECK = '1';
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', project(files)))).toBe('');
  });

  it('with the Workflow SDK (withWorkflow): its own flow route imports the module that calls resumeWith(, and the build on Vercel asks for the one trigger entry', () => {
    const files = { 'lib/pi.ts': PI_LIB, 'app/api/chat/route.ts': PI_ROUTE, 'app/page.tsx': 'export default function Page() { return null }\n', ...WORKFLOW_SDK };
    const dir = project(files);
    warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    const route = readFileSync(join(dir, OWN_FLOW), 'utf8');
    expect(route).toContain('import "../../../../../lib/pi";');
    expect(route).not.toContain('api/chat/route');
    expect(existsSync(join(dir, FLOW))).toBe(false);
    process.env.VERCEL = '1';
    process.env.VERCEL_DEPLOYMENT_ID = 'dpl_test';
    expect(() => applyDurableHarness({}, 'phase-production-build', project(files))).toThrow(/vercel\.json/);
  });

  it('counts a type-only import as none', () => {
    const dir = project({ 'lib/types.ts': "import type { Opener } from 'durable-harness/pi-durable'\nexport const resumeWith = (o: Opener) => o\nresumeWith(async () => null as never)\n" });
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(existsSync(join(dir, FLOW))).toBe(false);
  });
});

// ---------- fx agents (libfx's createFxAgent) ----------

describe('withDurableHarness, fx agents, without the Workflow SDK', () => {
  it('writes a flow route that is the fx agent’s wakeHandler() alone, and its trigger on libfx’s topic', async () => {
    const dir = project(FX_APP);
    const config = applyDurableHarness({}, 'phase-production-build', dir);
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain('import * as fx from "../../../../../lib/agent";');
    expect(route).toContain('const wake = fxWake([fx], ["lib/agent.ts"]);');
    expect(route).not.toContain("from 'durable-harness'");
    expect(route).not.toContain('flowHandler');
    expect(json(dir, CONFIG)).toEqual(flowFunctionConfig({ harness: false, fx: true }));
    expect(json(dir, CONFIG).workflows).toEqual({ maxDuration: 'max', experimentalTriggers: [LIBFX_TRIGGER] });
    expect(readFileSync(join(dir, 'app/.well-known/workflow/v1/.gitignore'), 'utf8')).toBe('*\n');
    // libfx loads a native addon: a package of its own, not bundled.
    expect(config.serverExternalPackages).toContain('libfx');
    // Next's trace finds libfx's addon and durable bundles by themselves.
    expect(config.outputFileTracingIncludes).toBeUndefined();
    const { POST, HEAD } = await routeOf(dir);
    expect(await (await POST(delivery(LIBFX_TOPIC))).text()).toBe(`fx main: ${LIBFX_TOPIC}`);
    expect(HEAD(new Request(`http://localhost${WORKFLOW_FLOW_ROUTE}?__health`, { method: 'HEAD' })).status).toBe(200);
  });

  it('hands each delivery to its consumer by topic in an app with both kinds of agent', async () => {
    const dir = project({ ...FX_APP, ...MINE });
    applyDurableHarness({}, 'phase-production-build', dir);
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain('import "../../../../../lib/mine";');
    expect(route).toContain('import * as fx from "../../../../../lib/agent";');
    expect(route).toContain("import { flowHandler } from 'durable-harness';");
    expect(json(dir, CONFIG).workflows.experimentalTriggers).toEqual([HARNESS_TRIGGER, LIBFX_TRIGGER]);
    const { POST } = await routeOf(dir);
    expect(await (await POST(delivery(LIBFX_TOPIC))).text()).toBe(`fx main: ${LIBFX_TOPIC}`);
    expect(await (await POST(delivery('__harness_wkf_workflow_session'))).text()).toBe(`flow ${WORKFLOW_FLOW_ROUTE}: __harness_wkf_workflow_session`);
    expect(await (await POST(delivery())).text()).toBe(`flow ${WORKFLOW_FLOW_ROUTE}: null`);
    // A World that delivers to the route over HTTP itself (world-local, world-postgres).
    expect((await (await POST(delivery(LIBFX_TOPIC, 'x-vqs-queue-name'))).text()).startsWith('fx main')).toBe(true);
    expect((await (await POST(delivery('__harness_wkf_workflow_session', 'x-vqs-queue-name'))).text()).startsWith('flow')).toBe(true);
  });

  it.each([
    ["import type { FxAgent } from 'libfx'"],
    ["import type {\n  FxAgent,\n} from 'libfx'"],
    ["import { type FxAgent } from 'libfx'"],
  ])('does not count a type-only import of libfx: %s', (imported) => {
    const dir = project({
      ...STUBS,
      'lib/agent.ts': `${imported}\nconst createFxAgent = (options: object) => options as FxAgent\nexport const agent: FxAgent = createFxAgent({})\n`,
    });
    applyDurableHarness({}, 'phase-production-build', dir);
    expect(existsSync(join(dir, FLOW))).toBe(false);
  });

  it.each([
    ['export const agent = createFxAgent({})'],
    ['export const agent: FxAgent = createFxAgent({})'],
    ['export default createFxAgent({})'],
    ['const agent = createFxAgent({})\nexport { agent as fx }'],
    ['const agent = createFxAgent({})\nexport default agent'],
    ['export const agent = ((globalThis as any).fx ??= createFxAgent({}))'],
    ['const logged = (a: FxAgent) => a\nexport const agent = logged(\n  createFxAgent({}),\n)'],
  ])('reaches an agent its module exports: %s', async (made) => {
    const dir = project({ ...STUBS, 'lib/agent.ts': `import { createFxAgent, type FxAgent } from 'libfx'\n${made}\n` });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
    expect((await (await routeOf(dir)).POST(delivery(LIBFX_TOPIC))).status).toBe(200);
  });

  it('warns at build time about an agent its module does not export, and says what to export', async () => {
    const dir = project({
      ...STUBS,
      'lib/agent.ts': "import { createFxAgent } from 'libfx'\n\nconst agent = createFxAgent({})\nexport function getAgent() {\n  return agent\n}\n",
    });
    const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(warned).toContain('lib/agent.ts calls createFxAgent(), but no top-level export there holds the agent');
    expect(warned).toContain('export const agent = createFxAgent({ ... })');
    // A factory it exports is not the agent either.
    const factory = project({ ...STUBS, 'lib/agent.ts': "import { createFxAgent } from 'libfx'\nexport const makeAgent = (name: string) => createFxAgent({ name })\n" });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', factory))).toContain('no top-level export there holds the agent');
    // At run time the route says the same when a wake comes (not when it loads), and the queue delivers again.
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { POST } = await routeOf(dir);
    expect(error).not.toHaveBeenCalled();
    const response = await POST(delivery(LIBFX_TOPIC));
    expect(error.mock.calls.flat().join(' ')).toContain('lib/agent.ts exports no fx agent');
    error.mockRestore();
    expect(response.status).toBe(500);
    expect(await response.text()).toContain('lib/agent.ts exports no fx agent');
  });

  it('warns that one fx agent per app takes libfx’s wakes, mounts the first, and lets `agents` choose', async () => {
    const files = {
      ...STUBS,
      'lib/a.ts': "import { createFxAgent } from 'libfx'\n/** One `createFxAgent()` per server. */\nexport const a = createFxAgent({ name: 'a' })\n",
      'lib/b.ts': "import { createFxAgent } from 'libfx'\nexport const b = createFxAgent({ name: 'b' })\n",
    };
    const dir = project(files);
    const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(warned).toContain('This app makes 2 fx agents (lib/a.ts, lib/b.ts)');
    expect(await (await (await routeOf(dir)).POST(delivery(LIBFX_TOPIC))).text()).toBe(`fx a: ${LIBFX_TOPIC}`);

    const chosen = project(files);
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', chosen, { agents: ['lib/b.ts'] }))).toBe('');
    expect(readFileSync(join(chosen, FLOW), 'utf8')).toContain('import * as fx from "../../../../../lib/b";');
    // `agents` in the order given.
    const ordered = project(files);
    warnings(() => applyDurableHarness({}, 'phase-production-build', ordered, { agents: ['lib/b.ts', 'lib/a.ts'] }));
    expect(readFileSync(join(ordered, FLOW), 'utf8')).toContain('import * as fx from "../../../../../lib/b";');
  });

  describe('with a libfx that runs each session on its own agent, by name (0.0.13-dev.1506 and later)', () => {
    /** libfx's wakeHandler() as of vercel-labs/fx#1254: a turn goes to the agent its session was created with (here, named in a header); none of that name, FX_NO_AGENT. */
    const BY_NAME = {
      ...STUBS,
      'node_modules/libfx/package.json': JSON.stringify({ name: 'libfx', version: '0.0.13-dev.1512.ga8421a0ce13b', type: 'module', exports: { '.': './index.js' } }),
      'node_modules/libfx/index.js': [
        'const agents = new Map();',
        "export function createFxAgent(options = {}) {",
        "  const agent = { name: options.name ?? 'default', wakeHandler: () => wakeHandler() };",
        '  agents.set(agent.name, agent);',
        '  return agent;',
        '}',
        'export function wakeHandler() {',
        '  return async (request) => {',
        "    const name = request.headers.get('x-test-agent') ?? 'default';",
        "    return agents.has(name) ? new Response(`fx ${name}: ${request.headers.get('ce-vqsqueuename')}`) : new Response(`FX_NO_AGENT ${name}`, { status: 500 });",
        '  };',
        '}',
        '',
      ].join('\n'),
    };
    const to = (agent: string) => {
      const request = delivery(LIBFX_TOPIC);
      request.headers.set('x-test-agent', agent);
      return request;
    };
    const A = "import { createFxAgent } from 'libfx'\nexport const support = createFxAgent({ name: 'support', instructions: 'Billing.' })\n";
    const B = "import { createFxAgent } from 'libfx'\nexport const coding = createFxAgent({\n  name: \"coding\",\n  model: 'anthropic/claude-opus-5.5',\n})\n";

    it('imports every module that exports an fx agent and mounts libfx’s own wakeHandler(): each turn reaches its agent', async () => {
      const dir = project({ ...BY_NAME, 'lib/support.ts': A, 'lib/coding.ts': B });
      expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
      const route = readFileSync(join(dir, FLOW), 'utf8');
      expect(route).toContain('import * as fx0 from "../../../../../lib/coding";');
      expect(route).toContain('import * as fx1 from "../../../../../lib/support";');
      expect(route).toContain("import * as libfx from 'libfx';");
      const { POST } = await routeOf(dir);
      expect(await (await POST(to('support'))).text()).toBe(`fx support: ${LIBFX_TOPIC}`);
      expect(await (await POST(to('coding'))).text()).toBe(`fx coding: ${LIBFX_TOPIC}`);
      expect((await POST(to('other'))).status).toBe(500);
    });

    it('still never imports a script that makes an agent for itself', async () => {
      const dir = project({
        ...BY_NAME,
        'lib/support.ts': A,
        'evals/run.ts': "import { createFxAgent } from 'libfx'\nconst agent = createFxAgent({ name: 'eval' })\nawait agent.session().prompt('2 + 2?').result\n",
      });
      expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
      expect(readFileSync(join(dir, FLOW), 'utf8')).not.toContain('evals/run');
      expect(await (await (await routeOf(dir)).POST(to('support'))).text()).toBe(`fx support: ${LIBFX_TOPIC}`);
    });

    it('warns when two agents share a name: one without any is "default"', () => {
      const unnamed = project({ ...BY_NAME, 'lib/support.ts': A, 'lib/other.ts': "import { createFxAgent } from 'libfx'\nexport const other = createFxAgent({ model: 'openai/gpt-5.5' })\n" });
      const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', unnamed));
      expect(warned).toContain('This app makes 2 fx agents (lib/other.ts, lib/support.ts), and one has no name (lib/other.ts)');
      expect(warned).toContain("Give each createFxAgent() a name of its own");
      const twice = project({ ...BY_NAME, 'lib/support.ts': A, 'lib/again.ts': A.replace('support =', 'again =') });
      expect(warnings(() => applyDurableHarness({}, 'phase-production-build', twice))).toContain('"support" is used twice (lib/again.ts, lib/support.ts)');
      // One agent needs no name.
      const one = project({ ...BY_NAME, 'lib/other.ts': "import { createFxAgent } from 'libfx'\nexport const other = createFxAgent({})\n" });
      expect(warnings(() => applyDurableHarness({}, 'phase-production-build', one))).toBe('');
    });
  });

  it('tells, from the libfx version, whether its sessions run on their own agent by name', () => {
    expect(routesByName('0.0.13-dev.1506.gbc968ad65622')).toBe(true);
    expect(routesByName('0.0.13-dev.1512.ga8421a0ce13b')).toBe(true);
    expect(routesByName('0.0.14-dev.1534.g6afa7c33dac5')).toBe(true);
    expect(routesByName('0.0.13-dev.1432.g1df91f0b4ebf')).toBe(false);
    expect(routesByName('0.0.13')).toBe(false);
    expect(routesByName(undefined)).toBe(false);
  });

  it.each([
    ["createFxAgent({ name: 'a', tools })", ['a']],
    ['createFxAgent({\n  model,\n  name: "b",\n})', ['b']],
    ['createFxAgent({ model })', [null]],
    ['createFxAgent()', [null]],
    ['createFxAgent({ name })', [undefined]],
    ['createFxAgent({ ...base, name: "c" })', [undefined]],
    ['createFxAgent(options)', [undefined]],
  ] as const)('reads the agent name in %s', (call, names) => {
    expect(fxAgentNames(`import { createFxAgent } from 'libfx'\nexport const agent = ${call}\n`)).toEqual(names);
  });

  it('mounts the module that exports its fx agent, not a script that makes one for itself', async () => {
    const dir = project({
      ...FX_APP,
      'evals/run.ts': "import { createFxAgent } from 'libfx'\nconst agent = createFxAgent({ name: 'eval' })\nawait agent.session().prompt('2 + 2?').result\n",
    });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toContain('This app makes 2 fx agents (lib/agent.ts, evals/run.ts)');
    expect(readFileSync(join(dir, FLOW), 'utf8')).not.toContain('evals/run');
    expect(await (await (await routeOf(dir)).POST(delivery(LIBFX_TOPIC))).text()).toBe(`fx main: ${LIBFX_TOPIC}`);
  });

  it.each(['export const mine = durableAgent({})', 'durableAgent({})'])('takes a listed module that makes both kinds of agent as both: %s', (harness) => {
    const dir = project({
      ...STUBS,
      'lib/agents.ts': `import { createFxAgent } from 'libfx'\nimport { durableAgent } from 'durable-harness'\nexport const fx = createFxAgent({})\n${harness}\n`,
    });
    applyDurableHarness({}, 'phase-production-build', dir, { agents: ['lib/agents.ts'] });
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain('import "../../../../../lib/agents";');
    expect(route).toContain('import * as fx from "../../../../../lib/agents";');
    expect(json(dir, CONFIG).workflows.experimentalTriggers).toEqual([HARNESS_TRIGGER, LIBFX_TRIGGER]);
  });

  it('writes no route for an fx agent whose module cannot resolve libfx, and says so', () => {
    const { 'node_modules/libfx/package.json': _p, 'node_modules/libfx/index.js': _i, ...rest } = FX_APP;
    const dir = project(rest);
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toContain('lib/agent.ts makes an fx agent, but libfx does not resolve from it');
    expect(existsSync(join(dir, FLOW))).toBe(false);
  });

  it('leaves libfx’s queue to a wake route the app subscribes itself', () => {
    const own = "import { agent } from '@/lib/agent'\nexport const POST = agent.wakeHandler()\n";
    const vercelJson = JSON.stringify({ functions: { 'app/api/libfx/route.ts': { experimentalTriggers: [{ type: 'queue/v2beta', topic: LIBFX_TOPIC }] } } });
    const fxOnly = project({ ...FX_APP, 'app/api/libfx/route.ts': own, 'vercel.json': vercelJson });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', fxOnly))).toBe('');
    expect(existsSync(join(fxOnly, FLOW))).toBe(false);

    const both = project({ ...FX_APP, 'app/api/libfx/route.ts': own, 'vercel.json': vercelJson, ...MINE });
    applyDurableHarness({}, 'phase-production-build', both);
    expect(readFileSync(join(both, FLOW), 'utf8')).not.toContain('lib/agent');
    expect(json(both, CONFIG)).toEqual(flowFunctionConfig());
  });

  it('takes libfx’s wakes when the app’s own wake route has no trigger, and says that route gets none', () => {
    const own = "import { agent } from '@/lib/agent'\nexport const POST = agent.wakeHandler()\n";
    const dir = project({ ...FX_APP, 'app/api/libfx/route.ts': own });
    const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(warned).toContain("app/api/libfx/route.ts mounts wakeHandler(), but no vercel.json or vercel.ts subscribes a function to libfx's topic");
    expect(json(dir, CONFIG).workflows.experimentalTriggers).toEqual([LIBFX_TRIGGER]);
  });

  it('finds an fx agent made with a createFxAgent of the app’s own that re-exports libfx’s, or wraps it', async () => {
    for (const fx of ["export { createFxAgent } from 'libfx'\n", "import { createFxAgent as create } from 'libfx'\nexport function createFxAgent(options: object) {\n  return create({ name: 'wrapped', ...options })\n}\n"]) {
      const dir = project({ ...STUBS, 'lib/fx.ts': fx, 'lib/agent.ts': "import { createFxAgent } from './fx'\nexport const agent = createFxAgent({})\n" });
      // The wrapper declares a createFxAgent; it makes no agent itself.
      expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
      expect(readFileSync(join(dir, FLOW), 'utf8')).toContain('import * as fx from "../../../../../lib/agent";');
      expect((await (await (await routeOf(dir)).POST(delivery(LIBFX_TOPIC))).text()).startsWith('fx ')).toBe(true);
    }
  });

  it('counts a conditional as one fx agent', () => {
    const dir = project({ ...STUBS, 'lib/agent.ts': "import { createFxAgent } from 'libfx'\nexport const agent = process.env.FAST\n  ? createFxAgent({ name: 'fast' })\n  : createFxAgent({ name: 'slow' })\n" });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
  });

  it('takes a listed module that calls createFxAgent( as the fx agent’s, wherever it imports it from', () => {
    const dir = project({ ...STUBS, 'lib/fx.ts': "export { createFxAgent } from 'libfx'\n", 'lib/agent.ts': "import { createFxAgent } from '@/lib/fx'\nexport const agent = createFxAgent({})\n" });
    applyDurableHarness({}, 'phase-production-build', dir, { agents: ['lib/agent.ts'] });
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain('import * as fx from "../../../../../lib/agent";');
    expect(route).not.toContain('flowHandler');
    expect(json(dir, CONFIG).workflows.experimentalTriggers).toEqual([LIBFX_TRIGGER]);
  });

  it('serves a listed module the build cannot read as either kind of agent', async () => {
    const dir = project({
      ...STUBS,
      'lib/factory.ts': "import { createFxAgent } from 'libfx'\nexport function makeAgent(name: string) {\n  return createFxAgent({ name })\n}\n",
      'lib/agent.ts': "import { makeAgent } from './factory'\nexport const agent = makeAgent('made')\n",
      'lib/helpers.ts': 'export const helpers = {}\n',
    });
    // Unlisted, the scan finds the factory; listed, the module that makes the agent is served as either kind.
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir, { agents: ['lib/helpers.ts', 'lib/agent.ts'] }))).toBe('');
    const route = readFileSync(join(dir, FLOW), 'utf8');
    expect(route).toContain('import "../../../../../lib/agent";');
    expect(route).toContain('import * as fx0 from "../../../../../lib/helpers";');
    expect(route).toContain('import * as fx1 from "../../../../../lib/agent";');
    expect(json(dir, CONFIG).workflows.experimentalTriggers).toEqual([HARNESS_TRIGGER, LIBFX_TRIGGER]);
    const { POST } = await routeOf(dir);
    expect(await (await POST(delivery(LIBFX_TOPIC))).text()).toBe(`fx made: ${LIBFX_TOPIC}`);
    expect(await (await POST(delivery('__harness_wkf_workflow_session'))).text()).toBe(`flow ${WORKFLOW_FLOW_ROUTE}: __harness_wkf_workflow_session`);
    // An app without libfx: such a module is a durable-harness agent's only, as before.
    const { 'node_modules/libfx/package.json': _p, 'node_modules/libfx/index.js': _i, ...rest } = STUBS;
    const harness = project({ ...rest, 'lib/agent.ts': "import { makeAgent } from './factory'\nexport const agent = makeAgent()\n" });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', harness, { agents: ['lib/agent.ts'] }))).toBe('');
    expect(json(harness, CONFIG)).toEqual(flowFunctionConfig());
  });

  it('keeps LIBFX_TOPIC equal to the topic libfx queues its wakes on', () => {
    const libfx = dirname(createRequire(import.meta.url).resolve('libfx'));
    const durable = readFileSync(join(libfx, 'durable.js'), 'utf8');
    expect(durable).toContain(`const queuePrefix = ${JSON.stringify(LIBFX_QUEUE_PREFIX)};`);
    expect(durable).toContain('const queueName = `${queuePrefix}session`;');
    expect(LIBFX_TOPIC).toBe(`${LIBFX_QUEUE_PREFIX}session`);
  });
});

describe('withDurableHarness, fx agents, with the Workflow SDK', () => {
  it('writes the fx route at its own path (libfx’s wakes) and the flow route (sessions’ turns), each its own consumer with its own entry', async () => {
    const dir = project({ ...FX_APP, ...MINE, ...WORKFLOW_SDK });
    const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(warned).toContain(`${OWN_FLOW} and ${OWN_FX} have no queue trigger`);
    expect(warned).toContain(JSON.stringify({ functions: { [OWN_FLOW]: FLOW_ENTRY, [OWN_FX]: FX_ENTRY } }, null, 2));
    const flow = readFileSync(join(dir, OWN_FLOW), 'utf8');
    expect(flow).toContain('import "../../../../../lib/mine";');
    expect(flow).not.toContain('lib/agent');
    const fx = readFileSync(join(dir, OWN_FX), 'utf8');
    expect(fx).toContain('import * as fx from "../../../../../lib/agent";');
    expect(fx).not.toContain('flowHandler');
    expect(existsSync(join(dir, CONFIG))).toBe(false);
    expect(await (await (await routeOf(dir, OWN_FLOW)).POST(delivery('__harness_wkf_workflow_session'))).text()).toBe(`flow ${FLOW_ROUTE}: __harness_wkf_workflow_session`);
    expect(await (await (await routeOf(dir, OWN_FX)).POST(delivery(LIBFX_TOPIC))).text()).toBe(`fx main: ${LIBFX_TOPIC}`);
  });

  it("checks the fx route's trigger on libfx's topic, and passes with its entry", () => {
    process.env.VERCEL = '1';
    process.env.VERCEL_DEPLOYMENT_ID = 'dpl_test';
    expect(() => applyDurableHarness({}, 'phase-production-build', project({ ...FX_APP, ...WORKFLOW_SDK }))).toThrow(new RegExp(`${OWN_FX} has no queue trigger`));
    const dir = project({ ...FX_APP, ...WORKFLOW_SDK, 'vercel.json': JSON.stringify({ functions: { [OWN_FX]: FX_ENTRY } }) });
    expect(warnings(() => applyDurableHarness({}, 'phase-production-build', dir))).toBe('');
    // Its own entry is not taken for a wake route of the app's own: the fx route is still written.
    expect(existsSync(join(dir, OWN_FX))).toBe(true);
  });

  it('leaves the app’s own fx route alone, and says it must mount wakeHandler()', () => {
    const own = "export const POST = () => new Response('mine')\n";
    const dir = project({ ...FX_APP, ...WORKFLOW_SDK, 'app/.well-known/durable-harness/v1/fx/route.ts': own });
    const warned = warnings(() => applyDurableHarness({}, 'phase-production-build', dir));
    expect(readFileSync(join(dir, 'app/.well-known/durable-harness/v1/fx/route.ts'), 'utf8')).toBe(own);
    expect(existsSync(join(dir, OWN_FX))).toBe(false);
    expect(warned).toContain('writes no fx route for the fx agent in lib/agent.ts');
  });

  it('routes and paths', () => {
    expect([FLOW_ROUTE, FX_ROUTE, WORKFLOW_FLOW_ROUTE]).toEqual(['/.well-known/durable-harness/v1/flow', '/.well-known/durable-harness/v1/fx', '/.well-known/workflow/v1/flow']);
  });
});
