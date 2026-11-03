import { mkdirSync, realpathSync, rmSync } from 'node:fs';
import { dirname, join, resolve, sep } from 'node:path';
import type { WorkspaceMachine } from 'harness-world-hands/runtime';
import {
  commandEnv,
  invocationOf,
  OPENCODE_TOOLS,
  type OpenCodeTool,
  PERMISSION_OF,
  SAVED_OUTPUT,
  SAVED_OUTPUT_REPLACEMENT,
  WORKSPACE_ROOT,
  type WorkspaceSlot,
  workspaceTool,
} from './environment.ts';
import type { Channel, FromHost, HostInit, InstructionBlob, OpenCodeEvent, ToHost } from './protocol.ts';
import { gatewayModelConfig } from './models.ts';

/**
 * The OpenCode host: OpenCode v2's embedded server (no listener: its router
 * runs in memory), in the drive's process, rebuilt from the session's
 * journaled events, with one plugin that makes it durable. It talks to the
 * drive over a `Channel`.
 *
 * OpenCode runs here with nothing of this machine in reach of the model: its
 * services that would touch this machine are replaced (`isolation`):
 *
 * - Its execution plane (`@opencode/Environment`, which every built-in tool
 *   reads, writes and runs commands through) is the session's workspace
 *   (environment.ts), or, without one, nothing: files and commands fail.
 *   Nothing in OpenCode spawns a process on this machine (git, formatters,
 *   MCP servers, ripgrep's download: its process spawner is OpenCode's
 *   "no execution plane" one).
 * - Its global paths (home, data, config, state, tmp) are the drive's own
 *   empty directory, never this machine's home (where it would find skills
 *   and instructions); its filesystem service sees only that directory, the
 *   database and the config directory (any other path is "not found"); no
 *   project instructions (AGENTS.md) or global ones are read; snapshots and
 *   version control are off; the environment it tells the model about is
 *   the workspace's.
 * - It loads no plugin from config, and of its own provider and web-search
 *   plugins only the AI Gateway's (`PLUGIN_OPERATIONS`): the others look for
 *   credentials and CLIs of this machine as they load.
 *
 * The plugin:
 *
 * - registers the drive's tools, each of which runs in the drive (`tool`
 *   requests), where it is a kernel effect keyed by OpenCode's tool-call id;
 * - keeps the built-ins the session has (`init.builtins`, on its workspace)
 *   and removes every other: each built-in call asks the drive before it
 *   runs (`execute.before`: a `builtin` begin, its intent committed in the
 *   drive, its workspace opened for it) and tells it the outcome after
 *   (`execute.after`); a call to anything else is refused before it runs;
 * - holds every model request until everything OpenCode committed is in the
 *   log (`model.request`, the barrier);
 * - skips OpenCode's title request, and tells the model where its tools act.
 *
 * Everything OpenCode commits for the session is shipped to the drive
 * (`events`) as soon as it is committed, and before every barrier and tool
 * call: the outbox. Opening a session replays the journaled events into an
 * empty database with OpenCode's own `Bus.replay`, restores the state its
 * commit hooks keep outside the events (the project row, the instruction
 * blobs, the execution claim, the resume count), and leaves resuming an
 * interrupted turn to OpenCode's own restart sweep (`recover`).
 *
 * OpenCode is imported dynamically, on the first drive, and nothing here
 * changes the process's environment. As it is imported, OpenCode's util
 * package computes static global paths from XDG_* and HOME (`Global.Path`,
 * and the lock directory of its non-Effect `Flock`); the host never uses
 * them: every path OpenCode reaches comes from its `@opencode/Global`
 * service, which is each drive's own directory (`isolation`), and its file
 * locks (`EffectFlock`) take their directory from that service. In 2.0.22
 * only code the host never runs reads the static ones (the CLI's log file
 * and background service, the non-Effect `Flock`). The boot test traces
 * every `node:fs` call of a drive, with the process's HOME planted with
 * canaries, so a version that starts using them fails it; a change to the
 * environment would crash a process that also runs libfx (harness.ts).
 */

/** A built-in's result as text: its output (or content) as OpenCode reports it. */
function resultText(result: { output?: unknown; content?: unknown } | undefined): string {
  const content = result?.content;
  if (Array.isArray(content)) {
    const text = content.map((c: { type?: string; text?: string }) => (c?.type === 'text' ? (c.text ?? '') : '')).join('');
    if (text) return text;
  }
  if (typeof content === 'string') return content;
  if (typeof result?.output === 'string') return result.output;
  return JSON.stringify(result?.output ?? result ?? null) ?? '';
}

function errorText(error: unknown): string {
  const e = error as { message?: unknown; error?: { message?: unknown } } | undefined;
  return String(e?.message ?? e?.error?.message ?? error ?? 'the tool failed');
}

async function importOpenCode() {
  const [
    effect,
    http,
    routes,
    client,
    sdkPlugins,
    bus,
    database,
    sessionSql,
    projectSql,
    restart,
    tool,
    environment,
    unavailable,
    ripgrepBinary,
    filesystem,
    search,
    pty,
    snapshot,
    vcs,
    discovery,
    builtins,
    instructions,
    pluginSource,
    shell,
    shellSelect,
    toolOutput,
    spawner,
  ] = await Promise.all([
    import('effect'),
    import('effect/unstable/http'),
    import('@opencode/server/routes'),
    import('@opencode/client'),
    import('@opencode/core/plugin/sdk'),
    import('@opencode/core/bus'),
    import('@opencode/core/database/database'),
    import('@opencode/core/session/sql'),
    import('@opencode/core/project/sql'),
    import('@opencode/core/session/execution/restart'),
    import('@opencode/schema/tool'),
    import('@opencode/core/environment/index'),
    import('@opencode/core/environment/unavailable'),
    import('@opencode/core/ripgrep/binary'),
    import('@opencode/core/filesystem'),
    import('@opencode/core/filesystem/search'),
    import('@opencode/core/pty'),
    import('@opencode/core/snapshot'),
    import('@opencode/core/vcs'),
    import('@opencode/core/instruction-discovery'),
    import('@opencode/core/instructions/builtins'),
    import('@opencode/core/instructions/index'),
    import('@opencode/core/config/plugin/source'),
    import('@opencode/core/shell'),
    import('@opencode/core/shell/select'),
    import('@opencode/core/tool-output'),
    import('effect/unstable/process/ChildProcessSpawner'),
  ]);
  return {
    Context: effect.Context,
    Effect: effect.Effect,
    Layer: effect.Layer,
    Logger: effect.Logger,
    References: effect.References,
    ManagedRuntime: effect.ManagedRuntime,
    Stream: effect.Stream,
    Sink: effect.Sink,
    Schema: effect.Schema,
    DateTime: effect.DateTime,
    PlatformError: effect.PlatformError,
    PlatformFileSystem: effect.FileSystem,
    HttpServer: http.HttpServer,
    HttpRouter: http.HttpRouter,
    HttpEffect: http.HttpEffect,
    createEmbeddedRoutes: routes.createEmbeddedRoutes,
    OpenCode: client.OpenCode,
    SdkPlugins: sdkPlugins.SdkPlugins,
    Bus: bus.Bus,
    Database: database.Database,
    InstructionBlobTable: sessionSql.InstructionBlobTable,
    ProjectTable: projectSql.ProjectTable,
    SessionRestart: restart.SessionRestart,
    Tool: tool.Tool,
    Environment: environment.Environment,
    EnvironmentUnavailable: unavailable.EnvironmentUnavailable,
    RipgrepBinary: ripgrepBinary.RipgrepBinary,
    FileSystem: filesystem.FileSystem,
    FileSystemSearch: search.FileSystemSearch,
    Pty: pty.Pty,
    Snapshot: snapshot.Snapshot,
    Vcs: vcs.Vcs,
    InstructionDiscovery: discovery.InstructionDiscovery,
    InstructionBuiltIns: builtins.InstructionBuiltIns,
    Instructions: instructions.Instructions,
    ConfigPluginSource: pluginSource.ConfigPluginSource,
    Shell: shell.Shell,
    ShellSelect: shellSelect.ShellSelect,
    ToolOutput: toolOutput.ToolOutput,
    ChildProcessSpawner: spawner,
  };
}

type OpenCodeModules = Awaited<ReturnType<typeof importOpenCode>>;
let modules: Promise<OpenCodeModules> | undefined;
/** OpenCode's modules, imported once per process. */
const loadOpenCode = () => (modules ??= importOpenCode());

const versioned = (type: string, version: number) => `${type}.${version}`;
/** `HARNESS_OPENCODE_DEBUG=1`: trace the host's side of the protocol on stderr. */
const debug = (...args: unknown[]) => {
  if (process.env.HARNESS_OPENCODE_DEBUG) console.error('[opencode host]', ...args);
};
const nameOf = (type: string) => type.replace(/\.\d+$/, '');

/** A directory and the path it resolves to (macOS's /tmp is /private/tmp): both are the same files. */
function bothForms(dir: string): string[] {
  const abs = resolve(dir);
  try {
    return [...new Set([abs, realpathSync(abs)])];
  } catch {
    return [abs];
  }
}

/** The LayerNode graph's nodes reachable from `roots`, by name: how the host finds the nodes of OpenCode's util package, which it does not import. */
function nodesByName(roots: unknown[]): Map<string, unknown[]> {
  const out = new Map<string, unknown[]>();
  const seen = new Set<unknown>();
  const visit = (node: unknown) => {
    if (!node || typeof node !== 'object' || seen.has(node)) return;
    seen.add(node);
    const key = Object.getOwnPropertySymbols(node).find((s) => s.description === 'LayerNode.Graph');
    const definition = key ? (node as Record<symbol, { name?: string; dependencies?: unknown[] }>)[key] : undefined;
    if (!definition) return;
    if (definition.name) out.set(definition.name, [...(out.get(definition.name) ?? []), node]);
    for (const dep of definition.dependencies ?? []) visit(dep);
  };
  for (const r of roots) visit(r);
  return out;
}

/**
 * The plugin operations the host's config source gives OpenCode: its
 * provider and web-search plugins off but the AI Gateway's
 * (`opencode.provider.gateway`, the `vercel/<model>` provider's SDK) and the
 * prompt-cache-key transform, which only edits model metadata. The others
 * read credentials, files and CLIs of this machine (Bedrock's AWS profile,
 * Vertex's gcloud, Azure's `az` on PATH) as they load.
 */
export const PLUGIN_OPERATIONS = [
  { type: 'remove', target: 'opencode.provider.*' },
  { type: 'remove', target: 'opencode.websearch.*' },
  { type: 'add', target: 'opencode.provider.gateway', options: {} },
  { type: 'add', target: 'opencode.provider.prompt-cache-key', options: {} },
] as const;

/** What the host gets besides its init. */
export interface HostOptions {
  /**
   * The session's workspace: the machine of the built-in call that runs
   * (environment.ts). Without one, OpenCode has no execution plane: its
   * files and commands fail (and the session has no built-ins).
   */
  workspace?: WorkspaceSlot;
}

/**
 * The replacements that keep OpenCode off this machine (see the module's
 * doc), for `createEmbeddedRoutes`.
 */
function isolation(oc: OpenCodeModules, init: HostInit, options: HostOptions): unknown[] {
  const { Context, Effect, Layer, Stream, Sink, PlatformError } = oc;
  const E = Effect as any;
  const L = Layer as any;
  const graph = nodesByName([(oc.Shell as any).node, (oc.Environment as any).node]);
  const nodes = (name: string): any[] => {
    const found = graph.get(name);
    if (!found?.length) throw new Error(`harness-world-opencode: OpenCode's ${name} service is not where this version of the harness looks for it (OpenCode 2.0.22)`);
    return found;
  };

  // The global paths: this drive's directory.
  const g = init.global;
  const global = {
    home: join(g, 'home'),
    data: join(g, 'data'),
    cache: join(g, 'cache'),
    config: join(g, 'config'),
    state: join(g, 'state'),
    tmp: join(g, 'tmp'),
    bin: join(g, 'cache', 'bin'),
    log: join(g, 'data', 'log'),
    repos: join(g, 'data', 'repos'),
  };
  class Global extends (Context as any).Service()('@opencode/Global') {}
  const globalLayer = L.effect(
    Global,
    E.sync(() => {
      for (const d of Object.values(global)) mkdirSync(d, { recursive: true, mode: 0o700 });
      return global;
    })
  );

  // The filesystem service: only this session's files. Everything else is "not found", without a look.
  const allowed = [g, dirname(init.dbPath), init.configDir].flatMap(bothForms);
  const inside = (p: unknown) => {
    if (typeof p !== 'string') return true;
    const abs = resolve(p);
    return allowed.some((r) => abs === r || abs.startsWith(r + sep));
  };
  const notFound = (method: string, path: unknown) =>
    (PlatformError as any).systemError({ _tag: 'NotFound', module: 'FileSystem', method, pathOrDescriptor: String(path), description: "not one of this session's files" });
  const confine = (fs: any) => {
    const one =
      (method: string) =>
      (p: unknown, ...rest: unknown[]) =>
        inside(p) ? fs[method](p, ...rest) : method === 'exists' ? E.succeed(false) : E.fail(notFound(method, p));
    const two =
      (method: string) =>
      (a: unknown, b: unknown, ...rest: unknown[]) =>
        inside(a) && inside(b) ? fs[method](a, b, ...rest) : E.fail(notFound(method, inside(a) ? b : a));
    const temp =
      (method: string) =>
      (o: { directory?: string } = {}) =>
        fs[method]({ ...o, directory: o.directory && inside(o.directory) ? o.directory : global.tmp });
    return {
      ...fs,
      ...Object.fromEntries(
        ['access', 'chmod', 'chown', 'exists', 'makeDirectory', 'open', 'readDirectory', 'readFile', 'readFileString', 'readLink', 'realPath', 'remove', 'stat', 'truncate', 'utimes', 'writeFile', 'writeFileString'].map((m) => [m, one(m)])
      ),
      ...Object.fromEntries(['copy', 'copyFile', 'link', 'rename', 'symlink'].map((m) => [m, two(m)])),
      ...Object.fromEntries(['makeTempDirectory', 'makeTempDirectoryScoped', 'makeTempFile', 'makeTempFileScoped'].map((m) => [m, temp(m)])),
      stream: (p: unknown, o: unknown) => (inside(p) ? fs.stream(p, o) : (Stream as any).fail(notFound('stream', p))),
      sink: (p: unknown, o: unknown) => (inside(p) ? fs.sink(p, o) : (Sink as any).fail(notFound('sink', p))),
      watch: (p: unknown, o: unknown) => (inside(p) ? fs.watch(p, o) : (Stream as any).empty),
    };
  };
  const FS = (oc.PlatformFileSystem as any).FileSystem;

  // The execution plane: the workspace, or nothing.
  const Env = oc.Environment as any;
  const slot = options.workspace;
  const failed = (path: string, cause: unknown) => new Env.Failed({ path, cause });
  const onMachine = <A>(path: string, work: (m: WorkspaceMachine) => Promise<A>, map?: (err: unknown) => unknown) =>
    E.tryPromise({
      try: () => {
        if (!slot) throw new Error('this session has no workspace');
        return work(slot.machine());
      },
      catch: (err: unknown) => map?.(err) ?? failed(path, err),
    });
  const code = (err: unknown) => (err as { code?: unknown } | undefined)?.code;
  const typeOf = (t: string) => (t === 'file' || t === 'directory' || t === 'symlink' ? t : 'other');
  const files = {
    read: (path: string, range?: { offset: number; length: number }) =>
      onMachine(
        path,
        async (m) => {
          const st = await m.stat(path);
          if (!st) throw Object.assign(new Error('not found'), { code: 'ENOENT' });
          if (st.type !== 'file') throw Object.assign(new Error(st.type), { code: 'EISDIR', actual: st.type });
          const bytes = await m.readFile(path);
          return { info: { type: 'file', size: bytes.length, mtimeMs: st.mtimeMs }, bytes: range ? bytes.subarray(range.offset, range.offset + range.length) : bytes };
        },
        (err) =>
          code(err) === 'ENOENT'
            ? new Env.NotFound({ path })
            : code(err) === 'EISDIR'
              ? new Env.WrongKind({ path, actual: typeOf(String((err as { actual?: string }).actual ?? 'directory')) })
              : undefined
      ),
    write: (path: string, bytes: Uint8Array) => onMachine(path, (m) => m.writeFile(path, bytes)),
    stat: (path: string) =>
      onMachine(
        path,
        async (m) => {
          const st = await m.lstat(path);
          if (!st) throw Object.assign(new Error('not found'), { code: 'ENOENT' });
          return { type: typeOf(st.type), size: st.size, mtimeMs: st.mtimeMs };
        },
        (err) => (code(err) === 'ENOENT' ? new Env.NotFound({ path }) : undefined)
      ),
    list: (path: string) =>
      onMachine(
        path,
        async (m) => {
          const st = await m.stat(path);
          if (!st) throw Object.assign(new Error('not found'), { code: 'ENOENT' });
          if (st.type !== 'directory') throw Object.assign(new Error(st.type), { code: 'ENOTDIR', actual: st.type });
          return (await m.list(path)).map((e) => ({ name: e.name, type: typeOf(e.type) }));
        },
        (err) =>
          code(err) === 'ENOENT'
            ? new Env.NotFound({ path })
            : code(err) === 'ENOTDIR'
              ? new Env.WrongKind({ path, actual: typeOf(String((err as { actual?: string }).actual ?? 'file')) })
              : undefined
      ),
    remove: (path: string) => onMachine(path, (m) => m.remove(path, { recursive: true, force: true })),
    move: (from: string, to: string) =>
      onMachine(
        from,
        async (m) => {
          if (!(await m.lstat(from))) throw Object.assign(new Error('not found'), { code: 'ENOENT' });
          await m.rename(from, to);
        },
        (err) => (code(err) === 'ENOENT' ? new Env.NotFound({ path: from }) : undefined)
      ),
    mkdir: (path: string) => onMachine(path, (m) => m.mkdir(path, { recursive: true })),
  };
  const CPS = oc.ChildProcessSpawner as any;
  const encoder = new TextEncoder();
  const spawner = slot
    ? CPS.make((command: any) =>
        E.suspend(() => {
          const abort = new AbortController();
          const standard = command?._tag === 'StandardCommand';
          const run: Promise<{ exitCode: number; stdout: string; stderr: string }> = (async () => {
            if (!standard) return { exitCode: 127, stdout: '', stderr: 'pipelines of commands are not supported on the workspace\n' };
            const machine = slot.machine();
            const invocation = invocationOf(String(command.command), (command.args ?? []).map(String));
            const opts = { cwd: typeof command.options?.cwd === 'string' ? command.options.cwd : WORKSPACE_ROOT, env: commandEnv(command.options?.env), signal: abort.signal };
            try {
              return invocation.kind === 'script'
                ? await machine.exec(invocation.script, opts)
                : await machine.exec(invocation.name, { ...opts, args: invocation.args });
            } catch (err) {
              // A command the workspace stopped (its time limit, the call cancelled): a failed command, not a spawn error.
              return { exitCode: 124, stdout: '', stderr: `${err instanceof Error ? err.message : String(err)}\n` };
            }
          })();
          run.catch(() => {});
          let running = true;
          void run.finally(() => (running = false));
          const out = (Stream as any).fromEffect(E.promise(() => run.then((r) => encoder.encode(r.stdout))));
          const err = (Stream as any).fromEffect(E.promise(() => run.then((r) => encoder.encode(r.stderr))));
          return E.succeed(
            CPS.makeHandle({
              pid: CPS.ProcessId(1),
              exitCode: E.promise(() => run.then((r) => CPS.ExitCode(r.exitCode))),
              isRunning: E.sync(() => running),
              kill: () => E.sync(() => abort.abort()),
              stdin: (Sink as any).drain,
              stdout: out,
              stderr: err,
              all: (Stream as any).concat(out, err),
              getInputFd: () => (Sink as any).drain,
              getOutputFd: () => (Stream as any).empty,
              unref: E.succeed(E.void),
            })
          );
        })
      )
    : (oc.EnvironmentUnavailable as any).spawner;
  const environment = L.succeed(Env.Service, Env.Service.of({ files: Env.makeFiles({ spawner, overrides: files }), spawner }));

  // What the model is told about its environment: OpenCode's own sources (the date), with the
  // environment the workspace's (no path of this machine), or none without a workspace. OpenCode's
  // own, edited, not ones made here: a bundler can give this module another copy of `effect` than
  // OpenCode's, and a source's codec only works with the copy that encodes it.
  const ENVIRONMENT = 'core/environment';
  const workspaceEnv = ['<env>', `  Working directory: ${WORKSPACE_ROOT}`, `  Workspace root folder: ${WORKSPACE_ROOT}`, '  Is directory a git repo: no', '  Platform: linux', '</env>'].join('\n');
  const told = (sources: readonly { key: string }[]) =>
    sources.flatMap((source) => (source.key !== ENVIRONMENT ? [source] : slot ? [{ ...source, read: E.succeed(workspaceEnv) }] : []));

  const unavailable = (what: string) => E.die(new Error(`${what} is unavailable: OpenCode reaches files only through the session's workspace`));
  const IB = oc.InstructionBuiltIns as any;
  const ID = oc.InstructionDiscovery as any;
  const RB = oc.RipgrepBinary as any;
  const FSys = oc.FileSystem as any;
  const Search = oc.FileSystemSearch as any;
  const P = oc.Pty as any;
  const Snap = oc.Snapshot as any;
  const V = oc.Vcs as any;
  const Sel = oc.ShellSelect as any;
  const Out = oc.ToolOutput as any;
  const Src = oc.ConfigPluginSource as any;
  const rewrite = (r: any) => ({
    ...r,
    content: Array.isArray(r?.content)
      ? r.content.map((c: any) => (c?.type === 'text' && typeof c.text === 'string' ? { ...c, text: c.text.replace(SAVED_OUTPUT, SAVED_OUTPUT_REPLACEMENT) } : c))
      : r?.content,
  });
  return [
    // Nothing spawns on this machine.
    ...nodes('effect/process/ChildProcessSpawner').map((n) => n.replace((oc.EnvironmentUnavailable as any).layer)),
    Env.node.replace(environment),
    nodes('@opencode/Global')[0].replace(globalLayer),
    ...nodes('effect/platform/FileSystem').map((n) => n.replace(n.mapLayer((layer: unknown) => L.effect(FS, E.map(FS, confine)).pipe(L.provide(layer))))),
    // The workspace's rg, not one found or downloaded on this machine.
    RB.node.replace(L.succeed(RB.Service, RB.Service.of({ filepath: E.succeed('rg') }))),
    // The shell is the workspace's bash, not one found on this machine.
    Sel.node.replace(Sel.node.mapLayer((layer: unknown) => L.effect(Sel.Service, E.map(Sel.Service, (s: any) => Sel.Service.of({ ...s, resolve: () => E.succeed('bash'), list: () => E.succeed([]) }))).pipe(L.provide(layer)))),
    // Truncated output says the rest is not shown, not where it was saved on this machine.
    Out.node.replace(Out.node.mapLayer((layer: unknown) => L.effect(Out.Service, E.map(Out.Service, (s: any) => Out.Service.of({ ...s, truncate: (r: unknown) => E.map(s.truncate(r), rewrite) }))).pipe(L.provide(layer)))),
    IB.node.replace(IB.node.mapLayer((layer: unknown) => L.effect(IB.Service, E.map(IB.Service, (s: any) => IB.Service.of({ load: () => E.map(s.load(), told) }))).pipe(L.provide(layer)))),
    ID.node.replace(ID.configured({ project: false, global: false })),
    // No plugins from config, and of OpenCode's own only the AI Gateway's provider: the others look for
    // credentials and CLIs of this machine when they load (Azure's searches PATH for `az`).
    Src.node.replace(L.succeed(Src.Service, Src.Service.of({ operations: () => E.succeed(PLUGIN_OPERATIONS), changes: () => (Stream as any).never }))),
    Snap.node.replace(Snap.noopLayer),
    V.node.replace(
      L.succeed(
        V.Service,
        V.Service.of({
          base: () => E.succeed(null),
          transform: () => E.succeed({ dispose: E.void }),
          reload: () => E.void,
          info: () => E.succeed({ branch: {} }),
          branches: () => E.succeed([]),
          status: () => E.succeed([]),
          diff: () => E.succeed([]),
        })
      )
    ),
    FSys.node.replace(
      L.succeed(FSys.Service, FSys.Service.of({ read: () => unavailable('FileSystem.read'), list: () => unavailable('FileSystem.list'), find: () => unavailable('FileSystem.find'), write: () => unavailable('FileSystem.write') }))
    ),
    Search.node.replace(L.succeed(Search.Service, Search.Service.of({ find: () => unavailable('FileSystemSearch.find') }))),
    P.node.replace(
      L.succeed(
        P.Service,
        P.Service.of({
          list: () => E.succeed([]),
          get: (ptyID: string) => E.fail(new P.NotFoundError({ ptyID })),
          create: () => unavailable('Pty.create'),
          update: (ptyID: string) => E.fail(new P.NotFoundError({ ptyID })),
          remove: (ptyID: string) => E.fail(new P.NotFoundError({ ptyID })),
          write: (ptyID: string) => E.fail(new P.NotFoundError({ ptyID })),
          attach: (ptyID: string) => E.fail(new P.NotFoundError({ ptyID })),
        })
      )
    ),
  ];
}

/**
 * Run the host until the drive closes it (or the channel ends). Resolves
 * once OpenCode is disposed. Throws if the session cannot be rebuilt.
 */
export async function runHost(init: HostInit, channel: Channel<ToHost, FromHost>, options: HostOptions = {}): Promise<void> {
  const oc = await loadOpenCode();
  const { Context, Effect, Layer } = oc;
  const send = (m: FromHost) => {
    debug('->', m.type, m.type === 'events' ? m.events.map((e) => `${e.seq}:${e.type}`).join(' ') : m.type === 'text' ? '' : JSON.stringify(m).slice(0, 200));
    channel.send(m);
  };

  // A fresh database: the log is the source of truth, this file a cache of it.
  for (const f of [init.dbPath, `${init.dbPath}-wal`, `${init.dbPath}-shm`]) rmSync(f, { force: true });
  mkdirSync(dirname(init.dbPath), { recursive: true, mode: 0o700 });
  mkdirSync(init.configDir, { recursive: true, mode: 0o700 });
  mkdirSync(init.global, { recursive: true, mode: 0o700 });
  // The session's built-ins (only OpenCode's seven, only with a workspace).
  const builtins = new Set(options.workspace ? init.builtins.filter((b): b is OpenCodeTool => (OPENCODE_TOOLS as readonly string[]).includes(b)) : []);

  // Gateway can serve IDs newer than OpenCode's bundled models.dev snapshot.
  // A catalog outage must not prevent using a model the snapshot already knows.
  const models = await gatewayModelConfig(init).catch((error) => {
    console.warn('[opencode] Gateway model discovery unavailable; using bundled metadata', error instanceof Error ? error.message : String(error));
    return undefined;
  });

  const config = {
    model: init.model,
    share: 'disabled',
    update: 'disable',
    snapshots: false,
    // OpenCode's built-in AI Gateway provider, through the drive's proxy:
    // the host holds a placeholder, the proxy adds the credential.
    providers: { vercel: { settings: { baseURL: init.gatewayBaseURL, apiKey: init.gatewayKey }, ...(models ? { models } : {}) } },
  };
  // OpenCode's own logs: warnings and errors on stderr (everything with HARNESS_OPENCODE_DEBUG).
  const logs = Layer.merge(
    oc.Logger.layer(
      [
        oc.Logger.make((o: any) => {
          const entry = oc.Logger.formatStructured.log(o) as { message: unknown; cause?: string };
          const [text, ...rest] = Array.isArray(entry.message) ? entry.message : [entry.message];
          // The failure itself, when OpenCode logs one (its message alone says only what failed).
          const cause = entry.cause ? [entry.cause.length > 4000 ? `${entry.cause.slice(0, 4000)}…` : entry.cause] : [];
          console.error(`[opencode ${String(o.logLevel).toLowerCase()}]`, text, ...rest.map((r) => (typeof r === 'string' ? r : JSON.stringify(r))), ...cause);
        }),
      ],
      { mergeWithExisting: false }
    ),
    Layer.succeed(oc.References.MinimumLogLevel, process.env.HARNESS_OPENCODE_DEBUG ? 'Info' : 'Warn')
  );
  const runtime = oc.ManagedRuntime.make(
    oc
      .createEmbeddedRoutes({
        app: { name: 'harness-world' },
        database: { path: init.dbPath },
        events: { persist: true },
        // Metadata above comes through the Gateway relay; don't fetch models.dev separately.
        models: { fetch: false },
        // No project config, no user config: only what the drive passes.
        config: { content: JSON.stringify(config), directory: init.configDir, project: false },
        fs: { fff: false, filewatcher: false },
      }, isolation(oc, init, options) as never)
      .pipe(Layer.provide(oc.HttpServer.layerServices), Layer.provideMerge(logs))
  );
  const run = <A>(effect: unknown) => runtime.runPromise(effect as never) as Promise<A>;
  let closed = false;
  const tail = new AbortController();
  let soon: ReturnType<typeof setTimeout> | undefined;
  const close = async () => {
    if (closed) return;
    closed = true;
    tail.abort();
    clearTimeout(soon);
    await runtime.dispose().catch(() => {});
  };

  try {
    const services = await runtime.runPromise(runtime.contextEffect);
    // Requests run with the host's loggers too.
    const requestContext = Context.make(oc.Logger.CurrentLoggers, Context.get(services, oc.Logger.CurrentLoggers)).pipe(
      Context.add(oc.References.MinimumLogLevel, Context.get(services, oc.References.MinimumLogLevel))
    );
    const handler = oc.HttpEffect.toWebHandlerWith(requestContext as never)(Context.get(services, oc.HttpRouter.HttpRouter).asHttpEffect() as never);
    const client = oc.OpenCode.make({ baseUrl: 'http://opencode.local', fetch: (req: RequestInfo | URL, reqInit?: RequestInit) => handler(new Request(req, reqInit)) });
    const db = Context.get(services, oc.Database.Service).db;
    // The session's id is ours (`ses_<runId>`, a run id is [A-Za-z0-9_-]): safe to inline.
    const where = `id = '${init.sessionId.replace(/'/g, "''")}'`;
    /** Hold the execution claim (what `SessionStore.claim` writes at turn start; a no-op if held). */
    const claim = () => run(db.run(`update session_v2 set time_suspended = ${Date.now()} where ${where} and time_suspended is null`));

    // Requests to the drive (barriers, tool calls), answered by `reply`.
    let nextId = 0;
    const waiting = new Map<number, { resolve: (m: Extract<ToHost, { type: 'reply' }>) => void; reject: (err: Error) => void }>();
    const request = (
      m:
        | { type: 'barrier'; kind: string }
        | { type: 'tool'; callId: string; name: string; input: unknown }
        | { type: 'builtin'; phase: 'begin'; callId: string; name: string; input: unknown }
        | { type: 'builtin'; phase: 'end'; callId: string; name: string; text: string; isError: boolean }
    ) =>
      new Promise<Extract<ToHost, { type: 'reply' }>>((resolve, reject) => {
        if (closed) return reject(new Error('the host is closed'));
        const id = ++nextId;
        waiting.set(id, { resolve, reject });
        send({ ...m, id });
      });

    // 1. Rebuild the session from the log, or create it.
    let shipped = -1;
    const sentBlobs = new Set(init.blobs.map((b) => b.hash));
    if (init.events.length > 0) {
      const created = init.events.find((e) => nameOf(e.type) === 'session.created');
      if (!created) throw new Error('the journal has no session.created event');
      const directory = String(created.data.location?.directory ?? init.directory);
      // session_v2's foreign key; the project id came from the machine that created the session.
      await run(db.insert(oc.ProjectTable).values({ id: created.data.projectID, worktree: directory, sandboxes: [] } as never).onConflictDoNothing().run());
      const bus = Context.get(services, oc.Bus.Service);
      // Replay runs OpenCode's projectors, which rebuild every table from the events.
      await run(
        Effect.forEach(
          init.events,
          (e: OpenCodeEvent) =>
            bus.replay({ id: e.id, type: e.type, created: e.created, seq: e.seq, aggregateID: init.sessionId, data: e.data } as never),
          { discard: true }
        )
      );
      if (init.blobs.length) await run(db.insert(oc.InstructionBlobTable).values(init.blobs as never).onConflictDoNothing().run());
      // State outside the events (written by commit hooks and plain updates), re-derived from them by the drive.
      if (init.claim) await claim();
      if (init.restarts) await run(db.run(`update session_v2 set resume_attempts = ${Number(init.restarts) | 0} where ${where}`));
      shipped = init.events.at(-1)!.seq;
    }

    // 2. The plugin.
    const names = new Set(init.tools.map((t) => t.name));
    /** The permission actions the session's tools ask for (OpenCode's built-ins' own, and each drive tool by its name). */
    const allowedActions = new Set([...[...builtins].map((b) => PERMISSION_OF[b]), ...builtins, ...names]);
    /**
     * What the log recorded for a built-in, over what OpenCode reported:
     * a note before the result, or the failure the log has instead of it
     * (and no host path in a result).
     */
    const settleAfter = (
      e: { status: string; result?: { output?: unknown; content?: unknown }; error?: unknown },
      reply: { note?: string; failed?: string } | undefined
    ) => {
      if (e.status !== 'error' && e.result) {
        const result = e.result as { output?: unknown; content?: unknown };
        const text = (t: string) => t.replace(SAVED_OUTPUT, SAVED_OUTPUT_REPLACEMENT);
        if (Array.isArray(result.content)) result.content = result.content.map((c: { type?: string; text?: string }) => (c?.type === 'text' && typeof c.text === 'string' ? { ...c, text: text(c.text) } : c));
        if (typeof result.output === 'string') result.output = text(result.output);
        else if (result.output && typeof result.output === 'object' && typeof (result.output as { output?: unknown }).output === 'string')
          result.output = { ...(result.output as object), output: text((result.output as { output: string }).output) };
        if (reply?.failed) result.content = [{ type: 'text', text: `Error: ${reply.failed}` }];
        else if (reply?.note) result.content = [{ type: 'text', text: reply.note }, ...(Array.isArray(result.content) ? result.content : [])];
      } else if (e.status === 'error' && reply?.note) {
        (e as { error?: unknown }).error = new Tool.Error({ message: `${reply.note}\n\n${errorText(e.error)}` });
      }
    };
    const { Tool } = oc;
    const plugin = {
      id: 'harness-world',
      effect: (ctx: any) =>
        Effect.gen(function* () {
          yield* ctx.tool.transform((editor: any) => {
            // The session's built-ins and the drive's tools: every other tool of OpenCode's goes.
            for (const t of editor.list()) if (!builtins.has(t.id)) editor.remove(t.id);
            for (const spec of init.tools) {
              editor.add({
                name: spec.name,
                description: spec.description,
                input: spec.inputSchema,
                // Called directly, not from OpenCode's code-mode sandbox.
                options: { codemode: false },
                execute: (input: unknown, context: { id: string }) =>
                  Effect.tryPromise({ try: () => callTool(String(context.id), spec.name, input), catch: (err) => err }).pipe(
                    Effect.orDie,
                    Effect.flatMap((r: { text?: string; isError?: boolean }) =>
                      r.isError ? Effect.fail(new Tool.Error({ message: r.text ?? 'the tool failed' })) : Effect.succeed({ content: r.text || '(no output)' })
                    )
                  ),
              });
            }
          });
          // Barrier coverage: a built-in runs once its intent is in the log; a call to anything else never runs.
          yield* ctx.tool.hook('execute.before', (e: { tool: string; id: string; input: unknown }) => {
            if (names.has(e.tool)) return Effect.void;
            if (!builtins.has(e.tool as OpenCodeTool)) return Effect.fail(new Tool.Error({ message: `No tool named ${e.tool} is available.` }));
            // No background commands: a command ends with its call.
            if (e.tool === 'shell' && e.input && typeof e.input === 'object') delete (e.input as { background?: unknown }).background;
            return Effect.tryPromise({
              try: () => beginBuiltin(String(e.id), e.tool, e.input),
              catch: (err) => new Tool.Error({ message: `Not run: ${err instanceof Error ? err.message : String(err)}` }),
            });
          });
          if (builtins.size)
            yield* ctx.tool.hook(
              'execute.after',
              (e: { tool: string; id: string; status: string; result?: { output?: unknown; content?: unknown }; error?: unknown }) =>
                names.has(e.tool) || !builtins.has(e.tool as OpenCodeTool)
                  ? Effect.void
                  : Effect.promise(async () => {
                      const r = await endBuiltin(String(e.id), e.tool, e.status === 'error' ? errorText(e.error) : resultText(e.result), e.status === 'error').catch((err) => {
                        debug('builtin end failed', err);
                        return undefined;
                      });
                      settleAfter(e, r);
                    })
            );
          // Where the built-ins run, in their definitions: the workspace's platform and shell, how long its files last.
          if (builtins.size) {
            const tools = (event: { tools?: Record<string, { description: string; input: Record<string, unknown> }> }) =>
              Effect.sync(() => {
                for (const name of builtins) {
                  const t = event.tools?.[name];
                  if (t) event.tools![name] = workspaceTool(name, t, (name === 'shell' ? init.notes?.shell : init.notes?.file) ?? '');
                }
              });
            yield* ctx.session.hook('context', tools);
            yield* ctx.session.hook('compaction', tools);
            yield* ctx.session.hook('generate', tools);
          }
          // Every path is the workspace's: nothing outside it, and no other action than the session's tools'.
          yield* ctx.permission.hook('evaluate', (e: { action: string; effect: string; message?: string }) =>
            Effect.sync(() => {
              if (e.action === 'external_directory') {
                e.effect = 'deny';
                e.message = `Only paths under ${WORKSPACE_ROOT} are available.`;
              } else if (e.action !== '*' && !allowedActions.has(e.action)) e.effect = 'deny';
            })
          );
          // The model barrier: every model request (the turn's, compaction's) waits for the log.
          yield* ctx.session.hook('model.request', (e: { kind: string }) => Effect.promise(() => barrier(e.kind)));
          yield* ctx.session.hook('title', (e: { result?: string }) =>
            Effect.sync(() => {
              e.result = init.title;
            })
          );
          if (init.system)
            yield* ctx.session.hook('context', (e: { system: unknown[] }) =>
              Effect.sync(() => {
                e.system.push({ type: 'text', text: init.system });
              })
            );
        }),
    };
    await run(Context.get(services, oc.SdkPlugins.Service).register(plugin as never));

    // 3. The outbox: everything committed since the last shipment, with the blobs it references.
    let shipping: Promise<void> = Promise.resolve();
    // The log's `after` is exclusive and must be >= 0: no events yet is no `after`.
    const after = () => (shipped >= 0 ? { after: shipped } : {});
    const shipOnce = async () => {
      const events: OpenCodeEvent[] = [];
      for await (const item of client.session.log({ sessionID: init.sessionId, ...after(), follow: false }) as AsyncIterable<any>) {
        if (!item?.durable || item.durable.seq <= shipped) continue;
        events.push({ id: item.id, seq: item.durable.seq, created: item.created, type: versioned(item.type, item.durable.version), data: item.data });
      }
      if (!events.length) return;
      const wanted = new Set<string>();
      for (const e of events) {
        if (nameOf(e.type) !== 'session.instructions.updated') continue;
        for (const h of Object.values(e.data?.delta ?? {})) if (typeof h === 'string' && !sentBlobs.has(h)) wanted.add(h);
      }
      let blobs: InstructionBlob[] = [];
      if (wanted.size) {
        const rows = await run<{ hash: string; value: unknown }[]>(db.select().from(oc.InstructionBlobTable).all());
        blobs = rows.filter((r) => wanted.has(r.hash)).map((r) => ({ hash: r.hash, value: r.value }));
        for (const b of blobs) sentBlobs.add(b.hash);
      }
      shipped = events.at(-1)!.seq;
      send({ type: 'events', events, blobs });
    };
    const ship = () => (shipping = shipping.then(shipOnce, shipOnce));
    const barrier = async (kind: string) => {
      await ship();
      const r = await request({ type: 'barrier', kind });
      if (!r.ok) throw new Error(`harness-world-opencode: the log refused: ${r.error}`);
    };
    /** A built-in is about to run: the drive commits its intent (or refuses: this throws). */
    const beginBuiltin = async (callId: string, name: string, input: unknown) => {
      // OpenCode's record of the call (`session.tool.called`) reaches the drive first.
      await ship();
      const r = await request({ type: 'builtin', phase: 'begin', callId, name, input });
      if (!r.ok) throw new Error(r.error);
    };
    const endBuiltin = async (callId: string, name: string, text: string, isError: boolean) => {
      await ship();
      const r = await request({ type: 'builtin', phase: 'end', callId, name, text, isError });
      return r.ok ? { ...(r.note ? { note: r.note } : {}), ...(r.failed ? { failed: r.failed } : {}) } : undefined;
    };
    const callTool = async (callId: string, name: string, input: unknown) => {
      // The call's intent (`session.tool.called`, committed before a tool runs) reaches the drive first.
      await ship();
      const r = await request({ type: 'tool', callId, name, input });
      if (!r.ok) throw new Error(`harness-world-opencode: ${name} failed in the drive: ${r.error}`);
      return { text: r.text, isError: r.isError };
    };

    if (init.events.length === 0) {
      await client.session.create({
        id: init.sessionId,
        title: init.title,
        location: { directory: init.directory },
        // Everything denied (code mode too) but the session's tools; never a path outside the workspace.
        permissions: [
          { action: '*', resource: '*', effect: 'deny' as const },
          ...[...allowedActions].map((action) => ({ action, resource: '*', effect: 'allow' as const })),
          { action: 'external_directory', resource: '*', effect: 'deny' as const },
        ],
      });
      await ship();
    }

    // Ship eagerly: wake on every committed event (the session exists now:
    // its log is a 404 before), after a short window so a burst of commits
    // is one shipment (the barriers ship at once). Re-opened if it ends
    // while the host runs.
    const shipSoon = () => {
      soon ??= setTimeout(() => {
        soon = undefined;
        void ship().catch((err) => debug('ship failed', err));
      }, 20);
    };
    void (async () => {
      while (!closed) {
        try {
          for await (const item of client.session.log({ sessionID: init.sessionId, ...after(), follow: true }, { signal: tail.signal }) as AsyncIterable<any>) {
            if (item?.durable) shipSoon();
          }
        } catch (err) {
          if (!closed) debug('tail failed', err);
        }
        if (!closed) await new Promise((r) => setTimeout(r, 100));
      }
    })();
    // Live text, ephemeral.
    void (async () => {
      for await (const ev of client.event.subscribe({ signal: tail.signal }) as AsyncIterable<any>) {
        if (ev?.type === 'session.text.delta' && ev.data?.sessionID === init.sessionId && ev.data.delta) send({ type: 'text', delta: ev.data.delta });
      }
    })().catch((err) => {
      if (!closed) debug('live text failed', err);
    });
    send({ type: 'ready', seq: shipped });

    // 4. The drive's instructions.
    const reply = (id: number, err?: unknown) =>
      send(err === undefined ? { type: 'reply', id, ok: true } : { type: 'reply', id, ok: false, error: err instanceof Error ? err.message : String(err) });
    for await (const m of channel.messages) {
      debug('<-', JSON.stringify(m).slice(0, 200));
      if (m.type === 'reply') {
        const w = waiting.get(m.id);
        waiting.delete(m.id);
        w?.resolve(m);
      } else if (m.type === 'prompt') {
        client.session.prompt({ sessionID: init.sessionId, id: m.messageId, text: m.text, delivery: m.delivery }).then(
          () => reply(m.id),
          (err: unknown) => reply(m.id, err)
        );
      } else if (m.type === 'recover') {
        (async () => {
          // The note first, without starting a drain (the sweep resumes the turn and owns its budget).
          await client.session.synthetic({
            sessionID: init.sessionId,
            text: m.note,
            description: 'Recovered from the log',
            metadata: { notice: 'harness-world-recovery' },
            resume: false,
          });
          await claim();
          runtime.runFork(Context.get(services, oc.SessionRestart.Service).resumeSuspendedSessions);
        })().then(
          () => reply(m.id),
          (err) => reply(m.id, err)
        );
      } else if (m.type === 'close') break;
    }
    for (const w of waiting.values()) w.reject(new Error('the host is closing'));
  } finally {
    await close();
  }
}
