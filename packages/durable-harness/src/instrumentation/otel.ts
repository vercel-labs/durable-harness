import { AsyncLocalStorage } from 'node:async_hooks';
import { ROOT_CONTEXT, context, type Context, type ContextManager } from '@opentelemetry/api';
import type { BasicTracerProvider, SpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { AgentRun, CaptureDecision, TraceErrorHandler } from '@vercel/agent-runs';
import type { World } from '@workflow/world';
import { AGENT_RUNS, type AgentRunsHook, type SessionTracing, type TraceAudience } from 'harness-world/agent-runs';
import { VERSION } from 'harness-world/names';
import { TraceStreams } from './checkpoints.ts';
import { traceDrive } from './drive.ts';
import { vercelSpanProcessor } from './vercel.ts';

export type { TraceAudience } from 'harness-world/agent-runs';
export type { CaptureDecision } from '@vercel/agent-runs';

export interface AgentRunsOptions {
  /** The audience of a session that records none (`traceAudience`). Default `unknown`. */
  readonly audience?: TraceAudience;
  /**
   * What a session's spans record, by its audience. Default: model and tool
   * inputs and outputs for `public`, metadata only (models, tools, timing,
   * usage) for `private` and `unknown`. Content a decision declines is never
   * recorded, checkpointed or exported.
   */
  readonly capture?: (audience: TraceAudience) => CaptureDecision;
  /** `service.name`. Default `durable-harness`. */
  readonly serviceName?: string;
  /** More destinations beside Agent Runs, with the same spans. */
  readonly spanProcessors?: readonly SpanProcessor[];
  /** Every tracing failure, which never fails agent work. Default: a warning. */
  readonly onError?: TraceErrorHandler;
}

export interface AgentRuns {
  /** Start tracing this process's sessions. Call once per process, from the host's instrumentation hook. */
  register(): void;
  /** Export ended spans and settle bounded, best-effort checkpoint writes. */
  forceFlush(): Promise<void>;
  shutdown(): Promise<void>;
}

const CONTENT: CaptureDecision = { emit: true, recordInputs: true, recordOutputs: true };
const METADATA: CaptureDecision = { emit: true, recordInputs: false, recordOutputs: false };
const defaultCapture = (audience: TraceAudience) => (audience === 'public' ? CONTENT : METADATA);

/** Optional tracing peers, loaded only by this entrypoint. */
const OTEL_SDK_PINS = {
  '@opentelemetry/sdk-trace-base': '2.2.0',
  '@opentelemetry/resources': '2.2.0',
  '@opentelemetry/otlp-transformer': '0.208.0',
  '@vercel/agent-runs': '0.0.1',
} as const;
const OTEL_SDK_INSTALL = `pnpm add ${Object.entries(OTEL_SDK_PINS).map(([name, version]) => `${name}@${version}`).join(' ')}`;

const sdk = await Promise.all([import('@opentelemetry/sdk-trace-base'), import('@opentelemetry/resources'), import('@opentelemetry/otlp-transformer'), import('@vercel/agent-runs')]).then(
  ([traceBase, resources, transformer, agentRuns]) => ({
    BasicTracerProvider: traceBase.BasicTracerProvider,
    resourceFromAttributes: resources.resourceFromAttributes,
    JsonTraceSerializer: transformer.JsonTraceSerializer,
    AgentSpanIdGenerator: agentRuns.AgentSpanIdGenerator,
    createAgentRun: agentRuns.createAgentRun,
    otelTelemetry: agentRuns.otelTelemetry,
  }),
  (error: unknown) => {
    const missing = missingPackage(error);
    if (!missing) throw error;
    return { missing: new Error(`durable-harness/instrumentation/otel: agentRuns() needs its optional tracing peers, and ${missing} is not installed. Install them: ${OTEL_SDK_INSTALL}`, { cause: error }) };
  }
);
function missingPackage(error: unknown): string | undefined {
  for (let e = error, depth = 0; e && typeof e === 'object' && depth < 5; e = (e as { cause?: unknown }).cause, depth++) {
    const { code, message } = e as { code?: unknown; message?: unknown };
    if (code !== 'ERR_MODULE_NOT_FOUND' && code !== 'MODULE_NOT_FOUND' && !/Cannot find (?:package|module)/.test(String(message))) continue;
    const name = Object.keys(OTEL_SDK_PINS).find((n) => String(message).includes(n));
    if (name) return name;
  }
  return undefined;
}

/** The async context the library's spans nest by, for a process whose app registered no OpenTelemetry. */
class AsyncContextManager implements ContextManager {
  private readonly storage = new AsyncLocalStorage<Context>();
  active(): Context {
    return this.storage.getStore() ?? ROOT_CONTEXT;
  }
  with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(ctx: Context, fn: F, thisArg?: ThisParameterType<F>, ...args: A): ReturnType<F> {
    return this.storage.run(ctx, () => fn.apply(thisArg, args));
  }
  bind<T>(ctx: Context, target: T): T {
    if (typeof target !== 'function') return target;
    const manager = this;
    return function (this: unknown, ...args: unknown[]) {
      return manager.with(ctx, () => (target as (...a: unknown[]) => unknown).apply(this, args));
    } as T;
  }
  enable(): this {
    return this;
  }
  disable(): this {
    this.storage.disable();
    return this;
  }
}

/**
 * Vercel Agent Runs for this process's sessions, with `@vercel/agent-runs`:
 * every turn a session runs, its model calls and tool calls, with usage. A
 * turn that goes on in a later invocation (out of time, a crash, a wake)
 * stays one turn: its checkpoint is kept with the session. Declaring it does
 * nothing; `register()` starts it. Its tracer provider is its own, so the
 * app's OpenTelemetry setup, if any, is left as it is.
 * Tracing requires Node.js 24 and its optional peers; other entrypoints support Node.js 22.
 */
export function agentRuns(options: AgentRunsOptions = {}): AgentRuns {
  if ('missing' in sdk) throw sdk.missing;
  const { BasicTracerProvider, resourceFromAttributes, JsonTraceSerializer, AgentSpanIdGenerator, createAgentRun, otelTelemetry } = sdk;
  const onError: TraceErrorHandler = (error, at) => {
    try {
      if (options.onError) void Promise.resolve(options.onError(error, at)).catch(() => {});
      else console.warn(`[durable-harness] Agent Runs: ${at.phase} failed`, error instanceof Error ? error.message : error);
    } catch { /* Diagnostics must not fail agent work. */ }
  };
  const capture = (audience: TraceAudience) => {
    try {
      return options.capture?.(audience) ?? defaultCapture(audience);
    } catch (error) {
      onError(error, { phase: 'start' });
      return METADATA;
    }
  };
  const audience = options.audience ?? 'unknown';
  let provider: BasicTracerProvider | undefined;
  const worlds = new WeakMap<World, { run: AgentRun; streams: TraceStreams }>();
  const live = new Set<WeakRef<TraceStreams>>();

  return {
    register() {
      if (provider) return;
      const idGenerator = new AgentSpanIdGenerator();
      provider = new BasicTracerProvider({
        resource: resourceFromAttributes({ 'service.name': options.serviceName ?? 'durable-harness', 'agent.framework.name': 'durable-harness' }),
        idGenerator,
        spanProcessors: [vercelSpanProcessor(JsonTraceSerializer, (error) => onError(error, { phase: 'complete' })), ...(options.spanProcessors ?? [])],
      });
      context.setGlobalContextManager(new AsyncContextManager());
      // Agent Runs reads spans of the `eve.agent` scope at schema version 4: the library writes the schema.
      const telemetry = otelTelemetry({ provider, idGenerator, tracerName: 'eve.agent', tracerVersion: VERSION });
      const forWorld = (world: World) => {
        let traced = worlds.get(world);
        if (!traced) {
          const streams = new TraceStreams(world, (error) => onError(error, { phase: 'checkpoint' }));
          traced = { streams, run: createAgentRun({ telemetry, checkpointer: streams.checkpointer, onError }) };
          worlds.set(world, traced);
          live.add(new WeakRef(streams));
        }
        return traced;
      };
      const hook: AgentRunsHook = {
        capture: given => capture(given ?? audience),
        onError,
        drive: (harness, ctx) => {
          const { run, streams } = forWorld(ctx.world);
          return traceDrive({ run, streams, audience, capture, version: VERSION }, harness, ctx);
        },
        session: ({ world, runId, agentName, audience: given }): SessionTracing => {
          const { run, streams } = forWorld(world);
          const decision = capture(given ?? audience);
          const identity = (turnId: string) => ({ conversationId: runId, runId, turnId });
          // Another process may have moved the session's turns on since this one last had it.
          streams.forget(runId);
          /** Steps of its turns, per execution (or this session): its number for the turn, and its next step. */
          const steps = new Map<string, { drive: Promise<number>; next: number }>();
          return {
            async turn({ turnId, sequence, title }) {
              streams.forget(runId, turnId);
              if (await streams.finished(runId, turnId)) return undefined;
              sequence ??= (await run.resume({ identity: identity(turnId) })) ? 0 : await streams.nextSequence(runId);
              return run.turn({
                identity: identity(turnId),
                agentName,
                sequence,
                framework: { name: 'durable-harness', version: VERSION },
                capture: decision,
                channel: { kind: 'http', origin: 'channel' },
                attributes: { 'agent.channel.audience': given ?? audience, ...(title && decision.recordInputs ? { 'agent.session.title': title.slice(0, 160) } : {}) },
              });
            },
            async resume(turnId) {
              streams.forget(runId, turnId);
              if (!(await run.resume({ identity: identity(turnId) }))) return undefined;
              // resume() restores the saved permissions; turn() narrows the whole tree to today's decision.
              const turn = await run.turn({ identity: identity(turnId), sequence: 0, capture: decision });
              turn.attributes({ 'agent.channel.audience': given ?? audience });
              return turn;
            },
            async step(turn, turnId, at) {
              const key = `${turnId}:${at?.execution ?? ''}`;
              let numbered = steps.get(key);
              if (!numbered) {
                const entry = (numbered = { drive: streams.nextDrive(runId, turnId), next: 0 });
                steps.set(key, entry);
                entry.drive.catch(() => steps.get(key) === entry && steps.delete(key));
                if (steps.size > 10_000) steps.delete(steps.keys().next().value!);
              }
              const stepIndex = at?.stepIndex ?? numbered.next++;
              return turn.attempt({ stepIndex, attempt: await numbered.drive });
            },
            parkedTools: turnId => streams.parkedTools(runId, turnId),
            parkTools: (turnId, tools) => streams.parkTools(runId, turnId, tools),
            flush: () => streams.flush(runId),
          };
        },
      };
      (globalThis as { [AGENT_RUNS]?: AgentRunsHook })[AGENT_RUNS] = hook;
    },
    async forceFlush() {
      await Promise.all([...live].map((reference) => {
        const streams = reference.deref();
        if (streams) return streams.flush();
        live.delete(reference);
      }));
      await provider?.forceFlush();
    },
    async shutdown() {
      await this.forceFlush();
      await provider?.shutdown();
    },
  };
}
