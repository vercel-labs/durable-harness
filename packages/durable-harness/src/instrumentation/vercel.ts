import { TraceFlags } from '@opentelemetry/api';
import type { ReadableSpan, SpanProcessor } from '@opentelemetry/sdk-trace-base';

type Reporter = (payload: unknown) => void;
type Serializer = typeof import('@opentelemetry/otlp-transformer').JsonTraceSerializer;

/** The current invocation's span reporter (Vercel's request context, where @vercel/otel's automatic exporter reports). */
function requestReporter(): Reporter | undefined {
  const reader = (globalThis as Record<symbol, { get?: () => { telemetry?: { reportSpans?: Reporter } } | undefined } | undefined>)[Symbol.for('@vercel/request-context')];
  try {
    const telemetry = reader?.get?.()?.telemetry;
    return telemetry?.reportSpans?.bind(telemetry);
  } catch {
    return undefined;
  }
}

/**
 * Vercel's Agent Runs destination: each sampled span, as it ends, reported
 * through the request context of the invocation that started it (else the
 * one it ends in). Off Vercel there is no request context and it reports
 * nothing.
 */
export function vercelSpanProcessor(serializer: Serializer, onError: (error: unknown) => void): SpanProcessor {
  const reporters = new WeakMap<object, Reporter>();
  return {
    onStart(span) {
      const report = requestReporter();
      if (report) reporters.set(span, report);
    },
    onEnd(span: ReadableSpan) {
      if ((span.spanContext().traceFlags & TraceFlags.SAMPLED) === 0) return;
      const report = reporters.get(span) ?? requestReporter();
      if (!report) return;
      try {
        const bytes = serializer.serializeRequest([span]);
        if (!bytes) throw new Error('could not serialize the span');
        report(JSON.parse(new TextDecoder().decode(bytes)));
      } catch (error) {
        onError(error);
      }
    },
    forceFlush: async () => {},
    shutdown: async () => {},
  };
}
