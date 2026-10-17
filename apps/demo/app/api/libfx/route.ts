import { fxAgent, log, refuseDisabled } from '@/lib/fx-durable';

// Interim, as the libfx durability doc says: Vercel Queues delivers libfx's
// session messages here until world-vercel can deliver to the function
// itself. vercel.json subscribes this route to libfx's topic and gives it
// 60 s, so a long turn stops at a safe point before the deadline and
// continues in the next invocation.
let wake: Promise<(request: Request) => Promise<Response>> | undefined;

/** `agent.wakeHandler()`, logging how long each delivery took and whether it asked to come back. */
export async function POST(request: Request): Promise<Response> {
  const disabled = refuseDisabled();
  if (disabled) return disabled;
  const started = Date.now();
  wake ??= fxAgent().then((agent) => agent.wakeHandler());
  const response = await (await wake)(request);
  const body = await response.clone().text().catch(() => '');
  log('wake', { ms: Date.now() - started, status: response.status, body: body.slice(0, 120) });
  return response;
}
