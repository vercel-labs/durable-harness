import { agentRuns } from 'durable-harness/instrumentation/otel';

// The demo's sessions are public in Agent Runs, their prompts, answers and
// tool content included: the demo is SSO-protected and its prompts are demo
// prompts. (A session that records its own audience keeps it.)
export default agentRuns({ audience: 'public' });
