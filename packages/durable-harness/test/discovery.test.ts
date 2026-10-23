import { describe, expect, it } from 'vitest';
import { harnessModule } from '../src/discovery.ts';

const CORE = "import { durableAgent } from 'durable-harness'; ";

describe('automatic harness discovery', () => {
  it.each([
    'export const sample = "durableAgent({})"',
    'export const sample = `durableAgent({})`',
    'export const sample = /durableAgent\\(/',
    '/* export const agent = durableAgent({}) */ export const sample = null',
    '// export const agent = durableAgent({})\nexport const sample = null',
    'const agent = durableAgent({}); await agent.session().prompt("hi").result',
    'export function makeAgent() { return durableAgent({}) }',
    'export const makeAgent = () => durableAgent({})',
    'export const makeAgent = async () => durableAgent({})',
    'export default function makeAgent() { return durableAgent({}) }',
    'export class Factory { agent = durableAgent({}) }',
    'export const factory = { make() { return durableAgent({}) } }',
    'const agent = durableAgent({}); export type Agent = typeof agent',
    'const agent = durableAgent({}); export const sample: typeof agent = null',
    'const agent = durableAgent({}); export { type agent }',
    'function helper(durableAgent: Function) { return durableAgent({}) }; export const value = helper(() => null)',
  ])('ignores samples, scripts, factories and type-only exports: %s', (body) => {
    expect(harnessModule(CORE + body).agent).toBe(false);
  });

  it.each([
    "export const sample = `import { durableAgent } from 'durable-harness'; durableAgent({})`",
    "import type { durableAgent } from 'durable-harness'; export const agent = durableAgent({})",
    "import { type durableAgent } from 'durable-harness'; export const agent = durableAgent({})",
    "import { flowHandler } from 'durable-harness'; const durableAgent = () => null; export const agent = durableAgent({})",
    "import { createCodingTools } from 'durable-harness/pi'; export const tools = createCodingTools({})",
    "import { pi } from 'durable-harness/pi'; export { pi }",
    "import { pi } from 'durable-harness/pi'; const agent = pi({}); export function helper(agent: unknown) { return agent }",
  ])('requires a real call of an imported agent factory: %s', (text) => {
    expect(harnessModule(text).agent).toBe(false);
  });

  it.each([
    'export const agent = durableAgent<Options>({})',
    'export const agent = <Agent>durableAgent({})',
    'const make = durableAgent; export const agent = make({})',
    'const agent = durableAgent({}); const getAgent = () => agent; export { getAgent }',
    'const agent = durableAgent({}); function getAgent() { return agent }; export default getAgent',
    'export const agent = wrap(durableAgent({}))',
    'export const agent = ready ? durableAgent({}) : durableAgent({})',
    'const agent = durableAgent({}); export const config = { agent }',
    'let agent; agent = durableAgent({}); export { agent }',
    'module.exports = durableAgent({})',
    'module.exports.agent = durableAgent({})',
    'const agent = durableAgent({}); exports.agent = agent',
  ])('follows actual calls and value exports: %s', (body) => {
    expect(harnessModule(CORE + body).agent).toBe(true);
  });

  it('preserves effect-only pi opener registration and resolves aliases', () => {
    const result = harnessModule("import { resumeWith as register, openWorldStorage as storage } from 'durable-harness/pi-durable'; register(async () => null); export async function POST() { return storage() }");
    expect(result).toMatchObject({ agent: true, piOpener: true, piSessions: true });
    expect(harnessModule("import { resumeWith } from 'durable-harness/pi-durable'; export function register() { resumeWith(async () => null) }").piOpener).toBe(false);
    expect(harnessModule("import { resumeWith } from 'durable-harness/pi-durable'; export const sample = 'resumeWith()'").piOpener).toBe(false);
  });

  it('does not evaluate template substitutions or confuse them with literal samples', () => {
    expect(harnessModule(CORE + 'export const example = `example: ${durableAgent({})}`').agent).toBe(true);
  });

  it('leaves syntax it cannot read to the explicit agents option', () => {
    expect(harnessModule(CORE + 'export const agent = durableAgent(').agent).toBe(false);
  });
});
