import type { Context } from '@earendil-works/chord';
import type { AssistantMessage, Message } from '@earendil-works/pi-ai';
import type { MemoryStorage, StorageWrite } from '@earendil-works/pi-durable';
import type { AttemptOperation, ModelOperation, ToolOperation, TurnOperation } from '@vercel/agent-runs';
import type { SessionTracing } from 'harness-world/agent-runs';
import { piModelOptions, piModelResult, piStepAttributes } from 'harness-world-pi/agent-runs';

type Entry = Awaited<ReturnType<MemoryStorage['scanEntries']>>['items'][number];
type TaskWrite = Extract<StorageWrite, { type: 'task' }>['value'];

const record = (v: unknown): Record<string, unknown> => (v !== null && typeof v === 'object' ? (v as Record<string, unknown>) : {});
const userText = (message: Message | undefined) =>
  message?.role !== 'user' ? undefined : typeof message.content === 'string' ? message.content : message.content.flatMap((p) => (p.type === 'text' ? [p.text] : [])).join('\n');

/**
 * Agent Runs for a pi-durable session, from the commits its storage accepts:
 * a turn per input, a step per generation task with its model call, and a
 * tool call per tool task. A step's index is its assistant entry's place in
 * the journal, so a wake in another process finds the same step (and its open
 * tool calls) in the turn's checkpoint. Replaying a journal traces nothing.
 */
export class PiTracing {
  private readonly turns = new Map<string, Promise<TurnOperation | undefined>>();
  /** Open steps by turn, newest last, with their model call while it runs. */
  private readonly steps = new Map<string, { index: number; op: AttemptOperation; model?: ModelOperation; task?: string }[]>();
  private readonly tools = new Map<string, ToolOperation>();
  /** The input each conversation is answering (the turn its tasks belong to), as this process saw it placed. */
  private readonly active = new Map<string, string>();

  constructor(
    private readonly session: SessionTracing,
    private readonly memory: MemoryStorage
  ) {}

  async commit(writes: readonly StorageWrite[], context: Context): Promise<void> {
    for (const write of writes) {
      if (write.type === 'submission' && write.value.type === 'input' && write.value.status === 'placed') this.active.set(String(write.value.conversationId), String(write.value.id));
    }
    for (const write of writes) {
      if (write.type !== 'task') continue;
      const task = write.value;
      if (task.kind !== 'pi.generation' && task.kind !== 'pi.tool') continue;
      const checkpoint = task.state.status === 'running' ? record(task.state.checkpoint) : undefined;
      if (task.kind === 'pi.generation' && checkpoint?.phase === 'request') await this.startGeneration(task, checkpoint, context);
      if (task.kind === 'pi.tool' && checkpoint?.phase === 'execute') await this.startTool(task, checkpoint, context);
      if (task.state.status === 'terminal') {
        const outcome = task.state.outcome;
        const entry = writes.find((w) => w.type === 'entry' && w.value.byTaskId === task.id && (w.value.kind === 'pi.assistant' || w.value.kind === 'pi.tool-result'));
        const output = entry?.type === 'entry' ? entry.value.model : outcome.status === 'completed' ? outcome.result : undefined;
        await this.end(task, output, context, outcome.status === 'completed' ? undefined : new Error(String(record(outcome).error ?? `Pi task ${outcome.status}`)));
      }
    }
    // A generation can wait for its tools before its task is terminal: its model call ends when the response commits.
    for (const write of writes) {
      if (write.type !== 'entry' || write.value.byTaskId === undefined) continue;
      if (write.value.kind === 'pi.assistant') await this.endModel(String(write.value.byTaskId), write.value.model?.[0] as AssistantMessage | undefined);
    }
    for (const write of writes) {
      if (write.type !== 'submission' || write.value.type !== 'input') continue;
      if (write.value.status !== 'done' && write.value.status !== 'unanswered') continue;
      const turnId = String(write.value.id);
      if (this.active.get(String(write.value.conversationId)) === turnId) this.active.delete(String(write.value.conversationId));
      const turn = await this.turnFor(turnId);
      for (const step of this.steps.get(turnId) ?? []) await step.op.complete();
      this.steps.delete(turnId);
      if (write.value.status === 'unanswered') await turn?.complete({ outcome: 'failed', failed: true, error: new Error('Pi input ended without an answer') });
      else await turn?.complete({ outcome: 'completed' });
      this.turns.delete(turnId);
    }
  }

  flush(): Promise<void> {
    return this.session.flush();
  }

  /** The turn: continued from its checkpoint, else started (with the session's first input as its title). */
  private turnFor(turnId: string, entries?: readonly Entry[]): Promise<TurnOperation | undefined> {
    let turn = this.turns.get(turnId);
    if (!turn) {
      turn = (async () => {
        const saved = await this.session.resume(turnId);
        if (saved || !entries) return saved;
        const users = entries.filter((e) => e.kind === 'pi.user');
        const title = userText(users.at(-1)?.model?.[0] as Message | undefined);
        return this.session.turn({ turnId, sequence: Math.max(0, users.length - 1), ...(title ? { title } : {}) });
      })();
      this.turns.set(turnId, turn);
    }
    return turn;
  }

  /** The input a task works for: Pi answers one input at a time, so the one placed in its conversation (a wake that did not see it placed asks the storage). */
  private async turnIdOf(task: TaskWrite, context: Context): Promise<string | undefined> {
    const conversation = String(task.conversationId);
    const known = this.active.get(conversation);
    if (known) return known;
    const placed = (await this.memory.scanSubmissions({ conversationId: task.conversationId, status: 'placed' }, 100, undefined, context)).items.filter((s) => s.type === 'input');
    const id = placed.at(-1)?.id;
    if (id !== undefined) this.active.set(conversation, String(id));
    return id === undefined ? undefined : String(id);
  }

  private async entries(task: TaskWrite, context: Context): Promise<readonly Entry[]> {
    return (await this.memory.scanEntries({ conversationId: task.conversationId }, 1000, undefined, context)).items;
  }

  private async startGeneration(task: TaskWrite, checkpoint: Record<string, unknown>, context: Context): Promise<void> {
    const turnId = await this.turnIdOf(task, context);
    if (!turnId) return;
    const steps = this.steps.get(turnId) ?? [];
    if (steps.some((s) => s.task === String(task.id))) return;
    const entries = await this.entries(task, context);
    const turn = await this.turnFor(turnId, entries);
    if (!turn || turn.finished) return;
    const index = entries.filter((e) => e.kind === 'pi.assistant').length;
    for (const step of steps.filter((s) => s.model === undefined)) await step.op.complete();
    const open = steps.filter((s) => s.model !== undefined);
    const op = turn.findAttempt({ stepIndex: index, attempt: 0 }) ?? (await turn.attempt({ stepIndex: index, attempt: 0 }));
    const model = record(checkpoint.model);
    const messages = entries.slice().reverse().flatMap((e) => (e.model ?? []) as Message[]);
    const call = await op.modelCall(piModelOptions({ provider: String(model.provider ?? 'unknown'), modelId: String(model.modelId ?? 'unknown') }, messages));
    this.steps.set(turnId, [...open, { index, op, model: call, task: String(task.id) }]);
  }

  private async startTool(task: TaskWrite, checkpoint: Record<string, unknown>, context: Context): Promise<void> {
    if (this.tools.has(String(task.id))) return;
    const turnId = await this.turnIdOf(task, context);
    if (!turnId) return;
    const entries = await this.entries(task, context);
    const turn = await this.turnFor(turnId, entries);
    if (!turn || turn.finished) return;
    const input = record(task.input);
    const callId = String(input.callId ?? task.id);
    const assistants = entries.filter((e) => e.kind === 'pi.assistant');
    const at = assistants.findIndex((e) => e.id === input.assistant);
    const message = assistants[at]?.model?.[0] as AssistantMessage | undefined;
    const call = message?.content.find((p) => p.type === 'toolCall' && p.id === callId);
    // Entries are newest first: the step's index counts the assistant entries before this one.
    const index = at < 0 ? assistants.length - 1 : assistants.length - 1 - at;
    const step = (this.steps.get(turnId) ?? []).find((s) => s.index === index)?.op ?? turn.findAttempt({ stepIndex: index, attempt: 0 });
    const tool = turn.findTool(callId) ?? (await step?.tool({ callId, name: call?.type === 'toolCall' ? call.name : 'unknown', arguments: checkpoint.arguments }));
    if (tool) this.tools.set(String(task.id), tool);
  }

  private async endModel(taskId: string, message: AssistantMessage | undefined, error?: unknown): Promise<void> {
    for (const steps of this.steps.values()) {
      const step = steps.find((s) => s.task === taskId && s.model);
      if (!step?.model) continue;
      const model = step.model;
      step.model = undefined;
      if (error !== undefined || !message) await model.fail(error ?? new Error('Pi generation ended without a response'));
      else {
        step.op.attributes(piStepAttributes(message));
        await model.complete({ outcome: 'completed', result: piModelResult(message) });
      }
    }
  }

  private async end(task: TaskWrite, output: unknown, context: Context, error?: unknown): Promise<void> {
    if (task.kind === 'pi.generation') return this.endModel(String(task.id), (output as AssistantMessage[] | undefined)?.[0] ?? (record(output).role === 'assistant' ? (output as AssistantMessage) : undefined), error);
    let tool = this.tools.get(String(task.id));
    this.tools.delete(String(task.id));
    if (!tool) {
      const turnId = await this.turnIdOf(task, context);
      tool = turnId ? (await this.turnFor(turnId))?.findTool(String(record(task.input).callId ?? task.id)) : undefined;
    }
    if (!tool) return;
    if (error !== undefined) await tool.fail(error);
    else await tool.complete({ outcome: 'completed', output });
  }
}
