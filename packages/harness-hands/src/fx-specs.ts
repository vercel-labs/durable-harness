/**
 * libfx's kernel tools as the model sees them: names and input schemas
 * vendored from fx (https://github.com/vercel-labs/fx, Apache-2.0),
 * `src/builtins/tools.zig` at commit 1a070af32cdfaf8cd0da6de3dc7f053302828b17
 * (tag v0.0.14, the commit libfx 0.0.14 is built from), rendered the
 * way `src/core/tooling/model_tool_schema.zig` writes a builtin's schema (keys
 * in its order: type, enum, minLength, maxLength, minimum, maximum,
 * description). `shell` is the process-only request union
 * (`shellProcessOnlySpec()`: run, interact and stop, no TTY), the one fx
 * advertises when it has no terminal.
 *
 * Names and schemas are the kernel's byte for byte (fx-tools.test.ts checks
 * them against the pinned libfx's native addon), so a model tuned on fx sees
 * the tools it knows. The descriptions are fx's with a reviewed list of
 * edited clauses (`FX_EDITS`): no paths outside the workspace, no image
 * attachments, and a shell whose commands complete in one call.
 */

/** Where the definitions come from. */
export const FX_SOURCE = Object.freeze({
  repository: 'vercel-labs/fx',
  path: 'src/builtins/tools.zig',
  commit: '1a070af32cdfaf8cd0da6de3dc7f053302828b17',
  libfx: '0.0.14',
});

/** The tools, in the order fx advertises them (`advertisement_order` in tools.zig). */
export const FX_TOOL_NAMES = ['read_file', 'glob_files', 'grep_files', 'edit_file', 'write_file', 'shell'] as const;
export type FxToolName = (typeof FX_TOOL_NAMES)[number];

const FILE_PATH =
  'File path relative to the workspace root, or an external path using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy.';
const SEARCH_ROOT =
  'Optional search root relative to the workspace root, or an external path using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy. Omit this field to use the current directory; never send an empty string. Narrow it when possible.';
const SESSION_ID = 'Owned execution handle returned by shell.run.';
/** `terminal_contracts.max_command_bytes`. */
const MAX_COMMAND_BYTES = 64 * 1024;
/** `managed_execution_contract.max_yield_time_ms`, `max_wait_ceiling_ms`. */
const MAX_YIELD_TIME_MS = 30_000;
const MAX_WAIT_CEILING_MS = 300_000;

/** fx's descriptions, verbatim. */
export const FX_DESCRIPTIONS: Readonly<Record<FxToolName, string>> = Object.freeze({
  read_file:
    'Read one file with bounded line-numbered output and optional start_line/line_count range. UTF-8 text returns as numbered lines; image files (PNG, JPEG, GIF, WebP up to 3.9MB) attach to the result so you can see them. Paths may be workspace-relative or external using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy. When to use: inspect an exact known path before editing or explaining code, or view an image file. When NOT to use: list directories, search many files, read non-image binary data, or bypass dedicated search tools.',
  glob_files:
    'Find file paths matching a glob pattern, with mode=count for exact path counts without listing entries. Paths may be workspace-relative or external using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy. When to use: locate files by name, extension, or directory pattern; narrow path or pattern if candidate caps appear. When NOT to use: search file contents, read files, run find, or count non-file concepts.',
  grep_files:
    'Search text files for a literal substring, optionally narrowed by path/include, with output modes for matching lines, files-with-matches, or counts plus head_limit/offset pagination and bounded context_lines for matches mode. Paths may be workspace-relative or external using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy. Use include as the type/path filter, such as *.zig. When to use: find exact symbols, strings, TODOs, or usage sites. When NOT to use: regex is not supported; avoid unknown-concept exploration, filename lookup, known-path reads, and shell grep; do not repeat the same or equivalent search after a caller search only finds a definition.',
  edit_file:
    'Edit an existing file by replacing one exact old_string occurrence with new_string. Paths may be workspace-relative or external using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy. When to use: make a focused patch after reading the file. When NOT to use: broad rewrites, ambiguous repeated text, generated formatting, missing files, or cross-file refactors.',
  write_file:
    'Create or overwrite a file using complete contents. Paths may be workspace-relative or external using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy. When to use: add a new file or intentionally replace an entire generated/small file. When NOT to use: targeted edits to existing files, partial replacements, deleting files, or unapproved external paths.',
  shell:
    "Run every command with shell.run. Fast commands complete in one call; commands still running after yield_time_ms return one owned session_id and remain available across turns. Use shell.interact with that exact session_id: omit chars to observe, or provide chars to send exact input and then observe. Use shell.stop only when termination is requested. output_delta is always terminal-safe; unsafe bytes are escaped while full_output_handle retains exact output, so do not run a separate command merely to test output safety or shell usability. Never detach with &, nohup, setsid, or double-forking. Each call starts a new shell with the user's startup files applied: their aliases, functions, and PATH are available, but cd, export, and alias changes do not carry over to the next call. In zsh, quote glob patterns meant for another program (for example '--include=*.zig') because unmatched globs are errors, and quote words that begin with =.",
});

/** fx's input schemas, verbatim (as the kernel renders them). */
export const FX_SCHEMAS: Readonly<Record<FxToolName, object>> = Object.freeze({
  read_file: {
    type: 'object',
    properties: {
      path: { type: 'string', description: FILE_PATH },
      start_line: { type: 'integer', description: 'Optional 1-based first line to return. Defaults to 1.' },
      line_count: { type: 'integer', description: 'Optional positive number of lines to return. Defaults to the normal read cap and is bounded.' },
    },
    required: ['path'],
  },
  glob_files: {
    type: 'object',
    properties: {
      pattern: { type: 'string', description: 'Glob pattern to match, such as src/**/*.zig or *.md.' },
      path: { type: 'string', minLength: 1, description: SEARCH_ROOT },
      mode: { type: 'string', enum: ['matches', 'count'], description: 'Use matches to return sample paths, or count to return an exact matching path count without listing entries.' },
    },
    required: ['pattern'],
  },
  grep_files: {
    type: 'object',
    properties: {
      pattern: { type: 'string', description: 'Literal plain-text pattern to search for.' },
      path: { type: 'string', minLength: 1, description: SEARCH_ROOT },
      include: { type: 'string', description: 'Optional glob pattern applied to candidate file paths before reading files, such as *.zig or src/**/*.ts.' },
      case_insensitive: { type: 'boolean', description: 'Search case-insensitively when true.' },
      mode: {
        type: 'string',
        enum: ['matches', 'files_with_matches', 'count'],
        description: 'Use matches for line matches, files_with_matches for unique matching paths, or count for exact matching-line and matching-file counts.',
      },
      head_limit: { type: 'integer', description: 'Optional positive maximum results to return for matches or files_with_matches. Defaults to the normal output cap.' },
      offset: { type: 'integer', description: 'Optional zero-based result offset for matches or files_with_matches pagination. Defaults to 0.' },
      context_lines: { type: 'integer', description: 'Optional non-negative number of lines before and after each emitted match in matches mode. Bounded by the tool.' },
    },
    required: ['pattern'],
  },
  edit_file: {
    type: 'object',
    properties: {
      path: { type: 'string', description: FILE_PATH },
      old_string: { type: 'string', description: 'Exact text to find in the file. Must match exactly once.' },
      new_string: { type: 'string', description: 'Text to replace old_string with.' },
    },
    required: ['path', 'old_string', 'new_string'],
  },
  write_file: {
    type: 'object',
    properties: {
      path: { type: 'string', description: FILE_PATH },
      content: { type: 'string', description: 'Complete file contents to write.' },
    },
    required: ['path', 'content'],
  },
  shell: {
    type: 'object',
    properties: {
      request: {
        oneOf: [
          {
            type: 'object',
            properties: {
              action: { type: 'string', enum: ['run'] },
              command: { type: 'string', maxLength: MAX_COMMAND_BYTES, description: 'Shell command to execute exactly once.' },
              cwd: { type: 'string', description: 'Working directory; defaults to the workspace.' },
              profile: { type: 'string', enum: ['clean', 'user'], description: 'Defaults to user; clean skips user startup files. Mutually exclusive with shell.' },
              yield_time_ms: {
                type: 'integer',
                minimum: 0,
                maximum: MAX_YIELD_TIME_MS,
                description: 'Initial observation window. Defaults to 30000; use 0 to return the owned running handle immediately.',
              },
              timeout_ms: {
                type: 'integer',
                minimum: 1,
                description:
                  'Set only when the user explicitly requests a finite deadline. Omit for commands intended to remain running, receive input, continue across turns, or be stopped later.',
              },
            },
            additionalProperties: false,
            required: ['action', 'command'],
          },
          {
            type: 'object',
            properties: {
              action: { type: 'string', enum: ['interact'] },
              session_id: { type: 'string', description: SESSION_ID },
              yield_time_ms: {
                type: 'integer',
                minimum: 0,
                maximum: MAX_WAIT_CEILING_MS,
                description:
                  'Wait before yielding output. Empty observations wait 5000-300000 ms; shorter values are raised to 5000. Non-empty input is capped at 30000 ms and keeps shorter requested waits. Defaults to 5000. If the process remains running, interact with the same session_id again; never rerun it.',
              },
            },
            additionalProperties: false,
            required: ['action', 'session_id'],
          },
          {
            type: 'object',
            properties: {
              action: { type: 'string', enum: ['stop'] },
              session_id: { type: 'string', description: SESSION_ID },
              force: { type: 'boolean', description: 'Use immediate force termination when true. Defaults to false.' },
            },
            additionalProperties: false,
            required: ['action', 'session_id'],
          },
        ],
      },
    },
    additionalProperties: false,
    required: ['request'],
  },
});

/** One reviewed change to an fx description: `from` occurs in it exactly once, and becomes `to`. */
export interface FxEdit {
  from: string;
  to: string;
  /** Why the clause changes. */
  why: string;
}

const PATHS: FxEdit = {
  from: 'Paths may be workspace-relative or external using an absolute path, ~/..., or a relative workspace escape such as ../...; external access is subject to permission policy.',
  to: 'Paths are workspace-relative, or absolute under /workspace; a path outside the workspace is refused.',
  why: 'a virtual workspace has nothing outside /workspace, and the permission policy refuses every external path',
};

/**
 * The reviewed edits to fx's descriptions for a virtual workspace, per
 * tool. Each one says what the tool does here, where fx's text describes the
 * CLI on a real machine.
 */
export const FX_EDITS: Readonly<Record<FxToolName, readonly FxEdit[]>> = Object.freeze({
  read_file: [
    {
      from: ' image files (PNG, JPEG, GIF, WebP up to 3.9MB) attach to the result so you can see them.',
      to: ' binary files are reported by size, not shown.',
      why: 'no image attachments on a virtual workspace',
    },
    PATHS,
    { from: 'explaining code, or view an image file.', to: 'explaining code.', why: 'no image attachments' },
    { from: 'read non-image binary data', to: 'read binary data', why: 'no image attachments' },
  ],
  glob_files: [PATHS],
  grep_files: [PATHS],
  edit_file: [PATHS],
  write_file: [PATHS, { from: ', deleting files, or unapproved external paths.', to: ', or deleting files.', why: 'there are no external paths to approve' }],
  shell: [
    {
      from: 'Fast commands complete in one call; commands still running after yield_time_ms return one owned session_id and remain available across turns. Use shell.interact with that exact session_id: omit chars to observe, or provide chars to send exact input and then observe. Use shell.stop only when termination is requested.',
      to: "Every command completes in its run call: it runs to its end or to the workspace's time limit, and nothing keeps running after it, so there is no session_id to interact with or stop.",
      why: 'a virtual shell has no processes that outlive a call (shell.interact and shell.stop answer that no such shell exists)',
    },
    {
      from: 'unsafe bytes are escaped while full_output_handle retains exact output, so',
      to: 'unsafe bytes are escaped, so',
      why: 'no full output handle',
    },
    {
      from: "Each call starts a new shell with the user's startup files applied: their aliases, functions, and PATH are available, but cd, export, and alias changes do not carry over to the next call.",
      to: 'Each call starts a new bash-compatible shell with no startup files: cd, export, and alias changes do not carry over to the next call.',
      why: 'no user startup files on a virtual workspace',
    },
    {
      from: " In zsh, quote glob patterns meant for another program (for example '--include=*.zig') because unmatched globs are errors, and quote words that begin with =.",
      to: '',
      why: 'the shell is bash-compatible, never zsh',
    },
  ],
});

/** Apply `edits` to `text`; each `from` must occur exactly once. */
export function applyEdits(text: string, edits: readonly FxEdit[]): string {
  let out = text;
  for (const edit of edits) {
    const at = out.indexOf(edit.from);
    if (at === -1 || out.indexOf(edit.from, at + 1) !== -1) {
      throw new Error(`harness-world-hands: fx description edit does not apply exactly once: ${JSON.stringify(edit.from)}`);
    }
    out = out.slice(0, at) + edit.to + out.slice(at + edit.from.length);
  }
  return out;
}

/** fx's description of `name` with the reviewed edits for a virtual workspace. */
export function fxDescription(name: FxToolName): string {
  return applyEdits(FX_DESCRIPTIONS[name], FX_EDITS[name]);
}
