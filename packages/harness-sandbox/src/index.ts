/**
 * harness-world-sandbox: the sandbox tier (docs/workspaces.md §5). A session's
 * Vercel Sandbox VM and Drive, armed with the runtime's fence, as a branded
 * `SessionMachine` (`sandboxSession`, and `attachInDrive` for the kernel); the
 * base snapshot (`prepareBase`, `baseRecipe`); the names. The in-VM relay and
 * the driver's link are internal. Not here, on purpose: `brandForTests` and the
 * stand-in (`test/local-machine.ts`), which no published entry may carry (§6
 * rule 2).
 */
export { apiErrorOf, ApiConflict, resolveProjectId, vercelSandboxApi } from './api.ts';
export type { ApiCredentials, BootSource, CleanupApi, CreateSandbox, DriveInfo, RunResult, SandboxApi, SandboxNetworkPolicy, VmHandle, VmLayout, VmStatus } from './api.ts';
export { cleanupIdle, deleteSession, rateLimiter, SESSION_NAME, SessionInUse } from './cleanup.ts';
export type { CleanupOptions, CleanupResult, CleanupSkip, DeletedSession, DeleteOptions, IfIdle } from './cleanup.ts';
export { baseName, baseRecipe, CLAUDE_CLI, cliPath, CODEX_CLI, devRelayArtifact, prepareBase } from './base.ts';
export type { BaseRecipe, PinnedCli, PrepareBaseOptions, RelayArtifact } from './base.ts';
export { defaultRegion, sessionName } from './names.ts';
export { acknowledgeInDrive, acknowledgeNotes, attachInDrive, FILES_LOST_NOTE, kernelSandbox, networkPolicy, parseArmReply, SANDBOX_DOC, SandboxBudgetExceeded, SandboxLost, sandboxSession } from './session.ts';
export type { KernelSandbox, SandboxNetworkOption, SandboxNetworkSession, SandboxNetworkValue, SandboxSessionOptions, SandboxSpec } from './session.ts';
export { DRIVE_FILES_DOC, DRIVE_FILES_EVENT, DRIVE_MANIFEST_LIMITS, DRIVE_ROOT, DriveFiles, driveManifestBehind, listDrive, mutatingCalls, readDriveManifest, SKIPPED_DIRS } from './files.ts';
export type { DriveEntry, DriveFile, DriveFilesEvent, DriveFilesOptions, DriveManifest, DriveReader, DriveSnapshot, ListDriveOptions } from './files.ts';
export { modelTunnel } from './tunnel.ts';
export { AGENT, CACHE_ROOT, MODEL_PORT, RELAY_COMMAND, RELAY_CONFIG, RELAY_PORT, WORKSPACE } from './vm.ts';
