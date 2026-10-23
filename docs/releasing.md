# Releasing durable-harness

One package is published from this repo: [`durable-harness`](../packages/durable-harness)
(unscoped, on npm as `durable-harness`). Every other workspace package is
private; the kernel, the harness adapters and pi-world are bundled into its
`dist`, so they never need a release of their own. Versions and changelogs are
managed with [changesets](https://github.com/changesets/changesets), set up as
in vercel/workflow, and the [Release workflow](../.github/workflows/release.yml)
publishes to npm with trusted publishing (no npm token in the repo).

## The 0.0.x rule

durable-harness stays on **0.0.x**: every release is a patch (0.0.1, 0.0.2,
...). `pnpm check:changesets` ([scripts/check-changesets.mjs](../scripts/check-changesets.mjs))
fails when

- a changeset bumps durable-harness by `minor` or `major` (0.1.0 or 1.0.0 next),
- a changeset names any other package (they are private: it would release
  nothing),
- durable-harness's version on the commit is outside 0.0.x, or the pending
  changesets cannot be assembled into a release plan (which would break
  `changeset version` on main).

CI runs it on every pull request in the separate `release-policy` job. The
command includes regression tests that reject minor/major changesets and
manually edited versions outside 0.0.x, including prereleases. It also accepts
ordinary patch releases and consumed Version Packages pull requests. The Release
workflow runs the same command before building. Both `pnpm ci:version` and
`pnpm ci:publish` also run it before versioning or publishing.

Leaving 0.0.x requires an **explicit human maintainer decision** and a separately
reviewed policy change to the guard (`LINE`) and its tests. A request to ship or
publish a release, or merging an automatically generated Version Packages pull
request, does not authorize changing this policy. There is no automatic major or
minor override.

## Adding a changeset

Every pull request that changes what durable-harness ships (its own `src`, or
any workspace package it bundles: `harness-world`, `harness-world-*`,
`pi-world`) adds a changeset:

```bash
pnpm changeset --patch durable-harness -m "OpenCode keeps its working directory across a resume"
# or interactively: pnpm changeset
```

That writes `.changeset/<random-name>.md`:

```md
---
"durable-harness": patch
---

OpenCode keeps its working directory across a resume
```

Commit it with the change. The summary becomes the CHANGELOG entry, with the
commit and pull request linked. Docs, the demo, tests and private packages
nobody ships need no changeset.

## How a release happens

The Release workflow runs on every push to `main` (and by hand, Actions >
Release > Run workflow):

1. **Changesets pending: version.** It opens or updates the "Version
   Packages" pull request (branch `changeset-release/main`). Its commit is
   `pnpm ci:version` (`changeset version`): the next patch version in
   `packages/durable-harness/package.json`, the entry in its `CHANGELOG.md`,
   and the consumed changesets deleted. The commit goes through the GitHub API,
   so it is signed, as the repo's rulesets require. The pull request is opened
   with a token broker's token once that is switched on (see "The release pull
   request's token" below), so its CI runs. Until then it is opened with
   `GITHUB_TOKEN`, so its CI does not start by itself: push to its branch, or
   close and reopen it, to run CI before merging.
2. **No changesets pending: publish.** Merging the Version Packages pull
   request leaves none, and the next run publishes: `pnpm ci:publish` builds
   durable-harness, checks its `dist` (`check-dist`: every import declared,
   every entry built, and no entry still holding a stub that is not released
   yet, `src/unreleased.ts`) and runs `changeset publish`, which publishes every public version not yet on npm (`pnpm
   publish`, run in GitHub Actions with the job's OIDC token). It tags
   `durable-harness@<version>` and creates a GitHub Release from the CHANGELOG
   entry.
3. **Verify.** [scripts/check-published.mjs](../scripts/check-published.mjs)
   checks that the version on the commit is on npm under `latest`, retrying for
   about ten minutes while the registry catches up, and fails the job if not.
   It runs on every push, so a version that never made it to npm keeps the job
   red until it does; and if changesets are pending while a version on `main`
   was never published (a release PR merged stale), the same run publishes it
   before refreshing the release PR.

A merge made with `GITHUB_TOKEN` does not trigger workflows. A person merging
the Version Packages pull request does; if a run is ever missing, run the
workflow by hand.

The published `package.json` names npm ranges for its `@workflow/*`
dependencies (caret ranges on the Workflow SDK's npm releases, no `catalog:`), its
`exports` point at `dist` (`publishConfig.exports`), and `prepack` builds
`dist`. To see exactly what would ship:

```bash
cd packages/durable-harness && pnpm pack --pack-destination /tmp/durable-harness-pack
pnpm exec changeset publish-plan   # what `changeset publish` would publish now
```

## One-time setup

npm can only trust a workflow for a package that exists, so the first versions
were published by hand: prereleases of 0.0.1 (`0.0.1-beta.N`), each packed from
`main` and published under `latest`. The version in
`packages/durable-harness/package.json` on `main` is always one that is on npm
under `latest`: the Release workflow publishes any version on `main` that npm
does not have, so a version that was never published (or a prerelease that was
not committed back) would be published from `main` as soon as the workflow can.
A hand-published prerelease is committed to `main` right after it is on npm.
The pending changeset still makes the next version 0.0.1 (a patch of a
prerelease is its release).

1. **A prerelease by hand** (only before the workflow is trusted). On a
   checkout of `main`, set the next `0.0.1-beta.N` in
   `packages/durable-harness/package.json`, then, logged in to npmjs.com as an
   owner of the package:

   ```bash
   pnpm install --frozen-lockfile
   pnpm --filter durable-harness build
   cd packages/durable-harness && pnpm pack --pack-destination /tmp/durable-harness-pack
   npm publish /tmp/durable-harness-pack/durable-harness-0.0.1-beta.N.tgz --tag latest --registry https://registry.npmjs.org/
   npm dist-tag add durable-harness@0.0.1-beta.N beta --registry https://registry.npmjs.org/
   ```

   `check-dist` refuses a build that still holds a stub that is not released
   yet, which is why a prerelease is packed rather than published with
   `pnpm ci:publish`. Move `beta` once `npm view durable-harness@0.0.1-beta.N`
   answers: npm takes a moment to make a new version visible. Then commit the
   version to `main`.
2. **Trust the Release workflow on npmjs.com.** On
   <https://www.npmjs.com/package/durable-harness/access>, under Trusted
   Publisher, choose GitHub Actions and enter organization `vercel-labs`,
   repository `durable-harness`, workflow filename `release.yml`, no
   environment. Then, under Publishing access, choose "Require two-factor
   authentication and disallow tokens": from then on only the workflow
   publishes, and 0.0.1 is published by merging the Version Packages pull
   request.
3. **Check it.** Run the Release workflow by hand: the check-published step
   should report the version on `main` as `ok`.

npm adds a provenance attestation to every version the workflow publishes
(the repository is public, and `repository` in the package.json points at
it).

GitHub side: the repository must let Actions create pull requests (Settings >
Actions > General, "Allow GitHub Actions to create and approve pull
requests"). The release commit and tags are signed by GitHub.

## The release pull request's token

A pull request opened with `GITHUB_TOKEN` does not run CI. So the Release
workflow can open the Version Packages pull request with a token from a
GitHub token broker instead: it exchanges the job's OIDC token for a one-hour
GitHub App token scoped to this repository with `contents: write` and
`pull_requests: write`, under a policy that allows `release.yml` on
`refs/heads/main`, on push or workflow_dispatch. Everything else the workflow
does (tags, GitHub Releases) stays on `GITHUB_TOKEN`.

It is configured with three repository variables:

| Variable | What |
| --- | --- |
| `GH_STS_URL` | The broker's base URL (its host is also the OIDC audience) |
| `GH_STS_POLICY` | The broker policy's id |
| `GH_STS_RELEASE` | `true` to use it; unset, the pull request is opened with `GITHUB_TOKEN` |

```bash
gh variable set GH_STS_RELEASE --body true --repo vercel-labs/durable-harness
```

Set `GH_STS_RELEASE` only once the policy exists: a broker may count denied
requests against the caller's IP, and GitHub's runner IPs are shared. With the
variable set, a failed exchange is a warning on the run, and the pull request
is opened with `GITHUB_TOKEN` as before. Unset it to go back. The workflow's
last step revokes the token.
