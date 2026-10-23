# Release policy

- `durable-harness` stays on `0.0.x`. Normal releases may increment only the patch version.
- A request to ship, publish, or release changes does **not** authorize a major or minor bump, changing the release line, or weakening its checks.
- Changing the release line requires an explicit decision from a human maintainer and a separately reviewed policy change, including the guard and its tests. Do not infer that decision from a Changesets release PR.
- Run `pnpm check:changesets` before versioning or publishing. See [docs/releasing.md](docs/releasing.md) for the release workflow.
