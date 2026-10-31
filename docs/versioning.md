# Versioning and releases

Hudsonbox has one product version, reported as `hudsonbox X.Y.Z`. This document
owns the version scheme, where the version lives, how it is bumped and how a
release is published. Release contents are recorded in the
[changelog](../CHANGELOG.md).

## Scheme

Versions are [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)
`MAJOR.MINOR.PATCH`: three non-negative integers without leading zeros, with no
prefix, pre-release or build suffix. Git tags add a `v`: version `0.0.1` is tag
`v0.0.1` and GitHub release "hudsonbox v0.0.1".

- **PATCH** for compatible fixes and performance work.
- **MINOR** for compatible features.
- **MAJOR** for incompatible changes.

Before `1.0.0` the project is in initial development. Any `0.x` minor release
may break the HTTP API, SDKs, CLI flags, database schema, host/guest protocols
or operator configuration; each breaking change is listed under
"Breaking changes" in the changelog. `1.0.0` is a deliberate decision, never an
automatic bump.

The version describes the source tree. It does not imply that a build is
qualified for a host configuration; the
[supported configuration](compatibility.md) and its release envelope own that.

## Single source of truth

The root [VERSION](../VERSION) file holds the version on one line. Every
manifest that carries the product version must equal it:

| File | Field |
| --- | --- |
| [Cargo.toml](../Cargo.toml) | `[workspace.package] version`, inherited by every crate |
| [Cargo.lock](../Cargo.lock) | each workspace crate's entry |
| [api/openapi.json](../api/openapi.json) | `info.version` |
| [api/conformance/workflow.json](../api/conformance/workflow.json) | `api_version` |
| [sdk/python/pyproject.toml](../sdk/python/pyproject.toml) | `[project] version`; `hudsonbox.__version__` comes from the generated schema |
| [sdk/typescript/package.json](../sdk/typescript/package.json) | `version`, plus its lockfile root entries |
| [sdk/mcp/pyproject.toml](../sdk/mcp/pyproject.toml) | `[project] version` and the exact `hudsonbox-client` pin |
| `sdk/mcp/src/hudsonbox_mcp/__init__.py` | `__version__`, also the MCP server version |
| Generated `_schema.py` and `schema.ts` | the OpenAPI version, kept exact by `generate_api.py --check` |

Rust binaries read the version from Cargo at build time. Host capabilities
report the supervisor and guest image versions, and the shared capability
contract rejects any that differ from the controller's own version. Guest images record `VERSION` in
their manifest, so images must be rebuilt for each release.

`--version` prints `hudsonbox X.Y.Z` for the API test client and
`<binary> (hudsonbox) X.Y.Z` for services and operator tools, for example
`sandbox-api (hudsonbox) 0.0.1`.

## Version tool

[scripts/version.py](../scripts/version.py) uses only the Python standard
library:

```sh
python3 scripts/version.py show            # print VERSION
python3 scripts/version.py check           # every manifest matches; changelog has a section
python3 scripts/version.py set 0.2.0       # rewrite VERSION and every manifest
python3 scripts/version.py next auto       # print the next version without changing files
python3 scripts/version.py bump auto       # apply it; also major, minor or patch
python3 scripts/version.py changelog 0.2.0 --write   # prepend a generated section
python3 scripts/version.py notes 0.2.0     # print that section, as used for release notes
```

`check` runs in the required `docs-check` job with the tool's unit tests, so a
manifest that drifts from `VERSION` blocks merging. `set` changes only the
version text in each file.

### Automatic bumps

`auto` reads every commit message since the newest `v*` tag (all history when
none exists) and applies [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

| Commit | Bump |
| --- | --- |
| `type!:` subject or a `BREAKING CHANGE:` footer | major (minor before 1.0.0) |
| `feat:` | minor |
| `fix:` or `perf:` | patch |
| anything else (`docs`, `test`, `ci`, `chore`, `refactor`, `release`) | none |

The highest bump wins. When nothing qualifies, the tool prints
`no release needed` and exits successfully without changing files. Because PRs
are squash-merged, the PR title is the commit subject; follow the
[title convention](../CONTRIBUTING.md#commit-and-pr-titles) and mark breaking
changes with `!` or a `BREAKING CHANGE:` footer in the squash message.

## Release flow

1. **Prepare.** A maintainer runs the Release workflow manually from `main`
   ("Run workflow", input `bump`: `auto`, `patch`, `minor` or `major`). It runs
   `version.py bump`, prepends a generated changelog section, pushes branch
   `release/vX.Y.Z` and opens the PR "release: hudsonbox vX.Y.Z". The same steps
   can be done locally with the commands above.
2. **Review.** Edit the generated changelog section for accuracy: group related
   changes, call out breaking changes, upgrade steps and known limitations.
   Required checks must pass as for any PR.
3. **Merge.** Squash-merge the release PR. Merging is the only release approval.
4. **Publish.** On every push to `main`, the Release workflow reads `VERSION`.
   If tag `vX.Y.Z` does not exist, it creates an annotated tag on that commit
   and the GitHub release "hudsonbox vX.Y.Z" with the changelog section followed
   by GitHub's generated notes. An existing tag is never moved or replaced, and
   rerunning the workflow only fills in a missing release for an existing tag.

Releases currently publish source archives and notes only. No binaries, guest
images or SDK packages are uploaded. Do not create `v*` tags by hand; a tag
that does not match `VERSION` on `main` would confuse `auto`.

### Release token

Pull requests opened with the default `GITHUB_TOKEN` do not trigger other
workflows, so the release PR would have no `docs-check` or `rust-check` runs and
could not merge. Maintainers should add a repository secret `RELEASE_TOKEN`: a
fine-grained personal access token or GitHub App token for this repository with
**Contents: read and write** and **Pull requests: read and write**. The prepare
job uses it when present. Without it, the job falls back to `GITHUB_TOKEN`, and
a maintainer must push a commit to the release branch (for example
`git commit --allow-empty -m "ci: run release checks"`) to start the required
checks. The fallback also requires the repository setting "Allow GitHub Actions
to create and approve pull requests".

Publishing on `main` uses only `GITHUB_TOKEN` with `contents: write`. Tags and
releases created with it do not trigger further workflows.

## Compatibility

Each release's changelog lists API, SDK, protocol and schema changes and
upgrade steps. Host, guest kernel, Firecracker and image envelopes are owned by
[supported configuration](compatibility.md); upgrade order and recovery by
[operator recovery](operator-recovery.md); the HTTP contract by
[OpenAPI](openapi.md).
