# Contributing to Hudsonbox

Hudsonbox is being designed in public by Hudson Frontier. Start with the [README](README.md) and [documentation guide](docs/README.md). The runtime is partially implemented; the [development guide](docs/implementation/dev-env.md) distinguishes local checks, controlled VM evidence and unfinished release gates.

The project is released under [Apache-2.0](LICENSE); contributions are accepted under the same terms, as its section 5 provides. No CLA or commit-signoff requirement is currently configured.

## From idea to pull request

1. **Agree on the problem.** Small fixes may go straight to a PR. Open an issue first for substantial features, architecture changes, or new dependencies. Describe the use case, scope, and acceptance criteria. Report vulnerabilities [privately](SECURITY.md).
2. **Create a branch.** Outside contributors fork the repository; maintainers use a branch in this repository. Start from current `main`. Use a descriptive name such as `feat/sandbox-create`, `fix/snapshot-cleanup`, or `docs/auth-flow`. Coding agents use `codex/` branches in isolated worktrees.
3. **Make one focused change.** Preserve unrelated edits. Update the authoritative document when behavior changes. Add tests that demonstrate the behavior or regression when executable code is involved. Do not claim a planned feature is implemented. If your change finishes work described in [docs/implementation](docs/implementation/README.md), move anything durable into the document that owns it and delete the note in the same pull request.
4. **Check locally.** Run the commands below and any tests relevant to the change. Report skipped checks and their reason.
5. **Open a PR against `main`.** Use the PR template. Draft PRs are welcome for early feedback; mark ready when the change and its validation are reviewable.
6. **Review and merge.** Address feedback and rerun affected checks after edits. Reviews are optional. Only @itsafal and @dipeshbabu are on the merge allowlist; they squash-merge after required checks pass and review threads are resolved.

The code owners are @itsafal and @dipeshbabu; either can review. Authors cannot approve their own PRs, but approval is not required to merge. Other contributors submit PRs for these maintainers to assess and merge. Seek a reviewer familiar with auth, isolation, snapshot, or recovery boundaries for changes in those areas; ownership alone is not evidence of expertise.

## Commit and PR titles

Use `type: short description` for the PR title and prefer the same style for commits:

```text
docs: explain sandbox pause and resume
feat: add sandbox creation
fix: retain disk reservations during cleanup
ci: validate documentation on pull requests
```

Common types are `feat`, `fix`, `docs`, `test`, `refactor`, `ci`, and `chore`; an optional scope is fine. Explain why in the body when useful. This is a writing convention, not a commit-message bot or release trigger. We squash merge, so intermediate commits do not need to be rewritten just to satisfy a message format.

## Checks available today

From the repository root, with Python 3.11 or newer, Node 24.9.0 or newer, the pinned Rust toolchain, protoc and the development PostgreSQL service:

```sh
make api-setup
make sdk-setup
make mcp-setup
make check
git diff --check
```

The `docs-check` GitHub Actions job checks repository Markdown for local inline links and anchors, closed code fences, and valid JSON examples, plus whitespace errors in the proposed commit diff. It does not fetch external links, render Mermaid, or validate sandbox behavior. Use inline Markdown links and ATX (`#`) headings for checked document references.

Keep the `docs-check` and `rust-check` job names stable because branch protection requires both checks from GitHub Actions. CI runs on every PR to `main` and on pushes to `main`, including fork PRs with read-only permissions and no repository secrets. GitHub may require maintainer approval before a first-time contributor's workflow runs. Use GitHub-hosted runners for untrusted PR checks. Do not run fork code on privileged sandbox hosts or introduce `pull_request_target` execution of contributor code.

The required `rust-check` job runs formatting, strict Clippy, PostgreSQL tests, explicit MinIO tests, Python and TypeScript SDK checks, optional MCP adapter checks, guest/host image checks, and [OpenAPI generation/conformance](docs/openapi.md). Keep both required statuses passing on the final PR head. `rust-check` is an aggregate gate: documentation-only PRs (Markdown under `docs/` and the root README, CONTRIBUTING and SECURITY files) skip the heavy runtime job, while unknown paths, runtime/configuration changes, pushes to `main` and manual runs execute the full suite. Scope detection failures fail the gate. Rust dependency artifacts, Python/npm downloads and the exact pinned MinIO image are cached; caches contain no instance data or credentials. The first run after a cache miss remains a full cold build. Broader [roadmap gates](docs/roadmap.md) remain separate. Real Firecracker tests need a controlled Linux/KVM environment. A docs-only PR does not need a VM test; a change to snapshot correctness does.

## Review and branch rules

`main` uses these repository settings:

- Require a pull request, with zero required approvals and no mandatory code-owner review. No PR/review bypass accounts, teams, or apps are configured.
- Restrict branch updates (including merges) to `itsafal` and `dipeshbabu`, with no teams or apps on the allowlist. Both are the current repository administrators. GitHub also grants branch access to repository administrators, so granting another account admin access expands who can merge.
- Require `docs-check` and `rust-check` success from GitHub Actions, and an up-to-date branch before merge.
- Require resolved review conversations and linear history; squash is the enabled PR merge method.
- No force pushes or branch deletion. Protections apply to administrators too; both maintainers must satisfy the PR and CI requirements.

CODEOWNERS routes optional review requests; it does not require approval. GitHub settings enforce the merge allowlist and CI gates. Maintainers decide whether a PR needs further review before merging. If checks fail, fix the cause or document a genuine infrastructure failure for maintainer investigation; do not disable a required check to land a PR.

## Coding agents

Agents follow the same contribution process: isolated worktree, focused branch, relevant validation, and a PR explaining results and limitations. They may not fabricate reviews, mark unrun checks as passed, or treat their own review as maintainer approval. Merging, tagging releases, and publishing artifacts require explicit maintainer authorization and must satisfy repository rules. Never put tokens, customer snapshots, or private output into commits or PRs.

## Releases

Merging is not releasing or deploying. There are no runtime releases or publishing workflow yet. Maintainers will publish versioned releases from reviewed `main` commits once the applicable delivery gates have evidence, with release notes, compatibility changes, upgrade instructions, and known limitations. Begin initial releases in `0.x`; select the first version when a usable artifact exists. Breaking changes must be called out even before `1.0`.

## Maintainer references

GitHub documents [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches) and [private vulnerability reporting](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/configure-vulnerability-reporting/configure-for-a-repository). Repository settings are managed on GitHub, not automatically applied by editing this file.
