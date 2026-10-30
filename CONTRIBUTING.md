# Contributing to Hudsonbox

Hudsonbox is being designed in public by Hudson Frontier. Start with the [README](README.md) and [documentation guide](docs/README.md). The runtime is partially implemented; the [development guide](docs/local-development.md) distinguishes local checks, controlled VM evidence and unfinished release gates.

The project is released under [Apache-2.0](LICENSE); contributions are accepted under the same terms, as its section 5 provides. No CLA or commit-signoff requirement is currently configured.

## From idea to pull request

1. **Agree on the problem.** Small fixes may go straight to a PR. Open an issue first for substantial features, architecture changes, or new dependencies. Describe the use case, scope, and acceptance criteria. Report vulnerabilities [privately](SECURITY.md).
2. **Create a branch.** Outside contributors fork the repository; maintainers use a branch in this repository. Start from current `main`. Use a descriptive name such as `feat/sandbox-create`, `fix/snapshot-cleanup`, or `docs/auth-flow`. Coding agents use `codex/` branches in isolated worktrees.
3. **Make one focused change.** Preserve unrelated edits. Update the authoritative document when behavior changes. Add tests that demonstrate the behavior or regression when executable code is involved. Do not claim a planned feature is implemented. Keep implementation tasks in GitHub issues. Put durable decisions and verified behavior in the owning document, and validation records in `docs/evidence`; remove superseded working notes.
4. **Check locally.** Run the commands below and any tests relevant to the change. Report skipped checks and their reason.
5. **Open a PR against `main`.** Use the PR template. Draft PRs are welcome for early feedback; mark ready when the change and its validation are reviewable.
6. **Review and merge.** Address feedback, rerun affected checks after edits, and obtain an approving review. @itsafal and @dipeshbabu may bypass the PR/review requirement. A maintainer squash-merges after required checks pass and review threads are resolved.

Maintainers have write/maintain/admin repository access. The initial code owners are the existing administrators, @itsafal and @dipeshbabu; either can review. Authors cannot approve their own PRs. @itsafal and @dipeshbabu have the explicit PR/review bypass; other maintainers need an approving review even on their own changes. Seek a reviewer familiar with auth, isolation, snapshot, or recovery boundaries for changes in those areas; ownership alone is not evidence of expertise.

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

Keep the job name stable because branch protection requires it. CI runs on every PR to `main` and on pushes to `main`, including fork PRs with read-only permissions and no repository secrets. GitHub may require maintainer approval before a first-time contributor's workflow runs. Use GitHub-hosted runners for untrusted PR checks. Do not run fork code on privileged sandbox hosts or introduce `pull_request_target` execution of contributor code.

The Rust job runs formatting, strict Clippy, PostgreSQL tests, explicit MinIO tests, and [OpenAPI generation/conformance](docs/openapi.md). Keep both hosted jobs passing on the final PR head. Broader [roadmap gates](docs/roadmap.md) remain separate. Real Firecracker tests need a controlled Linux/KVM environment. A docs-only PR does not need a VM test; a change to snapshot correctness does.

## Review and branch rules

`main` uses these repository settings:

- Require a pull request with at least one approving review and code-owner approval for owned files. The GitHub accounts `itsafal` and `dipeshbabu` may bypass the PR/review requirement; no teams or apps have that allowance.
- Stale approvals dismissed after changes, required `docs-check` success, and an up-to-date branch before merge.
- Resolved review conversations and linear history; squash is the enabled PR merge method.
- No force pushes or branch deletion. Protections apply to administrators too; the configured exceptions are the explicit PR/review bypasses for `itsafal` and `dipeshbabu`. Required checks still apply to both accounts, including on direct pushes.

CODEOWNERS routes review; GitHub settings enforce the gates. Changes to those settings must preserve the agreed review process. CI does not replace human review. If checks fail, fix the cause or document a genuine infrastructure failure for maintainer investigation; do not disable a required check to land a PR.

For the initial process PR, GitHub could not use a CODEOWNERS file that had not reached `main` yet. The ordinary approving-review requirement applied with the initial `itsafal` exception. Current PRs use the merged ownership file for routing and required code-owner approval, subject to the explicit account bypasses above.

## Coding agents

Agents follow the same contribution process: isolated worktree, focused branch, relevant validation, and a PR explaining results and limitations. They may not fabricate reviews, mark unrun checks as passed, or treat their own review as maintainer approval. Merging, tagging releases, and publishing artifacts require explicit maintainer authorization and must satisfy repository rules. Never put tokens, customer snapshots, or private output into commits or PRs.

## Releases

Merging is not releasing or deploying. There are no runtime releases or publishing workflow yet. Maintainers will publish versioned releases from reviewed `main` commits once the applicable delivery gates have evidence, with release notes, compatibility changes, upgrade instructions, and known limitations. Begin initial releases in `0.x`; select the first version when a usable artifact exists. Breaking changes must be called out even before `1.0`.

## Maintainer references

GitHub documents [protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches) and [private vulnerability reporting](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/configure-vulnerability-reporting/configure-for-a-repository). Repository settings are managed on GitHub, not automatically applied by editing this file.
