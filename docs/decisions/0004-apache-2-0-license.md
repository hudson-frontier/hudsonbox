# 0004: Apache-2.0 license

Status: Accepted
Date: 2026-09-21

## Context

The repository was public with no license file. Public visibility grants nothing, so nobody could legally reuse the work, and `CONTRIBUTING.md` asked each contributor to negotiate licensing with the maintainers individually. That is not a workable request, and it blocked outside contribution entirely.

The project intends an open-source release and self-hosting by its users, so the license had to permit both running and modifying the software in an operator's own infrastructure.

## Decision

Apache-2.0. The full text is in `LICENSE` at the repository root, copyright Hudson Frontier.

## Consequences

Contributors can now contribute, and self-hosters can run and modify the service, without a private arrangement.

Apache-2.0 carries an explicit patent grant, which MIT does not. For infrastructure software touching virtualization, that matters to the legal review a corporate adopter will run before deploying it.

It permits a third party to operate Hudsonbox as a hosted service in competition with us. We accept that: adoption matters more than that risk at this stage, and the operational burden of running this well is itself a meaningful barrier.

The choice is effectively one-way. Relicensing later needs the agreement of every contributor, so the time to have picked something more restrictive was before the first outside contribution.

## Alternatives considered

**MIT.** Shorter and equally permissive, but no patent grant. Apache-2.0 is the stronger version of the same position.

**AGPL-3.0.** Would prevent an unmodified hosted competitor, which is the one real risk of a permissive licence. Rejected because it deters exactly the corporate self-hosters this product is for; many organizations refuse AGPL dependencies outright.

**BSL 1.1 with a delayed open licence.** Protects the hosted-service case while converting to open source on a fixed date. Rejected because it is not open source on day one, and the repository has been describing itself as intending an open-source release.
