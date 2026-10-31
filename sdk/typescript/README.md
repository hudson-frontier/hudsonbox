# Hudsonbox TypeScript client

Unpublished Node HTTPS client for the 14 implemented Project API operations. Requires Node 24.9.0+ on Unix and an existing API installation. Build with `npm ci --ignore-scripts && npm run build`; `npm pack --ignore-scripts` produces a local tarball. Publication is disabled.

Import `Client`, `ClientError`, types and `stringify` from `@hudson-frontier/hudsonbox-client`. The constructor takes a trusted private configuration path. Generated methods use camelCase names and snake_case wire fields. Close clients when finished. This package is for server-side Node applications, not browser bundles containing project credentials.

Typed 64-bit response fields use `bigint`; safe numeric integer inputs are accepted, and unsafe numbers are rejected. Use the exported `stringify` for results. Save mutation keys and exact payloads before submitting; there are no implicit retries.

See the [client guide](https://github.com/hudson-frontier/hudsonbox/blob/main/docs/language-clients.md) for configuration, examples, streaming and verified files. Pre-1.0 versions are development versions, not production releases.
