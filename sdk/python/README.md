# Hudsonbox Python client

Unpublished asynchronous HTTPS client for the 14 implemented Project API operations. Requires Python 3.11+ on Unix and an existing API installation. Install from this source directory with `python -m pip install .`.

Use `from hudsonbox import Client, ClientError, models`. The constructor takes a trusted private configuration path; use `async with Client(path)` and await its generated snake_case methods. Save mutation keys and exact payloads before submission. A transport failure can leave admission unknown; the client never retries mutations implicitly.

See the [client guide](https://github.com/hudson-frontier/hudsonbox/blob/main/docs/language-clients.md) for setup, examples, integer handling, streaming and verified files. Pre-1.0 versions are development versions, not production releases.
