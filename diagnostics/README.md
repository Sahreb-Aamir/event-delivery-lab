# Read-only MCP event diagnostics

An MCP client can answer “what did the sender record for this event?” without publishing, receiver-control, or retry tools. This TypeScript/Zod companion reads the Java API through the official MCP SDK; it does not access Kafka or H2 directly. No model or API key is required.

## Run a complete example

Start the [local lab](../README.md#run-locally). Choose **Lose the acknowledgment**, wait for `DELIVERED`, and copy the UUID from **Delivery detail**. The composer contains the next event's ID.

Install Node 22 or newer. In another terminal, from the repository root:

```sh
cd diagnostics
npm ci --ignore-scripts
npm run build
npm run inspect -- <selected-event-uuid>
npm run inspect -- <selected-event-uuid> 1
```

Replace the placeholder, including angle brackets, with the actual UUID. These commands also work in PowerShell. Do not start another Maven build in the same checkout while the Java launcher runs; the TypeScript build uses separate output.

`inspect` is a real MCP client: it initializes the stdio server, calls `inspect_event`, prints structured output, and closes its child process. The first call should show `NETWORK_ERROR` followed by `SUCCEEDED`. The second returns only the latest attempt, with `totalAttempts: 2` and `attemptsTruncated: true`. This reads sender evidence, not receiver-side effects.

## Connect an MCP client

Build once, then configure your client's stdio server entry:

```json
{
  "command": "node",
  "args": ["/absolute/path/to/event-delivery-lab/diagnostics/dist/src/index.js"],
  "env": { "LAB_PORT": "8080" }
}
```

Use your absolute path (Windows paths can use forward slashes). Configuration containers differ between clients; this is the server entry, not a product-specific settings file. Use `node` directly so stdout contains only MCP messages. A manually started stdio server waits for a client rather than displaying a page.

The only tool is `inspect_event({eventId, attemptLimit?})`. The limit is an integer from 1 to 20, default 10. Recent attempts retain ascending ID order. `LAB_PORT` defaults to 8080 and accepts only integers 1–65535. The host is fixed to `127.0.0.1`; callers cannot supply URLs.

## Interpret results

| Result | Meaning |
| --- | --- |
| `lookup: "found"` | Validated receipt/status/attempt snapshot. `totalAttempts` counts the API history; `attemptsTruncated` reports omitted older attempts. |
| `detailTruncated: true` | Detail preview limited to 1,024 UTF-16 code units. |
| `lookup: "not_recorded"` | API returned 404. Accepted events may still await consumption; this does not prove no submission or remote action occurred. |
| `lookup: "unavailable"` | MCP error result with a bounded code. Do not interpret it as empty history. |

Codes distinguish `TIMEOUT`, `CANCELLED`, `BUSY`, `UNAVAILABLE`, `INVALID_RESPONSE`, and `RESPONSE_TOO_LARGE`. Upstream error bodies and exception details are not returned. A three-second network budget includes body reading; four queries may run concurrently per server. Extra queries fail with `BUSY`. Redirects and automatic retries are disabled.

The Java endpoint returns the whole event history. The companion rejects responses over 256 KiB before parsing them; it cannot efficiently query arbitrarily large histories. This is a buffering bound, not a hard process-memory or wall-clock guarantee. Validation checks supported states, matching event identities, and attempt ordering.

Messages/details remain untrusted data and are labeled accordingly. There is no model reasoning layer or prompt-injection immunity claim. There is no authentication, tenant isolation, receiver observation, dead-letter tool, background collection, or recovery action. Use the localhost synthetic lab. `PENDING` means unknown remote outcome; `DELIVERED` records an HTTP success. Existing [storage/crash limits](../docs/architecture.md#persistence-and-crash-windows) apply.

## Verify

```sh
npm test
```

Tests use a real MCP stdio client/server and owned loopback HTTP fixtures: discovery/schemas, invalid input before HTTP access, history/Unicode bounds, unknown IDs, redacted errors, malformed responses, identity/order checks, oversized streaming bodies, refused redirects, stalled-body deadline, admission, cancellation, and recovery. They complement the Java/Kafka suite and [recorded real-lab check](../docs/results.md#read-only-mcp-event-diagnostics).

Dependencies are pinned in `package-lock.json`. Development is AI-assisted and synthetic-only. Reference: [official TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk/tree/v1.x); its installed `examples/server/mcpServerOutputSchema.js` demonstrates the structured output API used here.
