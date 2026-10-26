# Read-only MCP diagnostics

An MCP client can answer “what did the sender record for this event?” and “what failure evidence remains in the recent dead-letter window?” without publishing, receiver-control, or retry tools. This TypeScript/Zod companion reads the Java API through the official MCP SDK; it does not access Kafka or H2 directly. No model or API key is required.

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

For an exhausted event that was manually retried, follow the [guided MCP recovery example](../docs/walkthrough.md#optional-inspect-the-recovered-event-through-mcp). It compares the retained failures with a bounded view of the successful attempt.

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

Two tools are available:

- `inspect_event({eventId, attemptLimit?})`: recent sender attempts in ascending ID order.
- `inspect_dead_letters({eventId?, limit?})`: newest matching dead-letter records in descending offset order. Omit the UUID to include records that never created a receipt.

Both limits are integers from 1 to 20, default 10. `LAB_PORT` defaults to 8080 and accepts only integers 1–65535. The host is fixed to `127.0.0.1`; callers cannot supply URLs.

## Interpret results

| Result | Meaning |
| --- | --- |
| `lookup: "found"` | Validated receipt/status/attempt snapshot. `totalAttempts` counts the API history; `attemptsTruncated` reports omitted older attempts. |
| `detailTruncated: true` | Detail preview limited to 1,024 UTF-16 code units. |
| `lookup: "not_recorded"` | API returned 404. Accepted events may still await consumption; this does not prove no submission or remote action occurred. |
| `lookup: "unavailable"` | MCP error result with a bounded code. Do not interpret it as empty history. |

Codes distinguish `TIMEOUT`, `CANCELLED`, `BUSY`, `UNAVAILABLE`, `INVALID_RESPONSE`, and `RESPONSE_TOO_LARGE`. HTTP error bodies and transport exception details are not returned. A three-second network budget includes body reading; four HTTP queries may run concurrently across both tools per server. Extra queries fail with `BUSY`. Redirects and automatic retries are disabled.

The Java endpoint returns the whole event history. The companion rejects responses over 256 KiB before parsing them; it cannot efficiently query arbitrarily large histories. This is a buffering bound, not a hard process-memory or wall-clock guarantee. Validation checks supported states, matching event identities, and attempt ordering.

Messages/details remain untrusted data and are labeled accordingly. There is no model reasoning layer or prompt-injection immunity claim. There is no authentication, tenant isolation, receiver observation, background collection, or recovery action. Use the localhost synthetic lab. `PENDING` means unknown remote outcome; `DELIVERED` records an HTTP success. Existing [storage/crash limits](../docs/architecture.md#persistence-and-crash-windows) apply.

## Inspect retained failure evidence

In the dashboard, choose **Exhaust automatic attempts** and wait for `FAILED`. Copy the UUID from **Delivery detail**, then run from `diagnostics` after building:

```sh
npm run inspect-dead-letters -- <selected-event-uuid> 1
npm run inspect-dead-letters -- all 10
```

The first call filters by exact event key. The second includes malformed and conflicting records, even when no receipt exists. Set receiver failures to zero and explicitly retry the failed event in the dashboard. After it becomes `DELIVERED`, repeat `inspect` and `inspect-dead-letters`: current sender success and the earlier dead letter can coexist. Inspection does not trigger recovery or erase Kafka records. See the [recorded full-path check](../docs/results.md#mcp-dead-letter-diagnostics).

| Snapshot field | Interpretation |
| --- | --- |
| `lookup: "snapshot"` | A validated response from the bounded API; `items` can be empty. |
| `startOffset`, `endOffsetExclusive` | Search window in partition 0 of `notification.requests.DLT`, at most 100 offset positions. Kafka gaps mean this is not a count of records. |
| `scannedRecords` | Records examined inside that window, before filtering/limiting. |
| `earlierRecordsNotScanned` | More retained records may exist before the searched window. Even `false` does not recover expired records or a previous broker lifetime. |
| `matchesTruncated` | More matches existed inside the window than the returned limit. No total match count is inferred. |
| `keyTruncated`, `payloadTruncated` | API preview flags, preserved unchanged. Key/payload previews are capped at 128/2,048 UTF-16 units. |
| `lookup: "unavailable"` | No snapshot; an MCP error result. HTTP `429` maps to `BUSY`; `404`/`503` are unavailable, never an empty successful search. |

Only selected exception headers and source coordinates are returned. Exception class/cause previews are at most 256 UTF-16 units and message previews at most 512; the Java API does not supply truncation flags for these headers. Payload, keys and exception text are untrusted data. Unknown response fields are stripped, and inconsistent windows/counts, ordering, filter keys or integers beyond JavaScript's safe range are rejected.

The same 256 KiB response bound applies. Large escaped previews may exceed it even within the item limit; lower the limit after `RESPONSE_TOO_LARGE`. The Java inspector admits one scan at a time and has its own three-second Kafka operation budget plus close time. The companion may time out first. Cancelling MCP closes its HTTP request; it does not establish that synchronous Java/Kafka work stops immediately. The embedded broker's dead letters reset on restart.

## Verify

```sh
npm test
```

Tests use a real MCP stdio client/server and owned loopback HTTP fixtures: discovery/schemas, invalid input before HTTP access, history/Unicode bounds, unknown IDs, redacted errors, malformed responses, identity/order checks, oversized streaming bodies, refused redirects, stalled-body deadline, admission, cancellation, and recovery. They complement the Java/Kafka suite and [recorded real-lab check](../docs/results.md#read-only-mcp-event-diagnostics).

[`dead-letters.test.ts`](test/dead-letters.test.ts) adds filtered/unfiltered windows, retained failure evidence alongside recovered state, preview/offset validation, unavailable-versus-empty results, and shared admission/cancellation across both tools. Its HTTP peers are controlled fixtures; the Java suite independently verifies that inspection does not advance consumer offsets.

### Check cancellation through MCP

From `diagnostics`, run the focused protocol check (also included in `npm test`):

```sh
npm run build
node --test dist/test/protocol-recovery.test.js
```

[`protocol-recovery.test.ts`](test/protocol-recovery.test.ts) uses the actual stdio server process and SDK client. Its HTTP peer starts a response but withholds the body. The tool must return an MCP error result with `TIMEOUT`, close that request, and remain responsive. It then fills all four inspection slots, verifies the fifth call returns `BUSY` without HTTP access, and cancels the four requests through the MCP client.

The initiating client observes rejected calls after cancellation. The test separately requires all four HTTP responses to close before fixture cleanup, then holds four fresh responses until every slot is admitted again. This distinguishes cancelling the client-side wait from actually releasing server-side work. Test diagnostics record the observed timing and counts in local/CI logs. These are controlled transport checks, not hard cancellation-latency guarantees or a Java-broker outage experiment.

Dependencies are pinned in `package-lock.json`. Development is AI-assisted and synthetic-only. Reference: [official TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk/tree/v1.x); its installed `examples/server/mcpServerOutputSchema.js` demonstrates the structured output API used here.
