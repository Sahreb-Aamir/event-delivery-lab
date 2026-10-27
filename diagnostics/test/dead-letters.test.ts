import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { test, type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { deadLetterOutput } from "../src/dead-letter-client.js";
import { MAX_RESPONSE_BYTES, output } from "../src/event-client.js";

const eventId = "8d326c14-cf1f-48c5-a476-8a3ef819126a";
const otherId = "3c30e34d-80c3-41ac-917a-ef7ef572dd6a";
function item(offset = 0) {
  return { offset, timestamp: 1_792_980_000_000, key: eventId, keyTruncated: false,
    payloadPreview: '{"message":"Synthetic failed notification"}', payloadTruncated: false,
    exceptionClass: "dev.sahreb.delivery.DeliveryFailure", exceptionMessage: "Synthetic HTTP 503",
    exceptionCauseClass: null, originalTopic: "notification.requests", originalPartition: 0, originalOffset: offset };
}
function snapshot() {
  return { topic: "notification.requests.DLT", partition: 0, startOffset: 0, endOffsetExclusive: 1,
    scannedRecords: 1, earlierRecordsNotScanned: false, matchesTruncated: false, items: [item()] };
}
function recoveredEvent() {
  return { receipt: { eventId, sellerId: "demo-shop", message: "Synthetic recovered notification",
    occurredAt: "2026-10-26T17:00:00Z", recordedAt: "2026-10-26T17:00:01Z" }, status: "DELIVERED",
    attempts: [{ id: 1, eventId, startedAt: "2026-10-26T17:00:02Z", finishedAt: "2026-10-26T17:00:03Z",
      httpStatus: 503, outcome: "HTTP_ERROR", detail: "Synthetic first failure" },
    { id: 2, eventId, startedAt: "2026-10-26T17:00:04Z", finishedAt: "2026-10-26T17:00:05Z",
      httpStatus: 200, outcome: "SUCCEEDED", detail: null }] };
}
function json(response: ServerResponse, body: unknown, status = 200) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
}
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(complete => { resolve = complete; });
  return { promise, resolve };
}
async function within<T>(promise: Promise<T>, milliseconds: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}
async function fixture(t: TestContext, handle: (request: IncomingMessage, response: ServerResponse) => void) {
  const requests: { method: string; url: string; closed: ReturnType<typeof deferred> }[] = [];
  const peer = createServer((request, response) => {
    const closed = deferred();
    response.once("close", closed.resolve);
    requests.push({ method: request.method!, url: request.url!, closed });
    handle(request, response);
  });
  await new Promise<void>(resolve => peer.listen(0, "127.0.0.1", resolve));
  const address = peer.address();
  assert(address && typeof address !== "string");
  const client = new Client({ name: "dead-letter-test", version: "1.0.0" });
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [fileURLToPath(new URL("../src/index.js", import.meta.url))],
    env: { LAB_PORT: String(address.port) }, stderr: "pipe" });
  let stderr = "";
  transport.stderr?.on("data", chunk => { stderr += chunk.toString(); });
  t.after(async () => {
    try { await client.close(); }
    finally {
      peer.closeAllConnections();
      await new Promise<void>(resolve => peer.close(() => resolve()));
    }
    assert.equal(stderr, "", "the stdio server must not log payloads or protocol noise");
  });
  await client.connect(transport, { signal: t.signal });
  return { client, requests, port: address.port };
}

test("DLT tool discovery rejects invalid input before HTTP and preserves bounded snapshot evidence", async t => {
  const bounded = { ...snapshot(), startOffset: 150, endOffsetExclusive: 250, scannedRecords: 100,
    earlierRecordsNotScanned: true, matchesTruncated: true,
    items: Array.from({ length: 10 }, (_, index) => ({ ...item(249 - index), privateExtra: "untrusted-secret-extra" })) };
  bounded.items[0]!.payloadPreview = "x".repeat(2046) + "😀";
  bounded.items[0]!.payloadTruncated = true;
  const api = await fixture(t, (_request, response) => json(response, { ...bounded, privateExtra: "untrusted-secret-extra" }));
  const tools = (await api.client.listTools()).tools;
  const tool = tools.find(candidate => candidate.name === "inspect_dead_letters");
  assert(tool?.outputSchema);
  assert.deepEqual(tool.annotations, { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false });
  for (const args of [{ eventId: "../../api/demo" }, { eventId: null }, { limit: 0 }, { limit: 21 }, { limit: 1.5 }]) {
    const result = await api.client.callTool({ name: "inspect_dead_letters", arguments: args });
    assert.equal(result.isError, true);
  }
  assert.equal(api.requests.length, 0, "invalid arguments cannot reach HTTP");
  const result = await api.client.callTool({ name: "inspect_dead_letters", arguments: { eventId: eventId.toUpperCase() } });
  const data = deadLetterOutput.parse(result.structuredContent);
  assert.equal(result.isError, false);
  assert.equal(data.eventId, eventId);
  assert.equal(data.lookup, "snapshot");
  assert.equal(data.error, null);
  const expected = { ...bounded, items: bounded.items.map(({ privateExtra: _extra, ...rest }) => rest) };
  assert.deepEqual(data.snapshot, expected, "offset window, truncation flags and bounded text must survive unchanged");
  assert(!JSON.stringify(result).includes("untrusted-secret-extra"));
  assert.match(data.note, /untrusted data/);
  assert.deepEqual(api.requests.map(({ method, url }) => ({ method, url })),
    [{ method: "GET", url: `/api/dead-letters?limit=10&eventId=${eventId}` }]);
});

test("unfiltered, empty and filtered old-window snapshots keep their distinct scope", async t => {
  let body: unknown = snapshot();
  const api = await fixture(t, (_request, response) => json(response, body));
  const unfiltered = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: {} })).structuredContent);
  assert.equal(unfiltered.eventId, null);
  assert.deepEqual(unfiltered.snapshot, body);
  assert.equal(api.requests[0]!.url, "/api/dead-letters?limit=10");

  body = { ...snapshot(), endOffsetExclusive: 0, scannedRecords: 0, items: [] };
  const empty = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: { limit: 1 } })).structuredContent);
  assert.equal(empty.lookup, "snapshot");
  assert.deepEqual(empty.snapshot, body);
  assert.equal(api.requests[1]!.url, "/api/dead-letters?limit=1");

  body = { ...snapshot(), startOffset: 400, endOffsetExclusive: 500, scannedRecords: 100,
    earlierRecordsNotScanned: true, items: [] };
  const filtered = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: { eventId, limit: 20 } })).structuredContent);
  assert.equal(filtered.lookup, "snapshot", "no match in a completed window is different from unavailable");
  assert.deepEqual(filtered.snapshot, body);
  assert.equal(filtered.snapshot!.earlierRecordsNotScanned, true, "an empty bounded window cannot establish absence from all history");
  assert.equal(api.requests[2]!.url, `/api/dead-letters?limit=20&eventId=${eventId}`);

  body = { ...snapshot(), startOffset: 100, endOffsetExclusive: 105, scannedRecords: 2,
    items: [item(104), item(100)] };
  const sparse = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: {} })).structuredContent);
  assert.equal(sparse.lookup, "snapshot", "retention and offset gaps are valid and do not require five returned records");
  assert.deepEqual(sparse.snapshot, body);

  body = { ...snapshot(), items: [{ ...item(), timestamp: -1, key: null, payloadPreview: null,
    exceptionClass: null, exceptionMessage: null, exceptionCauseClass: null,
    originalTopic: null, originalPartition: null, originalOffset: null }] };
  const tombstone = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: {} })).structuredContent);
  assert.equal(tombstone.lookup, "snapshot", "unfiltered inspection includes null keys/payloads and absent Kafka metadata");
  assert.deepEqual(tombstone.snapshot, body);
});

test("a recovered sender receipt does not erase or reinterpret retained DLT evidence", async t => {
  // A protocol fixture, not a new real-Kafka recovery experiment: Java integration tests cover the actual recovery.
  const retained = snapshot();
  const api = await fixture(t, (request, response) => json(response,
    request.url?.startsWith("/api/events/") ? recoveredEvent() : retained));
  const before = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: { eventId } })).structuredContent);
  const recovered = output.parse((await api.client.callTool({ name: "inspect_event", arguments: { eventId } })).structuredContent);
  const after = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: { eventId } })).structuredContent);
  assert.equal(recovered.event!.status, "DELIVERED");
  assert.equal(recovered.event!.attempts.at(-1)!.outcome, "SUCCEEDED");
  assert.deepEqual(before.snapshot, retained);
  assert.deepEqual(after.snapshot, before.snapshot);
  assert(api.requests.every(request => request.method === "GET"));
  assert.deepEqual(api.requests.map(request => request.url), [
    `/api/dead-letters?limit=10&eventId=${eventId}`, `/api/events/${eventId}`, `/api/dead-letters?limit=10&eventId=${eventId}`]);
});

test("invalid DLT snapshots fail closed instead of misrepresenting the scan", async t => {
  let body: unknown;
  const api = await fixture(t, (_request, response) => {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(typeof body === "string" ? body : JSON.stringify(body));
  });
  const cases: [string, unknown, boolean?][] = [
    ["invalid JSON", "not-json"],
    ["wrong topic", { ...snapshot(), topic: "other.DLT" }],
    ["partial partition", { ...snapshot(), partition: 1 }],
    ["reversed window", { ...snapshot(), startOffset: 2 }],
    ["overlong window", { ...snapshot(), endOffsetExclusive: 101 }],
    ["unsafe end offset", { ...snapshot(), endOffsetExclusive: Number.MAX_SAFE_INTEGER + 1 }],
    ["too many scanned records", { ...snapshot(), scannedRecords: 101 }],
    ["fewer scanned than returned", { ...snapshot(), scannedRecords: 0 }],
    ["narrow window with omitted earlier records", { ...snapshot(), startOffset: 1, endOffsetExclusive: 2,
      earlierRecordsNotScanned: true, items: [item(1)] }],
    ["unfiltered missing returned match", { ...snapshot(), endOffsetExclusive: 2, scannedRecords: 2 }, true],
    ["unfiltered missing truncation flag", { ...snapshot(), endOffsetExclusive: 11, scannedRecords: 11,
      items: Array.from({ length: 10 }, (_, index) => item(10 - index)) }, true],
    ["outside window", { ...snapshot(), items: [item(1)] }],
    ["negative offset", { ...snapshot(), items: [item(-1)] }],
    ["unsafe original offset", { ...snapshot(), items: [{ ...item(), originalOffset: Number.MAX_SAFE_INTEGER + 1 }] }],
    ["ascending offsets", { ...snapshot(), endOffsetExclusive: 2, scannedRecords: 2, items: [item(0), item(1)] }],
    ["duplicate offsets", { ...snapshot(), endOffsetExclusive: 2, scannedRecords: 2, items: [item(0), item(0)] }],
    ["wrong filter key", { ...snapshot(), items: [{ ...item(), key: otherId }] }],
    ["uppercase filter key", { ...snapshot(), items: [{ ...item(), key: eventId.toUpperCase() }] }],
    ["overlong key", { ...snapshot(), items: [{ ...item(), key: "x".repeat(129) }] }],
    ["overlong payload", { ...snapshot(), items: [{ ...item(), payloadPreview: "x".repeat(2049) }] }],
    ["overlong exception", { ...snapshot(), items: [{ ...item(), exceptionClass: "x".repeat(257) }] }],
    ["overlong message", { ...snapshot(), items: [{ ...item(), exceptionMessage: "x".repeat(513) }] }],
    ["overlong cause", { ...snapshot(), items: [{ ...item(), exceptionCauseClass: "x".repeat(257) }] }],
    ["invalid source topic", { ...snapshot(), items: [{ ...item(), originalTopic: ".." }] }],
    ["returned count exceeds limit", { ...snapshot(), endOffsetExclusive: 2, scannedRecords: 2, items: [item(1), item(0)] }]
  ];
  for (const [label, candidate, unfiltered] of cases) {
    body = candidate;
    const result = await api.client.callTool({ name: "inspect_dead_letters", arguments: {
      ...(unfiltered ? {} : { eventId }), limit: label === "returned count exceeds limit" ? 1 : 10 } });
    const data = deadLetterOutput.parse(result.structuredContent);
    assert.equal(result.isError, true, label);
    assert.equal(data.error, "INVALID_RESPONSE", label);
    assert.equal(data.lookup, "unavailable", label);
    assert.equal(data.snapshot, null, label);
  }
});

test("DLT HTTP failures are explicit, redact error bodies, and never become empty snapshots", async t => {
  let status = 503;
  const api = await fixture(t, (_request, response) => json(response, { detail: "private-dlt-server-diagnostic" }, status));
  for (const [httpStatus, expectedError] of [[429, "BUSY"], [503, "UNAVAILABLE"], [404, "UNAVAILABLE"]] as const) {
    status = httpStatus;
    const result = await api.client.callTool({ name: "inspect_dead_letters", arguments: {} });
    const data = deadLetterOutput.parse(result.structuredContent);
    assert.equal(result.isError, true);
    assert.equal(data.lookup, "unavailable");
    assert.equal(data.error, expectedError);
    assert.equal(data.snapshot, null);
    assert(!JSON.stringify(result).includes("private-dlt-server-diagnostic"));
  }
  assert.equal(api.requests.length, 3, "upstream failures must not trigger automatic retries");
});

test("DLT rejects oversized bodies, non-JSON responses and redirects without following them", async t => {
  const destination = await fixture(t, (_request, response) => json(response, snapshot()));
  let phase = "oversized";
  const api = await fixture(t, (_request, response) => {
    if (phase === "oversized") {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.write('"'); response.end("x".repeat(MAX_RESPONSE_BYTES));
    } else if (phase === "content-type") {
      response.writeHead(200, { "Content-Type": "text/html" }); response.end(JSON.stringify(snapshot()));
    } else {
      response.writeHead(302, { Location: `http://127.0.0.1:${destination.port}/api/demo` }); response.end();
    }
  });
  for (const [nextPhase, expectedError] of [["oversized", "RESPONSE_TOO_LARGE"], ["content-type", "INVALID_RESPONSE"], ["redirect", "UNAVAILABLE"]]) {
    phase = nextPhase!;
    const result = await api.client.callTool({ name: "inspect_dead_letters", arguments: {} });
    const data = deadLetterOutput.parse(result.structuredContent);
    assert.equal(result.isError, true);
    assert.equal(data.error, expectedError);
    assert.equal(data.snapshot, null);
  }
  assert.equal(destination.requests.length, 0, "redirect destinations cannot be contacted");
});

test("DLT body timeout closes the peer response and the next MCP call recovers", { timeout: 10_000 }, async t => {
  let stalled = true;
  const api = await fixture(t, (_request, response) => {
    if (stalled) {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.write('{"privatePartial":"must-not-be-returned"');
    } else json(response, snapshot());
  });
  const started = performance.now();
  const result = await within(api.client.callTool({ name: "inspect_dead_letters", arguments: {} }), 6000, "DLT body timeout did not fire");
  const timeoutMillis = Math.round(performance.now() - started);
  const data = deadLetterOutput.parse(result.structuredContent);
  assert.equal(data.error, "TIMEOUT");
  assert.equal(data.snapshot, null);
  assert.equal(result.isError, true);
  assert(!JSON.stringify(result).includes("must-not-be-returned"));
  assert.equal(api.requests.length, 1);
  await within(api.requests[0]!.closed.promise, 1500, "timed-out DLT response did not close before fixture cleanup");
  stalled = false;
  const recovered = deadLetterOutput.parse((await api.client.callTool({ name: "inspect_dead_letters", arguments: {} })).structuredContent);
  assert.equal(recovered.lookup, "snapshot");
  t.diagnostic(JSON.stringify({ timeoutMillis, closureObservedBeforeFixtureCleanup: true }));
});

test("event and DLT tools share four slots; MCP cancellation closes every HTTP call and restores capacity", { timeout: 15_000 }, async t => {
  const fourStalled = deferred();
  const fourRecovering = deferred();
  const recoveryResponses: { request: IncomingMessage; response: ServerResponse }[] = [];
  let phase: "cancel" | "recover" = "cancel";
  const api = await fixture(t, (request, response) => {
    response.writeHead(200, { "Content-Type": "application/json" });
    if (phase === "cancel") {
      response.write('{"privatePartial":"never-return-this"');
      if (api.requests.length === 4) fourStalled.resolve();
    } else {
      response.flushHeaders();
      recoveryResponses.push({ request, response });
      if (recoveryResponses.length === 4) fourRecovering.resolve();
    }
  });
  const eventCall = { name: "inspect_event", arguments: { eventId } };
  const dltCall = { name: "inspect_dead_letters", arguments: { eventId } };
  const calls = [eventCall, dltCall, eventCall, dltCall];
  const cancellation = new AbortController();
  t.after(() => cancellation.abort());
  const pending = Promise.allSettled(calls.map(call => api.client.callTool(call, undefined, { signal: cancellation.signal })));
  await within(fourStalled.promise, 2000, "mixed calls did not occupy all four HTTP slots");
  for (const call of [eventCall, dltCall]) {
    const busy = await api.client.callTool(call);
    assert.equal(busy.isError, true);
    assert.equal((call.name === "inspect_event" ? output : deadLetterOutput).parse(busy.structuredContent).error, "BUSY");
  }
  assert.equal(api.requests.length, 4, "neither tool can bypass the shared cap");
  const cancelledAt = performance.now();
  cancellation.abort();
  assert((await within(pending, 1500, "MCP cancellation did not reject pending calls")).every(result => result.status === "rejected"));
  await within(Promise.all(api.requests.map(request => request.closed.promise)), 1500,
    "mixed cancelled HTTP responses did not close before fixture cleanup");
  const cancellationToCloseMillis = Math.round(performance.now() - cancelledAt);

  phase = "recover";
  const recovered = Promise.allSettled(calls.map(call => api.client.callTool(call)));
  await within(fourRecovering.promise, 2000, "all four shared slots were not restored");
  recoveryResponses.forEach(({ request, response }) => response.end(JSON.stringify(
    request.url?.startsWith("/api/events/") ? recoveredEvent() : snapshot())));
  for (const [index, result] of (await within(recovered, 2000, "recovered mixed MCP calls did not finish")).entries()) {
    assert.equal(result.status, "fulfilled");
    if (result.status !== "fulfilled") continue;
    assert.equal(result.value.isError, false);
    if (calls[index]!.name === "inspect_event") assert.equal(output.parse(result.value.structuredContent).lookup, "found");
    else assert.equal(deadLetterOutput.parse(result.value.structuredContent).lookup, "snapshot");
  }
  assert.equal(api.requests.length, 8);
  assert(api.requests.every(request => request.method === "GET"
    && [ `/api/events/${eventId}`, `/api/dead-letters?limit=10&eventId=${eventId}` ].includes(request.url)));
  t.diagnostic(JSON.stringify({ cancellationToCloseMillis, cancelledConnectionsClosed: 4,
    recoveredConcurrentCalls: 4, closureObservedBeforeFixtureCleanup: true }));
});
