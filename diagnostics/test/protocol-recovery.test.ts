import assert from "node:assert/strict";
import { createServer, type ServerResponse } from "node:http";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { output } from "../src/event-client.js";

const eventId = "8d326c14-cf1f-48c5-a476-8a3ef819126a";
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

test("MCP timeout and cancellation close HTTP requests and restore all four slots", { timeout: 20_000 }, async t => {
  // The peer writes headers plus an incomplete JSON body. No Java app, database or external service is used.
  const requests: { method: string; url: string; closed: ReturnType<typeof deferred> }[] = [];
  const fourStalled = deferred();
  const fourRecovering = deferred();
  const recoveryResponses: ServerResponse[] = [];
  let phase: "timeout" | "cancel" | "recover" = "timeout";
  const peer = createServer((request, response) => {
    const closed = deferred();
    response.once("close", closed.resolve);
    requests.push({ method: request.method!, url: request.url!, closed });
    response.writeHead(200, { "Content-Type": "application/json" });
    if (phase === "recover") {
      // Hold every response until all four calls are admitted, so sequential success cannot hide a leaked slot.
      recoveryResponses.push(response);
      response.flushHeaders();
      if (recoveryResponses.length === 4) fourRecovering.resolve();
    } else {
      response.write('{"privateDiagnostic":"must-not-be-returned"');
      if (phase === "cancel" && requests.length === 5) fourStalled.resolve();
    }
  });
  await new Promise<void>(resolve => peer.listen(0, "127.0.0.1", resolve));
  const address = peer.address();
  assert(address && typeof address !== "string");
  const client = new Client({ name: "protocol-recovery-test", version: "1.0.0" });
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [fileURLToPath(new URL("../src/index.js", import.meta.url))],
    env: { LAB_PORT: String(address.port) }, stderr: "pipe" });
  let stderr = "";
  transport.stderr?.on("data", chunk => { stderr += chunk.toString(); });
  const cancellation = new AbortController();
  const evidence: Record<string, number | boolean> = {};
  try {
    await client.connect(transport, { signal: t.signal });
    const started = performance.now();
    const timeout = await within(client.callTool({ name: "inspect_event", arguments: { eventId } }),
      6000, "stalled MCP call did not return within the test guard");
    evidence.timeoutMillis = Math.round(performance.now() - started);
    assert.equal(timeout.isError, true);
    const unavailable = output.parse(timeout.structuredContent);
    assert.equal(unavailable.lookup, "unavailable");
    assert.equal(unavailable.error, "TIMEOUT");
    assert.equal(unavailable.event, null);
    assert(!JSON.stringify(timeout).includes("must-not-be-returned"));
    assert.equal(requests.length, 1, "the timeout must not retry HTTP");
    await within(requests[0]!.closed.promise, 1500, "timed-out HTTP response did not close before fixture cleanup");
    await client.ping();

    phase = "cancel";
    const pending = Array.from({ length: 4 }, () => client.callTool(
      { name: "inspect_event", arguments: { eventId } }, undefined, { signal: cancellation.signal }));
    const settled = Promise.allSettled(pending); // Attach rejection handlers before sending cancellation.
    await within(fourStalled.promise, 2000, "four concurrent MCP calls did not reach the peer");
    const busy = await client.callTool({ name: "inspect_event", arguments: { eventId } });
    assert.equal(busy.isError, true);
    assert.equal(output.parse(busy.structuredContent).error, "BUSY");
    assert.equal(requests.length, 5, "the rejected fifth call must not reach HTTP");
    const cancelledAt = performance.now();
    cancellation.abort();
    const cancelled = await within(settled, 1500, "MCP client calls did not reject after cancellation");
    assert(cancelled.every(result => result.status === "rejected"));
    // Observe peer closure while the MCP process and HTTP fixture are both still alive.
    await within(Promise.all(requests.slice(1).map(request => request.closed.promise)), 1500,
      "cancelled HTTP responses did not close before fixture cleanup");
    evidence.cancellationToCloseMillis = Math.round(performance.now() - cancelledAt);
    evidence.cancelledConnectionsClosed = 4;

    phase = "recover";
    const recovered = Promise.allSettled(Array.from({ length: 4 }, () => client.callTool(
      { name: "inspect_event", arguments: { eventId } })));
    await within(fourRecovering.promise, 2000, "all four inspection slots were not restored after cancellation");
    const body = { receipt: { eventId, sellerId: "demo-protocol", message: "Synthetic recovery record",
      occurredAt: "2026-10-21T17:00:00Z", recordedAt: "2026-10-21T17:00:01Z" }, status: "QUEUED", attempts: [] };
    recoveryResponses.forEach(response => response.end(JSON.stringify(body)));
    for (const result of await within(recovered, 2000, "recovered MCP calls did not finish")) {
      assert(result.status === "fulfilled", "recovered MCP calls must complete successfully");
      assert.equal(result.value.isError, false);
      assert.equal(output.parse(result.value.structuredContent).lookup, "found");
    }
    evidence.recoveredConcurrentCalls = 4;
    assert.equal(requests.length, 9);
    assert(requests.every(request => request.method === "GET" && request.url === `/api/events/${eventId}`));
    assert.equal(stderr, "", "protocol process must not emit data or exception diagnostics");
    evidence.closureObservedBeforeFixtureCleanup = true;
  } finally {
    cancellation.abort();
    try { await client.close(); }
    finally {
      peer.closeAllConnections();
      await new Promise<void>(resolve => peer.close(() => resolve()));
    }
  }
  t.diagnostic(JSON.stringify(evidence));
});
