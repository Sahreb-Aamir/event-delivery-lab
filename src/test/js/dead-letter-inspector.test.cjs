"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const { once } = require("node:events");
const { DeadLetterInspector, parseQuery, validateSnapshot, renderSnapshot, MAX_RESPONSE_BYTES, QUERY_TIMEOUT_MS }
  = require("../../main/resources/static/dead-letter-inspector.js");

const firstId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const secondId = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const entry = (offset = 0, key = firstId) => ({
  offset, timestamp: 1_792_880_000_000, key, keyTruncated: false,
  payloadPreview: "synthetic notification", payloadTruncated: false,
  exceptionClass: "DeliveryException", exceptionMessage: "HTTP 503", exceptionCauseClass: null,
  originalTopic: "notification.requests", originalPartition: 0, originalOffset: 0
});
const snapshot = (key = firstId) => ({ topic: "notification.requests.DLT", partition: 0,
  startOffset: 0, endOffsetExclusive: 1, scannedRecords: 1, earlierRecordsNotScanned: false,
  matchesTruncated: false, items: [entry(0, key)] });
const json = data => new Response(JSON.stringify(data), { headers: { "Content-Type": "application/json;charset=UTF-8" } });
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const nextTurn = () => new Promise(resolve => setImmediate(resolve));

test("query validation precedes HTTP and accepted queries use only a fixed GET with normalized parameters", async () => {
  const requests = [];
  const inspector = new DeadLetterInspector({ fetchImpl: async (path, options) => {
    requests.push({ path, options }); return json(snapshot());
  }, now: () => "2026-10-28T17:00:00.000Z" });
  for (const [id, limit] of [["not-a-uuid", 10], ["https://example.com", 10], [firstId, ""], [firstId, 0],
    [firstId, 21], [firstId, 1.5], [firstId, "1e1"], [firstId, "1&eventId=x"], [null, 10]]) {
    await inspector.inspect(id, limit);
    assert.equal(inspector.state.error, "INVALID_QUERY");
    assert.equal(inspector.state.snapshot, null);
  }
  assert.equal(requests.length, 0);
  await inspector.inspect(` ${firstId.toUpperCase()} `, "10");
  assert.equal(requests[0].path, `/api/dead-letters?limit=10&eventId=${firstId}`);
  assert.equal(requests[0].options.method, "GET");
  assert.equal(requests[0].options.redirect, "error");
  assert.equal(requests[0].options.cache, "no-store");
  assert.equal(requests[0].options.credentials, "omit");
  assert.equal(requests[0].options.body, undefined);
  assert.equal(inspector.state.phase, "snapshot");
  assert.equal(inspector.state.receivedAt, "2026-10-28T17:00:00.000Z");
  await inspector.inspect("", "1");
  assert.equal(requests[1].path, "/api/dead-letters?limit=1");
  assert.deepEqual(parseQuery("", 20), { eventId: null, limit: 20 });
  assert.equal(QUERY_TIMEOUT_MS, 5000);
  assert.equal(MAX_RESPONSE_BYTES, 262144);
});

test("editing a query aborts it and a late success cannot resurrect the cleared snapshot", async () => {
  const peer = deferred();
  let signal;
  const changes = [];
  const inspector = new DeadLetterInspector({ fetchImpl: (_, options) => { signal = options.signal; return peer.promise; },
    onChange: state => changes.push(state) });
  const pending = inspector.inspect(firstId, 10);
  assert.equal(inspector.state.phase, "loading");
  inspector.invalidate();
  assert.equal(signal.aborted, true);
  await pending;
  const count = changes.length;
  // This peer deliberately ignores AbortSignal and supplies its response after the form changed.
  peer.resolve(json(snapshot()));
  await nextTurn();
  assert.equal(changes.length, count);
  assert.deepEqual(inspector.state, { phase: "idle", query: null, snapshot: null, receivedAt: null, error: null });
});

test("a superseded query's late success or error cannot replace a newer successful search", async () => {
  for (const outcome of ["success", "error"]) {
    const older = deferred();
    const newer = deferred();
    const signals = [];
    const inspector = new DeadLetterInspector({ fetchImpl: (_, options) => {
      signals.push(options.signal); return signals.length === 1 ? older.promise : newer.promise;
    } });
    const first = inspector.inspect(firstId, 10);
    const second = inspector.inspect(secondId, 1);
    assert.equal(signals[0].aborted, true);
    assert.equal(signals[1].aborted, false);
    newer.resolve(json(snapshot(secondId)));
    await second;
    const current = inspector.state;
    if (outcome === "success") older.resolve(json(snapshot()));
    else older.reject(new Error("private upstream failure"));
    await first;
    await nextTurn();
    assert.strictEqual(inspector.state, current);
    assert.equal(current.query.eventId, secondId);
    assert.equal(current.snapshot.items[0].key, secondId);
  }
});

test("a new request immediately clears earlier success and errors never appear as empty or stale snapshots", async () => {
  for (const status of [429, 503, 404]) {
    const failed = deferred();
    let calls = 0;
    const inspector = new DeadLetterInspector({ fetchImpl: async () => ++calls === 1 ? json(snapshot()) : failed.promise });
    await inspector.inspect(firstId, 10);
    const pending = inspector.inspect(secondId, 10);
    assert.equal(inspector.state.phase, "loading");
    assert.equal(inspector.state.snapshot, null);
    assert.equal(inspector.state.receivedAt, null);
    failed.resolve(new Response("SECRET raw error body", { status }));
    await pending;
    assert.equal(inspector.state.phase, "error");
    assert.equal(inspector.state.error, status === 429 ? "BUSY" : "UNAVAILABLE");
    assert.equal(inspector.state.snapshot, null);
    assert.equal(JSON.stringify(inspector.state).includes("SECRET"), false);
  }
});

test("the deadline includes a stalled response body, closes the socket, and permits a later manual query", async t => {
  let count = 0;
  const closed = deferred();
  const server = http.createServer((request, response) => {
    count++;
    if (count === 1) {
      response.on("close", closed.resolve);
      response.writeHead(200, { "Content-Type": "application/json" });
      response.write('{"topic":');
    } else {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify(snapshot()));
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => { server.closeAllConnections(); server.close(); });
  const inspector = new DeadLetterInspector({ timeoutMs: 250,
    fetchImpl: (path, options) => fetch(`http://127.0.0.1:${server.address().port}${path}`, options) });
  await inspector.inspect(firstId, 10);
  assert.equal(count, 1, "the response body must actually begin before the timeout");
  assert.equal(inspector.state.error, "TIMEOUT");
  assert.equal(inspector.state.snapshot, null);
  const guard = setTimeout(() => closed.reject(new Error("stalled response did not close")), 2000);
  try { await closed.promise; } finally { clearTimeout(guard); }
  await inspector.inspect(firstId, 10);
  assert.equal(inspector.state.phase, "snapshot");
  assert.equal(count, 2, "no automatic retry or polling");
});

test("oversized streams are cancelled without exposing partial data and invalid JSON/type is rejected", async () => {
  let cancelled = false;
  const body = new ReadableStream({ start(controller) {
    controller.enqueue(new Uint8Array(MAX_RESPONSE_BYTES));
    controller.enqueue(new Uint8Array(1));
  }, cancel() { cancelled = true; } });
  const inspector = new DeadLetterInspector({ fetchImpl: async () => new Response(body, { headers: { "Content-Type": "application/json" } }) });
  await inspector.inspect("", 10);
  assert.equal(inspector.state.error, "RESPONSE_TOO_LARGE");
  assert.equal(inspector.state.snapshot, null);
  assert.equal(cancelled, true);
  for (const response of [new Response("{", { headers: { "Content-Type": "application/json" } }),
    new Response(JSON.stringify(snapshot()), { headers: { "Content-Type": "text/html" } }), json({ unexpected: true })]) {
    const invalid = new DeadLetterInspector({ fetchImpl: async () => response });
    await invalid.inspect("", 10);
    assert.equal(invalid.state.error, "INVALID_RESPONSE");
    assert.equal(invalid.state.snapshot, null);
  }
});

test("snapshot validation preserves sparse windows, missing metadata, and both independent truncation flags", () => {
  const sparse = snapshot();
  Object.assign(sparse, { startOffset: 50, endOffsetExclusive: 53, scannedRecords: 2, items: [entry(52, null), entry(50)] });
  Object.assign(sparse.items[0], { timestamp: -1, payloadPreview: null, exceptionClass: null,
    exceptionMessage: null, originalTopic: null, originalPartition: null, originalOffset: null });
  assert.deepEqual(validateSnapshot(sparse, parseQuery("", 10)), sparse);
  const truncated = { ...snapshot(), startOffset: 5, endOffsetExclusive: 105, scannedRecords: 3,
    earlierRecordsNotScanned: true, matchesTruncated: true, items: [entry(104)] };
  assert.deepEqual(validateSnapshot(truncated, parseQuery(firstId, 1)), truncated);
  const empty = { ...sparse, items: [], matchesTruncated: false };
  assert.deepEqual(validateSnapshot(empty, parseQuery(secondId, 10)), empty);
});

test("inconsistent windows, wrong event keys, order, unsafe numbers, and unbounded metadata cannot look successful", () => {
  const corruptions = [
    value => { value.topic = "other-topic"; }, value => { value.partition = 1; },
    value => { value.startOffset = 2; }, value => { value.endOffsetExclusive = 101; },
    value => { value.scannedRecords = 2; }, value => { value.scannedRecords = -1; },
    value => { value.endOffsetExclusive = Number.MAX_SAFE_INTEGER + 1; },
    value => { value.earlierRecordsNotScanned = true; }, value => { value.matchesTruncated = true; },
    value => { value.items[0].offset = 1; }, value => { value.items[0].offset = 0.5; },
    value => { value.items[0].timestamp = -2; }, value => { value.items[0].key = secondId; },
    value => { value.items[0].keyTruncated = true; }, value => { value.items[0].payloadPreview = "x".repeat(2049); },
    value => { value.items[0].payloadPreview = null; value.items[0].payloadTruncated = true; },
    value => { value.items[0].exceptionClass = "x".repeat(257); },
    value => { value.items[0].exceptionMessage = "x".repeat(513); },
    value => { value.items[0].originalTopic = ".."; }, value => { value.items[0].originalTopic = "bad/topic"; },
    value => { value.items[0].originalPartition = 2_147_483_648; },
    value => { value.items[0].originalOffset = Number.MAX_SAFE_INTEGER + 1; },
    value => { value.items.push(entry()); }, value => { delete value.items[0].exceptionCauseClass; }
  ];
  for (const corrupt of corruptions) {
    const data = snapshot(); corrupt(data);
    assert.throws(() => validateSnapshot(data, parseQuery(firstId, 10)), { code: "INVALID_RESPONSE" });
  }
  const reversed = { ...snapshot(), endOffsetExclusive: 2, scannedRecords: 2, items: [entry(0), entry(1)] };
  assert.throws(() => validateSnapshot(reversed, parseQuery("", 10)), { code: "INVALID_RESPONSE" });
  const omitted = { ...snapshot(), endOffsetExclusive: 2, scannedRecords: 2 };
  assert.throws(() => validateSnapshot(omitted, parseQuery("", 10)), { code: "INVALID_RESPONSE" });
  assert.throws(() => validateSnapshot({ ...snapshot(), items: [] }, parseQuery("", 10)), { code: "INVALID_RESPONSE" });
});

class TextNode {
  constructor(tag) { this.tag = tag; this.children = []; this.text = ""; }
  set innerHTML(_) { assert.fail("HTML interpolation must never be used for diagnostic data"); }
  set textContent(value) { this.text = String(value); this.children = []; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; this.text = ""; }
}
const descendants = node => [node, ...node.children.flatMap(descendants)];

test("rendering retains literal hostile text, names query/window/limits, and clears all records on error", () => {
  const hostile = '<img src=x onerror="alert(1)"><script>run()</script>';
  const data = snapshot(hostile);
  Object.assign(data.items[0], { payloadPreview: hostile, exceptionMessage: hostile, unknownHeader: "hidden raw header" });
  const validated = validateSnapshot(data, parseQuery("", 1));
  assert.equal(validated.items[0].unknownHeader, undefined);
  const document = { createElement: tag => new TextNode(tag) };
  const container = new TextNode("div");
  renderSnapshot(document, container, { phase: "snapshot", query: parseQuery("", 1), snapshot: validated,
    receivedAt: "2026-10-28T17:00:00.000Z" });
  const nodes = descendants(container);
  assert.equal(nodes.some(node => node.tag === "img" || node.tag === "script"), false);
  assert.equal(nodes.filter(node => node.text === hostile).length, 3);
  assert.equal(nodes.filter(node => node.tag === "details").length, 1);
  const text = nodes.map(node => node.text).join("\n");
  for (const expected of ["All recent records", "[0, 1)", "1 / 1 / 1", "Earlier records not scanned",
    "Matching records omitted by limit", "Key truncated", "Payload truncated", "Original topic"]) assert.ok(text.includes(expected));
  renderSnapshot(document, container, { phase: "error" });
  assert.deepEqual(container.children, []);
  renderSnapshot(document, container, { phase: "snapshot", query: parseQuery(secondId, 10),
    snapshot: { ...snapshot(), items: [] }, receivedAt: "now" });
  assert.ok(descendants(container).some(node => node.text.includes("does not prove that the event never failed")));
});
