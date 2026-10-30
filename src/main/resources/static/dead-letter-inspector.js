"use strict";

// This panel owns its query and snapshot; delivery selection and polling never change it.
(() => {
  const MAX_RESPONSE_BYTES = 262_144;
  const QUERY_TIMEOUT_MS = 5_000;
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const errors = {
    INVALID_QUERY: "Enter a complete event UUID, or leave it empty for all recent records. The limit must be a whole number from 1 to 20.",
    TIMEOUT: "Inspection timed out. No snapshot is shown. Inspect again when the local broker is available.",
    BUSY: "Another dead-letter inspection is running. No snapshot is shown. Try again after it finishes.",
    UNAVAILABLE: "Dead-letter inspection is unavailable. No snapshot is shown. Check that the local lab is running.",
    INVALID_RESPONSE: "The API returned an invalid or inconsistent snapshot. No result is shown.",
    RESPONSE_TOO_LARGE: "The response exceeded the 256 KiB inspection limit. No partial snapshot is shown."
  };
  const failure = code => Object.assign(new Error(code), { code });
  const object = value => value !== null && typeof value === "object" && !Array.isArray(value);
  const integer = (value, min = 0, max = Number.MAX_SAFE_INTEGER) => Number.isSafeInteger(value) && value >= min && value <= max;
  const nullableText = (value, max) => value === null || (typeof value === "string" && value.length <= max);

  function parseQuery(eventId, limit) {
    if (typeof eventId !== "string" || !/^(?:[1-9]|1[0-9]|20)$/.test(String(limit))) throw failure("INVALID_QUERY");
    eventId = eventId.trim().toLowerCase();
    if (eventId !== "" && !UUID.test(eventId)) throw failure("INVALID_QUERY");
    return { eventId: eventId || null, limit: Number(limit) };
  }

  function validateSnapshot(data, query) {
    if (!object(data) || data.topic !== "notification.requests.DLT" || data.partition !== 0
        || !integer(data.startOffset) || !integer(data.endOffsetExclusive) || !integer(data.scannedRecords, 0, 100)
        || typeof data.earlierRecordsNotScanned !== "boolean" || typeof data.matchesTruncated !== "boolean"
        || !Array.isArray(data.items) || data.items.length > query.limit || data.items.length > 20) throw failure("INVALID_RESPONSE");
    const width = data.endOffsetExclusive - data.startOffset;
    if (width < 0 || width > 100 || data.scannedRecords > width || data.items.length > data.scannedRecords
        || (data.earlierRecordsNotScanned && (data.startOffset === 0 || width !== 100))
        || (data.matchesTruncated && (data.items.length !== query.limit || data.scannedRecords <= data.items.length))) throw failure("INVALID_RESPONSE");
    const items = data.items.map((entry, index) => {
      if (!object(entry) || !integer(entry.offset) || !integer(entry.timestamp, -1)
          || entry.offset < data.startOffset || entry.offset >= data.endOffsetExclusive
          || (index > 0 && entry.offset >= data.items[index - 1].offset)
          || !nullableText(entry.key, 128) || typeof entry.keyTruncated !== "boolean"
          || !nullableText(entry.payloadPreview, 2048) || typeof entry.payloadTruncated !== "boolean"
          || !nullableText(entry.exceptionClass, 256) || !nullableText(entry.exceptionMessage, 512)
          || !nullableText(entry.exceptionCauseClass, 256)
          || !(entry.originalTopic === null || (typeof entry.originalTopic === "string"
            && /^[a-zA-Z0-9._-]{1,249}$/.test(entry.originalTopic) && entry.originalTopic !== "." && entry.originalTopic !== ".."))
          || !(entry.originalPartition === null || integer(entry.originalPartition, 0, 2_147_483_647))
          || !(entry.originalOffset === null || integer(entry.originalOffset))
          || (entry.key === null && entry.keyTruncated) || (entry.payloadPreview === null && entry.payloadTruncated)
          || (query.eventId !== null && (entry.key !== query.eventId || entry.keyTruncated))) throw failure("INVALID_RESPONSE");
      // Keep only the documented fields. Arbitrary headers or response fields are not displayed.
      return Object.fromEntries(["offset", "timestamp", "key", "keyTruncated", "payloadPreview", "payloadTruncated",
        "exceptionClass", "exceptionMessage", "exceptionCauseClass", "originalTopic", "originalPartition", "originalOffset"]
        .map(key => [key, entry[key]]));
    });
    if (query.eventId === null && (items.length !== Math.min(query.limit, data.scannedRecords)
        || data.matchesTruncated !== (data.scannedRecords > query.limit))) throw failure("INVALID_RESPONSE");
    return { topic: data.topic, partition: data.partition, startOffset: data.startOffset, endOffsetExclusive: data.endOffsetExclusive,
      scannedRecords: data.scannedRecords, earlierRecordsNotScanned: data.earlierRecordsNotScanned,
      matchesTruncated: data.matchesTruncated, items };
  }

  async function readSnapshot(fetchImpl, query, signal) {
    const params = new URLSearchParams({ limit: String(query.limit) });
    if (query.eventId !== null) params.set("eventId", query.eventId);
    const response = await fetchImpl(`/api/dead-letters?${params}`, {
      method: "GET", redirect: "error", cache: "no-store", credentials: "omit", signal, headers: { Accept: "application/json" }
    });
    if (signal.aborted || !response.ok) {
      if (response.body) void response.body.cancel().catch(() => {});
      if (signal.aborted) throw signal.reason;
      throw failure(response.status === 429 ? "BUSY" : "UNAVAILABLE");
    }
    if (response.headers.get("content-type")?.split(";")[0].trim().toLowerCase() !== "application/json" || !response.body) {
      if (response.body) void response.body.cancel().catch(() => {});
      throw failure("INVALID_RESPONSE");
    }
    const reader = response.body.getReader();
    const cancel = () => { void reader.cancel().catch(() => {}); };
    signal.addEventListener("abort", cancel, { once: true });
    const decoder = new TextDecoder();
    let size = 0;
    let text = "";
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (signal.aborted) throw signal.reason;
        if (done) break;
        size += value.byteLength;
        if (size > MAX_RESPONSE_BYTES) throw failure("RESPONSE_TOO_LARGE");
        text += decoder.decode(value, { stream: true });
      }
      text += decoder.decode();
      let data;
      try { data = JSON.parse(text); } catch { throw failure("INVALID_RESPONSE"); }
      return validateSnapshot(data, query);
    } finally {
      signal.removeEventListener("abort", cancel);
      cancel();
      reader.releaseLock();
    }
  }

  class DeadLetterInspector {
    constructor({ fetchImpl = globalThis.fetch.bind(globalThis), onChange = () => {},
      now = () => new Date().toISOString(), timeoutMs = QUERY_TIMEOUT_MS } = {}) {
      this.fetchImpl = fetchImpl;
      this.onChange = onChange;
      this.now = now;
      this.timeoutMs = timeoutMs;
      this.generation = 0;
      this.active = null;
      this.state = { phase: "idle", query: null, snapshot: null, receivedAt: null, error: null };
    }

    publish(state) { this.state = state; this.onChange(state); }

    invalidate() {
      this.generation++;
      this.active?.abort();
      this.active = null;
      this.publish({ phase: "idle", query: null, snapshot: null, receivedAt: null, error: null });
    }

    async inspect(eventId, limit) {
      this.invalidate();
      let query;
      try { query = parseQuery(eventId, limit); }
      catch { this.publish({ phase: "error", query: null, snapshot: null, receivedAt: null, error: "INVALID_QUERY" }); return; }
      const generation = this.generation;
      const controller = new AbortController();
      this.active = controller;
      this.publish({ phase: "loading", query, snapshot: null, receivedAt: null, error: null });
      let rejectAbort;
      const aborted = new Promise((_, reject) => { rejectAbort = () => reject(controller.signal.reason); });
      controller.signal.addEventListener("abort", rejectAbort, { once: true });
      const timer = setTimeout(() => controller.abort(failure("TIMEOUT")), this.timeoutMs);
      try {
        // A generation check also protects the panel if a peer ignores the abort signal.
        const snapshot = await Promise.race([readSnapshot(this.fetchImpl, query, controller.signal), aborted]);
        if (generation !== this.generation) return;
        this.publish({ phase: "snapshot", query, snapshot, receivedAt: this.now(), error: null });
      } catch (error) {
        if (generation !== this.generation) return;
        const code = errors[error?.code] ? error.code : "UNAVAILABLE";
        this.publish({ phase: "error", query, snapshot: null, receivedAt: null, error: code });
      } finally {
        clearTimeout(timer);
        controller.signal.removeEventListener("abort", rejectAbort);
        controller.abort();
        if (generation === this.generation) this.active = null;
      }
    }
  }

  function renderSnapshot(document, container, state) {
    container.replaceChildren();
    if (state.phase !== "snapshot") return;
    const data = state.snapshot;
    const node = (tag, text, className) => {
      const element = document.createElement(tag);
      if (text !== undefined) element.textContent = text;
      if (className) element.className = className;
      return element;
    };
    const field = (list, label, value, className) => {
      const row = node("div");
      row.append(node("dt", label), node("dd", value === null ? "Not recorded" : String(value), className));
      list.append(row);
    };
    const summary = node("dl", undefined, "dlt-summary");
    field(summary, "Query", state.query.eventId ?? "All recent records", "mono");
    field(summary, "Snapshot received (browser clock)", state.receivedAt);
    field(summary, "Topic / partition", `${data.topic} / ${data.partition}`, "mono");
    field(summary, "Offset window [start, end)", `[${data.startOffset}, ${data.endOffsetExclusive})`, "mono");
    field(summary, "Records scanned / shown / limit", `${data.scannedRecords} / ${data.items.length} / ${state.query.limit}`);
    field(summary, "Earlier records not scanned", data.earlierRecordsNotScanned ? "Yes — earlier retained offsets are outside this window." : "No — the scan started at the retained beginning.");
    field(summary, "Matching records omitted by limit", data.matchesTruncated ? "Yes — only the newest matching records are shown." : "No — all matches within this window are shown.");
    container.append(summary);
    if (data.items.length === 0) container.append(node("p",
      "No matching records in this snapshot. An empty window does not prove that the event never failed.", "dlt-empty"));
    for (const entry of data.items) {
      const detail = node("details", undefined, "dlt-record");
      detail.append(node("summary", `Offset ${entry.offset} · ${entry.key === null ? "No record key" : entry.key}${entry.keyTruncated ? " (key preview)" : ""}`));
      const fields = node("dl", undefined, "dlt-record-fields");
      field(fields, "Timestamp (epoch milliseconds)", entry.timestamp === -1 ? "Not available" : entry.timestamp);
      field(fields, "Record key", entry.key, "mono");
      field(fields, "Key truncated", entry.keyTruncated ? "Yes" : "No");
      field(fields, "Payload preview", entry.payloadPreview, "dlt-payload mono");
      field(fields, "Payload truncated", entry.payloadTruncated ? "Yes" : "No");
      field(fields, "Exception class (preview)", entry.exceptionClass, "mono");
      field(fields, "Exception message (preview)", entry.exceptionMessage, "dlt-payload");
      field(fields, "Cause class (preview)", entry.exceptionCauseClass, "mono");
      field(fields, "Original topic", entry.originalTopic, "mono");
      field(fields, "Original partition", entry.originalPartition);
      field(fields, "Original offset", entry.originalOffset);
      detail.append(fields);
      container.append(detail);
    }
  }

  function mount(document) {
    const form = document.getElementById("dead-letter-form");
    if (!form) return;
    const eventId = document.getElementById("dead-letter-event-id");
    const limit = document.getElementById("dead-letter-limit");
    const button = document.getElementById("dead-letter-inspect");
    const status = document.getElementById("dead-letter-status");
    const results = document.getElementById("dead-letter-results");
    const inspector = new DeadLetterInspector({ onChange(state) {
      button.disabled = state.phase === "loading";
      form.setAttribute("aria-busy", String(state.phase === "loading"));
      status.className = state.phase === "error" ? "dlt-status dlt-error" : "dlt-status";
      status.textContent = state.phase === "error" ? errors[state.error]
        : state.phase === "loading" ? `Inspecting ${state.query.eventId ?? "all recent records"} (limit ${state.query.limit})…`
        : state.phase === "snapshot" ? `Snapshot ready: ${state.snapshot.items.length} matching record(s) shown. Inspect again to refresh.`
        : "Enter an optional event ID, then inspect. This panel refreshes only when requested.";
      renderSnapshot(document, results, state);
    } });
    eventId.addEventListener("input", () => inspector.invalidate());
    limit.addEventListener("input", () => inspector.invalidate());
    form.addEventListener("submit", event => { event.preventDefault(); void inspector.inspect(eventId.value, limit.value); });
    inspector.invalidate();
  }

  if (typeof module === "object" && module.exports) {
    module.exports = { DeadLetterInspector, parseQuery, validateSnapshot, renderSnapshot, MAX_RESPONSE_BYTES, QUERY_TIMEOUT_MS };
  } else if (typeof document !== "undefined") mount(document);
})();
