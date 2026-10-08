"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const state = { events: [], receiver: null, selectedId: null, selectedEvent: null, lastPayload: null, busy: false, refreshing: false, connected: false };
  const statuses = new Set(["QUEUED", "DELIVERING", "RETRYING", "DELIVERED", "FAILED", "INTERRUPTED"]);
  const activeStatuses = DeliveryTracker.activeStatuses;
  const recoverableStatuses = DeliveryTracker.recoverableStatuses;
  const tracker = new DeliveryTracker();
  const scenarioButtons = [...document.querySelectorAll(".scenario-button")];
  let pollTimer;

  class ApiError extends Error {
    constructor(message, status) { super(message); this.status = status; }
  }

  async function request(path, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(path, { ...options, signal: controller.signal, cache: "no-store", headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}), ...options.headers } });
      const raw = await response.text();
      let body;
      try { body = raw ? JSON.parse(raw) : null; } catch { body = null; }
      if (!response.ok) {
        const description = body && [body.detail, body.message, body.error].find((value) => typeof value === "string");
        throw new ApiError(description || `The server returned HTTP ${response.status}.`, response.status);
      }
      return body;
    } catch (error) {
      if (error instanceof ApiError) throw error;
      throw new ApiError(error.name === "AbortError" ? "The API did not respond within 12 seconds." : "The API could not be reached. Check that the local application is running.", 0);
    } finally { clearTimeout(timeout); }
  }

  function post(path, body) {
    return request(path, { method: "POST", ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  }

  function newId() { $("event-id").value = crypto.randomUUID(); }
  function formatTime(value, withDate = false) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "Unknown time";
    return withDate ? date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" }) : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function element(tag, className, value) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (value !== undefined) node.textContent = value;
    return node;
  }

  function statusBadge(status) {
    const value = statuses.has(status) ? status : "UNKNOWN";
    return element("span", `status-badge status-${value.toLowerCase()}`, value.charAt(0) + value.slice(1).toLowerCase());
  }

  function showNotice(message, error = false) {
    const notice = $("action-notice");
    notice.textContent = message;
    notice.classList.toggle("error", error);
    notice.setAttribute("role", error ? "alert" : "status");
    notice.hidden = false;
  }

  function setBusy(busy) {
    state.busy = busy;
    $("event-form").setAttribute("aria-busy", String(busy));
    [$("send-button"), $("new-id-button")].forEach((button) => { button.disabled = busy; });
    updateScenarioControls();
    $("replay-button").disabled = busy || !state.lastPayload;
    $("send-button").firstElementChild.textContent = busy ? "Working…" : "Send event";
  }

  function formPayload(useFreshId = false) {
    if (!$("event-form").reportValidity()) return null;
    const sellerId = $("seller-id").value.trim();
    const message = $("message").value.trim();
    if (!sellerId || !message) {
      showNotice("Enter a seller ID and a notification containing more than whitespace.", true);
      (!sellerId ? $("seller-id") : $("message")).focus();
      return null;
    }
    const eventId = useFreshId ? crypto.randomUUID() : $("event-id").value;
    if (state.lastPayload?.eventId === eventId) {
      if (state.lastPayload.sellerId !== sellerId || state.lastPayload.message !== message) {
        showNotice("This ID was already submitted with a different payload. Choose New ID before changing its notification.", true);
        return null;
      }
      return { ...state.lastPayload };
    }
    return { eventId, sellerId, message, occurredAt: new Date().toISOString() };
  }

  async function sendPayload(payload, replay = false) {
    state.lastPayload = { ...payload };
    let previous = state.events.find((event) => event.receipt.eventId === payload.eventId);
    if (replay) {
      try { previous = await request(`/api/events/${encodeURIComponent(payload.eventId)}`); }
      catch (error) { if (error.status !== 404) throw error; }
    }
    await post("/api/events", payload);
    tracker.accepted(payload.eventId, previous);
    updateScenarioControls();
    state.selectedId = payload.eventId;
    showPendingDetail(payload.eventId);
    if (!replay) newId();
    showNotice(replay ? "The same event ID and original payload were accepted again. Inspect the receipt and attempts to verify replay behavior." : "Event accepted by the API. A receipt will appear after Kafka processing; accepted does not mean delivered.");
    await refresh();
  }

  function submissionError(error) {
    let message = error.message;
    if (error.status === 503) message += " The broker may be unavailable. Check the local services before retrying.";
    if (error.status === 0 || error.status >= 500) message += " Delivery state is uncertain; inspect recent events before submitting again. Replay preserves the original payload. This request was not automatically resubmitted.";
    showNotice(message, true);
  }

  async function runAction(action) {
    if (state.busy) return;
    setBusy(true);
    try { await action(); } catch (error) { submissionError(error); }
    finally { setBusy(false); }
  }

  function renderEvents(events) {
    const tbody = $("events-body");
    const focusedId = document.activeElement?.dataset.eventId;
    const fragment = document.createDocumentFragment();
    for (const event of events) {
      const receipt = event.receipt;
      const row = element("tr", receipt.eventId === state.selectedId ? "selected" : "");
      const eventCell = element("td");
      const select = element("button", "event-select");
      select.type = "button";
      select.dataset.eventId = receipt.eventId;
      select.setAttribute("aria-pressed", String(receipt.eventId === state.selectedId));
      select.setAttribute("aria-label", `Inspect event ${receipt.eventId}, ${event.status}`);
      select.append(element("strong", "mono", receipt.eventId.slice(0, 8)), element("span", "", receipt.sellerId));
      select.addEventListener("click", () => selectEvent(receipt.eventId));
      eventCell.append(select);
      const statusCell = element("td");
      statusCell.append(statusBadge(event.status));
      const attemptsCell = element("td", "attempt-number", String(event.attempts?.length ?? 0));
      const timeCell = element("td", "event-time", formatTime(receipt.recordedAt));
      timeCell.title = receipt.recordedAt || "";
      row.append(eventCell, statusCell, attemptsCell, timeCell);
      fragment.append(row);
    }
    tbody.replaceChildren(fragment);
    if (focusedId) [...tbody.querySelectorAll("button")].find((button) => button.dataset.eventId === focusedId)?.focus({ preventScroll: true });
    $("events-empty").hidden = events.length > 0;
    $("events-table-wrap").hidden = events.length === 0;
    $("metric-total").textContent = events.length;
    $("metric-delivered").textContent = events.filter((event) => event.status === "DELIVERED").length;
    $("metric-progress").textContent = events.filter((event) => activeStatuses.has(event.status)).length;
    $("metric-failed").textContent = events.filter((event) => recoverableStatuses.has(event.status)).length;
    updateScenarioControls();
  }

  function updateScenarioControls() {
    const active = tracker.size > 0;
    const controlsLocked = state.busy || active || !state.connected;
    scenarioButtons.forEach((button) => { button.disabled = controlsLocked; });
    $("set-failures-button").disabled = controlsLocked;
    $("failure-count").disabled = controlsLocked;
    $("retry-event-button").disabled = state.busy || tracker.has(state.selectedId);
    $("scenario-state").hidden = !active;
    $("scenario-state").textContent = active ? `${tracker.size} accepted or observed event${tracker.size === 1 ? " is" : "s are"} awaiting a final outcome. Scenario controls stay locked until those runs finish, including accepted retries waiting for a new attempt.` : "";
  }

  function showPendingDetail(id) {
    if (state.events.some((event) => event.receipt.eventId === id)) return;
    $("detail-content").hidden = true;
    $("detail-status").hidden = true;
    $("detail-empty").hidden = false;
    $("detail-empty").querySelector("p").textContent = `Waiting for receipt ${id.slice(0, 8)}. It will appear after Kafka consumption.`;
    renderReceiver(state.receiver);
  }

  function renderReceiver(receiver) {
    if (!receiver) return;
    $("receiver-requests").textContent = Number.isInteger(receiver.receivedRequests) ? receiver.receivedRequests : "—";
    $("receiver-effects").textContent = Number.isInteger(receiver.deliveryCount) ? receiver.deliveryCount : "—";
    const evidence = $("receiver-event-evidence");
    evidence.replaceChildren();
    evidence.classList.remove("record-found");
    if (!state.selectedId) {
      evidence.textContent = "Select an event to check its receiver-side record.";
      return;
    }
    const records = Array.isArray(receiver.deliveries) ? receiver.deliveries.filter((delivery) => delivery.eventId === state.selectedId) : [];
    evidence.append(element("span", "evidence-label", "SELECTED EVENT"), element("code", "evidence-id", state.selectedId));
    if (records.length) {
      evidence.classList.add("record-found");
      evidence.append(element("strong", "", `${records.length} receiver-side record${records.length === 1 ? "" : "s"}`), element("span", "evidence-description", "Matched by event ID in the controlled receiver’s current state."));
    } else {
      evidence.append(element("strong", "", "No matching receiver record"), element("span", "evidence-description", "The receiver may not have processed it yet, or its in-memory state may have reset."));
    }
  }

  function renderDetail(event) {
    if (!event?.receipt) return;
    state.selectedEvent = event;
    const receipt = event.receipt;
    $("detail-empty").hidden = true;
    $("detail-content").hidden = false;
    $("detail-status").hidden = false;
    $("detail-status").replaceChildren(statusBadge(event.status));
    $("detail-id").textContent = receipt.eventId;
    $("detail-seller").textContent = receipt.sellerId;
    $("detail-message").textContent = receipt.message;
    $("detail-occurred").textContent = formatTime(receipt.occurredAt, true);
    $("detail-occurred").title = receipt.occurredAt;
    $("detail-fetch-error").hidden = true;
    $("failed-actions").hidden = !recoverableStatuses.has(event.status);
    const interrupted = event.status === "INTERRUPTED";
    $("recovery-title").textContent = interrupted ? "The previous demo run was interrupted." : "Automatic delivery stopped.";
    $("recovery-description").textContent = interrupted ? "The old broker is gone. Any unfinished attempt has an unknown remote outcome. Explicit retry can repeat its effect if the receiver has forgotten its idempotency key." : "Inspect the attempts above. Clear injected failures before retrying.";
    $("retry-event-button").disabled = state.busy || tracker.has(receipt.eventId);
    const attempts = [...(event.attempts || [])].sort((a, b) => new Date(a.startedAt) - new Date(b.startedAt));
    $("detail-attempt-count").textContent = `${attempts.length} recorded`;
    $("attempts-empty").hidden = attempts.length > 0;
    const fragment = document.createDocumentFragment();
    attempts.forEach((attempt, index) => {
      const pending = attempt.outcome === "PENDING";
      const success = attempt.outcome === "SUCCEEDED";
      const item = element("li", "attempt");
      const marker = element("span", `attempt-marker${pending ? " pending" : success ? "" : " error"}`, pending ? "·" : success ? "✓" : "!");
      marker.setAttribute("aria-hidden", "true");
      const topLine = element("div", "attempt-topline");
      const outcomes = { PENDING: interrupted ? "Outcome unknown" : "Pending outcome", SUCCEEDED: "Delivered", HTTP_ERROR: "HTTP error", NETWORK_ERROR: "Network error" };
      const title = element("span", "attempt-title", `Attempt ${index + 1} · ${outcomes[attempt.outcome] || attempt.outcome || "Unknown outcome"}`);
      if (attempt.httpStatus != null) title.append(element("span", "attempt-code", `HTTP ${attempt.httpStatus}`));
      const time = element("time", "attempt-timestamp", formatTime(attempt.startedAt));
      time.dateTime = attempt.startedAt || "";
      time.title = attempt.startedAt || "";
      topLine.append(title, time);
      const details = [];
      if (attempt.detail) details.push(attempt.detail);
      if (attempt.finishedAt && attempt.startedAt) {
        const duration = new Date(attempt.finishedAt) - new Date(attempt.startedAt);
        if (Number.isFinite(duration) && duration >= 0) details.push(`${duration.toLocaleString()} ms`);
      }
      if (pending) details.push("No outcome was recorded. The receiver may have performed the action; this attempt remains unknown even after a later retry.");
      item.append(marker, topLine);
      if (details.length) item.append(element("p", "attempt-detail", details.join(" · ")));
      fragment.append(item);
    });
    $("attempts-list").replaceChildren(fragment);
    renderReceiver(state.receiver);
  }

  async function selectEvent(id) {
    state.selectedId = id;
    renderEvents(state.events);
    const cached = state.events.find((event) => event.receipt.eventId === id);
    if (cached) renderDetail(cached);
    try {
      const event = await request(`/api/events/${encodeURIComponent(id)}`);
      tracker.observe(event);
      updateScenarioControls();
      if (state.selectedId === id) renderDetail(event);
    } catch (error) {
      if (state.selectedId === id) {
        $("detail-fetch-error").textContent = `${error.message} Showing the most recent available detail.`;
        $("detail-fetch-error").hidden = false;
      }
    }
  }

  function connection(ok, message) {
    state.connected = ok;
    $("connection-dot").className = `connection-dot${ok ? "" : " offline"}`;
    $("connection-label").textContent = ok ? "API connected" : "API unavailable";
    $("network-error").hidden = ok;
    if (ok) $("last-updated").textContent = `Updated ${formatTime(new Date().toISOString())}`;
    else $("network-error-text").textContent = `${message} Previously loaded data may be stale.`;
    updateScenarioControls();
  }

  async function refresh() {
    if (state.refreshing) return;
    state.refreshing = true;
    $("refresh-button").disabled = true;
    try {
      const [eventsResult, receiverResult] = await Promise.allSettled([request("/api/events"), request("/api/demo")]);
      if (receiverResult.status === "fulfilled") {
        state.receiver = receiverResult.value;
        renderReceiver(state.receiver);
        $("receiver-error").hidden = true;
      } else {
        $("receiver-error").textContent = `${receiverResult.reason.message} Receiver observations may be stale.`;
        $("receiver-error").hidden = false;
      }
      if (eventsResult.status === "rejected") throw eventsResult.reason;
      const events = eventsResult.value;
      if (!Array.isArray(events) || events.some((event) => !event?.receipt?.eventId)) throw new ApiError("The API returned an unexpected event list.", 0);
      state.events = events;
      events.forEach((event) => tracker.observe(event));
      if (!state.selectedId && events.length) state.selectedId = events[0].receipt.eventId;
      // Pending/active IDs remain tracked when absent from the latest-20 window.
      const recentIds = new Set(events.map((event) => event.receipt.eventId));
      const lookupIds = [...new Set([...tracker.ids, ...(state.selectedId ? [state.selectedId] : [])])]
        .filter((id) => !recentIds.has(id));
      const directResults = await Promise.allSettled(lookupIds.map((id) => request(`/api/events/${encodeURIComponent(id)}`)));
      const details = new Map(events.map((event) => [event.receipt.eventId, event]));
      directResults.forEach((result, index) => {
        const id = lookupIds[index];
        if (result.status === "fulfilled") {
          details.set(id, result.value);
          tracker.observe(result.value);
        } else if (state.selectedId === id && result.reason.status !== 404) {
          $("detail-fetch-error").textContent = `${result.reason.message} Detail may be stale; pending controls remain locked.`;
          $("detail-fetch-error").hidden = false;
        }
        // A missing receipt is not a terminal delivery outcome: keep waiting.
      });
      renderEvents(events);
      connection(true);
      const selected = details.get(state.selectedId);
      if (selected) renderDetail(selected);
    } catch (error) { connection(false, error.message); }
    finally { state.refreshing = false; $("refresh-button").disabled = false; }
  }

  function startPolling() {
    clearInterval(pollTimer);
    $("live-label").textContent = document.hidden ? "REFRESH PAUSED" : "AUTO REFRESH";
    if (!document.hidden) pollTimer = setInterval(refresh, 2000);
  }

  $("event-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const payload = formPayload();
    if (payload) runAction(() => sendPayload(payload));
  });
  $("new-id-button").addEventListener("click", newId);
  $("message").addEventListener("input", () => { $("message-count").textContent = `${$("message").value.length} / 500`; });
  $("message-count").textContent = `${$("message").value.length} / 500`;
  $("replay-button").addEventListener("click", () => { if (state.lastPayload) runAction(() => sendPayload({ ...state.lastPayload }, true)); });
  scenarioButtons.forEach((button) => button.addEventListener("click", () => {
    if (tracker.size) return;
    const payload = formPayload(true);
    if (!payload) return;
    runAction(async () => {
      const count = Number(button.dataset.failures || 0);
      await post("/api/demo/failures", { count });
      await post("/api/demo/ack-delay", { count: button.id === "lost-ack-button" ? 1 : 0, delayMs: 2000 });
      $("failure-count").value = count;
      await sendPayload(payload);
    });
  }));
  $("set-failures-button").addEventListener("click", () => {
    if (tracker.size) return;
    if (!$("failure-count").reportValidity()) return;
    const count = Number($("failure-count").value);
    if (!Number.isInteger(count) || count < 0 || count > 10) { showNotice("Choose a whole number of receiver failures from 0 to 10.", true); return; }
    runAction(async () => {
      await post("/api/demo/failures", { count });
      showNotice(count === 0 ? "Injected receiver failures cleared. Failed or interrupted events can now be retried from their delivery detail." : `The receiver will fail its next ${count} request${count === 1 ? "" : "s"}. This counter is shared by all events.`);
    });
  });
  $("retry-event-button").addEventListener("click", () => {
    const id = state.selectedId;
    if (!id || tracker.has(id)) return;
    runAction(async () => {
      const previous = await request(`/api/events/${encodeURIComponent(id)}`);
      await post(`/api/events/${encodeURIComponent(id)}/retry`);
      tracker.accepted(id, previous);
      updateScenarioControls();
      showNotice("Manual retry accepted. Follow the selected event’s new HTTP attempts.");
      await refresh();
    });
  });
  $("refresh-button").addEventListener("click", refresh);
  $("retry-refresh-button").addEventListener("click", refresh);
  document.addEventListener("visibilitychange", () => { startPolling(); if (!document.hidden) refresh(); });
  newId();
  updateScenarioControls();
  refresh();
  startPolling();
})();
