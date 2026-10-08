"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const DeliveryTracker = require("../../main/resources/static/delivery-tracker.js");

const event = (id, status, attempts = 0) => ({ receipt: { eventId: id }, status, attempts: Array.from({ length: attempts }, (_, id) => ({ id })) });

test("accepted event remains pending while the receipt is absent, until a terminal observation", () => {
  const tracker = new DeliveryTracker();
  tracker.accepted("new-event");
  assert.equal(tracker.size, 1);
  // An empty recent list, or a direct GET returning 404, provides no observation.
  assert.deepEqual(tracker.ids, ["new-event"]);
  tracker.observe(event("new-event", "DELIVERING", 1));
  assert.equal(tracker.has("new-event"), true);
  tracker.observe(event("new-event", "DELIVERED", 1));
  assert.equal(tracker.size, 0);
});

test("manual retry cannot unlock on the previous FAILED snapshot", () => {
  const tracker = new DeliveryTracker();
  tracker.accepted("retry-event", event("retry-event", "FAILED", 3));
  tracker.observe(event("retry-event", "FAILED", 3));
  assert.equal(tracker.has("retry-event"), true);
  tracker.observe(event("retry-event", "DELIVERING", 4));
  assert.equal(tracker.has("retry-event"), true);
  tracker.observe(event("retry-event", "DELIVERED", 4));
  assert.equal(tracker.size, 0);
});

test("terminal failed retry unlocks only after additional attempts, even if polling misses active states", () => {
  const tracker = new DeliveryTracker();
  tracker.accepted("retry-event", event("retry-event", "FAILED", 3));
  tracker.observe(event("retry-event", "FAILED", 6));
  assert.equal(tracker.size, 0);
});

test("interrupted records are terminal and explicitly retryable without losing the baseline", () => {
  const tracker = new DeliveryTracker();
  tracker.observe(event("interrupted", "INTERRUPTED", 1));
  assert.equal(tracker.size, 0);
  tracker.accepted("interrupted", event("interrupted", "INTERRUPTED", 1));
  tracker.observe(event("interrupted", "INTERRUPTED", 1));
  assert.equal(tracker.has("interrupted"), true);
  tracker.observe(event("interrupted", "DELIVERED", 2));
  assert.equal(tracker.size, 0);
});

test("active observations remain tracked after leaving the recent window and can finish via direct lookup", () => {
  const tracker = new DeliveryTracker();
  tracker.observe(event("old-active", "RETRYING", 2));
  for (let i = 0; i < 20; i++) tracker.observe(event(`newer-${i}`, "DELIVERED", 1));
  assert.deepEqual(tracker.ids, ["old-active"]);
  tracker.observe(event("old-active", "FAILED", 3));
  assert.equal(tracker.size, 0);
});

test("replay of an already delivered event needs no new attempt; repeated acceptance preserves a retry baseline", () => {
  const tracker = new DeliveryTracker();
  tracker.accepted("delivered", event("delivered", "DELIVERED", 1));
  assert.equal(tracker.size, 0);
  tracker.accepted("failed", event("failed", "FAILED", 3));
  tracker.accepted("failed");
  tracker.observe(event("failed", "FAILED", 3));
  assert.equal(tracker.has("failed"), true);
});
