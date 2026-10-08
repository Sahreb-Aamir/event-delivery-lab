"use strict";

/** Tracks observations, not simulated delivery state. A receipt can lag its API acknowledgment. */
class DeliveryTracker {
  static activeStatuses = new Set(["QUEUED", "DELIVERING", "RETRYING"]);
  static terminalStatuses = new Set(["DELIVERED", "FAILED", "INTERRUPTED"]);
  static recoverableStatuses = new Set(["FAILED", "INTERRUPTED"]);

  constructor() { this.pending = new Map(); }

  accepted(eventId, previous = null) {
    // A successfully recorded exact replay is skipped by the consumer.
    if (previous?.status === "DELIVERED" || this.pending.has(eventId)) return;
    const awaitingNewAttempt = DeliveryTracker.recoverableStatuses.has(previous?.status);
    this.pending.set(eventId, {
      minimumAttempts: awaitingNewAttempt ? (previous.attempts?.length ?? 0) + 1 : 0
    });
  }

  observe(event) {
    const id = event?.receipt?.eventId;
    if (!id) return;
    if (DeliveryTracker.activeStatuses.has(event.status)) {
      if (!this.pending.has(id)) this.pending.set(id, { minimumAttempts: 0 });
      return;
    }
    const pending = this.pending.get(id);
    // A retry's old FAILED/INTERRUPTED snapshot is not evidence that the new run finished.
    if (pending && DeliveryTracker.terminalStatuses.has(event.status)
        && (event.attempts?.length ?? 0) >= pending.minimumAttempts) {
      this.pending.delete(id);
    }
  }

  has(eventId) { return this.pending.has(eventId); }
  get ids() { return [...this.pending.keys()]; }
  get size() { return this.pending.size; }
}

if (typeof module === "object" && module.exports) module.exports = DeliveryTracker;
