package dev.sahreb.delivery;

import java.time.Instant;
import java.util.UUID;

/** The first accepted payload for an event ID and when it was recorded. */
public record Receipt(
        UUID eventId,
        String sellerId,
        String message,
        Instant occurredAt,
        Instant recordedAt) {
}
