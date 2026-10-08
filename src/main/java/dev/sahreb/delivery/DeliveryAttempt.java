package dev.sahreb.delivery;

import java.time.Instant;
import java.util.UUID;

/** A persisted HTTP attempt; PENDING means its remote outcome is unknown. */
public record DeliveryAttempt(
        long id,
        UUID eventId,
        Instant startedAt,
        Instant finishedAt,
        Integer httpStatus,
        String outcome,
        String detail) {
}
