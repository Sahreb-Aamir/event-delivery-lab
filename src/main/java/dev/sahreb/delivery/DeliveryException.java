package dev.sahreb.delivery;

import java.util.UUID;

/** A recorded HTTP attempt failed; Kafka's error handler decides when to retry. */
public class DeliveryException extends RuntimeException {
    private final UUID eventId;

    public DeliveryException(UUID eventId, String message) {
        super(message);
        this.eventId = eventId;
    }

    public DeliveryException(UUID eventId, String message, Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
    }

    public UUID eventId() {
        return eventId;
    }
}
