package dev.sahreb.delivery;

import java.util.UUID;

/** An event ID was reused with different business fields. */
public class EventIdentityConflictException extends RuntimeException {

    private final UUID eventId;

    public EventIdentityConflictException(UUID eventId) {
        super("Event ID " + eventId + " already has a receipt with different business fields");
        this.eventId = eventId;
    }

    public UUID eventId() {
        return eventId;
    }
}
