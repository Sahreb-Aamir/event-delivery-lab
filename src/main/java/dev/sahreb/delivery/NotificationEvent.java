package dev.sahreb.delivery;

import java.time.Instant;
import java.util.UUID;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** A synthetic notification submitted to the local recording adapter. */
public record NotificationEvent(
        @NotNull UUID eventId,
        @NotBlank @Size(max = 64) @Pattern(regexp = "demo-[a-z0-9-]+") String sellerId,
        @NotBlank @Size(max = 500) String message,
        @NotNull Instant occurredAt) {
}
