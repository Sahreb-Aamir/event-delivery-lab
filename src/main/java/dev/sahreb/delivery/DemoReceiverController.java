package dev.sahreb.delivery;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** A deliberately local, in-memory receiver for synthetic failure exercises. */
@RestController
public class DemoReceiverController {
    private final Map<UUID, ReceivedEvent> received = new LinkedHashMap<>();
    private int failuresRemaining;
    private int acknowledgmentsToDelay;
    private int ackDelayMs;
    private int receivedRequests;

    @PostMapping("/demo/webhook")
    public ResponseEntity<Map<String, Object>> receive(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody NotificationEvent event) {
        if (!event.eventId().toString().equals(idempotencyKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must equal eventId");
        }
        int delay;
        boolean duplicate;
        synchronized (this) {
            receivedRequests++;
            ReceivedEvent existing = received.get(event.eventId());
            duplicate = existing != null;
            if (duplicate) {
                if (!existing.sellerId().equals(event.sellerId()) || !existing.message().equals(event.message())
                        || !existing.occurredAt().equals(event.occurredAt())) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency-Key already identifies a different payload");
                }
            } else {
                if (failuresRemaining > 0) {
                    failuresRemaining--;
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                            .body(Map.of("accepted", false, "message", "Synthetic receiver failure", "failuresRemaining", failuresRemaining));
                }
                received.put(event.eventId(), new ReceivedEvent(event.eventId(), event.sellerId(), event.message(),
                        event.occurredAt(), Instant.now()));
            }
            delay = acknowledgmentsToDelay > 0 ? ackDelayMs : 0;
            if (acknowledgmentsToDelay > 0) {
                acknowledgmentsToDelay--;
            }
        }
        // The side effect above is committed before this deliberately ambiguous ACK.
        // Never hold the receiver lock while waiting: another attempt must be able to arrive.
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Acknowledgment interrupted");
            }
        }
        return ResponseEntity.ok(Map.of("accepted", true, "duplicate", duplicate, "eventId", event.eventId()));
    }

    @PostMapping("/api/demo/failures")
    public synchronized ReceiverState failures(@Valid @RequestBody FailureRequest request) {
        failuresRemaining = request.count();
        return state();
    }

    @PostMapping("/api/demo/ack-delay")
    public synchronized ReceiverState acknowledgments(@Valid @RequestBody AckDelayRequest request) {
        acknowledgmentsToDelay = request.count();
        ackDelayMs = request.delayMs();
        return state();
    }

    @GetMapping("/api/demo")
    public synchronized ReceiverState state() {
        return new ReceiverState(failuresRemaining, acknowledgmentsToDelay, ackDelayMs, receivedRequests,
                received.size(), received.size(), List.copyOf(received.values()));
    }

    public record FailureRequest(@NotNull @Min(0) @Max(10) Integer count) { }

    public record AckDelayRequest(@NotNull @Min(0) @Max(10) Integer count,
            @NotNull @Min(0) @Max(5000) Integer delayMs) { }

    public record ReceiverState(int failuresRemaining, int acknowledgmentsToDelay, int ackDelayMs,
            int receivedRequests, int sideEffectCount, int deliveryCount, List<ReceivedEvent> deliveries) { }

    public record ReceivedEvent(UUID eventId, String sellerId, String message, Instant occurredAt, Instant receivedAt) { }
}
