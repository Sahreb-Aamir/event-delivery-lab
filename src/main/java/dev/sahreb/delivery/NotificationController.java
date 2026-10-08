package dev.sahreb.delivery;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/api/events")
public class NotificationController {
    private final ReceiptStore receipts;
    private final DeliveryStore deliveries;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final long acknowledgementTimeoutMillis;

    @Autowired
    public NotificationController(ReceiptStore receipts, DeliveryStore deliveries,
            KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
        this(receipts, deliveries, kafka, mapper, 5_000);
    }

    NotificationController(ReceiptStore receipts, DeliveryStore deliveries,
            KafkaTemplate<String, String> kafka, ObjectMapper mapper, long acknowledgementTimeoutMillis) {
        this.receipts = receipts;
        this.deliveries = deliveries;
        this.kafka = kafka;
        this.mapper = mapper;
        this.acknowledgementTimeoutMillis = acknowledgementTimeoutMillis;
    }

    @PostMapping
    public ResponseEntity<QueuedEvent> submit(@Valid @RequestBody NotificationEvent event) {
        return publish(event);
    }

    @GetMapping
    public List<EventView> recent() {
        return receipts.recent(20).stream().map(this::view).toList();
    }

    @GetMapping("/{eventId}")
    public EventView get(@PathVariable UUID eventId) {
        return view(receipt(eventId));
    }

    @PostMapping("/{eventId}/retry")
    public ResponseEntity<QueuedEvent> retry(@PathVariable UUID eventId) {
        Receipt receipt = receipt(eventId);
        String currentStatus = deliveries.status(eventId).status();
        if (!"FAILED".equals(currentStatus) && !"INTERRUPTED".equals(currentStatus)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Only FAILED or INTERRUPTED events can be retried");
        }
        return publish(new NotificationEvent(receipt.eventId(), receipt.sellerId(), receipt.message(), receipt.occurredAt()));
    }

    private ResponseEntity<QueuedEvent> publish(NotificationEvent event) {
        String json = mapper.writeValueAsString(event);
        try {
            kafka.send(KafkaConfig.REQUESTS, event.eventId().toString(), json)
                    .get(acknowledgementTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw unavailable(event.eventId(), interrupted);
        } catch (ExecutionException | TimeoutException | RuntimeException failure) {
            throw unavailable(event.eventId(), failure);
        }
        return ResponseEntity.accepted().body(new QueuedEvent(event.eventId(), "QUEUED"));
    }

    private ResponseStatusException unavailable(UUID eventId, Exception failure) {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Kafka acknowledgement unavailable. Delivery may still occur; retry the same payload with eventId " + eventId,
                failure);
    }

    private Receipt receipt(UUID eventId) {
        return receipts.find(eventId).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No recorded event with this ID"));
    }

    private EventView view(Receipt receipt) {
        DeliveryStatus delivery = deliveries.status(receipt.eventId());
        return new EventView(receipt, delivery.status(), delivery.attempts());
    }

    public record QueuedEvent(UUID eventId, String status) { }

    public record EventView(Receipt receipt, String status, List<DeliveryAttempt> attempts) { }
}
