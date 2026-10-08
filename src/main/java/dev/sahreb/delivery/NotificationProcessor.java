package dev.sahreb.delivery;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/** Records durable intent before sending HTTP; a receipt alone is not delivery. */
@Component
public class NotificationProcessor {
    private static final int MAX_EVENT_LENGTH = 16_384;
    private static final int MAX_RESPONSE_BYTES = 2_048;
    private final ReceiptStore receipts;
    private final DeliveryStore deliveries;
    private final ObjectMapper mapper;
    private final Validator validator;
    private final HttpClient client;
    private final URI webhook;
    private final Duration timeout;

    @Autowired
    public NotificationProcessor(ReceiptStore receipts, DeliveryStore deliveries, ObjectMapper mapper,
            Validator validator, @Value("${lab.webhook-url:http://127.0.0.1:8080/demo/webhook}") String webhook,
            @Value("${lab.webhook-timeout-ms:1000}") long timeoutMillis) {
        this(receipts, deliveries, mapper, validator,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                        .followRedirects(HttpClient.Redirect.NEVER).build(),
                URI.create(webhook), Duration.ofMillis(timeoutMillis));
    }

    NotificationProcessor(ReceiptStore receipts, DeliveryStore deliveries, ObjectMapper mapper,
            Validator validator, HttpClient client, URI webhook, Duration timeout) {
        if (!("http".equals(webhook.getScheme()) || "https".equals(webhook.getScheme()))
                || webhook.getHost() == null || webhook.getUserInfo() != null) {
            throw new IllegalArgumentException("lab.webhook-url must be an absolute HTTP(S) URL without credentials");
        }
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Webhook timeout must be positive");
        }
        this.receipts = receipts;
        this.deliveries = deliveries;
        this.mapper = mapper;
        this.validator = validator;
        this.client = client;
        this.webhook = webhook;
        this.timeout = timeout;
    }

    @KafkaListener(topics = KafkaConfig.REQUESTS)
    public void process(String rawEvent) {
        if (rawEvent == null || rawEvent.length() > MAX_EVENT_LENGTH) {
            throw new InvalidEventException("Event must be non-null and at most 16384 characters");
        }
        NotificationEvent event;
        try {
            event = mapper.readValue(rawEvent, NotificationEvent.class);
        } catch (RuntimeException malformed) {
            throw new InvalidEventException("Event is not valid notification JSON", malformed);
        }
        if (event == null || !validator.validate(event).isEmpty()) {
            throw new InvalidEventException("Event fails notification validation");
        }

        receipts.record(event);
        if (deliveries.hasDelivered(event.eventId())) {
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(webhook)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", event.eventId().toString())
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(event)))
                .build();
        long attempt = deliveries.begin(event.eventId());
        HttpResponse<String> response;
        CompletableFuture<HttpResponse<String>> exchange = client.sendAsync(request,
                ignored -> new BoundedBodySubscriber(MAX_RESPONSE_BYTES));
        try {
            // Bound the entire exchange, including a response body that stalls after headers.
            response = exchange.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            exchange.cancel(true);
            Thread.currentThread().interrupt();
            deliveries.finish(attempt, null, "NETWORK_ERROR", "HTTP delivery interrupted; receiver outcome is unknown");
            throw new DeliveryException(event.eventId(), "Webhook delivery interrupted", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            exchange.cancel(true);
            Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
            deliveries.finish(attempt, null, "NETWORK_ERROR",
                    cause.getClass().getSimpleName() + ": receiver outcome may be unknown");
            throw new DeliveryException(event.eventId(), "Webhook network failure", cause);
        }

        boolean succeeded = response.statusCode() >= 200 && response.statusCode() < 300;
        deliveries.finish(attempt, response.statusCode(), succeeded ? "SUCCEEDED" : "HTTP_ERROR", response.body());
        if (!succeeded) {
            throw new DeliveryException(event.eventId(), "Webhook returned HTTP " + response.statusCode());
        }
    }

    /** Cancels excess response data instead of buffering an unbounded receiver body. */
    private static final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<String> {
        private final ByteBuffer bytes;
        private final CompletableFuture<String> body = new CompletableFuture<>();
        private Flow.Subscription subscription;

        private BoundedBodySubscriber(int limit) {
            bytes = ByteBuffer.allocate(limit);
        }

        @Override
        public CompletionStage<String> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                int amount = Math.min(bytes.remaining(), buffer.remaining());
                ByteBuffer portion = buffer.duplicate();
                portion.limit(portion.position() + amount);
                bytes.put(portion);
                if (buffer.remaining() > amount) {
                    subscription.cancel();
                    complete(true);
                    return;
                }
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable failure) {
            body.completeExceptionally(failure);
        }

        @Override
        public void onComplete() {
            complete(false);
        }

        private void complete(boolean truncated) {
            body.complete(new String(bytes.array(), 0, bytes.position(), StandardCharsets.UTF_8)
                    + (truncated ? " [truncated]" : ""));
        }
    }
}
