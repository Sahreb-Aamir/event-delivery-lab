package dev.sahreb.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.datasource.url=jdbc:h2:mem:integration;DB_CLOSE_DELAY=-1",
        "logging.level.root=WARN"})
@EmbeddedKafka(partitions = 1, topics = {KafkaConfig.REQUESTS, KafkaConfig.DEAD_LETTERS})
@DirtiesContext
class DeliveryIntegrationTest {
    private static final AtomicInteger FAILURES = new AtomicInteger();
    private static final Map<String, AtomicInteger> EFFECTS = new ConcurrentHashMap<>();
    private static final HttpServer RECEIVER = receiver();

    @Value("${local.server.port}") int port;
    @Autowired ObjectMapper json;
    @MockitoSpyBean ReceiptStore receipts;
    @Autowired DeliveryStore deliveries;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired KafkaTemplate<String, String> template;
    private final HttpClient http = HttpClient.newHttpClient();

    @DynamicPropertySource
    static void receiverProperties(DynamicPropertyRegistry registry) {
        registry.add("lab.webhook-url", () -> "http://127.0.0.1:" + RECEIVER.getAddress().getPort() + "/hook");
    }

    private static HttpServer receiver() {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/hook", exchange -> {
                exchange.getRequestBody().readAllBytes();
                int status = FAILURES.getAndUpdate(count -> Math.max(0, count - 1)) > 0 ? 503 : 200;
                if (status == 200) {
                    EFFECTS.computeIfAbsent(exchange.getRequestHeaders().getFirst("Idempotency-Key"),
                            ignored -> new AtomicInteger()).incrementAndGet();
                }
                byte[] body = (status == 200 ? "accepted" : "temporary failure").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (Exception failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    @AfterAll
    static void stopReceiver() { RECEIVER.stop(0); }

    @BeforeEach
    void recoverReceiver() { FAILURES.set(0); }

    @Test
    void httpToKafkaToWebhookAndIdenticalReplayProducesOneReceiptAndEffect() throws Exception {
        var event = event("Order shipped");
        assertThat(post("/api/events", event).statusCode()).isEqualTo(202);
        delivered(event.eventId());
        var original = receipts.find(event.eventId()).orElseThrow();
        assertThat(post("/api/events", event).statusCode()).isEqualTo(202);
        // A marker behind the replay in the same partition proves it has been consumed.
        var marker = event("Replay barrier");
        post("/api/events", marker);
        delivered(marker.eventId());
        assertThat(receipts.find(event.eventId()).orElseThrow()).isEqualTo(original);
        assertThat(deliveries.status(event.eventId()).attempts()).hasSize(1);
        assertThat(EFFECTS.get(event.eventId().toString()).get()).isEqualTo(1);
        var response = http.send(HttpRequest.newBuilder(uri("/api/events/" + event.eventId())).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(json.readTree(response.body()).get("status").asText()).isEqualTo("DELIVERED");
    }

    @Test
    void twoTemporaryFailuresRecoverOnThirdAttempt() throws Exception {
        FAILURES.set(2);
        var event = event("Retry drill");
        post("/api/events", event);
        delivered(event.eventId());
        assertThat(deliveries.status(event.eventId()).attempts())
                .extracting(DeliveryAttempt::outcome).containsExactly("HTTP_ERROR", "HTTP_ERROR", "SUCCEEDED");
    }

    @Test
    void exhaustedRetriesReachDeadLetterAndExplicitReplayRecovers() throws Exception {
        FAILURES.set(3);
        var event = event("Recovery drill");
        try (var dlt = deadLetterConsumer()) {
            post("/api/events", event);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(deliveries.status(event.eventId()).status()).isEqualTo("FAILED"));
            assertDeadLetter(dlt, event.eventId().toString());
            assertThat(deliveries.status(event.eventId()).attempts()).hasSize(3);
            assertThat(post("/api/events/" + event.eventId() + "/retry", Map.of()).statusCode()).isEqualTo(202);
            delivered(event.eventId());
            assertThat(deliveries.status(event.eventId()).attempts()).hasSize(4);
            assertThat(EFFECTS.get(event.eventId().toString()).get()).isEqualTo(1);
            assertThat(post("/api/events/" + event.eventId() + "/retry", Map.of()).statusCode()).isEqualTo(409);
        }
    }

    @Test
    void conflictingEventIdGoesToDeadLetterWithoutChangingOriginal() throws Exception {
        var event = event("Original payload");
        post("/api/events", event);
        delivered(event.eventId());
        var conflicting = new NotificationEvent(event.eventId(), event.sellerId(), "Changed payload", event.occurredAt());
        try (var dlt = deadLetterConsumer()) {
            assertThat(post("/api/events", conflicting).statusCode()).isEqualTo(202);
            assertDeadLetter(dlt, event.eventId().toString());
        }
        assertThat(receipts.find(event.eventId()).orElseThrow().message()).isEqualTo("Original payload");
        assertThat(deliveries.status(event.eventId()).status()).isEqualTo("DELIVERED");
        assertThat(deliveries.status(event.eventId()).attempts()).hasSize(1);
    }

    @Test
    void invalidHttpAndUnknownEventAreRejected() throws Exception {
        var event = new NotificationEvent(UUID.randomUUID(), "real-shop", "", Instant.now());
        assertThat(post("/api/events", event).statusCode()).isEqualTo(400);
        assertThat(receipts.find(event.eventId())).isEmpty();
        assertThat(http.send(HttpRequest.newBuilder(uri("/api/events/" + UUID.randomUUID())).GET().build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
    }

    @Test
    void transientStorageFailureRetriesBeforeRecordingDelivery() throws Exception {
        var event = event("Storage recovery drill");
        doThrow(new DataAccessResourceFailureException("Synthetic database outage"))
                .doCallRealMethod().when(receipts).record(event);
        post("/api/events", event);
        delivered(event.eventId());
        assertThat(receipts.find(event.eventId())).isPresent();
        assertThat(deliveries.status(event.eventId()).attempts()).hasSize(1);
        assertThat(EFFECTS.get(event.eventId().toString()).get()).isEqualTo(1);
    }

    @Test
    void malformedBrokerMessageIsRetainedAndDoesNotBlockFollowingEvents() throws Exception {
        var id = UUID.randomUUID().toString();
        try (var dlt = deadLetterConsumer()) {
            template.send(KafkaConfig.REQUESTS, id, "{bad-json").get();
            assertDeadLetter(dlt, id);
        }
        var event = event("After malformed message");
        post("/api/events", event);
        delivered(event.eventId());
    }

    private NotificationEvent event(String message) {
        return new NotificationEvent(UUID.randomUUID(), "demo-shop", message, Instant.now());
    }

    private URI uri(String path) { return URI.create("http://127.0.0.1:" + port + path); }

    private HttpResponse<String> post(String path, Object body) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(12))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body))).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private void delivered(UUID eventId) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(deliveries.status(eventId).status()).isEqualTo("DELIVERED"));
    }

    private Consumer<String, String> deadLetterConsumer() {
        var properties = KafkaTestUtils.consumerProps("dlt-check-" + UUID.randomUUID(), "false", broker);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = new DefaultKafkaConsumerFactory<>(properties,
                new StringDeserializer(), new StringDeserializer()).createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, KafkaConfig.DEAD_LETTERS);
        return consumer;
    }

    private void assertDeadLetter(Consumer<String, String> consumer, String key) {
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                if (key.equals(record.key())) return true;
            }
            return false;
        });
    }
}
