package dev.sahreb.delivery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NotificationProcessorTest {
    private final ObjectMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private final NotificationEvent event = new NotificationEvent(UUID.randomUUID(), "demo-lost-ack",
            "Synthetic notification for a reproducible failure drill", Instant.parse("2026-10-07T12:00:00Z"));
    private JdbcTemplate jdbc;
    private ReceiptStore receipts;
    private DeliveryStore deliveries;
    private ValidatorFactory validation;
    private HttpServer server;
    private ExecutorService receiverThreads;

    @BeforeEach
    void setup() throws IOException {
        DriverManagerDataSource source = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(source);
        jdbc = new JdbcTemplate(source);
        receipts = new ReceiptStore(jdbc);
        deliveries = new DeliveryStore(jdbc);
        validation = Validation.buildDefaultValidatorFactory();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiverThreads = Executors.newCachedThreadPool();
        server.setExecutor(receiverThreads);
    }

    @AfterEach
    void cleanup() throws InterruptedException {
        server.stop(0);
        receiverThreads.shutdownNow();
        assertThat(receiverThreads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        validation.close();
        jdbc.execute("SHUTDOWN");
    }

    @ParameterizedTest(name = "lost acknowledgment with receiver idempotency = {0}")
    @ValueSource(booleans = { false, true })
    void lostAcknowledgmentRepeatsSideEffectUnlessReceiverDeduplicates(boolean idempotent) {
        AtomicInteger requests = new AtomicInteger();
        AtomicInteger sideEffects = new AtomicInteger();
        Set<String> accepted = ConcurrentHashMap.newKeySet();
        CountDownLatch releaseFirstAcknowledgment = new CountDownLatch(1);
        NotificationProcessor processor = start(exchange -> {
            int requestNumber = requests.incrementAndGet();
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            exchange.getRequestBody().readAllBytes();
            if (!idempotent || accepted.add(key)) {
                sideEffects.incrementAndGet();
            }
            if (requestNumber == 1) {
                waitFor(releaseFirstAcknowledgment);
            }
            respond(exchange, 200, "accepted");
        }, Duration.ofSeconds(1));

        try {
            assertThatThrownBy(() -> processor.process(json())).isInstanceOf(DeliveryException.class);
            assertThat(requests).hasValue(1);
            assertThat(sideEffects).hasValue(1);
            assertThat(deliveries.status(event.eventId()).status()).isEqualTo("RETRYING");
            assertThat(deliveries.status(event.eventId()).attempts()).singleElement()
                    .satisfies(attempt -> {
                        assertThat(attempt.outcome()).isEqualTo("NETWORK_ERROR");
                        assertThat(attempt.httpStatus()).isNull();
                    });

            processor.process(json());

            assertThat(requests).hasValue(2);
            assertThat(sideEffects).hasValue(idempotent ? 1 : 2);
            assertThat(receipts.count()).isEqualTo(1);
            assertThat(deliveries.status(event.eventId()).status()).isEqualTo("DELIVERED");
            assertThat(deliveries.status(event.eventId()).attempts())
                    .extracting(DeliveryAttempt::outcome).containsExactly("NETWORK_ERROR", "SUCCEEDED");

            // Once success is recorded locally, Kafka replay adds no HTTP attempt.
            processor.process(json());
            assertThat(requests).hasValue(2);
        } finally {
            releaseFirstAcknowledgment.countDown();
        }
    }

    @Test
    void temporaryHttpFailurePreservesReceiptAndRecordsSuccessfulRetry() {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<String> key = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        NotificationProcessor processor = start(exchange -> {
            key.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, requests.incrementAndGet() == 1 ? 503 : 200, "synthetic response");
        }, Duration.ofSeconds(2));

        assertThatThrownBy(() -> processor.process(json())).isInstanceOf(DeliveryException.class)
                .hasMessageContaining("503");
        Receipt first = receipts.find(event.eventId()).orElseThrow();
        processor.process(json());

        assertThat(key).hasValue(event.eventId().toString());
        assertThat(mapper.readValue(body.get(), NotificationEvent.class)).isEqualTo(event);
        assertThat(receipts.find(event.eventId())).contains(first);
        assertThat(deliveries.status(event.eventId()).attempts()).extracting(DeliveryAttempt::httpStatus)
                .containsExactly(503, 200);
    }

    @Test
    void delayedResponseBodyIsBoundedByTotalExchangeDeadline() {
        CountDownLatch releaseBody = new CountDownLatch(1);
        AtomicInteger headersSent = new AtomicInteger();
        NotificationProcessor processor = start(exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, 2);
            exchange.getResponseBody().write('o');
            exchange.getResponseBody().flush();
            headersSent.incrementAndGet();
            waitFor(releaseBody);
            exchange.getResponseBody().write('k');
            exchange.close();
        }, Duration.ofSeconds(1));
        long started = System.nanoTime();
        try {
            assertThatThrownBy(() -> processor.process(json())).isInstanceOf(DeliveryException.class);
            assertThat(headersSent).hasValue(1);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
            assertThat(deliveries.status(event.eventId()).attempts()).singleElement()
                    .satisfies(attempt -> assertThat(attempt.outcome()).isEqualTo("NETWORK_ERROR"));
        } finally {
            releaseBody.countDown();
        }
    }

    @Test
    void excessiveReceiverBodyIsTruncatedDuringDownload() {
        NotificationProcessor processor = start(exchange -> respond(exchange, 200, "x".repeat(65_536)),
                Duration.ofSeconds(2));
        processor.process(json());
        assertThat(deliveries.status(event.eventId()).attempts()).singleElement().satisfies(attempt -> {
            assertThat(attempt.outcome()).isEqualTo("SUCCEEDED");
            assertThat(attempt.detail()).isEqualTo("x".repeat(2_048) + " [truncated]");
        });
    }

    @Test
    void redirectsAreRecordedAsFailuresAndNeverFollowed() {
        AtomicInteger redirectedRequests = new AtomicInteger();
        server.createContext("/unexpected", exchange -> {
            redirectedRequests.incrementAndGet();
            respond(exchange, 200, "should not be reached");
        });
        NotificationProcessor processor = start(exchange -> {
            exchange.getResponseHeaders().set("Location", "/unexpected");
            respond(exchange, 302, "redirect");
        }, Duration.ofSeconds(2));
        assertThatThrownBy(() -> processor.process(json())).isInstanceOf(DeliveryException.class)
                .hasMessageContaining("302");
        assertThat(redirectedRequests).hasValue(0);
    }

    @Test
    void invalidAndConflictingEventsNeverReachReceiver() {
        AtomicInteger requests = new AtomicInteger();
        NotificationProcessor processor = start(exchange -> {
            requests.incrementAndGet();
            respond(exchange, 200, "accepted");
        }, Duration.ofSeconds(2));
        assertThatThrownBy(() -> processor.process("{" )).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> processor.process("null")).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> processor.process("x".repeat(16_385))).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> processor.process(mapper.writeValueAsString(
                new NotificationEvent(event.eventId(), "real-customer", "message", event.occurredAt()))))
                .isInstanceOf(InvalidEventException.class);
        assertThat(receipts.count()).isZero();
        assertThat(requests).hasValue(0);

        processor.process(json());
        assertThatThrownBy(() -> processor.process(mapper.writeValueAsString(
                new NotificationEvent(event.eventId(), event.sellerId(), "changed", event.occurredAt()))))
                .isInstanceOf(EventIdentityConflictException.class);
        assertThat(requests).hasValue(1);
        assertThat(deliveries.status(event.eventId()).status()).isEqualTo("DELIVERED");
    }

    private NotificationProcessor start(HttpHandler handler, Duration timeout) {
        server.createContext("/webhook", exchange -> {
            try {
                handler.handle(exchange);
            } catch (IOException disconnectedAfterTimeout) {
                exchange.close();
            }
        });
        server.start();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        return new NotificationProcessor(receipts, deliveries, mapper, validation.getValidator(), client,
                URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/webhook"), timeout);
    }

    private String json() {
        return mapper.writeValueAsString(event);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test acknowledgment was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
