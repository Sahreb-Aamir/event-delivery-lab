package dev.sahreb.delivery;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

class DeadLetterNetworkFailureTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void stalledKafkaConnectionFailsClosesAndAllowsTheSameInspectorToRecover() throws Exception {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("startedAt", Instant.now().toString());
        evidence.put("javaVersion", System.getProperty("java.version"));
        evidence.put("os", System.getProperty("os.name"));
        evidence.put("scope", "Real Kafka client against a TCP peer that accepts a Kafka request but never replies; "
                + "MockMvc exercises the controller and error mapping, not an HTTP server deadline.");
        evidence.put("outcome", "FAILED");
        ExecutorService requests = Executors.newFixedThreadPool(2);
        List<Future<?>> pending = new ArrayList<>();
        try (SilentKafkaPeer peer = new SilentKafkaPeer()) {
            var factory = new DefaultKafkaConsumerFactory<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, peer.address(),
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    // The public constructor must replace inherited long operation timeouts.
                    ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000,
                    ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000));
            DeadLetterInspector inspector = new DeadLetterInspector(factory);
            MockMvc mvc = MockMvcBuilders.standaloneSetup(new DeadLetterController(inspector))
                    .setControllerAdvice(new ApiExceptionHandler()).build();

            Future<Response> stalled = requests.submit(() -> request(mvc));
            pending.add(stalled);
            // Synchronize on a complete Kafka protocol frame, not a sleep or TCP accept alone.
            evidence.put("observedKafkaFrameBytes", peer.frameBytes.get(5, TimeUnit.SECONDS));
            Future<Response> overlapping = requests.submit(() -> request(mvc));
            pending.add(overlapping);
            Response rejected = overlapping.get(2, TimeUnit.SECONDS);
            evidence.put("overlappingStatus", rejected.status());
            assertThat(rejected.status()).isEqualTo(429);

            Response unavailable = stalled.get(6, TimeUnit.SECONDS);
            evidence.put("stalledStatus", unavailable.status());
            evidence.put("stalledElapsedMillis", unavailable.elapsedMillis());
            assertThat(unavailable.status()).isEqualTo(503);
            assertThat(unavailable.elapsedMillis()).isLessThan(6_000);
            assertThat(JSON.readTree(unavailable.body())).isEqualTo(JSON.readTree("""
                    {"message":"Dead-letter inspection is temporarily unavailable","errors":[]}
                    """));
            // The peer is still open: EOF must come from the client, before fixture cleanup.
            evidence.put("connectionsClosedBeforePeerCleanup", peer.awaitClientsClosed());

            // Broker startup and fixture production are deliberately outside query timing.
            try (LocalKafkaBroker broker = LocalKafkaBroker.start()) {
                String key = UUID.randomUUID().toString();
                String payload = "Synthetic record after a stalled inspection";
                seed(broker.bootstrapServers(), key, payload);
                factory.updateConfigs(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers()));
                Future<Response> recovered = requests.submit(() -> request(mvc));
                pending.add(recovered);
                Response available = recovered.get(6, TimeUnit.SECONDS);
                evidence.put("recoveredStatus", available.status());
                evidence.put("recoveredElapsedMillis", available.elapsedMillis());
                assertThat(available.status()).isEqualTo(200);
                var snapshot = JSON.readTree(available.body());
                assertThat(snapshot.path("scannedRecords").asInt()).isEqualTo(1);
                assertThat(snapshot.path("items").size()).isEqualTo(1);
                assertThat(snapshot.path("items").get(0).path("key").asText()).isEqualTo(key);
                assertThat(snapshot.path("items").get(0).path("payloadPreview").asText()).isEqualTo(payload);
            }
            evidence.put("outcome", "PASSED");
        } catch (Exception | AssertionError failure) {
            evidence.put("outcome", "FAILED");
            evidence.put("failure", failure.getClass().getSimpleName() + ": " + failure.getMessage());
            throw failure;
        } finally {
            try {
                pending.forEach(future -> future.cancel(true));
                requests.shutdownNow();
                assertThat(requests.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            } catch (Exception | AssertionError cleanupFailure) {
                evidence.put("outcome", "FAILED");
                evidence.put("cleanupFailure", cleanupFailure.getClass().getSimpleName() + ": "
                        + cleanupFailure.getMessage());
                throw cleanupFailure;
            } finally {
                Path report = Path.of("target", "surefire-reports", "dead-letter-network-stall.json");
                Files.createDirectories(report.getParent());
                Files.writeString(report, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
            }
        }
    }

    private static Response request(MockMvc mvc) throws Exception {
        long started = System.nanoTime();
        var response = mvc.perform(get("/api/dead-letters")).andReturn().getResponse();
        return new Response(response.getStatus(), response.getContentAsString(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }

    private static void seed(String bootstrap, String key, String payload) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap,
                "request.timeout.ms", 5_000, "default.api.timeout.ms", 10_000))) {
            admin.createTopics(List.of(new NewTopic(KafkaConfig.DEAD_LETTERS, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
        try (var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", bootstrap,
                "acks", "all", "max.block.ms", 5_000, "request.timeout.ms", 5_000,
                "delivery.timeout.ms", 10_000), new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(KafkaConfig.DEAD_LETTERS, key, payload))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private record Response(int status, String body, long elapsedMillis) { }

    /** Consumes the client's frame without sending any Kafka response or closing early. */
    private static final class SilentKafkaPeer implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket();
        private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
        private final ExecutorService worker = Executors.newCachedThreadPool();
        private final CompletableFuture<Integer> frameBytes = new CompletableFuture<>();
        private final CompletableFuture<Integer> clientsClosed = new CompletableFuture<>();
        private final AtomicInteger accepted = new AtomicInteger();
        private final AtomicBoolean observeClose = new AtomicBoolean();
        private final AtomicBoolean stopping = new AtomicBoolean();

        private SilentKafkaPeer() throws IOException {
            try {
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                listener.setSoTimeout(100);
                worker.submit(this::acceptConnections);
            } catch (IOException | RuntimeException failure) {
                listener.close();
                worker.shutdownNow();
                throw failure;
            }
        }

        private String address() {
            return "127.0.0.1:" + listener.getLocalPort();
        }

        private int awaitClientsClosed() throws Exception {
            observeClose.set(true);
            return clientsClosed.get(2, TimeUnit.SECONDS);
        }

        private void acceptConnections() {
            try {
                while (!stopping.get()) {
                    try {
                        Socket socket = listener.accept();
                        synchronized (connections) {
                            if (stopping.get()) {
                                socket.close();
                                return;
                            }
                            connections.add(socket);
                            accepted.incrementAndGet();
                            worker.submit(() -> readWithoutReplying(socket));
                        }
                    } catch (SocketTimeoutException emptyBacklog) {
                        // A timed-out accept after the request ended also drains queued connections.
                        if (observeClose.get() && connections.isEmpty() && accepted.get() > 0) {
                            clientsClosed.complete(accepted.get());
                        }
                    }
                }
            } catch (Exception failure) {
                if (!stopping.get()) fail(failure);
            }
        }

        private void readWithoutReplying(Socket socket) {
            try (socket) {
                socket.setSoTimeout(10_000);
                DataInputStream input = new DataInputStream(socket.getInputStream());
                byte[] prefix = input.readNBytes(Integer.BYTES);
                if (prefix.length == 0) return;
                assertThat(prefix).hasSize(Integer.BYTES);
                int length = ByteBuffer.wrap(prefix).getInt();
                assertThat(length).isBetween(8, 65_536);
                assertThat(input.readNBytes(length)).hasSize(length);
                frameBytes.complete(length + Integer.BYTES);
                byte[] buffer = new byte[1_024];
                while (input.read(buffer) != -1) {
                    // Drain any later request, still without responding.
                }
            } catch (Exception | AssertionError failure) {
                if (!stopping.get()) fail(failure);
            } finally {
                connections.remove(socket);
            }
        }

        private void fail(Throwable failure) {
            frameBytes.completeExceptionally(failure);
            clientsClosed.completeExceptionally(failure);
        }

        @Override
        public void close() throws Exception {
            try {
                stopping.set(true);
                listener.close();
                synchronized (connections) {
                    for (Socket socket : connections) socket.close();
                }
            } finally {
                worker.shutdownNow();
                assertThat(worker.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
