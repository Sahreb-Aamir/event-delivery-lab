package dev.sahreb.delivery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real launcher, Kafka, file database, HTTP side effect, forced stop, and explicit recovery. */
class FullAppCrashRecoveryTest {
    private static final ObjectMapper JSON = JsonMapper.builder().findAndAddModules().build();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private Path directory;
    private String jdbcUrl;
    private int port;
    private Process child;

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void fullApplicationRetainsUnknownAttemptAndRecoversAfterForcedStop() throws Exception {
        directory = Path.of("target", "crash-probe", "run-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(directory);
        jdbcUrl = "jdbc:h2:file:" + portable(directory.resolve("receipts"))
                + ";WRITE_DELAY=0;DB_CLOSE_ON_EXIT=FALSE";
        port = availableLoopbackPort();
        UUID target = UUID.randomUUID();
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("startedAt", Instant.now());
        manifest.put("javaVersion", System.getProperty("java.version"));
        manifest.put("javaVendor", System.getProperty("java.vendor"));
        manifest.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        manifest.put("eventId", target);
        manifest.put("httpAddress", "http://127.0.0.1:" + port);
        manifest.put("database", jdbcUrl);
        manifest.put("webhookTimeoutMs", 30_000);
        manifest.put("acknowledgmentDelayMs", 5_000);
        manifest.put("scope", "Owned application process termination; not a power-loss or exactly-once guarantee");
        save("manifest.json", manifest);

        try {
            start("first");
            List<JsonNode> seeded = new ArrayList<>();
            for (int index = 0; index < 6; index++) {
                UUID id = UUID.randomUUID();
                post("/api/events", event(id, "Prior synthetic delivery " + index), 202);
                JsonNode delivered = waitForStatus(id, "DELIVERED");
                assertThat(delivered.path("attempts").size()).isEqualTo(1);
                assertThat(delivered.at("/attempts/0/outcome").asText()).isEqualTo("SUCCEEDED");
                seeded.add(delivered);
                save("seeded.json", seeded);
            }

            post("/api/demo/ack-delay", Map.of("count", 1, "delayMs", 5_000), 200);
            NotificationEvent event = event(target, "Synthetic full-application crash recovery probe");
            save("submitted.json", event);
            post("/api/events", event, 202);
            JsonNode receiver = waitForReceiver(target);
            JsonNode before = get("/api/events/" + target);
            save("receiver-before.json", receiver);
            save("before.json", before);
            assertThat(receiver.path("sideEffectCount").asInt()).isEqualTo(7);
            assertThat(before.path("status").asText()).as("Must stop inside the delayed acknowledgment window")
                    .isEqualTo("DELIVERING");
            assertThat(before.path("attempts").size()).isEqualTo(1);
            assertThat(before.at("/attempts/0/outcome").asText()).isEqualTo("PENDING");
            assertThat(before.at("/attempts/0/finishedAt").isNull()).isTrue();

            assertThat(child.isAlive()).as("The owned application must still be running at the crash boundary").isTrue();
            stopOwnedChild();
            // Preserve one untouched snapshot. Opening H2 can perform recovery, so inspect a second copy.
            Path snapshot = directory.resolve("postkill-untouched.mv.db");
            copyAfterProcessExit(directory.resolve("receipts.mv.db"), snapshot);
            Files.copy(snapshot, directory.resolve("postkill-inspection.mv.db"));
            RawDatabase raw = inspectCopy();
            save("raw-database.json", raw);
            assertThat(raw.receipts()).hasSize(7);
            assertThat(raw.states()).hasSize(7);
            assertThat(raw.attempts()).hasSize(7);
            assertThat(raw.receipts().stream().filter(receipt -> receipt.eventId().equals(target)).toList())
                    .containsExactly(JSON.treeToValue(before.path("receipt"), Receipt.class));
            assertThat(raw.states()).containsEntry(target.toString(), "DELIVERING");
            assertThat(raw.attempts().stream().filter(attempt -> attempt.eventId().equals(target)).toList())
                    .containsExactly(JSON.treeToValue(before.at("/attempts/0"), DeliveryAttempt.class));
            for (JsonNode seed : seeded) {
                UUID seedId = UUID.fromString(seed.at("/receipt/eventId").asText());
                assertThat(raw.states()).containsEntry(seedId.toString(), "DELIVERED");
                assertThat(raw.attempts().stream().filter(attempt -> attempt.eventId().equals(seedId)).toList())
                        .containsExactly(JSON.treeToValue(seed.at("/attempts/0"), DeliveryAttempt.class));
            }

            start("restart");
            JsonNode interrupted = waitForStatus(target, "INTERRUPTED");
            save("after-restart.json", interrupted);
            assertThat(interrupted.path("receipt")).isEqualTo(before.path("receipt"));
            assertThat(interrupted.path("attempts")).isEqualTo(before.path("attempts"));
            JsonNode resetReceiver = get("/api/demo");
            save("receiver-restarted.json", resetReceiver);
            assertThat(resetReceiver.path("receivedRequests").asInt()).isZero();
            assertThat(resetReceiver.path("deliveries").size()).isZero();
            for (JsonNode seed : seeded) {
                assertThat(get("/api/events/" + seed.at("/receipt/eventId").asText())).isEqualTo(seed);
            }

            post("/api/events/" + target + "/retry", null, 202);
            JsonNode recovered = waitForStatus(target, "DELIVERED");
            save("after-retry.json", recovered);
            assertThat(recovered.path("receipt")).isEqualTo(before.path("receipt"));
            assertThat(recovered.path("attempts").size()).isEqualTo(2);
            assertThat(recovered.at("/attempts/0")).isEqualTo(before.at("/attempts/0"));
            assertThat(recovered.at("/attempts/1/id").asLong()).isGreaterThan(before.at("/attempts/0/id").asLong());
            assertThat(recovered.at("/attempts/1/outcome").asText()).isEqualTo("SUCCEEDED");
            assertThat(recovered.at("/attempts/1/httpStatus").asInt()).isEqualTo(200);
            JsonNode retriedReceiver = get("/api/demo");
            save("receiver-after-retry.json", retriedReceiver);
            assertThat(retriedReceiver.path("receivedRequests").asInt()).isEqualTo(1);
            assertThat(retriedReceiver.path("deliveries").size()).isEqualTo(1);
            assertThat(retriedReceiver.at("/deliveries/0/eventId").asText()).isEqualTo(target.toString());
            // This is a second effect across process lifetimes: receiver deduplication is deliberately volatile.
            save("result.json", Map.of("status", "PASSED", "finishedAt", Instant.now(),
                    "effectsAcrossReceiverLifetimes", 2, "oldAttemptOutcome", "PENDING"));
        } catch (Exception | AssertionError failure) {
            save("result.json", Map.of("status", "FAILED", "finishedAt", Instant.now(),
                    "failure", failure.toString()));
            failure.addSuppressed(new IllegalStateException("Crash probe artifacts: " + directory));
            throw failure;
        } finally {
            stopOwnedChild();
        }
    }

    private void start(String phase) throws Exception {
        Path temporary = Files.createDirectories(directory.resolve(phase + "-tmp"));
        String executable = System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java";
        String java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        // Surefire can place only its boot jar on java.class.path; prefer its full test classpath when present.
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> arguments = List.of("-Xmx384m", "-XX:TieredStopAtLevel=1",
                "-Djava.io.tmpdir=" + temporary, "-cp", classpath, LocalLab.class.getName(),
                "--server.address=127.0.0.1", "--server.port=" + port,
                "--spring.datasource.url=" + jdbcUrl, "--spring.datasource.username=sa", "--spring.datasource.password=",
                "--lab.webhook-url=http://127.0.0.1:" + port + "/demo/webhook", "--lab.webhook-timeout-ms=30000",
                "--logging.level.root=WARN");
        // An argument file also avoids Windows command-line limits for the Kafka test dependency classpath.
        Path argumentFile = directory.resolve(phase + "-java-arguments.txt");
        Files.writeString(argumentFile, String.join(System.lineSeparator(), arguments.stream()
                .map(value -> "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"").toList()));
        child = new ProcessBuilder(java, "@" + argumentFile).redirectErrorStream(true)
                .redirectOutput(directory.resolve(phase + ".log").toFile()).start();
        save(phase + "-process.json", Map.of("pid", child.pid(), "startedAt", Instant.now(),
                "java", java, "temporaryDirectory", temporary.toString()));
        await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            if (!child.isAlive()) {
                throw new IllegalStateException("Child exited; see " + directory.resolve(phase + ".log"));
            }
            try {
                assertThat(get("/api/demo").path("deliveries").isArray()).isTrue();
            } catch (IOException notReady) {
                throw new AssertionError("Waiting for the child HTTP listener", notReady);
            }
        });
    }

    private JsonNode waitForStatus(UUID eventId, String status) {
        JsonNode[] observed = {null};
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            observed[0] = get("/api/events/" + eventId);
            assertThat(observed[0].path("status").asText()).isEqualTo(status);
        });
        return observed[0];
    }

    private JsonNode waitForReceiver(UUID eventId) {
        JsonNode[] observed = {null};
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            observed[0] = get("/api/demo");
            boolean found = false;
            for (JsonNode received : observed[0].path("deliveries")) {
                found |= received.path("eventId").asText().equals(eventId.toString());
            }
            assertThat(found).as("Receiver must observe this exact event before the forced stop").isTrue();
        });
        return observed[0];
    }

    private JsonNode get(String path) throws Exception { return request(path, null, 200, false); }

    private JsonNode post(String path, Object body, int expectedStatus) throws Exception {
        return request(path, body, expectedStatus, true);
    }

    private JsonNode request(String path, Object body, int expectedStatus, boolean post) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(6));
        if (post) {
            request.header("Content-Type", "application/json").POST(body == null
                    ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        }
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s response: %s", path, response.body()).isEqualTo(expectedStatus);
        return JSON.readTree(response.body());
    }

    private RawDatabase inspectCopy() throws Exception {
        List<Receipt> receipts = new ArrayList<>();
        Map<String, String> states = new LinkedHashMap<>();
        List<DeliveryAttempt> attempts = new ArrayList<>();
        String inspectionUrl = "jdbc:h2:file:" + portable(directory.resolve("postkill-inspection")) + ";IFEXISTS=TRUE";
        try (Connection connection = DriverManager.getConnection(inspectionUrl, "sa", "");
                Statement statement = connection.createStatement()) {
            try (ResultSet rows = statement.executeQuery("SELECT * FROM receipts ORDER BY recorded_at, event_id")) {
                while (rows.next()) receipts.add(new Receipt(rows.getObject("event_id", UUID.class),
                        rows.getString("seller_id"), rows.getString("message"), instant(rows, "occurred_at"),
                        instant(rows, "recorded_at")));
            }
            try (ResultSet rows = statement.executeQuery("SELECT * FROM delivery_state ORDER BY event_id")) {
                while (rows.next()) states.put(rows.getObject("event_id", UUID.class).toString(), rows.getString("status"));
            }
            try (ResultSet rows = statement.executeQuery("SELECT * FROM delivery_attempts ORDER BY id")) {
                while (rows.next()) attempts.add(new DeliveryAttempt(rows.getLong("id"), rows.getObject("event_id", UUID.class),
                        instant(rows, "started_at"), instant(rows, "finished_at"), rows.getObject("http_status", Integer.class),
                        rows.getString("outcome"), rows.getString("detail")));
            }
        }
        return new RawDatabase(receipts, states, attempts);
    }

    private static Instant instant(ResultSet rows, String column) throws Exception {
        OffsetDateTime value = rows.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private void stopOwnedChild() throws Exception {
        if (child != null && child.isAlive()) {
            long pid = child.pid();
            child.destroyForcibly();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).as("Owned child %s must exit", pid).isTrue();
            save("process-" + pid + "-stopped.json", Map.of("pid", pid, "stoppedAt", Instant.now(),
                    "exitCode", child.exitValue(), "method", "Process.destroyForcibly"));
        }
    }

    private void save(String name, Object value) throws IOException {
        Files.writeString(directory.resolve(name), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value));
    }

    private static void copyAfterProcessExit(Path source, Path snapshot) throws Exception {
        // On Windows the OS file lock can briefly outlive Process.waitFor(). Never open the source in H2.
        for (int attempt = 0; ; attempt++) {
            try {
                Files.copy(source, snapshot, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (FileSystemException locked) {
                if (attempt >= 20) throw locked;
                Thread.sleep(50);
            }
        }
    }

    private static NotificationEvent event(UUID id, String message) {
        return new NotificationEvent(id, "demo-crash-probe", message, Instant.now());
    }

    private static int availableLoopbackPort() throws IOException {
        try (ServerSocket reservation = new ServerSocket()) {
            reservation.bind(new InetSocketAddress("127.0.0.1", 0));
            return reservation.getLocalPort();
        }
    }

    private static String portable(Path path) { return path.toString().replace('\\', '/'); }

    private record RawDatabase(List<Receipt> receipts, Map<String, String> states, List<DeliveryAttempt> attempts) { }
}
