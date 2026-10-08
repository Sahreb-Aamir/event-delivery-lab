package dev.sahreb.delivery;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Exercises an actual process crash, rather than a graceful connection close. */
public class H2CrashDurabilityTest {
    private static final UUID TARGET_ID = UUID.fromString("efdd4279-ce50-4b5f-b4c0-8529b467e32f");

    @ParameterizedTest(name = "pending intent survives process kill with WRITE_DELAY={0}")
    @ValueSource(ints = {0, 60_000})
    void synchronizedPendingIntentSurvivesForcedProcessKill(int writeDelay, @TempDir Path directory) throws Exception {
        String url = "jdbc:h2:file:" + directory.resolve("receipts").toAbsolutePath().toString().replace('\\', '/')
                + ";WRITE_DELAY=" + writeDelay + ";DB_CLOSE_ON_EXIT=FALSE";
        Path ready = directory.resolve("ready.txt");
        Path output = directory.resolve("writer.log");
        String executable = System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java";
        String java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        Process writer = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                H2CrashDurabilityTest.class.getName(), url, ready.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            await().atMost(Duration.ofSeconds(25)).until(() -> Files.exists(ready) || !writer.isAlive());
            assertThat(Files.exists(ready)).withFailMessage("Writer did not reach committed boundary: %s",
                    Files.readString(output)).isTrue();
            assertThat(Files.readString(ready)).isEqualTo("DELIVERING:1:PENDING");
            assertThat(writer.isAlive()).as("Writer must still own the open database when forcibly stopped").isTrue();
            writer.destroyForcibly();
            assertThat(writer.waitFor(10, TimeUnit.SECONDS)).isTrue();

            // Windows may briefly retain the OS file lock after the process signals exit.
            try (Connection anchor = reopenAfterProcessExit(url)) {
                JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(url, "sa", ""));
                ReceiptStore receipts = new ReceiptStore(jdbc);
                DeliveryStore deliveries = new DeliveryStore(jdbc);
                assertThat(receipts.count()).isEqualTo(7);
                assertThat(receipts.find(TARGET_ID)).isPresent();
                DeliveryStatus pending = deliveries.status(TARGET_ID);
                assertThat(pending.status()).isEqualTo("DELIVERING");
                assertThat(pending.attempts()).singleElement().satisfies(attempt -> {
                    assertThat(attempt.id()).isEqualTo(7);
                    assertThat(attempt.outcome()).isEqualTo("PENDING");
                    assertThat(attempt.finishedAt()).isNull();
                });
                assertThat(deliveries.interruptUnfinished()).isEqualTo(1);
                assertThat(deliveries.status(TARGET_ID))
                        .isEqualTo(new DeliveryStatus("INTERRUPTED", pending.attempts()));
            }
        } finally {
            if (writer.isAlive()) {
                writer.destroyForcibly();
                writer.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    /** Child entry point intentionally keeps its connection pool open until killed. */
    public static void main(String[] args) throws Exception {
        try (HikariDataSource source = new HikariDataSource()) {
            source.setJdbcUrl(args[0]);
            source.setUsername("sa");
            source.setPassword("");
            new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(source);
            JdbcTemplate jdbc = new JdbcTemplate(source);
            ReceiptStore receipts = new ReceiptStore(jdbc);
            DeliveryStore deliveries = new DeliveryStore(jdbc);
            for (int prior = 0; prior < 6; prior++) {
                UUID id = UUID.randomUUID();
                receipts.record(new NotificationEvent(id, "demo-crash-history", "Prior synthetic effect " + prior,
                        Instant.parse("2026-10-07T12:00:00Z")));
                deliveries.finish(deliveries.begin(id), 200, "SUCCEEDED", "Prior accepted effect");
            }
            receipts.record(new NotificationEvent(TARGET_ID, "demo-crash-probe", "Synthetic pending effect",
                    Instant.parse("2026-10-07T12:00:01Z")));
            deliveries.begin(TARGET_ID);
            DeliveryStatus status = deliveries.status(TARGET_ID);
            Path ready = Path.of(args[1]);
            Path marker = ready.resolveSibling("ready.tmp");
            Files.writeString(marker, status.status() + ":" + status.attempts().size()
                    + ":" + status.attempts().get(0).outcome());
            Files.move(marker, ready, StandardCopyOption.ATOMIC_MOVE);
            Thread.sleep(60_000);
        }
    }

    private static Connection reopenAfterProcessExit(String url) throws Exception {
        for (int attempt = 0; ; attempt++) {
            try {
                return DriverManager.getConnection(url, "sa", "");
            } catch (SQLException failure) {
                if (failure.getErrorCode() != 90020 || attempt >= 20) {
                    throw failure;
                }
                Thread.sleep(50);
            }
        }
    }
}
