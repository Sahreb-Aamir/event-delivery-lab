package dev.sahreb.delivery;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryStoreTest {

    private static final NotificationEvent EVENT = new NotificationEvent(UUID.randomUUID(),
            "demo-delivery-store", "Synthetic delivery test", Instant.parse("2026-10-07T12:34:56.123456789Z"));

    private DriverManagerDataSource dataSource;
    private JdbcTemplate jdbc;
    private DeliveryStore store;

    @BeforeEach
    void createStore() {
        dataSource = dataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        initializeSchema(dataSource);
        jdbc = new JdbcTemplate(dataSource);
        new ReceiptStore(jdbc).record(EVENT);
        store = new DeliveryStore(jdbc);
    }

    @AfterEach
    void closeStore() {
        jdbc.execute("SHUTDOWN");
    }

    @Test
    void pendingAttemptIsVisibleBeforeItFinishes() {
        assertThat(store.status(EVENT.eventId())).isEqualTo(new DeliveryStatus("QUEUED", java.util.List.of()));
        assertThat(store.hasDelivered(EVENT.eventId())).isFalse();

        long id = store.begin(EVENT.eventId());

        DeliveryStatus status = store.status(EVENT.eventId());
        assertThat(status.status()).isEqualTo("DELIVERING");
        assertThat(status.attempts()).singleElement().satisfies(attempt -> {
            assertThat(attempt.id()).isEqualTo(id);
            assertThat(attempt.eventId()).isEqualTo(EVENT.eventId());
            assertThat(attempt.startedAt()).isNotNull();
            assertThat(attempt.finishedAt()).isNull();
            assertThat(attempt.httpStatus()).isNull();
            assertThat(attempt.outcome()).isEqualTo("PENDING");
        });
    }

    @Test
    void failedEventCanRetryAndSucceedWithoutLosingHistory() {
        long failedAttempt = store.begin(EVENT.eventId());
        store.finish(failedAttempt, 503, "HTTP_ERROR", "Local fixture returned 503");
        assertThat(store.status(EVENT.eventId()).status()).isEqualTo("RETRYING");
        store.markFailed(EVENT.eventId());
        assertThat(store.status(EVENT.eventId()).status()).isEqualTo("FAILED");

        long successfulAttempt = store.begin(EVENT.eventId());
        assertThat(store.status(EVENT.eventId()).status()).isEqualTo("DELIVERING");
        store.finish(successfulAttempt, 204, "SUCCEEDED", "Local fixture accepted event");

        DeliveryStatus status = store.status(EVENT.eventId());
        assertThat(status.status()).isEqualTo("DELIVERED");
        assertThat(store.hasDelivered(EVENT.eventId())).isTrue();
        assertThat(status.attempts()).extracting(DeliveryAttempt::id).containsExactly(failedAttempt, successfulAttempt);
        assertThat(status.attempts()).extracting(DeliveryAttempt::outcome).containsExactly("HTTP_ERROR", "SUCCEEDED");
        assertThat(status.attempts()).extracting(DeliveryAttempt::httpStatus).containsExactly(503, 204);
        assertThat(status.attempts()).allSatisfy(attempt -> assertThat(attempt.finishedAt()).isNotNull());
    }

    @Test
    void networkErrorHasNoHttpStatus() {
        long id = store.begin(EVENT.eventId());
        store.finish(id, null, "NETWORK_ERROR", "Connection timed out");

        assertThat(store.status(EVENT.eventId()).status()).isEqualTo("RETRYING");
        assertThat(store.status(EVENT.eventId()).attempts()).singleElement().satisfies(attempt -> {
            assertThat(attempt.httpStatus()).isNull();
            assertThat(attempt.outcome()).isEqualTo("NETWORK_ERROR");
            assertThat(attempt.detail()).isEqualTo("Connection timed out");
        });
    }

    @Test
    void deliveredStateCannotBeOverwrittenByFailureOrAnotherBegin() {
        long id = store.begin(EVENT.eventId());
        store.finish(id, 200, "SUCCEEDED", "Accepted");
        DeliveryStatus delivered = store.status(EVENT.eventId());

        store.markFailed(EVENT.eventId());
        store.finish(id, 200, "SUCCEEDED", "Accepted");
        assertThatThrownBy(() -> store.begin(EVENT.eventId())).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> store.finish(id, 503, "HTTP_ERROR", "Cannot rewrite history"))
                .isInstanceOf(IllegalStateException.class);

        assertThat(store.status(EVENT.eventId())).isEqualTo(delivered);
    }

    @Test
    void deliveredReplayCannotBypassAnUnsuccessfulSynchronization() {
        long attemptId = store.begin(EVENT.eventId());
        AtomicBoolean blockSync = new AtomicBoolean(true);
        JdbcTemplate failingJdbc = new JdbcTemplate(dataSource) {
            @Override
            public void execute(String sql) {
                if (sql.equals("CHECKPOINT SYNC") && blockSync.get()) {
                    throw new DataAccessResourceFailureException("Simulated synchronization failure");
                }
                super.execute(sql);
            }
        };
        DeliveryStore retryingStore = new DeliveryStore(failingJdbc);
        assertThatThrownBy(() -> retryingStore.finish(attemptId, 200, "SUCCEEDED", "Accepted"))
                .isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(store.status(EVENT.eventId()).status()).isEqualTo("DELIVERED");
        assertThatThrownBy(() -> retryingStore.hasDelivered(EVENT.eventId()))
                .isInstanceOf(DataAccessResourceFailureException.class);

        blockSync.set(false);
        assertThat(retryingStore.hasDelivered(EVENT.eventId())).isTrue();
        assertThat(store.status(EVENT.eventId()).attempts()).hasSize(1);
    }

    @Test
    void failureBeforeFirstAttemptCanBeRecorded() {
        store.markFailed(EVENT.eventId());
        assertThat(store.status(EVENT.eventId())).isEqualTo(new DeliveryStatus("FAILED", java.util.List.of()));
    }

    @Test
    void freshDemoBrokerInterruptsOnlyUnfinishedEventsWithoutRewritingHistory() {
        UUID delivering = recordEvent("Delivering at shutdown");
        store.begin(delivering);
        DeliveryStatus pending = store.status(delivering);
        UUID retrying = recordEvent("Retrying at shutdown");
        store.finish(store.begin(retrying), null, "NETWORK_ERROR", "Receiver outcome unknown");
        DeliveryStatus retryHistory = store.status(retrying);
        UUID failed = recordEvent("Previously failed");
        store.finish(store.begin(failed), 503, "HTTP_ERROR", "Receiver unavailable");
        store.markFailed(failed);
        DeliveryStatus failedHistory = store.status(failed);
        UUID delivered = recordEvent("Previously delivered");
        store.finish(store.begin(delivered), 200, "SUCCEEDED", "Accepted");
        DeliveryStatus deliveredHistory = store.status(delivered);

        assertThat(store.interruptUnfinished()).isEqualTo(3);

        assertThat(store.status(EVENT.eventId())).isEqualTo(new DeliveryStatus("INTERRUPTED", java.util.List.of()));
        assertThat(store.status(delivering)).isEqualTo(new DeliveryStatus("INTERRUPTED", pending.attempts()));
        assertThat(store.status(delivering).attempts()).singleElement().satisfies(attempt -> {
            assertThat(attempt.outcome()).isEqualTo("PENDING");
            assertThat(attempt.finishedAt()).isNull();
        });
        assertThat(store.status(retrying)).isEqualTo(new DeliveryStatus("INTERRUPTED", retryHistory.attempts()));
        assertThat(store.status(failed)).isEqualTo(failedHistory);
        assertThat(store.status(delivered)).isEqualTo(deliveredHistory);
        assertThat(store.interruptUnfinished()).isZero();
    }

    @Test
    void interruptionRecoveryRollsBackIfQueuedStateInsertFails() {
        UUID delivering = recordEvent("Delivering at shutdown");
        store.begin(delivering);
        DeliveryStatus original = store.status(delivering);
        JdbcTemplate failingJdbc = new JdbcTemplate(dataSource) {
            @Override
            public int update(String sql) {
                if (sql.startsWith("INSERT INTO delivery_state")) {
                    throw new DataAccessResourceFailureException("Simulated queued state storage failure");
                }
                return super.update(sql);
            }
        };

        assertThatThrownBy(() -> new DeliveryStore(failingJdbc).interruptUnfinished())
                .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(store.status(delivering)).isEqualTo(original);
        assertThat(store.status(EVENT.eventId())).isEqualTo(new DeliveryStatus("QUEUED", java.util.List.of()));
    }

    @Test
    void beginRollsBackAttemptIfStateWriteFails() {
        JdbcTemplate failingJdbc = new JdbcTemplate(dataSource) {
            @Override
            public int update(String sql, Object... arguments) {
                if (sql.startsWith("INSERT INTO delivery_state")) {
                    throw new DataAccessResourceFailureException("Simulated state storage failure");
                }
                return super.update(sql, arguments);
            }
        };

        assertThatThrownBy(() -> new DeliveryStore(failingJdbc).begin(EVENT.eventId()))
                .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(store.status(EVENT.eventId())).isEqualTo(new DeliveryStatus("QUEUED", java.util.List.of()));
        assertThat(new ReceiptStore(jdbc).find(EVENT.eventId())).isPresent();
    }

    @Test
    void finishRollsBackAttemptIfStateWriteFails() {
        long attemptId = store.begin(EVENT.eventId());
        DeliveryStatus pending = store.status(EVENT.eventId());
        JdbcTemplate failingJdbc = new JdbcTemplate(dataSource) {
            @Override
            public int update(String sql, Object... arguments) {
                if (sql.startsWith("UPDATE delivery_state")) {
                    throw new DataAccessResourceFailureException("Simulated state storage failure");
                }
                return super.update(sql, arguments);
            }
        };

        assertThatThrownBy(() -> new DeliveryStore(failingJdbc).finish(attemptId, 200, "SUCCEEDED", "Accepted"))
                .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(store.status(EVENT.eventId())).isEqualTo(pending);
        assertThat(store.hasDelivered(EVENT.eventId())).isFalse();
    }

    @Test
    void pendingAttemptSurvivesReopenAndRemainsUnknownWhenRetried(@TempDir Path directory) throws Exception {
        String url = "jdbc:h2:file:" + directory.resolve("pending").toAbsolutePath().toString().replace('\\', '/')
                + ";WRITE_DELAY=0";
        DriverManagerDataSource firstDataSource = dataSource(url);
        DeliveryStatus pending;
        try (Connection connection = firstDataSource.getConnection()) {
            initializeSchema(firstDataSource);
            JdbcTemplate firstJdbc = new JdbcTemplate(firstDataSource);
            new ReceiptStore(firstJdbc).record(EVENT);
            DeliveryStore firstStore = new DeliveryStore(firstJdbc);
            firstStore.begin(EVENT.eventId());
            pending = firstStore.status(EVENT.eventId());
        }

        DriverManagerDataSource reopenedDataSource = dataSource(url);
        try (Connection connection = reopenedDataSource.getConnection()) {
            DeliveryStore reopenedStore = new DeliveryStore(new JdbcTemplate(reopenedDataSource));
            assertThat(reopenedStore.status(EVENT.eventId())).isEqualTo(pending);
            assertThat(reopenedStore.hasDelivered(EVENT.eventId())).isFalse();
            assertThat(reopenedStore.interruptUnfinished()).isEqualTo(1);
            assertThat(reopenedStore.status(EVENT.eventId()))
                    .isEqualTo(new DeliveryStatus("INTERRUPTED", pending.attempts()));
            assertThat(reopenedStore.interruptUnfinished()).isZero();

            long retriedAttempt = reopenedStore.begin(EVENT.eventId());
            reopenedStore.finish(retriedAttempt, 200, "SUCCEEDED", "Retry accepted");
            assertThat(reopenedStore.status(EVENT.eventId()).status()).isEqualTo("DELIVERED");
            assertThat(reopenedStore.status(EVENT.eventId()).attempts())
                    .extracting(DeliveryAttempt::outcome).containsExactly("PENDING", "SUCCEEDED");
            assertThat(reopenedStore.status(EVENT.eventId()).attempts().get(0)).isEqualTo(pending.attempts().get(0));
        }
    }

    @Test
    void completedDeliverySurvivesDatabaseReopen(@TempDir Path directory) throws Exception {
        String url = "jdbc:h2:file:" + directory.resolve("delivery").toAbsolutePath().toString().replace('\\', '/')
                + ";WRITE_DELAY=0";
        DriverManagerDataSource firstDataSource = dataSource(url);
        DeliveryStatus delivered;
        try (Connection connection = firstDataSource.getConnection()) {
            initializeSchema(firstDataSource);
            JdbcTemplate firstJdbc = new JdbcTemplate(firstDataSource);
            new ReceiptStore(firstJdbc).record(EVENT);
            DeliveryStore firstStore = new DeliveryStore(firstJdbc);
            long id = firstStore.begin(EVENT.eventId());
            firstStore.finish(id, 202, "SUCCEEDED", "Accepted by local fixture");
            delivered = firstStore.status(EVENT.eventId());
        }

        DriverManagerDataSource reopenedDataSource = dataSource(url);
        try (Connection connection = reopenedDataSource.getConnection()) {
            DeliveryStore reopenedStore = new DeliveryStore(new JdbcTemplate(reopenedDataSource));
            assertThat(reopenedStore.hasDelivered(EVENT.eventId())).isTrue();
            assertThat(reopenedStore.status(EVENT.eventId())).isEqualTo(delivered);
            reopenedStore.markFailed(EVENT.eventId());
            assertThat(reopenedStore.status(EVENT.eventId())).isEqualTo(delivered);
        }
    }

    private static DriverManagerDataSource dataSource(String url) {
        return new DriverManagerDataSource(url, "sa", "");
    }

    private UUID recordEvent(String message) {
        NotificationEvent event = new NotificationEvent(UUID.randomUUID(), EVENT.sellerId(), message, EVENT.occurredAt());
        new ReceiptStore(jdbc).record(event);
        return event.eventId();
    }

    private static void initializeSchema(DriverManagerDataSource dataSource) {
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(dataSource);
    }
}
