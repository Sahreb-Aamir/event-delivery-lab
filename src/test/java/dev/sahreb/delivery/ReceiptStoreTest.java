package dev.sahreb.delivery;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReceiptStoreTest {

    private static final NotificationEvent EVENT = new NotificationEvent(
            UUID.fromString("6791b4d1-6bd2-4459-a27f-a0cd446d0614"),
            "demo-seller-1", "Synthetic order notification", Instant.parse("2026-10-07T12:34:56.123456789Z"));

    private JdbcTemplate jdbc;
    private ReceiptStore store;

    @BeforeEach
    void createStore() {
        DriverManagerDataSource dataSource = dataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        initializeSchema(dataSource);
        jdbc = new JdbcTemplate(dataSource);
        store = new ReceiptStore(jdbc);
    }

    @AfterEach
    void closeStore() {
        jdbc.execute("SHUTDOWN");
    }

    @Test
    void identicalReplayKeepsOneUnchangedReceipt() {
        assertThat(store.record(EVENT)).isTrue();
        Receipt firstReceipt = store.find(EVENT.eventId()).orElseThrow();

        assertThat(firstReceipt.eventId()).isEqualTo(EVENT.eventId());
        assertThat(firstReceipt.sellerId()).isEqualTo(EVENT.sellerId());
        assertThat(firstReceipt.message()).isEqualTo(EVENT.message());
        assertThat(firstReceipt.occurredAt()).isEqualTo(EVENT.occurredAt());
        assertThat(firstReceipt.recordedAt()).isNotNull();
        assertThat(store.record(EVENT)).isFalse();
        assertThat(store.count()).isEqualTo(1);
        assertThat(store.find(EVENT.eventId())).contains(firstReceipt);
    }

    @Test
    void identicalReplayRetriesAnUnsuccessfulSynchronization() {
        AtomicBoolean blockSync = new AtomicBoolean(true);
        JdbcTemplate failingJdbc = new JdbcTemplate(jdbc.getDataSource()) {
            @Override
            public void execute(String sql) {
                if (sql.equals("CHECKPOINT SYNC") && blockSync.get()) {
                    throw new DataAccessResourceFailureException("Simulated synchronization failure");
                }
                super.execute(sql);
            }
        };
        ReceiptStore retryingStore = new ReceiptStore(failingJdbc);
        assertThatThrownBy(() -> retryingStore.record(EVENT)).isInstanceOf(DataAccessResourceFailureException.class);
        Receipt committed = store.find(EVENT.eventId()).orElseThrow();
        assertThatThrownBy(() -> retryingStore.record(EVENT)).isInstanceOf(DataAccessResourceFailureException.class);

        blockSync.set(false);
        assertThat(retryingStore.record(EVENT)).isFalse();
        assertThat(store.count()).isEqualTo(1);
        assertThat(store.find(EVENT.eventId())).contains(committed);
    }

    @Test
    void recentReceiptsAreNewestFirstAndLimited() {
        NotificationEvent older = new NotificationEvent(UUID.randomUUID(), EVENT.sellerId(), "Older", EVENT.occurredAt());
        NotificationEvent newer = new NotificationEvent(UUID.randomUUID(), EVENT.sellerId(), "Newer", EVENT.occurredAt());
        store.record(older);
        store.record(newer);
        jdbc.update("UPDATE receipts SET recorded_at = ? WHERE event_id = ?",
                EVENT.occurredAt().atOffset(java.time.ZoneOffset.UTC), older.eventId());
        jdbc.update("UPDATE receipts SET recorded_at = ? WHERE event_id = ?",
                EVENT.occurredAt().plusSeconds(1).atOffset(java.time.ZoneOffset.UTC), newer.eventId());

        assertThat(store.recent(1)).extracting(Receipt::eventId).containsExactly(newer.eventId());
        assertThat(store.recent(20)).extracting(Receipt::eventId).containsExactly(newer.eventId(), older.eventId());
        assertThatThrownBy(() -> store.recent(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.recent(101)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("changedPayloads")
    void conflictingReplayPreservesOriginalReceipt(NotificationEvent changed) {
        store.record(EVENT);
        Receipt firstReceipt = store.find(EVENT.eventId()).orElseThrow();

        assertThatThrownBy(() -> store.record(changed))
                .isInstanceOf(EventIdentityConflictException.class)
                .satisfies(exception -> assertThat(((EventIdentityConflictException) exception).eventId())
                        .isEqualTo(EVENT.eventId()));

        assertThat(store.count()).isEqualTo(1);
        assertThat(store.find(EVENT.eventId())).contains(firstReceipt);
    }

    private static Stream<NotificationEvent> changedPayloads() {
        return Stream.of(
                new NotificationEvent(EVENT.eventId(), "demo-other-seller", EVENT.message(), EVENT.occurredAt()),
                new NotificationEvent(EVENT.eventId(), EVENT.sellerId(), "Different synthetic message", EVENT.occurredAt()),
                new NotificationEvent(EVENT.eventId(), EVENT.sellerId(), EVENT.message(), EVENT.occurredAt().plusNanos(1)));
    }

    @Test
    void concurrentIdenticalInsertsCreateExactlyOneReceipt() throws Exception {
        int attempts = 8;
        ExecutorService executor = Executors.newFixedThreadPool(attempts);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        try {
            for (int i = 0; i < attempts; i++) {
                results.add(executor.submit(() -> {
                    if (!start.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Concurrent insert start timed out");
                    }
                    return store.record(EVENT);
                }));
            }
            start.countDown();
            int created = 0;
            for (Future<Boolean> result : results) {
                if (result.get(10, TimeUnit.SECONDS)) {
                    created++;
                }
            }
            assertThat(created).isEqualTo(1);
            assertThat(store.count()).isEqualTo(1);
            assertThat(store.find(EVENT.eventId())).isPresent();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void receiptAndDeduplicationSurviveClosingAndReopeningFileDatabase(@TempDir Path directory) throws Exception {
        String url = "jdbc:h2:file:" + directory.resolve("receipts").toAbsolutePath().toString().replace('\\', '/')
                + ";WRITE_DELAY=0";
        DriverManagerDataSource firstDataSource = dataSource(url);
        Receipt original;
        try (Connection connection = firstDataSource.getConnection()) {
            initializeSchema(firstDataSource);
            ReceiptStore firstStore = new ReceiptStore(new JdbcTemplate(firstDataSource));
            assertThat(firstStore.record(EVENT)).isTrue();
            original = firstStore.find(EVENT.eventId()).orElseThrow();
        }

        DriverManagerDataSource reopenedDataSource = dataSource(url);
        try (Connection connection = reopenedDataSource.getConnection()) {
            ReceiptStore reopenedStore = new ReceiptStore(new JdbcTemplate(reopenedDataSource));
            assertThat(reopenedStore.find(EVENT.eventId())).contains(original);
            assertThat(reopenedStore.record(EVENT)).isFalse();
            assertThat(reopenedStore.count()).isEqualTo(1);
            assertThat(reopenedStore.find(EVENT.eventId())).contains(original);
        }
    }

    private static DriverManagerDataSource dataSource(String url) {
        return new DriverManagerDataSource(url, "sa", "");
    }

    private static void initializeSchema(DriverManagerDataSource dataSource) {
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(dataSource);
    }
}
