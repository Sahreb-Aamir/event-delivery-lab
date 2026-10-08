package dev.sahreb.delivery;

import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Keeps local delivery state and attempt history. These transactions protect the
 * database only: a committed PENDING attempt cannot prove whether an HTTP side
 * effect happened before a crash. This store is not a distributed delivery claim.
 */
@Repository
public class DeliveryStore {

    private static final Set<String> FINISHED_OUTCOMES = Set.of("SUCCEEDED", "HTTP_ERROR", "NETWORK_ERROR");

    private static final RowMapper<DeliveryAttempt> ATTEMPT_MAPPER = (row, rowNumber) -> {
        OffsetDateTime finishedAt = row.getObject("finished_at", OffsetDateTime.class);
        return new DeliveryAttempt(
                row.getLong("id"), row.getObject("event_id", UUID.class),
                row.getObject("started_at", OffsetDateTime.class).toInstant(),
                finishedAt == null ? null : finishedAt.toInstant(),
                row.getObject("http_status", Integer.class), row.getString("outcome"), row.getString("detail"));
    };

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public DeliveryStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.transactions = new TransactionTemplate(
                new DataSourceTransactionManager(Objects.requireNonNull(jdbc.getDataSource())));
    }

    public boolean hasDelivered(UUID eventId) {
        boolean delivered = jdbc.queryForList("SELECT status FROM delivery_state WHERE event_id = ?", String.class, eventId)
                .contains("DELIVERED");
        if (delivered) {
            // A prior finish may have committed but failed to synchronize; replay must retry that boundary.
            H2Durability.sync(jdbc);
        }
        return delivered;
    }

    /** Commits the pending attempt and DELIVERING status before HTTP work starts. */
    public long begin(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId");
        long attemptId = Objects.requireNonNull(transactions.execute(transaction -> {
            List<String> states = lockedState(eventId);
            if (states.contains("DELIVERED")) {
                throw new IllegalStateException("Event " + eventId + " has already been delivered");
            }
            GeneratedKeyHolder key = new GeneratedKeyHolder();
            jdbc.update(connection -> {
                PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO delivery_attempts (event_id, started_at, outcome)
                        VALUES (?, ?, 'PENDING')
                        """, new String[] { "id" });
                statement.setObject(1, eventId);
                statement.setObject(2, Instant.now().atOffset(ZoneOffset.UTC));
                return statement;
            }, key);
            setState(eventId, states, "DELIVERING");
            return Objects.requireNonNull(key.getKey()).longValue();
        }));
        H2Durability.sync(jdbc);
        return attemptId;
    }

    public void finish(long attemptId, Integer httpStatus, String outcome, String detail) {
        if (outcome == null || !FINISHED_OUTCOMES.contains(outcome)) {
            throw new IllegalArgumentException("Unsupported finished attempt outcome: " + outcome);
        }
        transactions.executeWithoutResult(transaction -> {
            List<DeliveryAttempt> found = jdbc.query("""
                    SELECT id, event_id, started_at, finished_at, http_status, outcome, detail
                    FROM delivery_attempts WHERE id = ? FOR UPDATE
                    """, ATTEMPT_MAPPER, attemptId);
            if (found.isEmpty()) {
                throw new IllegalArgumentException("Unknown delivery attempt: " + attemptId);
            }
            DeliveryAttempt attempt = found.get(0);
            if (!attempt.outcome().equals("PENDING")) {
                if (Objects.equals(attempt.httpStatus(), httpStatus) && attempt.outcome().equals(outcome)
                        && Objects.equals(attempt.detail(), detail)) {
                    return;
                }
                throw new IllegalStateException("Delivery attempt " + attemptId + " is already finished");
            }
            jdbc.update("""
                    UPDATE delivery_attempts SET finished_at = ?, http_status = ?, outcome = ?, detail = ?
                    WHERE id = ?
                    """, Instant.now().atOffset(ZoneOffset.UTC), httpStatus, outcome, detail, attemptId);
            jdbc.update("""
                    UPDATE delivery_state SET status = ? WHERE event_id = ? AND status <> 'DELIVERED'
                    """, outcome.equals("SUCCEEDED") ? "DELIVERED" : "RETRYING", attempt.eventId());
        });
        H2Durability.sync(jdbc);
    }

    public void markFailed(UUID eventId) {
        transactions.executeWithoutResult(transaction -> {
            List<String> states = lockedState(eventId);
            if (!states.contains("DELIVERED")) {
                setState(eventId, states, "FAILED");
            }
        });
        H2Durability.sync(jdbc);
    }

    /**
     * Called only by the fresh embedded-broker launcher before its consumers start.
     * Its previous queue is gone, so unfinished durable intents need explicit
     * recovery. Attempt history remains untouched: PENDING still means unknown.
     */
    public int interruptUnfinished() {
        int interrupted = Objects.requireNonNull(transactions.execute(transaction -> {
            int changed = jdbc.update("""
                    UPDATE delivery_state SET status = 'INTERRUPTED'
                    WHERE status IN ('DELIVERING', 'RETRYING')
                    """);
            changed += jdbc.update("""
                    INSERT INTO delivery_state (event_id, status)
                    SELECT receipt.event_id, 'INTERRUPTED' FROM receipts receipt
                    WHERE NOT EXISTS (
                        SELECT 1 FROM delivery_state state WHERE state.event_id = receipt.event_id
                    )
                    """);
            return changed;
        }));
        H2Durability.sync(jdbc);
        return interrupted;
    }

    public DeliveryStatus status(UUID eventId) {
        List<String> states = jdbc.queryForList(
                "SELECT status FROM delivery_state WHERE event_id = ?", String.class, eventId);
        List<DeliveryAttempt> attempts = jdbc.query("""
                SELECT id, event_id, started_at, finished_at, http_status, outcome, detail
                FROM delivery_attempts WHERE event_id = ? ORDER BY id
                """, ATTEMPT_MAPPER, eventId);
        return new DeliveryStatus(states.isEmpty() ? "QUEUED" : states.get(0), attempts);
    }

    private List<String> lockedState(UUID eventId) {
        return jdbc.queryForList(
                "SELECT status FROM delivery_state WHERE event_id = ? FOR UPDATE", String.class, eventId);
    }

    private void setState(UUID eventId, List<String> existing, String status) {
        if (existing.isEmpty()) {
            jdbc.update("INSERT INTO delivery_state (event_id, status) VALUES (?, ?)", eventId, status);
        } else {
            jdbc.update("UPDATE delivery_state SET status = ? WHERE event_id = ?", status, eventId);
        }
    }
}
