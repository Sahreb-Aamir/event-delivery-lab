package dev.sahreb.delivery;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Records one local receipt per event ID. The database primary key resolves
 * concurrent inserts; an in-memory check is never used as the authority.
 */
@Repository
public class ReceiptStore {

    private static final RowMapper<Receipt> RECEIPT_MAPPER = (row, rowNumber) -> new Receipt(
            row.getObject("event_id", UUID.class),
            row.getString("seller_id"),
            row.getString("message"),
            row.getObject("occurred_at", OffsetDateTime.class).toInstant(),
            row.getObject("recorded_at", OffsetDateTime.class).toInstant());

    private final JdbcTemplate jdbc;

    public ReceiptStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts with JDBC autocommit before returning. An identical replay returns
     * false, while a changed payload throws without modifying the first receipt.
     */
    public boolean record(NotificationEvent event) {
        Objects.requireNonNull(event, "event");
        try {
            jdbc.update("""
                    INSERT INTO receipts (event_id, seller_id, message, occurred_at, recorded_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    event.eventId(), event.sellerId(), event.message(),
                    event.occurredAt().atOffset(ZoneOffset.UTC), Instant.now().atOffset(ZoneOffset.UTC));
        } catch (DuplicateKeyException duplicate) {
            Receipt existing = find(event.eventId()).orElseThrow(
                    () -> new IllegalStateException("Receipt missing after duplicate event ID", duplicate));
            if (!Objects.equals(existing.sellerId(), event.sellerId())
                    || !Objects.equals(existing.message(), event.message())
                    || !Objects.equals(existing.occurredAt(), event.occurredAt())) {
                throw new EventIdentityConflictException(event.eventId());
            }
            H2Durability.sync(jdbc);
            return false;
        }
        H2Durability.sync(jdbc);
        return true;
    }

    public Optional<Receipt> find(UUID eventId) {
        return jdbc.query("""
                SELECT event_id, seller_id, message, occurred_at, recorded_at
                FROM receipts WHERE event_id = ?
                """, RECEIPT_MAPPER, eventId).stream().findFirst();
    }

    public long count() {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT COUNT(*) FROM receipts", Long.class));
    }

    public List<Receipt> recent(int limit) {
        if (limit < 1 || limit > 100) {
            throw new IllegalArgumentException("Receipt limit must be between 1 and 100");
        }
        return jdbc.query("""
                SELECT event_id, seller_id, message, occurred_at, recorded_at
                FROM receipts ORDER BY recorded_at DESC, event_id LIMIT ?
                """, RECEIPT_MAPPER, limit);
    }
}
