package dev.sahreb.delivery;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Explicitly asks H2 to flush and synchronize its file after a local commit. */
final class H2Durability {
    private H2Durability() { }

    static void sync(JdbcTemplate jdbc) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("H2 synchronization must follow the database transaction commit");
        }
        // This is an H2-specific persistence boundary, not a power-loss or remote-effect guarantee.
        jdbc.execute("CHECKPOINT SYNC");
    }
}
