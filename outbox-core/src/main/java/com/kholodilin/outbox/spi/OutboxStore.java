package com.kholodilin.outbox.spi;

import java.time.Instant;
import java.util.List;

import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;

/**
 * Persistence port for a single outbox channel table.
 */
public interface OutboxStore {

    /** Inserts a NEW row and returns the generated event id. */
    long insert(OutboxInsert insert);

    /**
     * Claims rows by id for publishing (sets PROCESSING + lease).
     *
     * @return claimed records (may be a subset of {@code eventIds})
     */
    List<OutboxRecord> claimByIds(List<Long> eventIds, String lockedBy, Instant lockedUntil);

    /** Marks claimed rows as SENT and clears the lease. */
    void markSent(List<Long> eventIds, Instant sentAt);

    /** Marks a single row FAILED or DEAD and clears the lease. */
    void markFailed(long eventId, int retryCount, OutboxStatus status);

    /**
     * Claims recoverable ACTIVE rows with a short lease for multi-instance coordination.
     *
     * @return claimed event ids
     */
    List<Long> claimRecoverableIds(int batchSize, String lockedBy, Instant lockedUntil);

    /** Clears lease so the publisher can claim immediately after recovery enqueue. */
    void clearLease(List<Long> eventIds);

    /**
     * Returns ids from {@code eventIds} that are still ACTIVE and not leased (safe to re-enqueue).
     */
    List<Long> findReenqueueableIds(List<Long> eventIds);

    /** Loads full records by id (used when claim already returned them; optional helper). */
    List<OutboxRecord> findByIds(List<Long> eventIds);
}
