package com.kholodilin.outbox.model;

/**
 * Outbox row lifecycle statuses.
 *
 * <p>ACTIVE statuses have codes {@code < 100}; ARCHIVE statuses have codes {@code >= 100}.
 */
public enum OutboxStatus {
    NEW(0),
    PROCESSING(1),
    FAILED(2),
    DEAD(101),
    SENT(110);

    /** Partition boundary: statuses below this value live in the ACTIVE partition. */
    public static final int ARCHIVE_THRESHOLD = 100;

    private final int code;

    OutboxStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public boolean isActive() {
        return code < ARCHIVE_THRESHOLD;
    }

    public static OutboxStatus fromCode(int code) {
        for (OutboxStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown outbox status code: " + code);
    }
}
