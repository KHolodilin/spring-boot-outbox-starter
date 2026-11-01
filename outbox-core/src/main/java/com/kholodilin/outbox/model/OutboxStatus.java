package com.kholodilin.outbox.model;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    /**
     * Partition boundary: statuses below this value live in the ACTIVE partition.
     */
    public static final int ARCHIVE_THRESHOLD = 100;

    private final int code;

    private static final Map<Integer, OutboxStatus> BY_CODE =
            Stream.of(values())
                    .collect(
                            Collectors.toMap(
                                    status -> status.code,
                                    Function.identity()
                            )
                    );

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
        OutboxStatus status = BY_CODE.get(code);

        if (status == null) {
            throw new IllegalArgumentException("Unknown outbox status code: " + code);
        }
        return status;
    }
}
