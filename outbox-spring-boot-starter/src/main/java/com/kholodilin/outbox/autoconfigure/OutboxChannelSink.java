package com.kholodilin.outbox.autoconfigure;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.stereotype.Component;

/**
 * Binds an {@link com.kholodilin.outbox.spi.OutboxSink} bean to a named outbox channel.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface OutboxChannelSink {

    /** Channel name, e.g. {@code orders} or {@code default}. */
    String value();
}
