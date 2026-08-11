package com.kholodilin.outbox.queue.memory;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryOutboxDispatchQueueTest {

    @Test
    void dedupInFlightAndBackpressure() throws Exception {
        InMemoryOutboxDispatchQueue queue = new InMemoryOutboxDispatchQueue(2);

        assertThat(queue.offer(1L)).isTrue();
        assertThat(queue.offer(1L)).isFalse();
        assertThat(queue.offer(2L)).isTrue();
        assertThat(queue.offer(3L)).isFalse();
        assertThat(queue.pressure()).isEqualTo(1.0);

        Long first = queue.poll(Duration.ofMillis(50));
        assertThat(first).isEqualTo(1L);
        assertThat(queue.offer(1L)).isFalse();

        queue.acknowledge(List.of(1L));
        assertThat(queue.offer(1L)).isTrue();

        List<Long> drained = queue.drain(10);
        assertThat(drained).containsExactly(2L, 1L);
        queue.acknowledge(drained);
        assertThat(queue.size()).isZero();
    }
}
