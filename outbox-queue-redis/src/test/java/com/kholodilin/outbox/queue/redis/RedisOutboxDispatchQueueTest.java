package com.kholodilin.outbox.queue.redis;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisOutboxDispatchQueueTest {

    @Test
    void offerFailsOpenWhenRedisThrows() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForSet()).thenThrow(new RuntimeException("down"));

        RedisOutboxDispatchQueue queue = new RedisOutboxDispatchQueue(redis, "outbox:test:", 10);
        assertThat(queue.offer(1L)).isFalse();
        assertThat(queue.size()).isZero();
        assertThat(queue.capacity()).isEqualTo(10);
        assertThat(queue.pressure()).isZero();
    }

    @Test
    void offerAndPollHappyPath() throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        SetOperations<String, String> setOps = mock(SetOperations.class);
        @SuppressWarnings("unchecked")
        ListOperations<String, String> listOps = mock(ListOperations.class);

        when(redis.opsForSet()).thenReturn(setOps);
        when(redis.opsForList()).thenReturn(listOps);
        when(setOps.add(anyString(), anyString())).thenReturn(1L);
        when(listOps.size(anyString())).thenReturn(0L);
        when(listOps.leftPop(anyString())).thenReturn("5", (String) null);

        RedisOutboxDispatchQueue queue = new RedisOutboxDispatchQueue(redis, "outbox:test", 10);
        assertThat(queue.offer(5L)).isTrue();
        assertThat(queue.poll(Duration.ofMillis(10))).isEqualTo(5L);
        queue.acknowledge(List.of(5L));
        assertThat(queue.poll(Duration.ofMillis(20))).isNull();
    }

    @Test
    void rejectsDuplicateAndFullQueue() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        SetOperations<String, String> setOps = mock(SetOperations.class);
        @SuppressWarnings("unchecked")
        ListOperations<String, String> listOps = mock(ListOperations.class);
        when(redis.opsForSet()).thenReturn(setOps);
        when(redis.opsForList()).thenReturn(listOps);
        when(setOps.add(anyString(), eq("1"))).thenReturn(0L);
        when(setOps.add(anyString(), eq("2"))).thenReturn(1L);
        when(listOps.size(anyString())).thenReturn(10L);

        RedisOutboxDispatchQueue queue = new RedisOutboxDispatchQueue(redis, "outbox:x:", 10);
        assertThat(queue.offer(1L)).isFalse();
        assertThat(queue.offer(2L)).isFalse();
    }

    @Test
    void drainAndSizeFailOpen() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        SetOperations<String, String> setOps = mock(SetOperations.class);
        @SuppressWarnings("unchecked")
        ListOperations<String, String> listOps = mock(ListOperations.class);
        when(redis.opsForSet()).thenReturn(setOps);
        when(redis.opsForList()).thenReturn(listOps);
        when(listOps.leftPop(anyString())).thenReturn("1", "2", (String) null);
        when(listOps.size(anyString())).thenThrow(new RuntimeException("size"));

        RedisOutboxDispatchQueue queue = new RedisOutboxDispatchQueue(redis, "outbox:d:", 5);
        assertThat(queue.drain(0)).isEmpty();
        assertThat(queue.drain(5)).containsExactly(1L, 2L);
        assertThat(queue.size()).isZero();
        queue.acknowledge(null);
    }
}
