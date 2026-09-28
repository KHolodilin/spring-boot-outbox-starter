package com.kholodilin.outbox.autoconfigure.smoke;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class OutboxBoot4SmokeApplication {

    @Bean
    RecordingOutboxSink recordingOutboxSink() {
        return new RecordingOutboxSink();
    }
}
