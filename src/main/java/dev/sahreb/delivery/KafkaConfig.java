package dev.sahreb.delivery;

import java.time.Duration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    public static final String REQUESTS = "notification.requests";
    public static final String DEAD_LETTERS = "notification.requests.DLT";

    @Bean
    NewTopic requestsTopic() {
        return TopicBuilder.name(REQUESTS).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(DEAD_LETTERS).partitions(1).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler deliveryErrorHandler(KafkaTemplate<String, String> template,
                                             DeliveryStore deliveries) {
        var publisher = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(DEAD_LETTERS, record.partition()));
        publisher.setFailIfSendResultIsError(true);
        publisher.setWaitForSendResultTimeout(Duration.ofSeconds(5));
        var handler = new DefaultErrorHandler((record, exception) -> {
            // A failed recovery must throw: never acknowledge a record that was not retained.
            publisher.accept(record, exception);
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                if (cause instanceof DeliveryException delivery) {
                    deliveries.markFailed(delivery.eventId());
                    break;
                }
            }
        }, new FixedBackOff(250L, 2L));
        handler.addNotRetryableExceptions(InvalidEventException.class, EventIdentityConflictException.class);
        // Storage must recover before we can truthfully record an outcome or offer replay.
        // An outage deliberately stalls this single-partition lab instead of skipping intent.
        handler.setBackOffFunction((record, exception) -> {
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                if (cause instanceof DataAccessException) {
                    return new FixedBackOff(1_000L, FixedBackOff.UNLIMITED_ATTEMPTS);
                }
            }
            return new FixedBackOff(250L, 2L);
        });
        handler.setAckAfterHandle(true);
        return handler;
    }
}
