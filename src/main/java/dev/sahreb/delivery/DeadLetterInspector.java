package dev.sahreb.delivery;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/** A bounded snapshot reader. Assignment never joins or advances the delivery consumer group. */
@Service
public class DeadLetterInspector {
    private static final int WINDOW_SIZE = 100;
    private static final Duration QUERY_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(100);
    private static final String UNAVAILABLE = "Dead-letter inspection is temporarily unavailable";

    private final Supplier<Consumer<String, String>> consumers;
    private final Duration timeout;
    private final Semaphore query = new Semaphore(1);

    @Autowired
    public DeadLetterInspector(ConsumerFactory<?, ?> factory) {
        this(() -> new KafkaConsumer<>(configuration(factory),
                new StringDeserializer(), new StringDeserializer()), QUERY_TIMEOUT);
    }

    DeadLetterInspector(Supplier<Consumer<String, String>> consumers, Duration timeout) {
        this.consumers = Objects.requireNonNull(consumers);
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Inspection timeout must be positive");
        }
        this.timeout = timeout;
    }

    private static HashMap<String, Object> configuration(ConsumerFactory<?, ?> factory) {
        var properties = new HashMap<String, Object>(factory.getConfigurationProperties());
        properties.remove(ConsumerConfig.GROUP_ID_CONFIG);
        properties.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "dead-letter-inspector-" + UUID.randomUUID());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, WINDOW_SIZE);
        // Kafka can still return an oversized first batch; these are fetch targets, not memory caps.
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 65_536);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 65_536);
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) QUERY_TIMEOUT.toMillis());
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) QUERY_TIMEOUT.toMillis());
        return properties;
    }

    public Snapshot inspect(UUID eventId, int limit) {
        if (limit < 1 || limit > WINDOW_SIZE) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be between 1 and 100");
        }
        if (!query.tryAcquire()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                    "A dead-letter inspection is already running");
        }
        Consumer<String, String> consumer = null;
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            consumer = consumers.get();
            return scan(consumer, eventId, limit, deadline);
        } catch (ResponseStatusException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, UNAVAILABLE, failure);
        } finally {
            try {
                if (consumer != null) consumer.close(CLOSE_TIMEOUT);
            } catch (RuntimeException ignored) {
                // Closing has its own small budget and must not obscure the scan's outcome.
            } finally {
                query.release();
            }
        }
    }

    private Snapshot scan(Consumer<String, String> consumer, UUID eventId, int limit, long deadline) {
        var partitions = consumer.partitionsFor(KafkaConfig.DEAD_LETTERS, remaining(deadline));
        if (partitions == null || partitions.size() != 1 || partitions.get(0).partition() != 0) {
            // This lab has one partition. Never present a partial multi-partition view as complete.
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, UNAVAILABLE);
        }
        var partition = new TopicPartition(KafkaConfig.DEAD_LETTERS, 0);
        var assignment = List.of(partition);
        consumer.assign(assignment);
        long beginning = consumer.beginningOffsets(assignment, remaining(deadline)).get(partition);
        long end = consumer.endOffsets(assignment, remaining(deadline)).get(partition);
        if (beginning < 0 || end < beginning) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, UNAVAILABLE);
        }
        long start = Math.max(beginning, end - WINDOW_SIZE);
        consumer.seek(partition, start);
        int scanned = 0;
        var matches = new ArrayList<Item>();
        String key = eventId == null ? null : eventId.toString();
        while (consumer.position(partition, remaining(deadline)) < end) {
            Duration budget = remaining(deadline);
            var records = consumer.poll(budget.compareTo(Duration.ofMillis(100)) < 0
                    ? budget : Duration.ofMillis(100));
            for (var record : records.records(partition)) {
                if (record.offset() < start || record.offset() >= end) continue;
                scanned++;
                if (key == null || key.equals(record.key())) matches.add(item(record));
            }
        }
        remaining(deadline); // An expired query must not report an incomplete scan as an empty result.
        matches.sort(Comparator.comparingLong(Item::offset).reversed());
        return new Snapshot(KafkaConfig.DEAD_LETTERS, 0, start, end, scanned, start > beginning,
                matches.size() > limit, List.copyOf(matches.subList(0, Math.min(limit, matches.size()))));
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0 || Thread.currentThread().isInterrupted()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, UNAVAILABLE);
        }
        return Duration.ofNanos(nanos);
    }

    private static Item item(ConsumerRecord<String, String> record) {
        Headers headers = record.headers();
        return new Item(record.offset(), record.timestamp(), preview(record.key(), 128),
                truncated(record.key(), 128), preview(record.value(), 2_048), truncated(record.value(), 2_048),
                textHeader(headers, KafkaHeaders.DLT_EXCEPTION_FQCN, 256),
                textHeader(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE, 512),
                textHeader(headers, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, 256),
                originalTopic(headers), integerHeader(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET));
    }

    private static String originalTopic(Headers headers) {
        byte[] value = header(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (value == null || value.length > 249) return null;
        String topic = new String(value, StandardCharsets.UTF_8);
        return topic.matches("[a-zA-Z0-9._-]{1,249}") && !topic.equals(".") && !topic.equals("..")
                ? topic : null;
    }

    private static String textHeader(Headers headers, String name, int limit) {
        byte[] value = header(headers, name);
        if (value == null) return null;
        // Decode only a bounded prefix, even when a broker record contains a large header.
        return preview(new String(value, 0, Math.min(value.length, limit * 4), StandardCharsets.UTF_8), limit);
    }

    private static Integer integerHeader(Headers headers, String name) {
        byte[] value = header(headers, name);
        if (value == null || value.length != Integer.BYTES) return null;
        int number = ByteBuffer.wrap(value).getInt();
        return number < 0 ? null : number;
    }

    private static Long longHeader(Headers headers, String name) {
        byte[] value = header(headers, name);
        if (value == null || value.length != Long.BYTES) return null;
        long number = ByteBuffer.wrap(value).getLong();
        return number < 0 ? null : number;
    }

    private static byte[] header(Headers headers, String name) {
        var header = headers.lastHeader(name);
        return header == null ? null : header.value();
    }

    private static String preview(String value, int limit) {
        if (!truncated(value, limit)) return value;
        int end = Character.isHighSurrogate(value.charAt(limit - 1))
                && Character.isLowSurrogate(value.charAt(limit)) ? limit - 1 : limit;
        return value.substring(0, end);
    }

    private static boolean truncated(String value, int limit) {
        return value != null && value.length() > limit;
    }

    public record Snapshot(String topic, int partition, long startOffset, long endOffsetExclusive,
            int scannedRecords, boolean earlierRecordsNotScanned, boolean matchesTruncated, List<Item> items) { }

    public record Item(long offset, long timestamp, String key, boolean keyTruncated,
            String payloadPreview, boolean payloadTruncated, String exceptionClass, String exceptionMessage,
            String exceptionCauseClass, String originalTopic, Integer originalPartition, Long originalOffset) { }
}
