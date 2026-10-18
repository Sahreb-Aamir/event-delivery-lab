package dev.sahreb.delivery;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeadLetterInspectorTest {
    private static final TopicPartition PARTITION = new TopicPartition(KafkaConfig.DEAD_LETTERS, 0);
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    @Test
    void fixesLatestHundredOffsetWindowAndExcludesRecordsAppendedAfterSnapshot() {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (long offset = 50; offset <= 150; offset++) {
            records.add(record(offset, "synthetic-" + offset, "payload", new RecordHeaders()));
        }
        Fixture fixture = new Fixture(10, 150, records);

        DeadLetterInspector.Snapshot snapshot = fixture.inspector(TIMEOUT).inspect(null, 5);

        assertThat(snapshot.topic()).isEqualTo(KafkaConfig.DEAD_LETTERS);
        assertThat(snapshot.partition()).isZero();
        assertThat(snapshot.startOffset()).isEqualTo(50);
        assertThat(snapshot.endOffsetExclusive()).isEqualTo(150);
        assertThat(snapshot.scannedRecords()).isEqualTo(100);
        assertThat(snapshot.earlierRecordsNotScanned()).isTrue();
        assertThat(snapshot.matchesTruncated()).isTrue();
        assertThat(snapshot.items()).extracting(DeadLetterInspector.Item::offset)
                .containsExactly(149L, 148L, 147L, 146L, 145L);
        verify(fixture.consumer).assign(List.of(PARTITION));
        verify(fixture.consumer).seek(PARTITION, 50);
        fixture.assertClosedOnce();
    }

    @Test
    void respectsRetentionFloorAndFiltersByExactKeyBeforeTakingNewestMatches() {
        UUID target = UUID.randomUUID();
        List<ConsumerRecord<String, String>> records = List.of(
                record(120, "other", "payload", new RecordHeaders()),
                record(123, target.toString(), "old match", new RecordHeaders()),
                record(147, target.toString(), "new match", new RecordHeaders()),
                record(148, UUID.randomUUID().toString(), "{\"eventId\":\"" + target + "\"}",
                        new RecordHeaders()),
                record(149, "other", "payload", new RecordHeaders()));
        Fixture fixture = new Fixture(120, 150, records);

        DeadLetterInspector.Snapshot snapshot = fixture.inspector(TIMEOUT).inspect(target, 1);

        assertThat(snapshot.startOffset()).isEqualTo(120);
        assertThat(snapshot.scannedRecords()).isEqualTo(5);
        assertThat(snapshot.earlierRecordsNotScanned()).isFalse();
        assertThat(snapshot.matchesTruncated()).isTrue();
        assertThat(snapshot.items()).singleElement().satisfies(item -> {
            assertThat(item.offset()).isEqualTo(147);
            assertThat(item.key()).isEqualTo(target.toString());
            assertThat(item.payloadPreview()).isEqualTo("new match");
        });
        verify(fixture.consumer).seek(PARTITION, 120);
        fixture.assertClosedOnce();
    }

    @Test
    void boundsUntrustedFieldsAndTreatsMissingOrMalformedHeadersAsUnknown() {
        Headers valid = new RecordHeaders()
                .add(KafkaHeaders.DLT_EXCEPTION_FQCN, utf8("older.Exception"))
                .add(KafkaHeaders.DLT_EXCEPTION_FQCN, utf8("c".repeat(300)))
                .add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, utf8("m".repeat(600)))
                .add(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, utf8("e".repeat(300)))
                .add(KafkaHeaders.DLT_ORIGINAL_TOPIC, utf8(KafkaConfig.REQUESTS))
                .add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(2).array())
                .add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(42).array());
        Headers malformed = new RecordHeaders()
                .add(KafkaHeaders.DLT_EXCEPTION_FQCN, null)
                .add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, null)
                .add(KafkaHeaders.DLT_ORIGINAL_TOPIC, utf8("invalid/topic"))
                .add(KafkaHeaders.DLT_ORIGINAL_PARTITION, new byte[] { 0, 0, 1 })
                .add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(-1).array());
        Fixture fixture = new Fixture(0, 2, List.of(
                record(0, "k".repeat(129), "v".repeat(2_049), valid),
                record(1, null, null, malformed)));

        List<DeadLetterInspector.Item> items = fixture.inspector(TIMEOUT).inspect(null, 10).items();

        assertThat(items).hasSize(2);
        DeadLetterInspector.Item unknown = items.get(0);
        assertThat(unknown.key()).isNull();
        assertThat(unknown.keyTruncated()).isFalse();
        assertThat(unknown.payloadPreview()).isNull();
        assertThat(unknown.payloadTruncated()).isFalse();
        assertThat(unknown.exceptionClass()).isNull();
        assertThat(unknown.exceptionMessage()).isNull();
        assertThat(unknown.exceptionCauseClass()).isNull();
        assertThat(unknown.originalTopic()).isNull();
        assertThat(unknown.originalPartition()).isNull();
        assertThat(unknown.originalOffset()).isNull();
        DeadLetterInspector.Item bounded = items.get(1);
        assertThat(bounded.key()).isEqualTo("k".repeat(128));
        assertThat(bounded.keyTruncated()).isTrue();
        assertThat(bounded.payloadPreview()).isEqualTo("v".repeat(2_048));
        assertThat(bounded.payloadTruncated()).isTrue();
        assertThat(bounded.exceptionClass()).isEqualTo("c".repeat(256));
        assertThat(bounded.exceptionMessage()).isEqualTo("m".repeat(512));
        assertThat(bounded.exceptionCauseClass()).isEqualTo("e".repeat(256));
        assertThat(bounded.originalTopic()).isEqualTo(KafkaConfig.REQUESTS);
        assertThat(bounded.originalPartition()).isEqualTo(2);
        assertThat(bounded.originalOffset()).isEqualTo(42);
        fixture.assertClosedOnce();
    }

    @Test
    void incompleteScanFailsInsteadOfReturningAnApparentlyEmptySnapshot() {
        Fixture fixture = new Fixture(0, 1, List.of());
        when(fixture.consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        assertStatus(503, () -> fixture.inspector(Duration.ofMillis(25)).inspect(null, 10));

        fixture.assertClosedOnce();
    }

    @Test
    void brokerFailureClosesConsumerAndReleasesTheNextInspection() {
        Fixture fixture = new Fixture(0, 1,
                List.of(record(0, "synthetic", "payload", new RecordHeaders())));
        AtomicInteger polls = new AtomicInteger();
        when(fixture.consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
            if (polls.getAndIncrement() == 0) {
                throw new KafkaException("Synthetic broker outage");
            }
            return fixture.batch();
        });
        DeadLetterInspector inspector = fixture.inspector(TIMEOUT);

        assertStatus(503, () -> inspector.inspect(null, 10));
        assertThat(inspector.inspect(null, 10).items()).hasSize(1);

        assertThat(fixture.created).hasValue(2);
        verify(fixture.consumer, times(2)).close(Duration.ofMillis(100));
    }

    @Test
    void overlappingInspectionIsRejectedAndPermitIsReleasedAfterCompletion() throws Exception {
        Fixture fixture = new Fixture(0, 1,
                List.of(record(0, "synthetic", "payload", new RecordHeaders())));
        CountDownLatch polling = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(fixture.consumer.poll(any(Duration.class))).thenAnswer(invocation -> {
            polling.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Test did not release blocked inspection");
            }
            return fixture.batch();
        });
        DeadLetterInspector inspector = fixture.inspector(Duration.ofSeconds(5));
        var executor = Executors.newSingleThreadExecutor();
        try {
            var first = executor.submit(() -> inspector.inspect(null, 10));
            assertThat(polling.await(3, TimeUnit.SECONDS)).isTrue();

            assertStatus(429, () -> inspector.inspect(null, 10));
            assertThat(fixture.created).hasValue(1);
            release.countDown();
            assertThat(first.get(3, TimeUnit.SECONDS).items()).hasSize(1);
            assertThat(inspector.inspect(null, 10).items()).hasSize(1);

            assertThat(fixture.created).hasValue(2);
            verify(fixture.consumer, times(2)).close(Duration.ofMillis(100));
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void metadataCannotResetAnAlreadyExpiredOverallDeadline() {
        Fixture fixture = new Fixture(0, 1, List.of());
        when(fixture.consumer.partitionsFor(eq(KafkaConfig.DEAD_LETTERS), any(Duration.class)))
                .thenAnswer(invocation -> {
                    Thread.sleep(30);
                    return List.of(partitionInfo(0));
                });

        assertStatus(503, () -> fixture.inspector(Duration.ofMillis(5)).inspect(null, 10));

        verify(fixture.consumer, never()).poll(any(Duration.class));
        fixture.assertClosedOnce();
    }

    @Test
    void unexpectedPartitionCountFailsInsteadOfSilentlyIgnoringOtherDeadLetters() {
        Fixture fixture = new Fixture(0, 0, List.of());
        when(fixture.consumer.partitionsFor(eq(KafkaConfig.DEAD_LETTERS), any(Duration.class)))
                .thenReturn(List.of(partitionInfo(0), partitionInfo(1)));

        assertStatus(503, () -> fixture.inspector(TIMEOUT).inspect(null, 10));

        verify(fixture.consumer, never()).poll(any(Duration.class));
        fixture.assertClosedOnce();
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 101 })
    void invalidLimitIsRejectedBeforeConnectingToKafka(int limit) {
        Fixture fixture = new Fixture(0, 0, List.of());

        assertStatus(400, () -> fixture.inspector(TIMEOUT).inspect(null, limit));

        assertThat(fixture.created).hasValue(0);
    }

    private static void assertStatus(int code, Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(ResponseStatusException.class,
                exception -> assertThat(exception.getStatusCode().value()).isEqualTo(code));
    }

    private static ConsumerRecord<String, String> record(long offset, String key, String value,
                                                         Headers headers) {
        return new ConsumerRecord<>(PARTITION.topic(), PARTITION.partition(), offset, 1_700_000_000_000L,
                TimestampType.CREATE_TIME, key == null ? -1 : key.length(), value == null ? -1 : value.length(),
                key, value, headers, Optional.empty());
    }

    private static PartitionInfo partitionInfo(int partition) {
        return new PartitionInfo(KafkaConfig.DEAD_LETTERS, partition, null, new Node[0], new Node[0]);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** Models position progress, including compacted gaps and appends beyond the captured end. */
    private static final class Fixture {
        private final Consumer<String, String> consumer;
        private final AtomicInteger created = new AtomicInteger();
        private final AtomicLong position = new AtomicLong();
        private final long end;
        private final List<ConsumerRecord<String, String>> records;

        @SuppressWarnings("unchecked")
        private Fixture(long beginning, long end, List<ConsumerRecord<String, String>> records) {
            this.end = end;
            this.records = records;
            consumer = mock(Consumer.class, invocation -> {
                String method = invocation.getMethod().getName();
                if (method.equals("subscribe") || method.startsWith("commit")
                        || method.equals("enforceRebalance")) {
                    throw new AssertionError("Read-only inspection called " + method);
                }
                return Answers.RETURNS_DEFAULTS.answer(invocation);
            });
            when(consumer.partitionsFor(eq(KafkaConfig.DEAD_LETTERS), any(Duration.class)))
                    .thenReturn(List.of(partitionInfo(0)));
            when(consumer.beginningOffsets(anyCollection(), any(Duration.class)))
                    .thenReturn(Map.of(PARTITION, beginning));
            when(consumer.endOffsets(anyCollection(), any(Duration.class)))
                    .thenReturn(Map.of(PARTITION, end));
            doAnswer(invocation -> {
                position.set(invocation.getArgument(1));
                return null;
            }).when(consumer).seek(eq(PARTITION), anyLong());
            when(consumer.position(eq(PARTITION), any(Duration.class))).thenAnswer(ignored -> position.get());
            when(consumer.poll(any(Duration.class))).thenAnswer(ignored -> batch());
        }

        private ConsumerRecords<String, String> batch() {
            List<ConsumerRecord<String, String>> remaining = records.stream()
                    .filter(record -> record.offset() >= position.get()).toList();
            position.set(remaining.isEmpty() ? end : remaining.get(remaining.size() - 1).offset() + 1);
            return new ConsumerRecords<>(Map.of(PARTITION, remaining));
        }

        private DeadLetterInspector inspector(Duration timeout) {
            return new DeadLetterInspector(() -> {
                created.incrementAndGet();
                return consumer;
            }, timeout);
        }

        private void assertClosedOnce() {
            assertThat(created).hasValue(1);
            verify(consumer).close(Duration.ofMillis(100));
        }
    }
}
