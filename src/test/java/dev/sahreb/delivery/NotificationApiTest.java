package dev.sahreb.delivery;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class NotificationApiTest {
    private final ObjectMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private final NotificationEvent event = new NotificationEvent(UUID.randomUUID(), "demo-api",
            "Synthetic API example", Instant.parse("2026-10-07T12:00:00.123456789Z"));
    private ReceiptStore receipts;
    private DeliveryStore deliveries;
    private KafkaTemplate<String, String> kafka;
    private MockMvc mvc;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        receipts = mock(ReceiptStore.class);
        deliveries = mock(DeliveryStore.class);
        kafka = mock(KafkaTemplate.class);
        NotificationController controller = new NotificationController(receipts, deliveries, kafka, mapper, 25);
        mvc = MockMvcBuilders.standaloneSetup(controller, new DemoReceiverController())
                .setControllerAdvice(new ApiExceptionHandler()).build();
    }

    @Test
    void acknowledgedSubmissionReturnsQueuedAndPreservesPayloadAndKey() throws Exception {
        when(kafka.send(eq(KafkaConfig.REQUESTS), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(event)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(event.eventId().toString()))
                .andExpect(jsonPath("$.status").value("QUEUED"));
        ArgumentCaptor<String> sent = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(KafkaConfig.REQUESTS), eq(event.eventId().toString()), sent.capture());
        assertThat(mapper.readValue(sent.getValue(), NotificationEvent.class)).isEqualTo(event);
    }

    @Test
    void missingAcknowledgmentReturns503WithSameIdRetryInstructions() throws Exception {
        when(kafka.send(eq(KafkaConfig.REQUESTS), anyString(), anyString())).thenReturn(new CompletableFuture<>());
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(event)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(containsString("Delivery may still occur")))
                .andExpect(content().string(containsString("retry the same payload")))
                .andExpect(content().string(containsString(event.eventId().toString())));
        verifyNoInteractions(receipts, deliveries);
    }

    @Test
    void synchronousProducerFailureIsAlsoServiceUnavailable() throws Exception {
        when(kafka.send(eq(KafkaConfig.REQUESTS), anyString(), anyString()))
                .thenThrow(new org.springframework.kafka.KafkaException("broker unavailable"));
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(event)))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void invalidEventAndMalformedJsonAreRejectedBeforePublishing() throws Exception {
        NotificationEvent invalid = new NotificationEvent(event.eventId(), "customer-data", " ", null);
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(invalid)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Request validation failed"));
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(kafka);
    }

    @Test
    void missingEventCannotBeReadOrRetried() throws Exception {
        when(receipts.find(event.eventId())).thenReturn(Optional.empty());
        mvc.perform(get("/api/events/{id}", event.eventId())).andExpect(status().isNotFound());
        mvc.perform(post("/api/events/{id}/retry", event.eventId())).andExpect(status().isNotFound());
        verifyNoInteractions(kafka);
    }

    @ParameterizedTest
    @ValueSource(strings = { "DELIVERED", "DELIVERING", "RETRYING", "QUEUED" })
    void deliveredOrActiveEventCannotBeManuallyRetried(String currentStatus) throws Exception {
        when(receipts.find(event.eventId())).thenReturn(Optional.of(receipt()));
        when(deliveries.status(event.eventId())).thenReturn(new DeliveryStatus(currentStatus, List.of()));
        mvc.perform(post("/api/events/{id}/retry", event.eventId())).andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("Only FAILED or INTERRUPTED events can be retried"));
        verifyNoInteractions(kafka);
    }

    @ParameterizedTest
    @ValueSource(strings = { "FAILED", "INTERRUPTED" })
    void failedOrInterruptedEventRetryPublishesOriginalStoredEvent(String currentStatus) throws Exception {
        when(receipts.find(event.eventId())).thenReturn(Optional.of(receipt()));
        when(deliveries.status(event.eventId())).thenReturn(new DeliveryStatus(currentStatus, List.of()));
        when(kafka.send(eq(KafkaConfig.REQUESTS), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        mvc.perform(post("/api/events/{id}/retry", event.eventId())).andExpect(status().isAccepted());
        ArgumentCaptor<String> sent = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(KafkaConfig.REQUESTS), eq(event.eventId().toString()), sent.capture());
        assertThat(mapper.readValue(sent.getValue(), NotificationEvent.class)).isEqualTo(event);
    }

    @Test
    void syntheticReceiverFailsThenDeduplicatesAndRejectsConflictingPayload() throws Exception {
        mvc.perform(post("/api/demo/failures").contentType(MediaType.APPLICATION_JSON).content("{\"count\":1}"))
                .andExpect(status().isOk());
        mvc.perform(post("/demo/webhook").header("Idempotency-Key", event.eventId())
                        .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(event)))
                .andExpect(status().isServiceUnavailable());
        for (boolean duplicate : List.of(false, true)) {
            mvc.perform(post("/demo/webhook").header("Idempotency-Key", event.eventId())
                            .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(event)))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.duplicate").value(duplicate));
        }
        NotificationEvent conflicting = new NotificationEvent(event.eventId(), event.sellerId(), "Changed", event.occurredAt());
        mvc.perform(post("/demo/webhook").header("Idempotency-Key", event.eventId())
                        .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(conflicting)))
                .andExpect(status().isConflict());
        mvc.perform(get("/api/demo"))
                .andExpect(jsonPath("$.sideEffectCount").value(1))
                .andExpect(jsonPath("$.deliveryCount").value(1))
                .andExpect(jsonPath("$.receivedRequests").value(4));
    }

    @Test
    void receiverControlsEnforceDocumentedBoundsAndIdempotencyHeader() throws Exception {
        for (String invalid : List.of("{\"count\":-1}", "{\"count\":11}", "{}")) {
            mvc.perform(post("/api/demo/failures").contentType(MediaType.APPLICATION_JSON).content(invalid))
                    .andExpect(status().isBadRequest());
        }
        for (String invalid : List.of("{\"count\":1,\"delayMs\":5001}", "{\"count\":1,\"delayMs\":-1}", "{\"count\":1}")) {
            mvc.perform(post("/api/demo/ack-delay").contentType(MediaType.APPLICATION_JSON).content(invalid))
                    .andExpect(status().isBadRequest());
        }
        mvc.perform(post("/api/demo/ack-delay").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"count\":1,\"delayMs\":2000}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.acknowledgmentsToDelay").value(1))
                .andExpect(jsonPath("$.ackDelayMs").value(2000));
        mvc.perform(post("/demo/webhook").header("Idempotency-Key", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(event)))
                .andExpect(status().isBadRequest());
    }

    private Receipt receipt() {
        return new Receipt(event.eventId(), event.sellerId(), event.message(), event.occurredAt(), Instant.now());
    }
}
