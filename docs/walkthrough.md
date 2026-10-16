# A guided review of Event Delivery Lab

Use these three stops to explain the problem, inspect the evidence, and discuss the limits. See the [recorded walkthrough check](results.md#guided-walkthrough-on-a-reused-demo) for an observed run.

## Before starting

Follow the [README setup](../README.md#run-locally), using the default built-in receiver and timeouts. Keep one browser tab open, stop other event writers, and wait for active runs to finish. Existing receipts are fine; do not delete the database. Receiver counters are cumulative within this process: note their values before each action and compare deltas. The selected event's UUID and attempt history are the primary evidence; counter deltas corroborate them.

## 1. Lose an acknowledgment after the action

Choose **Lose the acknowledgment**. Wait for `DELIVERED`, then inspect **Delivery detail**: expect `NETWORK_ERROR` followed by `SUCCEEDED`. In **Receiver observation**, the selected UUID should have one receiver-side record. With no other writers, the counters should increase by two requests and one effect.

The receiver records its effect before delaying the response. A timeout therefore cannot establish that nothing happened. On retry, the same idempotency key finds the existing effect. Follow the [processor](../src/main/java/dev/sahreb/delivery/NotificationProcessor.java) and [receiver](../src/main/java/dev/sahreb/delivery/DemoReceiverController.java) across that boundary.

The dashboard demonstrates the idempotent receiver. The separate [naive-versus-idempotent experiment](failure-drills.md#compare-naive-and-idempotent-receivers) changes receiver behavior and verifies two effects versus one; there is no naive-mode dashboard switch. Finish the three stops, then stop the launcher with `Ctrl+C` before running Maven tests in the same checkout. Another build can replace the running demo's classes; run one Maven process at a time.

## 2. Replay the exact event

Without reloading, choose **Replay last submitted event**. After acceptance and refresh, expect the same receipt, two attempts, and no additional receiver request or effect. The replay preserves the original timestamp as well as the ID and message.

The consumer skips HTTP after a recorded success. A UI snapshot alone cannot prove the replay was consumed: the [integration test](../src/test/java/dev/sahreb/delivery/DeliveryIntegrationTest.java) puts another event behind it in the partition and waits for that marker before asserting unchanged history.

## 3. Inspect a failure before recovering it

Choose **Exhaust automatic attempts**. Wait for `FAILED` and three HTTP `503` attempts. Copy **Event ID from Delivery detail**, not the composer: the composer already contains the next ID.

Replace `<selected-event-uuid>` below. macOS/Linux:

```sh
curl "http://127.0.0.1:8080/api/dead-letters?eventId=<selected-event-uuid>&limit=20"
```

PowerShell:

```powershell
Invoke-RestMethod 'http://127.0.0.1:8080/api/dead-letters?eventId=<selected-event-uuid>&limit=20' | ConvertTo-Json -Depth 6
```

Expect a matching key, failure headers, original record coordinates, and the searched `startOffset`/`endOffsetExclusive` window. Note the item's dead-letter `offset`. Repeat the GET: inspection should leave delivery history unchanged.

Set **Fail next receiver requests** to `0`, choose **Apply**, then **Retry this event**. Expect `DELIVERED` with four total attempts. Repeat the GET: the original dead letter should remain at the same offset. Recovery adds an attempt; it does not erase the failure evidence. The [integration checks](../src/test/java/dev/sahreb/delivery/DeliveryIntegrationTest.java) also verify unchanged committed offsets, consumer groups, and receipt count during inspection.

This API searches only the latest 100 offset positions. An empty result is not a full-history conclusion; retention and broker restart can remove records. See [response interpretation](failure-drills.md#inspect-dead-letters-without-advancing-consumer-offsets).

## Explain the choices in about a minute

> I built this independent, AI-assisted Java/Kafka experiment to understand retry ambiguity. It separates broker acceptance, a local receipt, an HTTP attempt, and a receiver-side effect. The controlled lost-acknowledgment comparison sends two requests in both cases: a naive receiver performs two effects, while receiver-side idempotency produces one. The workbench makes that difference inspectable and lets someone recover exhausted events without erasing earlier attempts. Kafka exposes the queue boundary; a simple webhook sender would not inherently need it. H2 makes local history easy to inspect, but its transaction cannot include a remote action. This is a reproducible learning and debugging tool, not an employer incident or production delivery claim.

For follow-up questions, use the [storage lifetimes and crash windows](architecture.md#persistence-and-crash-windows): receiver deduplication is volatile, the embedded broker starts fresh, and the [earlier persistence anomaly](results.md#what-a-harder-test-exposed) remains unresolved despite later passing crash tests. Read-only diagnostics improve observation; they do not supply automatic recovery or exactly-once delivery.
