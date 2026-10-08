# Reproducible failure drills

Start the [local workbench](../README.md#run-locally) and open [127.0.0.1:8080](http://127.0.0.1:8080). These drills use synthetic notifications and the built-in receiver.

Each scenario creates a fresh event from the composer. Run them one at a time: injected failures and delayed responses are shared by all events, and another request can consume the counter. The dashboard locks scenario and receiver-control changes while an accepted or observed event remains in progress, including submissions whose receipts have not appeared yet. A manual retry stays pending until attempts advance beyond the previous run. Polling runs every two seconds while the page is visible, so short intermediate states may not appear on screen; the attempt history remains available.

The source links show the checks behind these drills. The automated lost-acknowledgment comparison has passed with the results below; the interactive steps describe the same expected behavior. These are controlled experiments, not benchmarks or claims about an external service.

## Lose the acknowledgment

**Question:** Can the sender record a timeout even though the receiver already performed the action?

1. Keep the default synthetic notification or write another synthetic message.
2. Choose **Lose the acknowledgment**.
3. Inspect the selected event's delivery attempts and its exact ID in **Receiver observation**.

The scenario clears injected failures, configures one delayed acknowledgment with `count: 1, delayMs: 2000`, and submits a fresh event. The built-in receiver stores the effect before pausing. The sender's default 1,000 ms deadline expires, then it retries with the same `Idempotency-Key`.

Expected evidence:

| Observation | Expected |
| --- | --- |
| Local receipts for the ID | 1 |
| Attempt 1 | `NETWORK_ERROR`, with no HTTP status |
| Attempt 2 | `SUCCEEDED`, HTTP `200` |
| Final sender state | `DELIVERED` |
| Receiver entries matching the event ID | 1 |

The first timeout does not say that the receiver did nothing. The receiver's existing record allows the second request to return a deduplicated response. The dashboard's request/effect counters cover the whole current process: use the selected ID rather than interpreting a global total as one event's result.

This drill demonstrates the idempotent case. The next experiment provides the controlled comparison.

## Compare naive and idempotent receivers

Run the focused parameterized test:

```sh
./mvnw "-Dtest=NotificationProcessorTest#lostAcknowledgmentRepeatsSideEffectUnlessReceiverDeduplicates" test
```

Windows PowerShell:

```powershell
.\mvnw.cmd "-Dtest=NotificationProcessorTest#lostAcknowledgmentRepeatsSideEffectUnlessReceiverDeduplicates" test
```

[`NotificationProcessorTest`](../src/test/java/dev/sahreb/delivery/NotificationProcessorTest.java) starts a real local HTTP receiver and deliberately holds the first acknowledgment after incrementing its side-effect counter. It calls the processor again after the timeout, using the original event. The parameter changes only whether the receiver deduplicates the idempotency key.

| Receiver behavior | HTTP requests | Stored receipts | Side effects | Sender outcomes |
| --- | --- | --- | --- | --- |
| Naive: performs work for every request | 2 | 1 | 2 | `NETWORK_ERROR`, then `SUCCEEDED` |
| Idempotent: performs work once per key | 2 | 1 | 1 | `NETWORK_ERROR`, then `SUCCEEDED` |

These values were verified in the passing parameterized experiment. Its scope is the processor, H2, and real HTTP; the retry is invoked directly to isolate the failure mode. [`DeliveryIntegrationTest`](../src/test/java/dev/sahreb/delivery/DeliveryIntegrationTest.java) separately checks the API/Kafka/consumer path and retry policy. Neither test claims durable exactly-once delivery to arbitrary receivers.

Both cases have one sender-side receipt. That is why a unique database key on the sender cannot, by itself, prevent repeated remote effects.

## Recover after two temporary failures

Choose **Recover from failure** and inspect the resulting event.

The receiver returns `503` for its next two new-event requests. Expected attempts are `HTTP_ERROR`, `HTTP_ERROR`, `SUCCEEDED`, with HTTP statuses `503`, `503`, `200`. The final state is `DELIVERED`; one receiver-side record exists for the ID. The injected failures happen before the receiver records an effect.

The normal policy allows an initial attempt plus two retries, 250 ms apart. This interval is retry backoff, not a promise that the whole operation completes in 500 ms.

Executable check: `DeliveryIntegrationTest#twoTemporaryFailuresRecoverOnThirdAttempt`.

## Exhaust attempts, then recover explicitly

1. Choose **Exhaust automatic attempts**.
2. Wait for `FAILED` and inspect three failed HTTP attempts.
3. Set **Fail next receiver requests** to `0` and choose **Apply**.
4. Choose **Retry this event** in delivery detail.

Expected behavior: the initial run produces three `HTTP_ERROR` attempts and publishes the original record to `notification.requests.DLT`. Manual retry republishes the stored event. It preserves the receipt and three earlier attempts, then adds a fourth `SUCCEEDED` attempt when the receiver accepts it.

The retry endpoint accepts `FAILED` or `INTERRUPTED` events. Attempting it for a delivered event returns `409`. Successful manual recovery does not erase the original dead-letter record.

Executable check: `DeliveryIntegrationTest#exhaustedRetriesReachDeadLetterAndExplicitReplayRecovers`.

## Recover unfinished work after a demo restart

The embedded broker starts fresh each time. If the previous run stopped with a receipt still queued, delivering, or retrying, the demo launcher marks that event `INTERRUPTED` before starting the new consumer. The old broker's record is gone; the persisted receipt and attempt history remain.

Select the interrupted event, inspect its attempts, clear any injected failures, and choose **Retry this event** if you want to republish the original payload. An old `PENDING` attempt stays unknown even if a later attempt succeeds. The receiver may already have acted, and its in-memory idempotency map resets with the application, so retry can repeat a side effect.

This startup behavior applies only to the local embedded-broker launcher. The packaged application using an external broker does not rewrite unfinished states on startup. Storage tests cover interruption reconciliation and preserve attempt history; this is not a claim of automatic restart-safe delivery.

## Replay an identical event

Choose **Successful delivery**, wait for `DELIVERED`, then choose **Replay last submitted event**.

The button preserves the ID, seller, message, and original occurrence timestamp. Expected behavior: one unchanged receipt, no extra HTTP attempt after the recorded success, and no extra effect at the controlled receiver. The broker can contain another copy while the consumer recognizes the already delivered event.

The replay button's payload exists only in the current browser page. Reloading clears it. For a recorded failed event, the server's **Retry this event** action reconstructs the original payload from storage.

Executable check: `DeliveryIntegrationTest#httpToKafkaToWebhookAndIdenticalReplayProducesOneReceiptAndEffect`.

## Understand conflicts and malformed records

The HTTP API validates shape and field constraints before publication. It does not perform synchronous receipt-identity checks.

Submitting a valid event with an existing ID but a different message, seller, or `occurredAt` can therefore return `202`. During consumption, the store rejects the conflict and keeps the first receipt unchanged. The conflicting Kafka record goes to the dead-letter topic without another HTTP delivery. The dashboard intentionally keeps the original event view; it has no dead-letter-only record inspector yet.

Malformed broker JSON bypasses HTTP validation but is rejected by the consumer and retained in the dead-letter topic. It should not indefinitely prevent a following valid event from being delivered.

Executable checks:

- `DeliveryIntegrationTest#conflictingEventIdGoesToDeadLetterWithoutChangingOriginal`
- `DeliveryIntegrationTest#malformedBrokerMessageIsRetainedAndDoesNotBlockFollowingEvents`
- `NotificationApiTest#invalidEventAndMalformedJsonAreRejectedBeforePublishing`

## Interpret uncertainty correctly

| What you see | What to do |
| --- | --- |
| Submission returns `503` or the connection drops | Inspect recent events. The broker may have accepted the record; replay the original complete payload if retrying. |
| Submission returned `202`, then a read returns `404` | Allow the consumer to record the receipt; acceptance and recording are asynchronous. |
| An attempt is `NETWORK_ERROR` | Check receiver-side evidence. A timeout does not establish whether the effect happened. |
| An event is `INTERRUPTED` after demo restart and an old attempt remains `PENDING` | Treat the old remote outcome as unknown. Explicit retry uses a fresh broker and can repeat the effect. |
| Events stop progressing during a database outage | Storage failures retry indefinitely and can stall the single partition. Restore storage before expecting progress. |
| The receiver is empty after application restart | Its counters and idempotency map reset, even when sender-side H2 receipts remain. |

The embedded broker is also fresh after restart. These interactive drills do not establish restart-safe queue recovery. See [storage lifetimes and crash windows](architecture.md#persistence-and-crash-windows) before extending the experiment.
