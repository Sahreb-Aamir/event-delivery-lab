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

The embedded broker starts fresh each time. If the previous run stopped with a receipt still queued, delivering, or retrying, the demo launcher marks that event `INTERRUPTED` before starting the new consumer. The old broker's record is gone. Startup reconciliation acts on the receipt and attempt history that survive; hard-stop persistence has the [limitations recorded in the experiments](results.md#what-a-harder-test-exposed).

Select the interrupted event, inspect its attempts, clear any injected failures, and choose **Retry this event** if you want to republish the original payload. An old `PENDING` attempt stays unknown even if a later attempt succeeds. The receiver may already have acted, and its in-memory idempotency map resets with the application, so retry can repeat a side effect.

This startup behavior applies only to the local embedded-broker launcher. The packaged application using an external broker does not rewrite unfinished states on startup. Storage tests cover interruption reconciliation and preserve attempt history; this is not a claim of automatic restart-safe delivery.

## Force-stop the full application

**Question:** Does the full launcher preserve an unknown attempt when the receiver has acted but the sender is killed before recording a response?

Run the focused test (also included in `verify`):

```sh
./mvnw -Dtest=FullAppCrashRecoveryTest test
```

Windows PowerShell:

```powershell
.\mvnw.cmd "-Dtest=FullAppCrashRecoveryTest" test
```

[`FullAppCrashRecoveryTest`](../src/test/java/dev/sahreb/delivery/FullAppCrashRecoveryTest.java) starts `LocalLab` in a child JVM with real Kafka, a fresh file database, and an isolated loopback HTTP port. It sends six successful events, delays the next receiver acknowledgment, and waits for the new event's exact ID at the receiver and a `PENDING` sender attempt. Only then does it forcibly terminate its own child process.

The test inspects a database copy before restarting the app, asserts that the receipt and pending attempt survived, and then checks that startup reconciliation produces `INTERRUPTED`. Explicit retry must preserve the old unknown attempt and add a new `SUCCEEDED` attempt. The receiver resets on restart, so the same event performs another effect in the new process; this drill does not demonstrate durable receiver deduplication.

Evidence is retained in a unique directory under `target/crash-probe/`: runtime/configuration details, HTTP observations, a database snapshot from before restart, raw database observations, and both application logs. CI uploads this evidence alongside the Java test reports. Copy it elsewhere before running `clean` if you need to investigate a failure. The test never uses the dashboard's `data/receipts` database. Run one Maven build at a time in a checkout so another build cannot replace the child process's classes during the experiment.

This is a controlled process-termination check, not a power-loss or persistent-broker recovery test. The [earlier unexplained loss](results.md#what-a-harder-test-exposed) remains an open investigation even when this check passes.

## Replay an identical event

Choose **Successful delivery**, wait for `DELIVERED`, then choose **Replay last submitted event**.

The button preserves the ID, seller, message, and original occurrence timestamp. Expected behavior: one unchanged receipt, no extra HTTP attempt after the recorded success, and no extra effect at the controlled receiver. The broker can contain another copy while the consumer recognizes the already delivered event.

The replay button's payload exists only in the current browser page. Reloading clears it. For a recorded failed event, the server's **Retry this event** action reconstructs the original payload from storage.

Executable check: `DeliveryIntegrationTest#httpToKafkaToWebhookAndIdenticalReplayProducesOneReceiptAndEffect`.

## Understand conflicts and malformed records

The HTTP API validates shape and field constraints before publication. It does not perform synchronous receipt-identity checks.

Submitting a valid event with an existing ID but a different message, seller, or `occurredAt` can therefore return `202`. During consumption, the store rejects the conflict and keeps the first receipt unchanged. The conflicting Kafka record goes to the dead-letter topic without another HTTP delivery. The dashboard intentionally keeps the original event view; use the read-only API below to inspect the conflicting record.

Malformed broker JSON bypasses HTTP validation but is rejected by the consumer and retained in the dead-letter topic. It should not indefinitely prevent a following valid event from being delivered.

Executable checks:

- `DeliveryIntegrationTest#conflictingEventIdGoesToDeadLetterWithoutChangingOriginal`
- `DeliveryIntegrationTest#malformedBrokerMessageIsRetainedAndDoesNotBlockFollowingEvents`
- `NotificationApiTest#invalidEventAndMalformedJsonAreRejectedBeforePublishing`

## Inspect dead letters without advancing consumer offsets

**Question:** Which record failed, why was it retained, and can we inspect it without triggering another delivery?

After **Exhaust automatic attempts** reaches `FAILED`, copy the UUID from **Delivery detail** into **Dead-letter inspector** and choose **Inspect dead letters**. Leave the filter empty for all recent matches; the dashboard returns up to 20, default 10. Expand a record to see its payload and selected failure headers. This is a manual snapshot: inspect again after recovery to see current retained evidence.

For the same API from a terminal, replace `<event-id>` in this read-only request:

```sh
curl "http://127.0.0.1:8080/api/dead-letters?eventId=<event-id>&limit=20"
```

Windows PowerShell:

```powershell
Invoke-RestMethod 'http://127.0.0.1:8080/api/dead-letters?eventId=<event-id>&limit=20' | ConvertTo-Json -Depth 6
```

Omit `eventId` to list recent dead letters, including malformed records that never created a receipt. The filter matches the exact Kafka key produced by the lab, not text inside the payload. Results are newest first by dead-letter offset. Each item includes a bounded payload preview, exception class/message, and original topic/partition/offset when its headers are available and valid. A conflicting-ID record can therefore show a changed payload while the original receipt stays unchanged.

Repeat the request, then explicitly retry the failed event through the dashboard. Inspection does not modify delivery history; successful retry adds an attempt and leaves the original dead letter available until broker retention or restart removes it. The integration suite checks actual committed offsets and consumer-group inventory before and after repeated inspection.

| Response field or status | Meaning |
| --- | --- |
| `startOffset`, `endOffsetExclusive` | Snapshot window searched in partition 0 of `notification.requests.DLT` |
| `scannedRecords` | Records examined within that window; offset gaps are possible |
| `earlierRecordsNotScanned` | Older retained offsets fell outside the latest-100-offset window |
| `matchesTruncated` | More records matched within the window than the requested result limit |
| `keyTruncated`, `payloadTruncated` | The returned text is a preview, not the complete original value |
| `200` with empty `items` | No matching record in this window; not proof that the event never failed |
| `400` | Invalid UUID/type or `limit` outside 1–100 (default 20) |
| `429` | Another inspection is already running; retry later |
| `503` | The scan could not complete, or the topic is missing/has unsupported partitioning; do not interpret this as an empty topic |

The scan has a shared 3-second Kafka-operation budget plus up to 100 ms for closing its consumer. It supports this lab's single-partition topic only, and never joins a consumer group, commits offsets, creates topics, republishes records, or writes to H2. Kafka retention and the fresh embedded broker limit available history regardless of the window flags. Full-history search is not implemented.

The dashboard clears old results when the query changes or fails. Its five-second HTTP timeout and 256 KiB response cap bound waiting and buffered output; a large escaped preview can exceed that cap even within the item limit. Reduce the limit and inspect again. Error states do not establish an empty topic. Changing a query cancels the browser's HTTP request, while the Java scan remains governed by its own budget.

Executable checks: [`DeadLetterInspectorTest`](../src/test/java/dev/sahreb/delivery/DeadLetterInspectorTest.java) covers bounds, deadlines, filtering and malformed headers. [`DeliveryIntegrationTest`](../src/test/java/dev/sahreb/delivery/DeliveryIntegrationTest.java) checks actual HTTP failures, conflicts, malformed broker records, and unchanged offsets/groups/receipts.

## Simulate a stalled broker connection

**Question:** If a broker address accepts a connection but never responds, does inspection fail clearly and leave the next query usable?

```sh
./mvnw -Dtest=DeadLetterNetworkFailureTest test
```

Windows PowerShell:

```powershell
.\mvnw.cmd "-Dtest=DeadLetterNetworkFailureTest" test
```

[`DeadLetterNetworkFailureTest`](../src/test/java/dev/sahreb/delivery/DeadLetterNetworkFailureTest.java) uses the production inspector constructor and a real Kafka client. An owned loopback TCP peer accepts and reads client bytes but withholds all responses. The test checks the controller's `503` response, `429` for an overlapping inspection, and peer-observed connection closure. It then points the same inspector's dedicated connection configuration at a healthy embedded Kafka broker and requires a `200` response containing a seeded synthetic record.

The servlet boundary is exercised with MockMvc; the Kafka transport is real. A generous test time limit catches a hung query without treating the observation as a performance benchmark or an absolute HTTP deadline. This covers stalled negotiation/metadata, not every outage timing or DNS, TLS, filesystem, or JVM failure. No running dashboard, database, or external broker is touched.

Measured evidence is saved to `target/surefire-reports/dead-letter-network-stall.json` and included in CI test artifacts. Copy it before running `clean` if it is needed for investigation.

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
