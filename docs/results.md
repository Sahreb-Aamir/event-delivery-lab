# Recorded experiments

These are controlled correctness experiments, not performance benchmarks or production usage metrics. All payloads are synthetic.

## Environment and commands

Local verification: October 7–8, 2026, Windows, Temurin JDK 17.0.19+10, Maven wrapper 3.9.16, Spring Boot 4.1.1, H2 2.5.250. Dashboard state tests used Node 24.14.1. The application itself does not require Node.

```powershell
.\mvnw.cmd verify
node --test src/test/js/delivery-tracker.test.cjs
.\mvnw.cmd spring-boot:test-run@local-lab
```

The Java suite passed 50 tests with zero failures, errors, or skipped tests. The six dashboard state tests passed. The [workflow](../.github/workflows/verify.yml) also checks Java 17 and 21, plus the dashboard state tests on Node 22.

## Lost acknowledgment: isolate the cause

[`lostAcknowledgmentRepeatsSideEffectUnlessReceiverDeduplicates`](../src/test/java/dev/sahreb/delivery/NotificationProcessorTest.java) runs against a real local HTTP server. Its first request performs the action, then withholds the acknowledgment until the sender times out. The processor retries the exact event.

| Receiver behavior | HTTP requests | Receiver side effects | Stored receipts |
| --- | ---: | ---: | ---: |
| Deliberately naive | 2 | 2 | 1 |
| Honors the idempotency key | 2 | 1 | 1 |

Both cases record `NETWORK_ERROR`, then `SUCCEEDED`. This demonstrates why one sender-side receipt is insufficient to prevent duplicate remote effects. The test invokes the processor retry directly to isolate this boundary; the integration suite separately exercises Kafka's retry policy.

## Browser experiment

The documented local command started the real embedded broker and dashboard. The **Lose the acknowledgment** button produced the following observations for synthetic event `f49aa442-888a-418c-843a-12712d187f05`:

- Two stored attempts: a timeout followed by HTTP 200 with `duplicate: true`.
- Two receiver requests, one receiver-side record matched to that event ID.
- Exact replay after success retained two attempts and one receiver effect.

![The running dashboard showing two requests and one receiver-side effect](dashboard.png)

The **Exhaust automatic attempts** drill produced three HTTP 503 attempts and `FAILED`. **Retry this event** then produced a fourth, successful HTTP attempt while preserving the previous three. The integration suite independently verifies that exhaustion retains the Kafka record in the dead-letter topic.

Keyboard activation of the failure drill worked. The layout was checked at the desktop viewport and a 390-pixel narrow viewport, with no horizontal page overflow. No browser console errors were observed during these checks. This is a manual usability check, not a comprehensive accessibility audit.

## What a harder test exposed

An early manual force-stop of the full application retained the receipt but lost its most recent pending-attempt row, even though the API had shown that row before termination. Graceful close/reopen tests had passed. Smaller isolated process-kill experiments did not reproduce the loss, so its precise cause remains unresolved.

The lab now explicitly asks H2 to `CHECKPOINT SYNC` after local commits, before proceeding to HTTP delivery or acknowledging successful processing. This adds a file-synchronization boundary; it is not a demonstrated diagnosis or complete fix for the earlier loss. A repeat against the earlier development database also lost a pending row after this change.

[`H2CrashDurabilityTest`](../src/test/java/dev/sahreb/delivery/H2CrashDurabilityTest.java) starts a separate writer JVM with six existing delivered events, records a new pending attempt, then forcibly terminates the writer while its connection pool remains open. Both tested configurations (`WRITE_DELAY=0` and `60000`) retained all seven receipts and the pending attempt. Startup reconciliation then marked the unfinished event `INTERRUPTED` without changing its unknown `PENDING` outcome. Separate failure-injection tests verify that retries cannot bypass a failed synchronization step.

Two consecutive full-application cycles using the documented Maven launcher and a fresh database then passed: the pending attempt survived forced termination, startup marked it `INTERRUPTED`, and explicit retry retained the old unknown attempt alongside a new successful one. Two additional cycles against a copy of the earlier database also passed. Every drill waited for its newly generated event ID to appear at the receiver before termination; an aggregate counter was not used as that signal.

The discrepancy remains unresolved. The passing experiments cover process termination under their tested conditions, not every crash timing, power failure, storage controller, or filesystem fault. Do not depend on this experimental lab as the sole audit record for real notifications. Investigating the earlier loss is follow-up work; the lost-acknowledgment comparison does not depend on a crash-safety claim.

The local broker lifecycle test also verifies real Kafka connectivity, loopback-only socket binding, released ports, and temporary-storage cleanup. The full launcher was checked at runtime: HTTP, broker, and controller listeners all used `127.0.0.1`.

## Interpretation

The controlled receiver's deduplication map is in memory. A restart resets it. HTTP success confirms a response, not downstream business completion. The embedded broker also starts fresh on every run. See the [architecture and crash windows](architecture.md) and [failure drills](failure-drills.md) for the limits of each observation.
