# Why this project exists

## A concrete question

What happens when a webhook receiver performs the work but the sender never receives the acknowledgment? A retry can repeat the side effect. A sender-side receipt alone cannot prove what happened at the receiver.

Event Delivery Lab is an independent, local experiment for answering that question. It deliberately creates ambiguous acknowledgments, temporary failures, duplicate events, and exhausted retries, then shows the attempts and resulting receiver-side effects. The useful output is a reproducible experiment with evidence, plus a small workbench for exploring recovery behavior.

## Personal connection and truthful origin

My engineering background includes Java, Python, Kafka, notification services, and integrations. This independent project investigates a failure mode relevant to that work using synthetic data. Its evidence comes from controlled experiments in this repository.

I built this lab to investigate how retries can create duplicate notifications, and to make the recovery behavior observable and testable. Every claimed result must link to a reproducible test or recorded experiment. Development is AI-assisted. This repository contains no employer implementation or data.

## Who would use it

- A backend engineer checking what an idempotency key actually protects.
- Someone learning or explaining the boundary between Kafka acknowledgment, local persistence, and remote HTTP side effects.
- A developer trying controlled failure drills before adapting the patterns to a local webhook receiver.

The first useful session should take minutes: run the app, trigger two temporary failures, inspect three attempts, exhaust retries, replay after recovery, and run the lost-acknowledgment comparison. The lab should work without accounts, an AI model, paid services, employer access, or Docker in its embedded-broker demo mode.

## Why build it when alternatives exist

[Hookdeck](https://hookdeck.com/docs/cli) already offers forwarding, inspection, and replay for local development. [Webhook.site](https://docs.webhook.site/custom-actions) offers programmable workflows, request/response inspection, and replay. They are good choices when those capabilities are the actual need.

This project has a narrower purpose: a readable Java/Kafka reference implementation and executable, locally controlled failure experiments. It does not claim a novel webhook platform or superiority over existing products. Kafka makes the queue/consumer boundary visible for the experiment; a simple webhook sender would not inherently need Kafka.

## Evidence that the problem is real

[Stripe's webhook guidance](https://docs.stripe.com/webhooks) documents retries and duplicate event handling. [AWS's discussion of idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/) explains the ambiguity after a timeout and how caller-provided request identifiers can make retries safer. These motivate the experiment; this lab does not implement Stripe, Shopify, or AWS protocol compatibility.

## Definition of done for the first complete release

1. One documented command starts a real embedded Kafka broker, Spring Boot service, local database, browser dashboard, and controlled HTTP receiver.
2. An HTTP submission is acknowledged only after Kafka accepts it. Consumption records a durable receipt and HTTP attempts. Same-ID/same-payload replays do not create another receipt; conflicting payloads preserve the original.
3. Temporary failures retry with a bounded policy. Exhausted records are retained in a Kafka dead-letter topic; the UI can inspect and explicitly retry failed valid events.
4. A reproducible lost-acknowledgment test demonstrates duplicate side effects in a deliberately naive receiver and one effect with receiver-side idempotency. This is an experiment, not a production exactly-once guarantee.
5. Automated tests exercise real Kafka/HTTP integration, data persistence, concurrent deduplication, conflicts, validation, retry/dead-letter behavior, and the lost-acknowledgment comparison. CI passes remotely.
6. The dashboard works with keyboard navigation and displays actual backend state; synthetic examples and limitations are visible.
7. The README leads with the problem, demonstrates the payoff, gives tested commands, and links to architecture, failure drills, actual results, and limits. No invented personal incident, usage, metrics, or adoption.
8. Reviewed source is committed and pushed to Sahreb-Aamir/event-delivery-lab. Repository visibility follows the portfolio publication review; no employer data or unrelated private code is included.

Read-only MCP diagnostics are a follow-on unless the core release is already complete and verified. Distributed scheduling, authentication, production-grade storage, real email/SMS, external tunnels, and multi-instance guarantees are out of scope for this release.

## Stop or rethink

If the lost-acknowledgment comparison cannot show the stated failure and fix, do not market that result. If the project becomes mostly setup with no fast, reproducible learning or debugging payoff, simplify it. Novelty and commit volume are not success criteria.
