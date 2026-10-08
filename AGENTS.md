# Working on Event Delivery Lab

Read `docs/project-brief.md` first. Build a local failure-experiment workbench that demonstrates lost acknowledgments, retries, and receiver-side idempotency. The complete first release includes HTTP delivery to a controlled receiver, a dashboard, and executable failure drills. MCP diagnostics are follow-on work.

- Use only synthetic fixtures. Never copy employer code, data, or private project integrations.
- Preserve honest guarantees: Kafka delivery may repeat; a database receipt is not evidence that an external webhook succeeded.
- Every reliability change needs a meaningful failure/replay test. Run `./mvnw verify` (Windows: `mvnw.cmd verify`).
- Keep the default HTTP listener on loopback. The lab has no authentication and must not be exposed publicly.
- Keep timestamps real and document measured results only. No commit-count targets.
- Use one focused commit per coherent change; do not commit local databases, logs, credentials, or build output.
- Keep this repository private until the portfolio plan's publication review is complete.
