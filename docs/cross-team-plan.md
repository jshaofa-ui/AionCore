# Controlled cross-team messaging implementation plan

Goal: implement the approved design in docs/cross-team-design.md without changing the running App.

1. Add failing API contract/authorization tests for the new lead-only tool and CLI path.
2. Add strict request/receipt types and default-disabled policy validation. Test owner, star topology, bounds and expiry.
3. Integrate native mailbox persistence and system-initiated target wake. Test durable idempotency, conflicting retries, rate limits, task-loop limits, and consumed receipts.
4. Expose through the existing authenticated runtime tool dispatch and CLI; retain old tool/session behavior. Log only correlation IDs/status.
5. Run affected crate tests, format and clippy; build the backend and run native CLI capability readback. Obtain independent employee review against original requirements, source and raw test output.
6. Prepare deployment/rollback artifacts. Stop before live App replacement or enabling new cross-team access; present the concrete result for final activation approval if required.

Execution: AionUi programming department prepares DeepSeek Flash developer and independent OpenCode reviewer. Codex handles source integration, difficult failures, desktop configuration and acceptance supervision. Do not duplicate active work; use isolated file ownership. Flash 402 stopped delegation until the user reported recharge; verify real recovery before claiming it usable.
