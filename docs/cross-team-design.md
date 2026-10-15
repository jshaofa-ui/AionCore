# Controlled cross-team messaging

Task: XTEAM-20261015-001. User approved keeping separate departments, deterministic routing, deduplication, receipts, rate limits, and compact summaries/references on 2026-10-15.

## Scope

Add a separate lead-only native team tool and CLI command. Preserve existing intra-team messaging and the ordinary session tool's team exclusions. Identity comes from the existing authenticated team runtime context, never request-supplied user/slot identities. Revalidate both teams against the same owner and their current lead roster.

An operator-supplied startup policy explicitly identifies the owner, hub team and allowed department teams. Default disabled, no inferred routing by display name, no worker-to-worker or department-to-department delivery. This first release exposes activation through the server's startup policy file, not a settings UI. The live App will not be replaced during development.

## Wire contract

`team_cross_team_message` / `aioncore team cross-team-message`: operation `send` or `status`, target_team_id, request_id; send additionally requires task_id, summary (1..1200 Unicode characters), references (up to 8 absolute local paths), expiry timestamp (at most one hour ahead). The server serializes this compact envelope without opening or uploading referenced files. No implicit conversation-history access.

## Delivery and receipts

Store an idempotent native mailbox row addressed to the destination's current lead, with the real qualified source identity. Wake only that lead using a system-initiated directed intent, preserving pauses and bypassing human slash-command parsing. Never impersonate a user message. Return queued only after durable persistence; receipt `consumed` means mailbox acknowledgement, not business completion. Status checks do not wake either model. No automatic acknowledgement message/ack-of-ack.

Use a deterministic mailbox ID derived from owner/source/destination/request ID and reject reuse with changed content. Preserve deduplication and rate accounting across restarts using stored mailbox records. Initial limit: 10 messages/hour per ordered pair; 4 messages/task across both directions to bound feedback loops. Serialize cross-team acceptance within one backend instance. Existing single backend instance lock is assumed; distributed multi-writer operation is out of scope.

## Observability and verification

Use existing native mailbox/activity UI for payload and read receipts; structured info logs carry IDs/status only. Never log summary, references, credentials or file contents. Validate defaults, authorization, route topology, input bounds, duplicate/conflicting IDs, concurrency, expiry, rate/loop bounds, destination failure, durable receipts and no slash-command execution. In-memory SQLite integration tests exercise real repository behavior; synthetic test agents prove scheduling separately from real model invocation. Independent review and real App acceptance remain separate gates.
