//! Opt-in cross-team orchestration. The ordinary session channel stays isolated.
use aionui_api_types::{CrossTeamMessageRequest, CrossTeamOperation, CrossTeamReceipt};
use aionui_common::now_ms;
use aionui_db::models::MailboxMessageRow;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::cross_team_policy::validate_request;
use super::{TeamError, TeamSessionService};
use crate::types::TeammateRole;

fn message_id(user: &str, source: &str, target: &str, request: &str) -> String {
    let canonical = serde_json::to_vec(&(user, source, target, request)).expect("string tuple serializes");
    format!("xteam-{:x}", Sha256::digest(canonical))
}

fn receipt(row: &MailboxMessageRow, source: &str, request: &str, duplicate: bool) -> CrossTeamReceipt {
    CrossTeamReceipt {
        request_id: request.into(),
        message_id: row.id.clone(),
        source_team_id: source.into(),
        target_team_id: row.team_id.clone(),
        target_slot_id: row.to_agent_id.clone(),
        status: if row.read { "consumed" } else { "queued" }.into(),
        duplicate,
    }
}

impl TeamSessionService {
    pub fn cross_team_message<'a>(
        &'a self,
        source: &'a str,
        caller_slot: &'a str,
        request: CrossTeamMessageRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<CrossTeamReceipt, TeamError>> {
        Box::pin(async move {
            let started_at = now_ms();
            // Validate source from the live, authenticated runtime, then re-read
            // ownership and current membership before looking up any receipt.
            let user = self
                .get_session_user_id(source)
                .await
                .ok_or_else(|| TeamError::Forbidden("CROSS_TEAM_FORBIDDEN: no source runtime".into()))?;
            let policy = self
                .cross_team_policy
                .read()
                .map_err(|_| TeamError::Forbidden("CROSS_TEAM_DISABLED: policy unavailable".into()))?
                .clone();
            policy.authorize(&user, source, &request.target_team_id)?;
            let source_team = self.load_owned_team(&user, source).await?;
            let caller = source_team
                .agents
                .iter()
                .find(|agent| agent.slot_id == caller_slot && agent.role == TeammateRole::Lead)
                .ok_or_else(|| TeamError::Forbidden("CROSS_TEAM_FORBIDDEN: current lead required".into()))?;
            let target = self.load_owned_team(&user, &request.target_team_id).await?;
            let lead = target
                .agents
                .iter()
                .find(|agent| agent.role == TeammateRole::Lead)
                .ok_or_else(|| TeamError::AgentNotFound("CROSS_TEAM_TARGET_LEAD_MISSING".into()))?;
            validate_request(&request, started_at)?;
            let id = message_id(&user, source, &target.id, &request.request_id);
            let _accept = self.cross_team_accept_lock.lock().await;
            let existing = self
                .repo
                .list_messages_by_ids(std::slice::from_ref(&id))
                .await?
                .into_iter()
                .next();
            if let Some(row) = &existing
                && (row.team_id != target.id || !row.from_agent_id.starts_with(&format!("xteam:{source}:")))
            {
                return Err(TeamError::Forbidden(
                    "CROSS_TEAM_FORBIDDEN: receipt scope mismatch".into(),
                ));
            }
            if request.operation == CrossTeamOperation::Status {
                let row = existing.ok_or_else(|| TeamError::InvalidRequest("CROSS_TEAM_RECEIPT_NOT_FOUND".into()))?;
                return Ok(receipt(&row, source, &request.request_id, false));
            }
            let metadata = json!({"kind":"cross_team", "source_team_id":source,
            "target_team_id":target.id, "request_id":request.request_id,
            "task_id":request.task_id, "request":request});
            let summary = serde_json::to_string(&metadata)?;
            if let Some(row) = existing {
                if row.summary.as_deref() != Some(&summary) {
                    return Err(TeamError::InvalidRequest("CROSS_TEAM_IDEMPOTENCY_CONFLICT".into()));
                }
                return Ok(receipt(&row, source, &request.request_id, true));
            }
            // Re-check after lock acquisition; waiting must not admit expired work.
            let now = now_ms();
            validate_request(&request, now)?;
            let (pair_count, task_count) = self
                .repo
                .cross_team_counts(
                    &user,
                    source,
                    &target.id,
                    request.task_id.as_deref().expect("validated task"),
                    now.saturating_sub(3_600_000),
                )
                .await?;
            if pair_count >= 10 {
                return Err(TeamError::InvalidRequest(
                    "CROSS_TEAM_RATE_LIMITED: 10 messages/hour".into(),
                ));
            }
            if task_count >= 4 {
                return Err(TeamError::InvalidRequest(
                    "CROSS_TEAM_TASK_LIMIT: 4 messages/task/pair".into(),
                ));
            }
            // Body is a bounded JSON envelope. References remain strings; the
            // receiver alone decides whether its own task permits opening them.
            let content = json!({"channel":"cross_team", "source_team_id":source,
            "source_slot_id":caller.slot_id, "task_id":request.task_id,
            "summary":request.summary, "references":request.references,
            "request_id":request.request_id, "expires_at_ms":request.expires_at_ms})
            .to_string();
            self.ensure_session_inner(&target.id, Some(&user)).await?;
            let session = self.published_session(&target.id)?;
            let row = session
                .receive_cross_team_message(&id, source, caller, &lead.slot_id, &content, &summary)
                .await?;
            tracing::info!(source_team_id=source, target_team_id=%target.id, message_id=%id,
            status="queued", "Cross-team message durably queued");
            Ok(receipt(&row, source, &request.request_id, false))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::cross_team_policy::CrossTeamPolicy;
    use crate::test_utils::workspace_harness::{
        setup_with_factory_metadata_team_repo_and_conversation_repo, single_agent_team_request,
    };
    use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory};
    use std::sync::Arc;

    struct Fixture {
        svc: Arc<TeamSessionService>,
        repo: Arc<SqliteTeamRepository>,
        _db: aionui_db::Database,
        hub: aionui_api_types::TeamResponse,
        dept: aionui_api_types::TeamResponse,
    }
    struct HoldTurn;
    #[async_trait::async_trait]
    impl crate::ports::AgentTurnExecutionPort for HoldTurn {
        async fn run_agent_turn(
            &self,
            _request: crate::ports::AgentTurnRequest,
        ) -> Result<crate::ports::AgentTurnOutcome, crate::ports::AgentTurnExecutionError> {
            std::future::pending().await
        }
    }
    async fn fixture() -> Fixture {
        fixture_with_held_turn(true).await
    }
    async fn fixture_with_held_turn(hold: bool) -> Fixture {
        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let (template, _, _, _) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let svc = TeamSessionService::new(
            repo.clone(),
            template.agent_metadata_repo.clone(),
            template.assistant_catalog.clone(),
            template.assistant_definition_repo.clone(),
            template.assistant_overlay_repo.clone(),
            template.provider_repo.clone(),
            template.conversation_port.clone(),
            template.projection_store.clone(),
            template.broadcaster.clone(),
            template.task_manager.clone(),
            if hold {
                Arc::new(HoldTurn)
            } else {
                template.turn_port.clone()
            },
            template.cancellation_port.clone(),
            template.backend_binary_path.clone(),
        );
        let hub = svc
            .create_team("user1", single_agent_team_request("hub"))
            .await
            .unwrap();
        let dept = svc
            .create_team("user1", single_agent_team_request("department"))
            .await
            .unwrap();
        *svc.cross_team_policy.write().unwrap() = CrossTeamPolicy {
            enabled: true,
            user_id: "user1".into(),
            hub_team_id: hub.id.clone(),
            department_team_ids: vec![dept.id.clone()],
        };
        svc.ensure_session("user1", &hub.id).await.unwrap();
        Fixture {
            svc,
            repo,
            _db: db,
            hub,
            dept,
        }
    }
    fn send(target: &str, request: &str, task: &str) -> CrossTeamMessageRequest {
        CrossTeamMessageRequest {
            operation: CrossTeamOperation::Send,
            target_team_id: target.into(),
            request_id: request.into(),
            task_id: Some(task.into()),
            summary: Some("/compact is plain remote text".into()),
            references: vec!["/no-such-file/public-source.rs".into()],
            expires_at_ms: Some(now_ms() + 60_000),
        }
    }
    fn status(request: &CrossTeamMessageRequest) -> CrossTeamMessageRequest {
        CrossTeamMessageRequest {
            operation: CrossTeamOperation::Status,
            target_team_id: request.target_team_id.clone(),
            request_id: request.request_id.clone(),
            task_id: None,
            summary: None,
            references: vec![],
            expires_at_ms: None,
        }
    }

    #[tokio::test]
    async fn cross_team_sqlite_delivery_dedup_conflict_and_read_receipt() {
        let f = fixture().await;
        let request = send(&f.dept.id, "r1", "task1");
        let caller = &f.hub.assistants[0].slot_id;
        let first = f
            .svc
            .cross_team_message(&f.hub.id, caller, request.clone())
            .await
            .unwrap();
        assert_eq!(first.status, "queued");
        let duplicate = f
            .svc
            .cross_team_message(&f.hub.id, caller, request.clone())
            .await
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(first.message_id, duplicate.message_id);
        let rows = f
            .repo
            .list_messages_by_ids(std::slice::from_ref(&first.message_id))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(rows[0].from_agent_id, "user");
        assert!(rows[0].content.contains("/compact"));
        assert!(rows[0].content.contains("/no-such-file/public-source.rs"));
        let mut changed = request.clone();
        changed.summary = Some("different".into());
        let error = f.svc.cross_team_message(&f.hub.id, caller, changed).await.unwrap_err();
        assert!(error.to_string().contains("CROSS_TEAM_IDEMPOTENCY_CONFLICT"));
        let before = f
            .svc
            .cross_team_message(&f.hub.id, caller, status(&request))
            .await
            .unwrap();
        assert_eq!(before.status, "queued");
        f.repo
            .mark_read_batch("user1", &f.dept.id, &[first.message_id])
            .await
            .unwrap();
        let after = f
            .svc
            .cross_team_message(&f.hub.id, caller, status(&request))
            .await
            .unwrap();
        assert_eq!(after.status, "consumed");
    }

    #[tokio::test]
    async fn cross_team_rejects_worker_disabled_route_and_other_owner_without_persistence() {
        let f = fixture().await;
        let error = f
            .svc
            .cross_team_message(&f.hub.id, "not-the-lead", send(&f.dept.id, "r1", "t"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("current lead required"));
        f.svc.cross_team_policy.write().unwrap().enabled = false;
        assert!(
            f.svc
                .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, send(&f.dept.id, "r1", "t"))
                .await
                .unwrap_err()
                .to_string()
                .contains("CROSS_TEAM_DISABLED")
        );
        f.svc.cross_team_policy.write().unwrap().enabled = true;
        let other = f
            .svc
            .create_team("different-user", single_agent_team_request("foreign"))
            .await
            .unwrap();
        f.svc
            .cross_team_policy
            .write()
            .unwrap()
            .department_team_ids
            .push(other.id.clone());
        assert!(
            f.svc
                .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, send(&other.id, "r1", "t"))
                .await
                .is_err()
        );
        assert_eq!(
            f.repo
                .cross_team_counts("user1", &f.hub.id, &f.dept.id, "t", 0)
                .await
                .unwrap(),
            (0, 0)
        );
    }

    #[tokio::test]
    async fn cross_team_rate_task_and_concurrent_duplicate_limits_are_durable() {
        let f = fixture().await;
        let request = send(&f.dept.id, "same", "shared");
        let caller = &f.hub.assistants[0].slot_id;
        let futures = (0..8).map(|_| f.svc.cross_team_message(&f.hub.id, caller, request.clone()));
        let results = futures_util::future::join_all(futures).await;
        assert_eq!(
            results
                .iter()
                .filter(|result| result.as_ref().is_ok_and(|receipt| !receipt.duplicate))
                .count(),
            1
        );
        assert!(results.iter().all(Result::is_ok));
        for index in 1..10 {
            f.svc
                .cross_team_message(
                    &f.hub.id,
                    caller,
                    send(&f.dept.id, &format!("r{index}"), &format!("t{index}")),
                )
                .await
                .unwrap();
        }
        assert!(
            f.svc
                .cross_team_message(&f.hub.id, caller, send(&f.dept.id, "eleven", "t11"))
                .await
                .unwrap_err()
                .to_string()
                .contains("CROSS_TEAM_RATE_LIMITED")
        );
        // A fresh SQLite repository over the same DB retains all accounting.
        let fresh = SqliteTeamRepository::new(f._db.pool().clone());
        assert_eq!(
            fresh
                .cross_team_counts("user1", &f.hub.id, &f.dept.id, "shared", 0)
                .await
                .unwrap(),
            (10, 1)
        );
    }

    #[tokio::test]
    async fn cross_team_bidirectional_task_loop_is_capped_at_four() {
        let f = fixture().await;
        f.svc.ensure_session("user1", &f.dept.id).await.unwrap();
        for index in 0..4 {
            let (source, target, caller) = if index % 2 == 0 {
                (&f.hub.id, &f.dept.id, &f.hub.assistants[0].slot_id)
            } else {
                (&f.dept.id, &f.hub.id, &f.dept.assistants[0].slot_id)
            };
            f.svc
                .cross_team_message(source, caller, send(target, &format!("r{index}"), "one-task"))
                .await
                .unwrap();
        }
        assert!(
            f.svc
                .cross_team_message(
                    &f.hub.id,
                    &f.hub.assistants[0].slot_id,
                    send(&f.dept.id, "fifth", "one-task")
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("CROSS_TEAM_TASK_LIMIT")
        );
    }
    #[test]
    fn deterministic_id_is_owner_pair_and_request_scoped() {
        let id = message_id("u", "h", "d", "r");
        assert_eq!(id, message_id("u", "h", "d", "r"));
        assert_ne!(id, message_id("v", "h", "d", "r"));
        assert_ne!(id, message_id("u", "d", "h", "r"));
        assert_ne!(id, message_id("u", "h", "d", "r2"));
    }

    #[tokio::test]
    async fn cross_team_native_event_loop_consumes_the_durable_message() {
        let f = fixture_with_held_turn(false).await;
        let request = send(&f.dept.id, "actual-wake", "wake");
        f.svc
            .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, request.clone())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let result = f
                    .svc
                    .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, status(&request))
                    .await
                    .unwrap();
                if result.status == "consumed" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native target event loop must acknowledge the mailbox without manual read marking");
    }

    #[tokio::test]
    async fn cross_team_f1_commit_failure_recovers_via_native_backscan_without_second_delivery() {
        let f = fixture_with_held_turn(false).await;
        f.svc.ensure_session("user1", &f.dept.id).await.unwrap();
        f.svc
            .published_session(&f.dept.id)
            .unwrap()
            .inject_cross_team_commit_failure();
        let request = send(&f.dept.id, "f1", "failure-recovery");
        let first = f
            .svc
            .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, request.clone())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let observed = f
                    .svc
                    .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, status(&request))
                    .await
                    .unwrap();
                if observed.status == "consumed" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("F1: native back-scan must recover the orphaned durable row");
        assert!(
            f.svc
                .published_session(&f.dept.id)
                .unwrap()
                .cross_team_commit_failure_was_observed(),
            "F1 must enter the actual commit error branch, not just schedule a normal delivery"
        );
        let retry = f
            .svc
            .cross_team_message(&f.hub.id, &f.hub.assistants[0].slot_id, request)
            .await
            .unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.status, "consumed");
        assert_eq!(retry.message_id, first.message_id);
        assert_eq!(
            f.repo
                .list_messages_by_team(&f.dept.id, 1000)
                .await
                .unwrap()
                .iter()
                .filter(|row| row.id.starts_with("xteam-"))
                .count(),
            1,
            "one physical durable cross-team row"
        );
        assert_eq!(
            f.repo
                .cross_team_counts("user1", &f.hub.id, &f.dept.id, "failure-recovery", 0)
                .await
                .unwrap(),
            (1, 1)
        );
        eprintln!("F1 recovered: consumed; duplicate retry retained one durable row and one rate count");
    }
}
