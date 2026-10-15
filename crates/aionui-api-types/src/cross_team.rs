//! Compact, identity-free requests for opt-in cross-team collaboration.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossTeamOperation {
    Send,
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossTeamMessageRequest {
    pub operation: CrossTeamOperation,
    pub target_team_id: String,
    pub request_id: String,
    pub task_id: Option<String>,
    pub summary: Option<String>,
    #[serde(default)]
    pub references: Vec<String>,
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossTeamReceipt {
    pub request_id: String,
    pub message_id: String,
    pub source_team_id: String,
    pub target_team_id: String,
    pub target_slot_id: String,
    /// queued/consumed are transport receipts, never business-task approval.
    pub status: String,
    pub duplicate: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn callers_cannot_override_authenticated_identity() {
        for field in [
            "user_id",
            "source_team_id",
            "from_slot_id",
            "to_slot_id",
            "history",
            "files",
        ] {
            let mut request = json!({"operation":"status", "target_team_id":"department", "request_id":"r1"});
            request[field] = json!("forged");
            assert!(
                serde_json::from_value::<CrossTeamMessageRequest>(request).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn unknown_operations_are_rejected() {
        assert!(
            serde_json::from_value::<CrossTeamMessageRequest>(json!({
                "operation":"broadcast", "target_team_id":"*", "request_id":"r1"
            }))
            .is_err()
        );
    }

    #[test]
    fn status_does_not_require_a_task_payload() {
        let request: CrossTeamMessageRequest = serde_json::from_value(json!({
            "operation":"status", "target_team_id":"department", "request_id":"r1"
        }))
        .unwrap();
        assert_eq!(request.operation, CrossTeamOperation::Status);
        assert!(request.summary.is_none());
        assert!(request.references.is_empty());
    }
}
