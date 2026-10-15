use crate::error::TeamError;
use aionui_api_types::{CrossTeamMessageRequest, CrossTeamOperation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CrossTeamPolicy {
    pub(crate) enabled: bool,
    pub(crate) user_id: String,
    pub(crate) hub_team_id: String,
    pub(crate) department_team_ids: Vec<String>,
}

fn invalid(message: &str) -> TeamError {
    TeamError::InvalidRequest(format!("CROSS_TEAM_INVALID: {message}"))
}
fn identifier(value: &str) -> bool {
    !value.trim().is_empty() && value.chars().count() <= 128 && !value.chars().any(char::is_control)
}
impl CrossTeamPolicy {
    pub(crate) fn from_env() -> Self {
        let Some(path) = std::env::var_os("AIONUI_CROSS_TEAM_POLICY_FILE") else {
            return Self::default();
        };
        match std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok())
        {
            Some(policy) => policy,
            None => {
                tracing::warn!("Cross-team startup policy unavailable; channel disabled");
                Self::default()
            }
        }
    }
    pub(crate) fn authorize(&self, user: &str, source: &str, target: &str) -> Result<(), TeamError> {
        if !self.enabled {
            return Err(TeamError::Forbidden("CROSS_TEAM_DISABLED".into()));
        }
        let valid = self.user_id == user
            && identifier(&self.user_id)
            && identifier(&self.hub_team_id)
            && !self.department_team_ids.is_empty()
            && self.department_team_ids.len() <= 64
            && self
                .department_team_ids
                .iter()
                .all(|id| identifier(id) && id != &self.hub_team_id)
            && source != target
            && ((source == self.hub_team_id && self.department_team_ids.iter().any(|id| id == target))
                || (target == self.hub_team_id && self.department_team_ids.iter().any(|id| id == source)));
        if !valid {
            return Err(TeamError::Forbidden(
                "CROSS_TEAM_FORBIDDEN: route or owner not allowed".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_request(request: &CrossTeamMessageRequest, now: i64) -> Result<(), TeamError> {
    if !identifier(&request.target_team_id) || !identifier(&request.request_id) {
        return Err(invalid("invalid team/request identifier"));
    }
    if request.operation == CrossTeamOperation::Status {
        if request.task_id.is_some()
            || request.summary.is_some()
            || !request.references.is_empty()
            || request.expires_at_ms.is_some()
        {
            return Err(invalid("status must not contain send payload"));
        }
        return Ok(());
    }
    if !request.task_id.as_deref().is_some_and(identifier) {
        return Err(invalid("invalid task identifier"));
    }
    let summary = request.summary.as_deref().ok_or_else(|| invalid("summary required"))?;
    if summary.trim().is_empty() || summary.chars().count() > 1200 || summary.contains('\0') {
        return Err(invalid("summary must be 1..1200 characters"));
    }
    if request.references.len() > 8
        || request.references.iter().any(|reference| {
            reference.chars().count() > 1024
                || reference.chars().any(char::is_control)
                || !std::path::Path::new(reference).is_absolute()
        })
    {
        return Err(invalid(
            "references must be up to 8 absolute paths, each at most 1024 characters",
        ));
    }
    if !request
        .expires_at_ms
        .is_some_and(|expiry| expiry > now && expiry <= now.saturating_add(3_600_000))
    {
        return Err(invalid("expiry must be in the next hour"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy() -> CrossTeamPolicy {
        CrossTeamPolicy {
            enabled: true,
            user_id: "u".into(),
            hub_team_id: "h".into(),
            department_team_ids: vec!["d".into(), "e".into()],
        }
    }
    fn request() -> CrossTeamMessageRequest {
        CrossTeamMessageRequest {
            operation: CrossTeamOperation::Send,
            target_team_id: "d".into(),
            request_id: "r".into(),
            task_id: Some("t".into()),
            summary: Some("summary".into()),
            references: vec!["/nonexistent/source.rs".into()],
            expires_at_ms: Some(2000),
        }
    }
    #[test]
    fn disabled_owner_and_star_topology() {
        assert!(
            CrossTeamPolicy::default()
                .authorize("u", "h", "d")
                .unwrap_err()
                .to_string()
                .contains("DISABLED")
        );
        let p = policy();
        assert!(p.authorize("u", "h", "d").is_ok());
        assert!(p.authorize("u", "d", "h").is_ok());
        for (u, s, t) in [("v", "h", "d"), ("u", "d", "e"), ("u", "h", "h"), ("u", "h", "unknown")] {
            assert!(p.authorize(u, s, t).unwrap_err().to_string().contains("FORBIDDEN"));
        }
    }
    #[test]
    fn unicode_bounds_and_references_are_validated_without_opening_files() {
        let mut r = request();
        r.summary = Some("字".repeat(1200));
        assert!(validate_request(&r, 1000).is_ok());
        r.summary = Some("字".repeat(1201));
        assert!(validate_request(&r, 1000).unwrap_err().to_string().contains("1200"));
        r = request();
        r.references = vec!["/source".into(); 9];
        assert!(validate_request(&r, 1000).is_err());
        r = request();
        r.references = vec!["relative".into()];
        assert!(validate_request(&r, 1000).is_err());
        r = request();
        r.request_id = "r".repeat(129);
        assert!(validate_request(&r, 1000).is_err());
    }
    #[test]
    fn expiry_edges_and_status_payload_are_strict() {
        for expiry in [999, 1000, 3_601_001] {
            let mut r = request();
            r.expires_at_ms = Some(expiry);
            assert!(validate_request(&r, 1000).is_err());
        }
        let mut r = request();
        r.expires_at_ms = Some(3_601_000);
        assert!(validate_request(&r, 1000).is_ok());
        r.operation = CrossTeamOperation::Status;
        assert!(validate_request(&r, 1000).is_err());
        r.task_id = None;
        r.summary = None;
        r.references.clear();
        r.expires_at_ms = None;
        assert!(validate_request(&r, 1000).is_ok());
    }
}
