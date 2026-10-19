//! Refresh a Team conversation's assistant snapshot without replacing its identity.
use super::*;
use aionui_api_types::TeamAssistantConfiguration;
use sha2::{Digest, Sha256};

impl ConversationService {
    async fn team_assistant_sync_plan(
        &self,
        user_id: &str,
        conversation_id: &str,
        assistant_id: &str,
    ) -> Result<Option<(ConversationRow, AssistantSnapshot, String)>, ConversationError> {
        let row = self
            .conversation_repo
            .get(user_id, conversation_id)
            .await?
            .ok_or_else(|| ConversationError::NotFound {
                id: conversation_id.to_owned(),
            })?;
        let mut extra: serde_json::Value = serde_json::from_str(&row.extra)
            .map_err(|e| ConversationError::internal(format!("Invalid conversation configuration: {e}")))?;
        if TeamSessionBinding::team_id_marker_from_extra_str(&row.extra).is_none() {
            return Err(ConversationError::BadRequest {
                reason: "Assistant synchronization requires a Team conversation".into(),
            });
        }
        let previous = self
            .conversation_repo
            .get_assistant_snapshot(user_id, conversation_id)
            .await?;
        if previous.as_ref().is_some_and(|s| s.assistant_id != assistant_id) {
            return Err(ConversationError::BadRequest {
                reason: "Assistant does not match Team conversation".into(),
            });
        }
        // An explicitly cleared rule must not fall back to the old frozen rule.
        if let Some(obj) = extra.as_object_mut() {
            obj.remove("preset_context");
            obj.remove("preset_rules");
        }
        let Some(mut snapshot) = self
            .resolve_assistant_snapshot(
                user_id,
                assistant_id,
                extra.get("assistant_locale").and_then(|v| v.as_str()),
                &AssistantConversationOverrides::default(),
                &extra,
            )
            .await?
        else {
            return Ok(None);
        };
        // Auto fields preserve this member's explicit choices. Fixed template
        // fields deliberately replace them. Never carry a model into another CLI.
        if let Some(old) = previous.as_ref().filter(|old| old.agent_id == snapshot.agent_id) {
            if snapshot.default_modes.model == "auto" {
                snapshot.resolved_defaults.model = old.resolved_model_id.clone().or_else(|| {
                    extra
                        .get("current_model_id")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned)
                });
            }
            if snapshot.default_modes.permission == "auto" {
                snapshot.resolved_defaults.permission = old
                    .resolved_permission_value
                    .clone()
                    .or_else(|| extra.get("session_mode").and_then(|v| v.as_str()).map(str::to_owned));
            }
            if snapshot.default_modes.thought_level == "auto" {
                snapshot.resolved_defaults.thought_level = old.resolved_thought_level_value.clone();
            }
        }
        snapshot.created_at = 0;
        let mut runtime_identity =
            serde_json::to_value(&snapshot).map_err(|e| ConversationError::internal(e.to_string()))?;
        if let Some(obj) = runtime_identity.as_object_mut() {
            for key in ["created_at", "name", "avatar", "avatar_type"] {
                obj.remove(key);
            }
        }
        let bytes = serde_json::to_vec(&runtime_identity)
            .map_err(|e| ConversationError::internal(format!("Cannot fingerprint assistant configuration: {e}")))?;
        let fingerprint = format!("{:x}", Sha256::digest(bytes));
        Ok(Some((row, snapshot, fingerprint)))
    }

    /// Read-only planning used while a member is busy. No running snapshot changes.
    pub async fn team_assistant_configuration_fingerprint(
        &self,
        user_id: &str,
        conversation_id: &str,
        assistant_id: &str,
    ) -> Result<Option<String>, ConversationError> {
        Ok(self
            .team_assistant_sync_plan(user_id, conversation_id, assistant_id)
            .await?
            .map(|(_, _, fingerprint)| fingerprint))
    }

    /// Called only while the Team attach/restart lease excludes new work.
    pub async fn synchronize_team_assistant_configuration(
        &self,
        user_id: &str,
        conversation_id: &str,
        assistant_id: &str,
    ) -> Result<Option<TeamAssistantConfiguration>, ConversationError> {
        let Some((row, snapshot, fingerprint)) = self
            .team_assistant_sync_plan(user_id, conversation_id, assistant_id)
            .await?
        else {
            return Ok(None);
        };
        let result = TeamAssistantConfiguration {
            fingerprint: fingerprint.clone(),
            name: snapshot.name.clone(),
            backend: snapshot.runtime_backend.clone(),
            model: snapshot.resolved_defaults.model.clone(),
        };
        let mut extra: serde_json::Value = serde_json::from_str(&row.extra)
            .map_err(|e| ConversationError::internal(format!("Invalid conversation configuration: {e}")))?;
        if extra
            .get("assistant_configuration_fingerprint")
            .and_then(|v| v.as_str())
            == Some(&fingerprint)
        {
            return Ok(Some(result));
        }
        let backend_changed = extra.get("backend").and_then(|v| v.as_str()) != Some(snapshot.runtime_backend.as_str())
            || row.r#type != snapshot.agent_type.serde_name();
        let defaults = &snapshot.resolved_defaults;
        let previous = self
            .conversation_repo
            .get_assistant_snapshot(user_id, conversation_id)
            .await?;
        let reset_context = backend_changed
            || previous.as_ref().is_some_and(|old| {
                old.rules_content != snapshot.rules.content
                    || serde_json::from_str::<Vec<String>>(&old.resolved_skill_ids).unwrap_or_default()
                        != defaults.skill_ids
                    || serde_json::from_str::<Vec<String>>(&old.resolved_disabled_builtin_skill_ids).unwrap_or_default()
                        != defaults.disabled_builtin_skill_ids
            });
        let model_json = if snapshot.agent_type == AgentType::Aionrs {
            match defaults.model.as_deref() {
                Some(model) => {
                    let providers = self
                        .provider_repo()
                        .ok_or_else(|| ConversationError::internal("Provider repository unavailable"))?
                        .list(user_id)
                        .await?;
                    let old_provider = row
                        .model
                        .as_deref()
                        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                        .and_then(|v| v.get("provider_id").and_then(|p| p.as_str()).map(str::to_owned));
                    let candidates: Vec<_> = providers
                        .iter()
                        .filter(|p| {
                            p.enabled
                                && serde_json::from_str::<Vec<String>>(&p.models)
                                    .unwrap_or_default()
                                    .iter()
                                    .any(|m| m == model)
                        })
                        .collect();
                    let provider = candidates
                        .iter()
                        .find(|p| Some(p.id.as_str()) == old_provider.as_deref())
                        .copied()
                        .or_else(|| (candidates.len() == 1).then(|| candidates[0]))
                        .ok_or_else(|| ConversationError::BadRequest {
                            reason: "Assistant model needs an unambiguous enabled provider".into(),
                        })?;
                    Some(serde_json::json!({"provider_id":provider.id,"model":model,"use_model":model}).to_string())
                }
                None if !backend_changed => row.model.clone(),
                None => {
                    return Err(ConversationError::BadRequest {
                        reason: "Switching to Aion requires a configured model".into(),
                    });
                }
            }
        } else {
            None
        };
        let skills = compute_initial_skills(
            &self.skill_resolver.auto_inject_names().await,
            &defaults.skill_ids,
            &defaults.disabled_builtin_skill_ids,
        );
        let obj = extra
            .as_object_mut()
            .ok_or_else(|| ConversationError::internal("Invalid conversation configuration"))?;
        obj.insert("backend".into(), snapshot.runtime_backend.clone().into());
        obj.insert("agent_id".into(), snapshot.agent_id.clone().into());
        obj.insert("agent_source".into(), snapshot.agent_source.clone().into());
        obj.insert("skills".into(), serde_json::json!(skills));
        obj.insert("preset_context".into(), serde_json::Value::Null);
        obj.insert("preset_rules".into(), serde_json::Value::Null);
        let rules_key = if snapshot.agent_type == AgentType::Aionrs {
            "preset_rules"
        } else {
            "preset_context"
        };
        obj.insert(rules_key.into(), snapshot.rules.content.clone().into());
        for (key, value) in [
            ("current_model_id", &defaults.model),
            ("session_mode", &defaults.permission),
            ("current_mode_id", &defaults.permission),
            ("thought_level", &defaults.thought_level),
        ] {
            obj.insert(
                key.into(),
                value
                    .clone()
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null),
            );
        }
        // No foreign resume anchor may be sent to a replacement engine. Rules
        // and skills are init-time configuration too, so open a new backend
        // context while retaining every visible message and project artifact.
        if reset_context {
            for key in ["session_id", "sessionId", "session_snapshot", "fork", "loaded_skills"] {
                obj.remove(key);
            }
        }
        self.conversation_repo
            .update(
                user_id,
                conversation_id,
                &ConversationRowUpdate {
                    agent_type: Some(snapshot.agent_type.serde_name().to_owned()),
                    model: Some(model_json),
                    extra: Some(extra.to_string()),
                    updated_at: Some(now_ms()),
                    ..Default::default()
                },
            )
            .await?;
        let skill_ids =
            serde_json::to_string(&defaults.skill_ids).map_err(|e| ConversationError::internal(e.to_string()))?;
        let disabled = serde_json::to_string(&defaults.disabled_builtin_skill_ids)
            .map_err(|e| ConversationError::internal(e.to_string()))?;
        let mcp_ids =
            serde_json::to_string(&defaults.mcp_ids).map_err(|e| ConversationError::internal(e.to_string()))?;
        self.conversation_repo
            .upsert_assistant_snapshot(
                user_id,
                &UpsertConversationAssistantSnapshotParams {
                    conversation_id,
                    assistant_definition_id: &snapshot.assistant_definition_id,
                    assistant_id,
                    assistant_source: &snapshot.assistant_source,
                    agent_id: &snapshot.agent_id,
                    rules_content: &snapshot.rules.content,
                    default_model_mode: &snapshot.default_modes.model,
                    resolved_model_id: defaults.model.as_deref(),
                    default_permission_mode: &snapshot.default_modes.permission,
                    resolved_permission_value: defaults.permission.as_deref(),
                    default_thought_level_mode: &snapshot.default_modes.thought_level,
                    resolved_thought_level_value: defaults.thought_level.as_deref(),
                    default_skills_mode: &snapshot.default_modes.skills,
                    resolved_skill_ids: &skill_ids,
                    resolved_disabled_builtin_skill_ids: &disabled,
                    default_mcps_mode: &snapshot.default_modes.mcps,
                    resolved_mcp_ids: &mcp_ids,
                },
            )
            .await?;
        if backend_changed {
            self.acp_session_repo.delete_for_user(user_id, conversation_id).await?;
        }
        if matches!(snapshot.agent_type, AgentType::Acp | AgentType::Antigravity) {
            if self
                .acp_session_repo
                .get_for_user(user_id, conversation_id)
                .await?
                .is_none()
            {
                self.acp_session_repo
                    .create(&CreateAcpSessionParams {
                        user_id,
                        conversation_id,
                        agent_source: &snapshot.agent_source,
                        agent_id: &snapshot.agent_id,
                    })
                    .await?;
            }
            if reset_context {
                self.acp_session_repo
                    .clear_session_id_for_user(user_id, conversation_id)
                    .await?;
            }
            let mut selections = HashMap::<String, String>::new();
            for (key, value) in [
                ("model", &defaults.model),
                ("mode", &defaults.permission),
                ("effort", &defaults.thought_level),
            ] {
                if let Some(value) = value {
                    selections.insert(key.to_owned(), value.clone());
                }
            }
            let selections =
                serde_json::to_string(&selections).map_err(|e| ConversationError::internal(e.to_string()))?;
            self.acp_session_repo
                .save_runtime_state_for_user(
                    user_id,
                    conversation_id,
                    &SaveRuntimeStateParams {
                        current_model_id: Some(defaults.model.as_deref()),
                        current_mode_id: Some(defaults.permission.as_deref()),
                        config_selections_json: Some(Some(&selections)),
                        context_usage_json: reset_context.then_some(None),
                    },
                )
                .await?;
        }
        let resolved = self.skill_resolver.resolve_skills_for_user(user_id, &skills).await;
        self.skill_resolver
            .sync_skill_view(user_id, conversation_id, &resolved)
            .await;
        // Commit the applied marker last: a failed write must remain retryable.
        self.update_extra(
            user_id,
            conversation_id,
            serde_json::json!({"assistant_configuration_fingerprint":fingerprint}),
        )
        .await?;
        self.broadcaster.broadcast(WebSocketMessage::new(
            "conversation.listChanged",
            serde_json::json!({
                "user_id":user_id,"conversation_id":conversation_id,"action":"updated"
            }),
        ));
        info!(
            user_id,
            conversation_id, assistant_id, "Team assistant configuration snapshot synchronized"
        );
        Ok(Some(result))
    }
}
