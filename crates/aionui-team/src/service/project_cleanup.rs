//! Durable, model-free cleanup of explicitly accepted project receipts.
//! Runs in the owning team service, never by persisting an employee token.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Artifact {
    path: PathBuf,
    sha256: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct Receipt {
    project_id: String,
    team_id: String,
    task_id: String,
    status: String,
    participant_slots: Vec<String>,
    files: Vec<Artifact>,
    #[serde(default)]
    cleared: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    resetting: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}
fn files_unchanged(files: &[Artifact]) -> bool {
    !files.is_empty()
        && files.iter().all(|file| {
            if !file.path.is_absolute() {
                return false;
            }
            let Ok(mut input) = std::fs::File::open(&file.path) else {
                return false;
            };
            let mut hash = Sha256::new();
            let mut buffer = [0_u8; 65536];
            loop {
                match input.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => hash.update(&buffer[..n]),
                    Err(_) => return false,
                }
            }
            format!("{:x}", hash.finalize()) == file.sha256
        })
}
fn save_private(path: &Path, value: &impl Serialize) -> Result<(), TeamError> {
    use std::io::Write;
    let tmp = path.with_extension(format!("tmp-{}", generate_id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| TeamError::InvalidRequest(e.to_string()))?;
    let result = (|| -> std::io::Result<()> {
        let mut output = options.open(&tmp)?;
        output.write_all(&bytes)?;
        output.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result.map_err(|e| TeamError::InvalidRequest(format!("Project receipt write failed: {e}")))
}
fn unfinished(task: &TeamTask) -> bool {
    matches!(
        task.status,
        crate::types::TaskStatus::Pending | crate::types::TaskStatus::InProgress
    )
}
fn slot_busy(session: &TeamSession, slot: &str) -> bool {
    session.work_coordinator().slot_snapshot(slot).is_some_and(|work| {
        work.active_batch.is_some() || work.queued_foreground_count > 0 || work.queued_background_count > 0
    })
}
impl TeamSessionService {
    pub(super) async fn process_project_receipts(&self, session: &Arc<TeamSession>) -> Result<(), TeamError> {
        let team = self.load_owned_team(session.user_id(), session.team_id()).await?;
        let state = Path::new(&team.workspace).join("ops/project-lifecycle");
        let policy: serde_json::Value = match std::fs::read(state.join("policy.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => return Ok(()),
        };
        if policy["enabled"] != true || policy["engine"] != "native" || policy["team_id"] != team.id {
            return Ok(());
        }
        let dir = state.join("receipts");
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Ok(());
        };
        save_private(
            &state.join("native-watcher-status.json"),
            &serde_json::json!({
                "engine":"native", "team_id":team.id, "heartbeat_ms":now_ms(), "model_calls":0,
                "credentials_persisted":false,
            }),
        )?;
        for entry in entries {
            let path = entry.map_err(|e| TeamError::InvalidRequest(e.to_string()))?.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") || path.is_symlink() {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|e| TeamError::InvalidRequest(e.to_string()))?;
            let Ok(mut r) = serde_json::from_slice::<Receipt>(&bytes) else {
                continue;
            };
            if r.extra.get("cleanup_engine").and_then(|v| v.as_str()) != Some("native-v1") {
                continue;
            }
            if r.team_id != team.id || !matches!(r.status.as_str(), "pending_clear" | "finalizing") {
                continue;
            }
            let tasks = session.scheduler().list_tasks().await?;
            if !tasks
                .iter()
                .any(|t| t.id == r.task_id && t.status == crate::types::TaskStatus::Completed)
            {
                continue;
            }
            if r.resetting.is_some() {
                r.status = "blocked_interrupted_reset".into();
                save_private(&path, &r)?;
                continue;
            }
            if !files_unchanged(&r.files) {
                r.status = "blocked_artifact_changed".into();
                save_private(&path, &r)?;
                continue;
            }
            let Some(leader) = team.agents.iter().find(|a| a.role == TeammateRole::Lead) else {
                continue;
            };
            if r.participant_slots.is_empty()
                || r.participant_slots.iter().collect::<HashSet<_>>().len() != r.participant_slots.len()
                || !r.participant_slots.contains(&leader.slot_id)
                || r.participant_slots
                    .iter()
                    .any(|s| !team.agents.iter().any(|a| &a.slot_id == s))
            {
                r.status = "blocked_invalid_participants".into();
                save_private(&path, &r)?;
                continue;
            }
            r.status = "pending_clear".into();
            let mut slots = r.participant_slots.clone();
            slots.sort_by_key(|s| s == &leader.slot_id);
            slots.dedup();
            for slot in slots {
                if r.cleared.contains_key(&slot) {
                    continue;
                }
                let fresh_tasks = session.scheduler().list_tasks().await?;
                if slot_busy(session, &slot)
                    || fresh_tasks
                        .iter()
                        .any(|t| unfinished(t) && t.owner.as_deref() == Some(&slot))
                {
                    continue;
                }
                if slot == leader.slot_id
                    && (r
                        .participant_slots
                        .iter()
                        .any(|s| s != &slot && !r.cleared.contains_key(s))
                        || team.agents.iter().any(|a| slot_busy(session, &a.slot_id))
                        || fresh_tasks.iter().any(unfinished))
                {
                    continue;
                }
                let agent = team
                    .agents
                    .iter()
                    .find(|a| a.slot_id == slot)
                    .expect("validated participant");
                // Warm dormant runtimes without sending a prompt; this also applies saved assistant defaults.
                if matches!(
                    session.member_runtimes().snapshot(&slot),
                    MemberRuntimeSnapshot::Absent | MemberRuntimeSnapshot::Failed { .. }
                ) {
                    if let ReserveAttach::Start(lease) = session.member_runtimes().reserve_attach(&slot, false) {
                        let Some(service) = self.self_ref.upgrade() else {
                            return Ok(());
                        };
                        let outcome = attach_member_runtime(
                            service,
                            session.clone(),
                            session.user_id().to_owned(),
                            agent.clone(),
                            self.task_manager.clone(),
                            lease,
                            false,
                        )
                        .await;
                        if !matches!(outcome, AttachOutcome::Ready) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                if !files_unchanged(&r.files) {
                    r.status = "blocked_artifact_changed".into();
                    save_private(&path, &r)?;
                    break;
                }
                let external = policy["external_reset_slots"].get(&slot);
                let mut external_confirmation = None;
                if let Some(external) = external {
                    if external["kind"] != "openclaw" || external["conversation_id"] != agent.conversation_id {
                        r.status = "blocked_external_reset_binding".into();
                        save_private(&path, &r)?;
                        break;
                    }
                    let markers = state.join("openclaw-reset");
                    std::fs::create_dir_all(&markers).map_err(|e| TeamError::InvalidRequest(e.to_string()))?;
                    let marker = markers.join(format!("{}.json", agent.conversation_id));
                    let confirmation = marker.with_extension("confirmed.json");
                    // A previous project's confirmation cannot satisfy this receipt.
                    save_private(&marker, &serde_json::json!({"project_id":r.project_id,"slot_id":slot}))?;
                    external_confirmation = Some(confirmation);
                }
                // Persist intent before resetting: a crash cannot silently reset a later project's context.
                r.resetting = Some(slot.clone());
                save_private(&path, &r)?;
                let outcome = self
                    .clear_agent_context_inner(session.user_id(), &team.id, &slot, true)
                    .await;
                match outcome {
                    Ok(outcome)
                        if outcome.reset_status == TeamContextResetStatus::Completed
                            && outcome.runtime_status == TeamContextResetRuntimeStatus::Ready =>
                    {
                        if let Some(confirmation) = &external_confirmation {
                            let confirmed = std::fs::read(confirmation)
                                .ok()
                                .and_then(|v| serde_json::from_slice::<serde_json::Value>(&v).ok());
                            if !confirmed
                                .is_some_and(|v| v["project_id"] == r.project_id && v["reset_confirmed"] == true)
                            {
                                r.status = "blocked_external_reset_unconfirmed".into();
                                save_private(&path, &r)?;
                                break;
                            }
                        }
                        r.cleared.insert(
                            slot.clone(),
                            serde_json::json!({"at_ms":now_ms(),"native_result":outcome}),
                        );
                        r.resetting = None;
                        save_private(&path, &r)?;
                        info!(
                            team_id = team.id,
                            slot_id = slot,
                            "accepted project participant context cleared"
                        );
                    }
                    Err(TeamError::MemberBusy { .. } | TeamError::ContextResetUnavailable { .. }) => {
                        // These failures are returned before any reset side effect.
                        // A new foreground turn wins; retry at the next idle boundary.
                        r.resetting = None;
                        save_private(&path, &r)?;
                        continue;
                    }
                    result => {
                        r.status = "blocked_native_reset".into();
                        r.extra
                            .insert("reset_error".into(), serde_json::json!(format!("{result:?}")));
                        save_private(&path, &r)?;
                        warn!(
                            team_id = team.id,
                            slot_id = slot,
                            "accepted project reset needs attention"
                        );
                        break;
                    }
                }
            }
            if r.cleared.len() == r.participant_slots.len() {
                r.status = "cleared".into();
                r.extra.insert("cleared_at_ms".into(), now_ms().into());
                save_private(&path, &r)?;
                info!(team_id = team.id, "accepted project context cleanup completed");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cleanup_requires_nonempty_unchanged_files() {
        assert!(!files_unchanged(&[]));
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("result.txt");
        std::fs::write(&p, "accepted").unwrap();
        let files = vec![Artifact {
            path: p.clone(),
            sha256: format!("{:x}", Sha256::digest(b"accepted")),
        }];
        assert!(files_unchanged(&files));
        std::fs::write(&p, "changed").unwrap();
        assert!(!files_unchanged(&files));
        std::fs::remove_file(&p).unwrap();
        assert!(!files_unchanged(&files));
    }
    #[test]
    fn receipt_roundtrip_retains_handoff_and_reset_intent() {
        let original = serde_json::json!({"project_id":"p","team_id":"t","task_id":"job","status":"pending_clear","participant_slots":["worker","lead"],"files":[],"handoff":"kept","resetting":"worker","cleared":{}});
        let r: Receipt = serde_json::from_value(original).unwrap();
        let value = serde_json::to_value(r).unwrap();
        assert_eq!(value["handoff"], "kept");
        assert_eq!(value["resetting"], "worker");
    }
    #[test]
    fn receipt_writes_are_atomic_and_private() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receipt.json");
        save_private(&path, &serde_json::json!({"status":"cleared"})).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(&path).unwrap()).unwrap()["status"],
            "cleared"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
    }
}
