use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::action::{ActionExecutor, MessageResult};
use crate::message_service::ChannelMessageService;
use crate::session::SessionManager;
use crate::stream_relay::{ChannelSender, ChannelStreamRelay, RelayConfig, throttle_ms_for_platform};
use crate::types::{ActionBehavior, OutgoingMessageType, UnifiedIncomingMessage, UnifiedOutgoingMessage};

/// Orchestrates the full channel message lifecycle.
///
/// Consumes incoming IM messages from `message_rx` and tool confirmation
/// callbacks from `confirm_rx`, driving the pipeline:
/// 1. ActionExecutor routing (auth → action/AI dispatch)
/// 2. For Dispatched: send_to_agent + spawn ChannelStreamRelay
/// 3. For Action: reply via plugin
/// 4. Forward tool confirmations to the agent
pub struct ChannelOrchestrator {
    action_executor: Arc<ActionExecutor>,
    message_service: Arc<ChannelMessageService>,
    session_manager: Arc<SessionManager>,
    sender: Arc<dyn ChannelSender>,
    outbox_dir: std::path::PathBuf,
    queues: Arc<dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ChannelOrchestrator {
    pub fn new(
        action_executor: Arc<ActionExecutor>,
        message_service: Arc<ChannelMessageService>,
        session_manager: Arc<SessionManager>,
        sender: Arc<dyn ChannelSender>,
        outbox_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            action_executor,
            message_service,
            session_manager,
            sender,
            outbox_dir,
            queues: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Start the message loop. Runs until both channels close.
    pub async fn run(
        self,
        mut message_rx: mpsc::Receiver<UnifiedIncomingMessage>,
        mut confirm_rx: mpsc::Receiver<(String, String)>,
    ) {
        info!("ChannelOrchestrator started");

        let mut outbox_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        outbox_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(msg) = message_rx.recv() => {
                    self.handle_message(msg).await;
                }
                Some((call_id, value)) = confirm_rx.recv() => {
                    handle_confirm(&call_id, &value);
                }
                _ = outbox_interval.tick() => {
                    self.flush_outbox().await;
                }
                else => break,
            }
        }

        info!("ChannelOrchestrator stopped (channels closed)");
    }

    /// Sends queued outbox replies back to their originating channel chat.
    async fn flush_outbox(&self) {
        let Ok(mut entries) = tokio::fs::read_dir(&self.outbox_dir).await else { return; };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = tokio::fs::read(&path).await else { continue; };
            let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else { continue; };
            let owner_user_id = v.get("owner_user_id").and_then(|x| x.as_str()).unwrap_or("");
            let plugin_id = v.get("plugin_id").and_then(|x| x.as_str()).unwrap_or("");
            let chat_id = v.get("chat_id").and_then(|x| x.as_str()).unwrap_or("");
            let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
            if owner_user_id.is_empty() || plugin_id.is_empty() || chat_id.is_empty() || text.is_empty() {
                continue;
            }
            let outgoing = UnifiedOutgoingMessage {
                message_type: OutgoingMessageType::Text,
                text: Some(text.to_owned()),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                image_url: None,
                file_url: None,
                file_name: None,
                media_actions: None,
                reply_to_message_id: None,
                silent: None,
            };
            if self
                .sender
                .send_message(owner_user_id, plugin_id, chat_id, outgoing)
                .await
                .is_ok()
            {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }

    async fn handle_message(&self, msg: UnifiedIncomingMessage) {
        let platform = msg.platform;
        let chat_id = msg.chat_id.clone();
        let plugin_id = platform.to_string();
        let text = msg.content.text.clone();
        let message_owner_user_id = msg
            .owner_user_id
            .clone()
            .or_else(|| self.action_executor.owner_user_id().map(ToOwned::to_owned));

        let executor = Arc::clone(&self.action_executor);
        let msg_svc = Arc::clone(&self.message_service);
        let session_mgr = Arc::clone(&self.session_manager);
        let sender = Arc::clone(&self.sender);

        let queue_key = serde_json::to_string(&(message_owner_user_id.as_deref(), platform.to_string(), &chat_id))
            .expect("channel key");
        let queues = Arc::clone(&self.queues);
        tokio::spawn(async move {
            match executor.handle_incoming_message(&msg).await {
                Ok(MessageResult::Action(response)) => {
                    if let Some(owner_user_id) = message_owner_user_id.as_deref() {
                        send_action_response(&sender, owner_user_id, &plugin_id, &chat_id, &response).await;
                    } else {
                        warn!("dropping channel action response without owner user");
                    }
                }
                Ok(MessageResult::Dispatched {
                    owner_user_id,
                    session_id,
                    conversation_id,
                }) => {
                    // Serialize a chat through the final relay, not only request submission.
                    let queue = Arc::clone(
                        queues
                            .entry(queue_key)
                            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                            .value(),
                    );
                    let _turn = queue.lock().await;
                    let mut text = text;
                    let files = if msg.content.attachments.as_ref().is_some_and(|a| !a.is_empty()) {
                        let prepared = match sender.download_attachments(&owner_user_id, &plugin_id, &msg).await {
                            Ok(downloaded) => msg_svc.prepare_documents(&owner_user_id, &msg, downloaded).await,
                            Err(error) => Err(error),
                        };
                        match prepared {
                            Ok(prepared) => {
                                text = format!("{}\n\n{}", text, prepared.instruction);
                                prepared.files
                            }
                            Err(error) => {
                                warn!("channel attachment preparation failed");
                                let outgoing = ChannelMessageService::build_streaming_message(&format!(
                                    "附件尚未完成接收或归档：{error}。不会将其标成已读取。"
                                ));
                                let _ = sender
                                    .send_message(&owner_user_id, &plugin_id, &chat_id, outgoing)
                                    .await;
                                return;
                            }
                        }
                    } else {
                        vec![]
                    };
                    handle_dispatched(
                        &msg_svc,
                        &session_mgr,
                        &sender,
                        &owner_user_id,
                        &session_id,
                        conversation_id.as_deref(),
                        &text,
                        files,
                        platform,
                        &plugin_id,
                        &chat_id,
                    )
                    .await;
                }
                Ok(MessageResult::AlreadyProcessing) => {
                    info!(chat_id = %chat_id, "message ignored: already processing");
                }
                Err(e) => {
                    error!(error = %e, "failed to handle incoming message");
                }
            }
        });
    }
}

async fn send_action_response(
    sender: &Arc<dyn ChannelSender>,
    owner_user_id: &str,
    plugin_id: &str,
    chat_id: &str,
    response: &crate::types::ActionResponse,
) {
    if let Some(text) = &response.text {
        let outgoing = UnifiedOutgoingMessage {
            message_type: OutgoingMessageType::Text,
            text: Some(text.clone()),
            parse_mode: response.parse_mode,
            buttons: response.buttons.clone(),
            keyboard: response.keyboard.clone(),
            image_url: None,
            file_url: None,
            file_name: None,
            media_actions: None,
            reply_to_message_id: None,
            silent: None,
        };

        match response.behavior {
            ActionBehavior::Edit => {
                if let Some(ref edit_id) = response.edit_message_id {
                    let _ = sender
                        .edit_message(owner_user_id, plugin_id, chat_id, edit_id, outgoing)
                        .await;
                }
            }
            _ => {
                let _ = sender.send_message(owner_user_id, plugin_id, chat_id, outgoing).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_dispatched(
    msg_svc: &Arc<ChannelMessageService>,
    session_mgr: &Arc<SessionManager>,
    sender: &Arc<dyn ChannelSender>,
    owner_user_id: &str,
    session_id: &str,
    conversation_id: Option<&str>,
    text: &str,
    files: Vec<aionui_api_types::ChatFileRef>,
    platform: crate::types::PluginType,
    plugin_id: &str,
    chat_id: &str,
) {
    let session = match session_mgr.get_session_by_id(owner_user_id, session_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            warn!(session_id = %session_id, "session not found after dispatch");
            return;
        }
        Err(e) => {
            error!(error = %e, "failed to get session");
            return;
        }
    };

    let send_result = match msg_svc
        .send_to_agent_with_files(owner_user_id, &session, text, platform, files)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to send to agent");
            let err_msg = UnifiedOutgoingMessage {
                message_type: OutgoingMessageType::Text,
                text: Some(format!("\u{274c} Failed to process: {e}")),
                parse_mode: None,
                buttons: None,
                keyboard: None,
                image_url: None,
                file_url: None,
                file_name: None,
                media_actions: None,
                reply_to_message_id: None,
                silent: None,
            };
            let _ = sender.send_message(owner_user_id, plugin_id, chat_id, err_msg).await;
            return;
        }
    };

    // Bind conversation to session if newly created
    if conversation_id.is_none()
        && let Err(e) = session_mgr
            .bind_conversation(owner_user_id, session_id, &send_result.conversation_id)
            .await
    {
        warn!(error = %e, "failed to bind conversation to session");
    }

    // Spawn stream relay if we got a subscription
    if let Some(rx) = send_result.stream_rx {
        let relay_config = RelayConfig {
            owner_user_id: owner_user_id.to_owned(),
            platform,
            plugin_id: plugin_id.to_owned(),
            chat_id: chat_id.to_owned(),
            throttle_ms: throttle_ms_for_platform(platform),
        };
        let relay = ChannelStreamRelay::new(relay_config, Arc::clone(sender));
        relay.run(rx).await;
    } else {
        warn!(
            conversation_id = %send_result.conversation_id,
            "no agent task for stream subscription"
        );
    }
}

/// Forward a tool confirmation callback to the active agent.
fn handle_confirm(call_id: &str, value: &str) {
    // Channel conversations use yoloMode which auto-approves everything,
    // so this path is rarely hit. When needed, we can add a
    // call_id→conversation_id lookup via IWorkerTaskManager.
    info!(call_id = %call_id, value = %value, "forwarding tool confirmation");
}
