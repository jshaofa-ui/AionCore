use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use reqwest::Client;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::constants::{WEIXIN_MAX_BACKOFF, WEIXIN_POLL_TIMEOUT};
use crate::error::ChannelError;
use crate::plugin::{ChannelPlugin, PluginCallbacks};
use crate::types::{
    BotInfo, MessageContentType, PluginConfig, PluginStatus, PluginType, UnifiedIncomingMessage, UnifiedMessageContent,
    UnifiedOutgoingMessage, UnifiedUser,
};

use super::api::WeixinApi;
use super::types::{ITEM_TYPE_TEXT, ITEM_TYPE_VOICE, WeixinRawItem, WeixinRawMessage};

/// Default base URL for the iLink Bot API.
const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";

/// WeChat (iLink Bot) platform plugin.
///
/// Connects via long-polling (buffer-based `getupdates`), handles text/voice
/// messages. Does not support editing messages (WeChat limitation);
/// `edit_message` sends a new reply instead.
pub struct WeixinPlugin {
    status: PluginStatus,
    bot_info: Option<BotInfo>,
    last_error: Option<String>,
    api: Option<Arc<WeixinApi>>,
    poll_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    context_tokens: Arc<DashMap<String, String>>,
    pending_media: Arc<DashMap<String, (std::time::Instant, super::types::MediaItemData, bool)>>,
}

impl Default for WeixinPlugin {
    fn default() -> Self {
        Self {
            status: PluginStatus::Created,
            bot_info: None,
            last_error: None,
            api: None,
            poll_handle: None,
            shutdown_tx: None,
            context_tokens: Arc::new(DashMap::new()),
            pending_media: Arc::new(DashMap::new()),
        }
    }
}

impl WeixinPlugin {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ChannelPlugin for WeixinPlugin {
    async fn initialize(&mut self, config: PluginConfig, callbacks: PluginCallbacks) -> Result<(), ChannelError> {
        self.status = PluginStatus::Initializing;

        let bot_token = config
            .credentials
            .bot_token
            .as_deref()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                self.status = PluginStatus::Error;
                self.last_error = Some("Missing WeChat bot_token".into());
                ChannelError::InvalidConfig("Missing WeChat bot_token".into())
            })?;

        let account_id = config
            .credentials
            .account_id
            .as_deref()
            .filter(|a| !a.is_empty())
            .ok_or_else(|| {
                self.status = PluginStatus::Error;
                self.last_error = Some("Missing WeChat account_id".into());
                ChannelError::InvalidConfig("Missing WeChat account_id".into())
            })?;

        let base_url = config
            .credentials
            .extra
            .get("baseUrl")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_BASE_URL);

        let http_client = Client::builder()
            .timeout(Duration::from_secs(WEIXIN_POLL_TIMEOUT.as_secs() + 10))
            .build()
            .map_err(|e| {
                self.status = PluginStatus::Error;
                self.last_error = Some(format!("HTTP client init failed: {e}"));
                ChannelError::ConnectionFailed(format!("HTTP client init failed: {e}"))
            })?;

        let api = Arc::new(WeixinApi::new(http_client, base_url, bot_token));

        self.bot_info = Some(BotInfo {
            id: account_id.to_string(),
            username: None,
            display_name: format!("WeChat Bot ({account_id})"),
        });

        info!(account_id, "WeChat bot initialized");

        self.api = Some(api);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let api_clone = Arc::clone(self.api.as_ref().expect("api just set"));
        let context_tokens = Arc::clone(&self.context_tokens);
        self.poll_handle = Some(tokio::spawn(poll_loop(
            api_clone,
            callbacks.message_tx,
            shutdown_rx,
            context_tokens,
            Arc::clone(&self.pending_media),
        )));

        self.status = PluginStatus::Ready;
        Ok(())
    }

    async fn download_attachments(
        &self,
        message: &UnifiedIncomingMessage,
    ) -> Result<Vec<crate::documents::DownloadedAttachment>, ChannelError> {
        let mut result = vec![];
        let mut total = 0;
        for file in message.content.attachments.as_deref().unwrap_or_default() {
            let id = file
                .file_id
                .as_deref()
                .ok_or_else(|| ChannelError::PlatformApi("Missing attachment identifier".into()))?;
            let (_, item, image) = self.pending_media.get(id).map(|v| v.clone()).ok_or_else(|| {
                ChannelError::PlatformApi("Attachment receipt expired; please resend the file".into())
            })?;
            let downloaded = super::media::download(&item, image).await?;
            total += downloaded.bytes.len();
            if total > crate::documents::MAX_DOCUMENT_BYTES {
                return Err(ChannelError::PlatformApi("Attachment batch exceeds size limit".into()));
            }
            self.pending_media.remove(id);
            result.push(downloaded);
        }
        Ok(result)
    }

    async fn start(&mut self) -> Result<(), ChannelError> {
        self.status = PluginStatus::Starting;
        self.status = PluginStatus::Running;
        info!("WeChat plugin started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), ChannelError> {
        self.status = PluginStatus::Stopping;

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(handle) = self.poll_handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        self.api = None;
        self.context_tokens.clear();
        self.status = PluginStatus::Stopped;
        info!("WeChat plugin stopped");
        Ok(())
    }

    async fn send_message(&self, chat_id: &str, message: UnifiedOutgoingMessage) -> Result<String, ChannelError> {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| ChannelError::PlatformApi("Plugin not initialized".into()))?;

        let text = message.text.as_deref().unwrap_or("").to_string();
        let context_token = self.context_tokens.get(chat_id).map(|v| v.clone());

        api.send_message(chat_id, &text, context_token.as_deref()).await?;
        Ok(String::new())
    }

    /// WeChat does not support editing messages.
    async fn edit_message(
        &self,
        chat_id: &str,
        _message_id: &str,
        message: UnifiedOutgoingMessage,
    ) -> Result<(), ChannelError> {
        let _ = self.send_message(chat_id, message).await?;
        Ok(())
    }

    fn active_user_count(&self) -> usize {
        0
    }

    fn bot_info(&self) -> Option<&BotInfo> {
        self.bot_info.as_ref()
    }

    fn plugin_type(&self) -> PluginType {
        PluginType::Weixin
    }

    fn status(&self) -> PluginStatus {
        self.status
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

// ---------------------------------------------------------------------------
// Long-polling loop (buffer-based protocol)
// ---------------------------------------------------------------------------

async fn poll_loop(
    api: Arc<WeixinApi>,
    message_tx: tokio::sync::mpsc::Sender<UnifiedIncomingMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    context_tokens: Arc<DashMap<String, String>>,
    pending_media: Arc<DashMap<String, (std::time::Instant, super::types::MediaItemData, bool)>>,
) {
    let mut buf = String::new();
    let mut consecutive_failures: u32 = 0;

    loop {
        if *shutdown_rx.borrow() {
            debug!("WeChat poll loop received shutdown signal");
            break;
        }

        // Reduce each round to success or a failure reason. On success we also
        // advance the buffer and dispatch messages; on API error / transport
        // error we only record the reason.
        let outcome: Result<(), String> = match api.get_updates(&buf).await {
            Ok(resp) => {
                let is_api_error = resp.ret.unwrap_or(0) != 0 || resp.errcode.unwrap_or(0) != 0;
                if is_api_error {
                    Err(format!(
                        "getupdates API error ret={:?} errcode={:?}",
                        resp.ret, resp.errcode
                    ))
                } else {
                    if let Some(new_buf) = resp.get_updates_buf {
                        buf = new_buf;
                    }
                    for msg in resp.msgs.unwrap_or_default() {
                        handle_message(&msg, &message_tx, &context_tokens, &pending_media).await;
                    }
                    Ok(())
                }
            }
            Err(e) => Err(e.to_string()),
        };

        match outcome {
            Ok(()) => {
                if poll_log_action(consecutive_failures, true) == PollLogAction::Recovered {
                    info!(consecutive_failures, "WeChat poll recovered");
                }
                consecutive_failures = 0;
            }
            Err(reason) => {
                let action = poll_log_action(consecutive_failures, false);
                consecutive_failures += 1;
                let delay = backoff_delay(consecutive_failures);
                match action {
                    PollLogAction::StartedFailing => {
                        warn!(error = %reason, "WeChat poll started failing");
                    }
                    PollLogAction::StillFailing => {
                        debug!(
                            error = %reason,
                            consecutive_failures,
                            next_backoff_secs = delay.as_secs(),
                            "WeChat poll still failing"
                        );
                    }
                    // Silent / Recovered are not reachable on the failure branch.
                    _ => {}
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown_rx.changed() => {
                        debug!("WeChat poll loop shutdown during backoff");
                        break;
                    }
                }
            }
        }
    }

    debug!("WeChat poll loop exited");
}

/// Exponential backoff delay for WeChat poll failures: `2^n` seconds,
/// capped at `WEIXIN_MAX_BACKOFF` (10 minutes). `saturating_pow` guards
/// against overflow on long failure streaks.
fn backoff_delay(consecutive_failures: u32) -> Duration {
    let secs = 2u64
        .saturating_pow(consecutive_failures)
        .min(WEIXIN_MAX_BACKOFF.as_secs());
    Duration::from_secs(secs)
}

/// What to log for a single WeChat poll outcome, decided from the failure
/// streak *before* this outcome is applied. Keeps the log-level policy in
/// one pure, testable place; the loop performs the actual logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollLogAction {
    /// Success while healthy — log nothing.
    Silent,
    /// First failure after a healthy streak (0 -> 1) — log once at warn.
    StartedFailing,
    /// Repeated failure while already failing — log at debug only.
    StillFailing,
    /// First success after one or more failures — log once at info.
    Recovered,
}

fn poll_log_action(prev_failures: u32, succeeded: bool) -> PollLogAction {
    match (succeeded, prev_failures) {
        (true, 0) => PollLogAction::Silent,
        (true, _) => PollLogAction::Recovered,
        (false, 0) => PollLogAction::StartedFailing,
        (false, _) => PollLogAction::StillFailing,
    }
}

// ---------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------

async fn handle_message(
    msg: &WeixinRawMessage,
    message_tx: &tokio::sync::mpsc::Sender<UnifiedIncomingMessage>,
    context_tokens: &DashMap<String, String>,
    pending_media: &DashMap<String, (std::time::Instant, super::types::MediaItemData, bool)>,
) {
    let from_user_id = match &msg.from_user_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return,
    };

    // Store context_token for reply use
    if let Some(ctx) = &msg.context_token
        && !ctx.is_empty()
    {
        context_tokens.insert(from_user_id.clone(), ctx.clone());
    }

    let items = msg.item_list.as_deref().unwrap_or_default();
    let (mut content_type, mut text, _has_media) = extract_content(items);
    pending_media.retain(|_, v| v.0.elapsed() < Duration::from_secs(3600));
    let mut attachments = vec![];
    for item in items {
        let data = match item.item_type {
            Some(2) => item.image_item.as_ref().map(|v| (v, true)),
            Some(4) => item.file_item.as_ref().map(|v| (v, false)),
            _ => None,
        };
        if let Some((data, image)) = data {
            if pending_media.len() >= 128 {
                break;
            }
            let id = uuid::Uuid::new_v4().to_string();
            pending_media.insert(id.clone(), (std::time::Instant::now(), data.clone(), image));
            attachments.push(crate::types::UnifiedAttachment {
                file_id: Some(id),
                file_name: data.file_name.clone(),
                mime_type: None,
                file_size: None,
                url: None,
            });
        }
    }
    if !attachments.is_empty() {
        content_type = MessageContentType::Document;
        if text.is_empty() {
            text = "请接收并整理这些附件到公司资料库。".to_owned();
        }
    }
    if text.is_empty() {
        return;
    }

    let display_name = if from_user_id.len() > 6 {
        from_user_id[from_user_id.len() - 6..].to_string()
    } else {
        from_user_id.clone()
    };

    let unified = UnifiedIncomingMessage {
        owner_user_id: None,
        id: msg
            .msg_id
            .clone()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| format!("receipt-{}", uuid::Uuid::new_v4())),
        platform: PluginType::Weixin,
        chat_id: from_user_id.clone(),
        user: UnifiedUser {
            id: from_user_id,
            username: None,
            display_name,
            avatar_url: None,
        },
        content: UnifiedMessageContent {
            content_type,
            text,
            attachments: if attachments.is_empty() {
                None
            } else {
                Some(attachments)
            },
        },
        timestamp: chrono_now(),
        reply_to_message_id: None,
        action: None,
        raw: None,
    };

    let _ = message_tx.send(unified).await;
}

/// Extract text content from item_list.
///
/// Returns (content_type, combined_text, has_media_items).
fn extract_content(items: &[WeixinRawItem]) -> (MessageContentType, String, bool) {
    let mut text_parts: Vec<&str> = Vec::new();
    let mut has_media = false;

    for item in items {
        match item.item_type {
            Some(ITEM_TYPE_TEXT) => {
                if let Some(ref ti) = item.text_item
                    && let Some(ref t) = ti.text
                {
                    let trimmed = t.trim();
                    if !trimmed.is_empty() {
                        text_parts.push(trimmed);
                    }
                }
            }
            Some(ITEM_TYPE_VOICE) => {
                if let Some(ref vi) = item.voice_item
                    && let Some(ref t) = vi.text
                {
                    let trimmed = t.trim();
                    if !trimmed.is_empty() {
                        text_parts.push(trimmed);
                    }
                }
            }
            Some(2) | Some(4) => {
                has_media = true;
            }
            _ => {}
        }
    }

    let text = text_parts.join("\n\n");

    let content_type = if text.starts_with('/') {
        MessageContentType::Command
    } else {
        MessageContentType::Text
    };

    (content_type, text, has_media)
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PluginCredentials;
    use std::collections::HashMap;

    // -- backoff_delay ---------------------------------------------------------

    #[test]
    fn backoff_delay_exponential_curve() {
        assert_eq!(backoff_delay(1), Duration::from_secs(2));
        assert_eq!(backoff_delay(2), Duration::from_secs(4));
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
        assert_eq!(backoff_delay(4), Duration::from_secs(16));
        assert_eq!(backoff_delay(5), Duration::from_secs(32));
        assert_eq!(backoff_delay(9), Duration::from_secs(512));
    }

    #[test]
    fn backoff_delay_caps_at_ten_minutes() {
        assert_eq!(backoff_delay(10), Duration::from_secs(600));
        assert_eq!(backoff_delay(20), Duration::from_secs(600));
        // Must not overflow on large counts.
        assert_eq!(backoff_delay(u32::MAX), Duration::from_secs(600));
    }

    // -- poll_log_action -------------------------------------------------------

    #[test]
    fn log_action_silent_when_healthy_success() {
        assert_eq!(poll_log_action(0, true), PollLogAction::Silent);
    }

    #[test]
    fn log_action_started_failing_on_first_failure() {
        assert_eq!(poll_log_action(0, false), PollLogAction::StartedFailing);
    }

    #[test]
    fn log_action_still_failing_on_repeat_failure() {
        assert_eq!(poll_log_action(1, false), PollLogAction::StillFailing);
        assert_eq!(poll_log_action(9, false), PollLogAction::StillFailing);
    }

    #[test]
    fn log_action_recovered_after_failures() {
        assert_eq!(poll_log_action(1, true), PollLogAction::Recovered);
        assert_eq!(poll_log_action(5, true), PollLogAction::Recovered);
    }

    // -- extract_content -------------------------------------------------------

    #[test]
    fn extract_text_only() {
        let items = vec![make_text_item("Hello world")];
        let (ct, text, has_media) = extract_content(&items);
        assert_eq!(ct, MessageContentType::Text);
        assert_eq!(text, "Hello world");
        assert!(!has_media);
    }

    #[test]
    fn extract_command() {
        let items = vec![make_text_item("/start")];
        let (ct, text, _) = extract_content(&items);
        assert_eq!(ct, MessageContentType::Command);
        assert_eq!(text, "/start");
    }

    #[test]
    fn extract_voice_text() {
        let items = vec![WeixinRawItem {
            item_type: Some(ITEM_TYPE_VOICE),
            voice_item: Some(super::super::types::VoiceItem {
                text: Some("transcribed text".into()),
            }),
            ..Default::default()
        }];
        let (ct, text, _) = extract_content(&items);
        assert_eq!(ct, MessageContentType::Text);
        assert_eq!(text, "transcribed text");
    }

    #[test]
    fn extract_mixed_text_and_voice() {
        let items = vec![
            make_text_item("Hello"),
            WeixinRawItem {
                item_type: Some(ITEM_TYPE_VOICE),
                voice_item: Some(super::super::types::VoiceItem {
                    text: Some("voice part".into()),
                }),
                ..Default::default()
            },
        ];
        let (_, text, _) = extract_content(&items);
        assert_eq!(text, "Hello\n\nvoice part");
    }

    #[test]
    fn extract_media_items_detected() {
        let items = vec![WeixinRawItem {
            item_type: Some(2),
            image_item: Some(super::super::types::MediaItemData {
                media: Some(super::super::types::MediaEncryptInfo {
                    full_url: None,
                    encrypt_query_param: Some("enc".into()),
                    aes_key: Some("key".into()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }];
        let (_, text, has_media) = extract_content(&items);
        assert!(text.is_empty());
        assert!(has_media);
    }

    #[test]
    fn extract_empty_items() {
        let items: Vec<WeixinRawItem> = vec![];
        let (_, text, has_media) = extract_content(&items);
        assert!(text.is_empty());
        assert!(!has_media);
    }

    // -- WeixinPlugin constructor -----------------------------------------------

    #[test]
    fn new_plugin_initial_state() {
        let plugin = WeixinPlugin::new();
        assert_eq!(plugin.status(), PluginStatus::Created);
        assert!(plugin.bot_info().is_none());
        assert!(plugin.last_error().is_none());
        assert_eq!(plugin.plugin_type(), PluginType::Weixin);
        assert_eq!(plugin.active_user_count(), 0);
    }

    // -- initialize validation --------------------------------------------------

    #[tokio::test]
    async fn initialize_missing_bot_token_fails() {
        let mut plugin = WeixinPlugin::new();
        let config = make_config(None, Some("acc_1"));
        let callbacks = make_callbacks();
        let result = plugin.initialize(config, callbacks).await;
        assert!(result.is_err());
        assert_eq!(plugin.status(), PluginStatus::Error);
        assert_eq!(plugin.last_error(), Some("Missing WeChat bot_token"));
    }

    #[tokio::test]
    async fn initialize_missing_account_id_fails() {
        let mut plugin = WeixinPlugin::new();
        let config = make_config(Some("tok_1"), None);
        let callbacks = make_callbacks();
        let result = plugin.initialize(config, callbacks).await;
        assert!(result.is_err());
        assert_eq!(plugin.status(), PluginStatus::Error);
        assert_eq!(plugin.last_error(), Some("Missing WeChat account_id"));
    }

    #[tokio::test]
    async fn initialize_empty_bot_token_fails() {
        let mut plugin = WeixinPlugin::new();
        let config = make_config(Some(""), Some("acc_1"));
        let callbacks = make_callbacks();
        let result = plugin.initialize(config, callbacks).await;
        assert!(result.is_err());
        assert_eq!(plugin.status(), PluginStatus::Error);
    }

    // -- Test helpers -----------------------------------------------------------

    fn make_text_item(text: &str) -> WeixinRawItem {
        WeixinRawItem {
            item_type: Some(ITEM_TYPE_TEXT),
            text_item: Some(super::super::types::TextItem {
                text: Some(text.into()),
            }),
            ..Default::default()
        }
    }

    fn make_config(bot_token: Option<&str>, account_id: Option<&str>) -> PluginConfig {
        PluginConfig {
            credentials: PluginCredentials {
                token: None,
                app_id: None,
                app_secret: None,
                encrypt_key: None,
                verification_token: None,
                client_id: None,
                client_secret: None,
                account_id: account_id.map(String::from),
                bot_token: bot_token.map(String::from),
                app_token: None,
                extra: HashMap::new(),
            },
            config: None,
        }
    }

    fn make_callbacks() -> PluginCallbacks {
        let (message_tx, _) = tokio::sync::mpsc::channel(16);
        let (confirm_tx, _) = tokio::sync::mpsc::channel(16);
        PluginCallbacks { message_tx, confirm_tx }
    }
}

#[cfg(test)]
mod received_file_tests {
    use super::*;
    #[tokio::test]
    async fn pure_file_is_forwarded_with_opaque_media_reference() {
        let msg: super::super::types::WeixinRawMessage=serde_json::from_value(serde_json::json!({"msg_id":"fixture-message","from_user_id":"paired-user","item_list":[{"type":4,"file_item":{"file_name":"工程合同.docx","media":{"encrypt_query_param":"private-download-ticket","aes_key":"private-key"}}}]})).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let contexts = DashMap::new();
        let pending = DashMap::new();
        handle_message(&msg, &tx, &contexts, &pending).await;
        let incoming = rx.recv().await.unwrap();
        assert_eq!(incoming.content.content_type, MessageContentType::Document);
        assert_eq!(incoming.content.attachments.as_ref().unwrap().len(), 1);
        assert!(incoming.raw.is_none());
        let reference = incoming.content.attachments.unwrap()[0].file_id.clone().unwrap();
        assert!(pending.contains_key(&reference));
        assert!(!reference.contains("private"));
    }
}
