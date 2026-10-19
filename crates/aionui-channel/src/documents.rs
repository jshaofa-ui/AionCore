//! Received files are archived before model dispatch. No credentials enter notes.
use crate::{error::ChannelError, types::UnifiedIncomingMessage};
use aionui_api_types::ChatFileRef;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
pub const MAX_DOCUMENT_BYTES: usize = 50 * 1024 * 1024;
pub struct DownloadedAttachment {
    pub name: String,
    pub bytes: Vec<u8>,
}
pub struct PreparedDocuments {
    pub files: Vec<ChatFileRef>,
    pub instruction: String,
}
fn error(message: &str) -> ChannelError {
    ChannelError::MessageSendFailed(message.to_owned())
}
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), ChannelError> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|_| error("Cannot store received attachment"))?;
    file.write_all(bytes)
        .map_err(|_| error("Cannot store received attachment"))
}
pub async fn archive(
    policy_path: &Path,
    owner: &str,
    message: &UnifiedIncomingMessage,
    attachments: Vec<DownloadedAttachment>,
) -> Result<PreparedDocuments, ChannelError> {
    if attachments.is_empty() {
        return Ok(PreparedDocuments {
            files: vec![],
            instruction: String::new(),
        });
    }
    let bytes = std::fs::read(policy_path).map_err(|_| error("附件已识别，但尚未配置公司资料库归档入口"))?;
    let policy: Value = serde_json::from_slice(&bytes).map_err(|_| error("Invalid document archive policy"))?;
    if policy["enabled"] != true || policy["owner_user_id"] != owner {
        return Err(error("Document archive is not enabled for this channel owner"));
    }
    let vault = PathBuf::from(policy["vault"].as_str().ok_or_else(|| error("Missing archive vault"))?)
        .canonicalize()
        .map_err(|_| error("Archive vault is unavailable"))?;
    let command = policy["command"]
        .as_array()
        .ok_or_else(|| error("Missing archive command"))?;
    let arguments: Vec<&str> = command
        .iter()
        .map(|v| v.as_str().ok_or_else(|| error("Invalid archive command")))
        .collect::<Result<_, _>>()?;
    if arguments.is_empty() {
        return Err(error("Missing archive command"));
    }
    let digest = format!(
        "{:x}",
        Sha256::digest(format!("{owner}:{}:{}", message.platform, message.id))
    );
    let folder = policy_path
        .parent()
        .ok_or_else(|| error("Invalid archive policy path"))?
        .join("channel-received-files")
        .join(digest);
    std::fs::create_dir_all(&folder).map_err(|_| error("Cannot create attachment cache"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&folder, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| error("Cannot protect attachment cache"))?;
    }
    let mut files = vec![];
    for (i, item) in attachments.into_iter().enumerate() {
        if item.bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(error("Attachment exceeds the configured size limit"));
        }
        let ext = Path::new(&item.name)
            .extension()
            .and_then(|v| v.to_str())
            .filter(|v| v.len() <= 10 && v.chars().all(|c| c.is_ascii_alphanumeric()))
            .unwrap_or("bin");
        let path = folder.join(format!("file-{i}.{ext}"));
        write_private(&path, &item.bytes)?;
        files.push(json!({"path":path,"name":item.name}));
    }
    let request = folder.join("archive-request.json");
    write_private(&request,serde_json::to_string(&json!({"vault":vault,"channel":message.platform.to_string(),"message_id":message.id,"source_timestamp_seconds":message.timestamp,"files":files})).map_err(|_|error("Cannot encode archive request"))?.as_bytes())?;
    let mut process = aionui_runtime::Builder::clean_cli(arguments[0]);
    process.args(&arguments[1..]).arg("--request").arg(&request);
    for (name, _) in std::env::vars_os() {
        let key = name.to_string_lossy();
        if key.contains("TOKEN") || key.contains("SECRET") || key.contains("API_KEY") || key.starts_with("AIONUI_") {
            process.env_remove(name);
        }
    }
    let output = tokio::time::timeout(std::time::Duration::from_secs(120), process.output())
        .await
        .map_err(|_| error("Document archiving timed out; received originals are retained"))?
        .map_err(|_| error("Could not launch document archiver"))?;
    if !output.status.success() {
        return Err(error("Document archiving failed; received originals are retained"));
    }
    let result: Value = serde_json::from_slice(&output.stdout).map_err(|_| error("Invalid archive receipt"))?;
    let documents = result["documents"]
        .as_array()
        .ok_or_else(|| error("Missing archive receipt"))?;
    if documents.is_empty() {
        return Err(error("No documents were archived"));
    }
    let mut refs = vec![];
    for doc in documents {
        for field in ["note_path", "text_path"] {
            if let Some(p) = doc[field].as_str() {
                let path = Path::new(p)
                    .canonicalize()
                    .map_err(|_| error("Archived document is missing"))?;
                if !path.starts_with(&vault) || !path.is_file() {
                    return Err(error("Archive receipt points outside the vault"));
                }
                refs.push(ChatFileRef::Local {
                    path: path.to_string_lossy().into_owned(),
                });
            }
        }
    }
    Ok(PreparedDocuments{files:refs,instruction:"附件已保存到Obsidian公司资料库，附带原件、来源及提取状态。请优先读取整理笔记及提取正文，按项目整理摘要、合同双方、金额、日期、关键条款和标签，更新对应笔记。此批工程合同用户允许云端处理，不强制本地推理；其他资料沿用其授权。未提取/OCR待处理或不确定内容须标待核对，不得声称读过不存在的正文。原件、来源及版本不得覆盖删除。".to_owned()})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MessageContentType, PluginType, UnifiedMessageContent, UnifiedUser};
    fn incoming() -> UnifiedIncomingMessage {
        UnifiedIncomingMessage {
            owner_user_id: Some("owner".into()),
            id: "file-event".into(),
            platform: PluginType::Weixin,
            chat_id: "private-chat".into(),
            user: UnifiedUser {
                id: "paired".into(),
                username: None,
                display_name: "fixture".into(),
                avatar_url: None,
            },
            content: UnifiedMessageContent {
                content_type: MessageContentType::Document,
                text: String::new(),
                attachments: None,
            },
            timestamp: 0,
            reply_to_message_id: None,
            action: None,
            raw: None,
        }
    }
    #[tokio::test]
    async fn owner_scope_is_checked_before_running_archiver() {
        let t = tempfile::tempdir().unwrap();
        let policy = t.path().join("policy.json");
        std::fs::write(
            &policy,
            json!({"enabled":true,"owner_user_id":"different-owner"}).to_string(),
        )
        .unwrap();
        assert!(
            archive(
                &policy,
                "owner",
                &incoming(),
                vec![DownloadedAttachment {
                    name: "a.txt".into(),
                    bytes: b"document".to_vec()
                }]
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn native_archive_passes_files_and_requires_vault_containment() {
        let t = tempfile::tempdir().unwrap();
        let vault = t.path().join("vault");
        std::fs::create_dir(&vault).unwrap();
        let helper = t.path().join("fixture.py");
        std::fs::write(&helper,"import sys,json,pathlib\nr=json.load(open(sys.argv[2]));v=pathlib.Path(r['vault']);p=v/'received.md';p.write_text('received '+pathlib.Path(r['files'][0]['path']).read_text());print(json.dumps({'documents':[{'note_path':str(p),'text_path':None}]}))").unwrap();
        let policy = t.path().join("policy.json");
        std::fs::write(
            &policy,
            json!({"enabled":true,"owner_user_id":"owner","vault":vault,"command":["python3",helper]}).to_string(),
        )
        .unwrap();
        let result = archive(
            &policy,
            "owner",
            &incoming(),
            vec![DownloadedAttachment {
                name: "工程合同.txt".into(),
                bytes: b"fixture".to_vec(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(result.files.len(), 1);
        assert!(
            std::fs::read_to_string(vault.join("received.md"))
                .unwrap()
                .contains("fixture")
        );
    }
}
