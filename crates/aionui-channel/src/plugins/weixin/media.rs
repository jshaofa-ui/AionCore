//! iLink media protocol verified against the installed Hermes Weixin adapter.
use super::types::MediaItemData;
use crate::{
    documents::{DownloadedAttachment, MAX_DOCUMENT_BYTES},
    error::ChannelError,
};
use aes::cipher::{BlockDecrypt, KeyInit};
use base64::Engine;
fn err(message: &str) -> ChannelError {
    ChannelError::PlatformApi(message.to_owned())
}
fn parse_key(value: &str) -> Result<Vec<u8>, ChannelError> {
    let decoded = if value.len() == 32 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        value.as_bytes().to_vec()
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|_| err("Invalid attachment encryption key"))?
    };
    if decoded.len() == 16 {
        return Ok(decoded);
    }
    if decoded.len() == 32 && decoded.iter().all(|b| b.is_ascii_hexdigit()) {
        return (0..32)
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(
                    std::str::from_utf8(&decoded[i..i + 2]).map_err(|_| err("Invalid attachment key"))?,
                    16,
                )
                .map_err(|_| err("Invalid attachment key"))
            })
            .collect();
    }
    Err(err("Invalid attachment key size"))
}
fn decrypt(mut bytes: Vec<u8>, key: &str) -> Result<Vec<u8>, ChannelError> {
    let key = parse_key(key)?;
    if bytes.is_empty() || !bytes.len().is_multiple_of(16) {
        return Err(err("Invalid encrypted attachment length"));
    }
    let cipher = aes::Aes128::new_from_slice(&key).map_err(|_| err("Invalid attachment key"))?;
    for part in bytes.chunks_exact_mut(16) {
        let mut block = aes::cipher::Block::<aes::Aes128>::default();
        block.copy_from_slice(part);
        cipher.decrypt_block(&mut block);
        part.copy_from_slice(&block);
    }
    let pad = *bytes.last().ok_or_else(|| err("Empty attachment"))? as usize;
    if pad == 0 || pad > 16 || !bytes[bytes.len() - pad..].iter().all(|b| *b as usize == pad) {
        return Err(err("Attachment decryption padding failed"));
    }
    bytes.truncate(bytes.len() - pad);
    Ok(bytes)
}
pub async fn download(item: &MediaItemData, image: bool) -> Result<DownloadedAttachment, ChannelError> {
    let media = item
        .media
        .as_ref()
        .ok_or_else(|| err("Attachment has no download metadata"))?;
    let mut url = if let Some(param) = media.encrypt_query_param.as_deref() {
        let mut url = reqwest::Url::parse("https://novac2c.cdn.weixin.qq.com/c2c/download").expect("fixed CDN");
        url.query_pairs_mut().append_pair("encrypted_query_param", param);
        url
    } else {
        reqwest::Url::parse(media.full_url.as_deref().ok_or_else(|| err("Missing attachment URL"))?)
            .map_err(|_| err("Invalid attachment URL"))?
    };
    let allowed = [
        "novac2c.cdn.weixin.qq.com",
        "ilinkai.weixin.qq.com",
        "wx.qlogo.cn",
        "thirdwx.qlogo.cn",
        "res.wx.qq.com",
        "mmbiz.qpic.cn",
        "mmbiz.qlogo.cn",
    ];
    if url.scheme() != "https"
        || !allowed.contains(&url.host_str().unwrap_or(""))
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(err("Attachment URL is outside the WeChat CDN allowlist"));
    }
    url.set_fragment(None);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(90))
        .build()
        .map_err(|_| err("Cannot create attachment downloader"))?;
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| err("WeChat attachment download failed"))?;
    if !response.status().is_success() {
        return Err(err(&format!("WeChat attachment download HTTP {}", response.status())));
    }
    if response
        .content_length()
        .is_some_and(|n| n > MAX_DOCUMENT_BYTES as u64 + 16)
    {
        return Err(err("Attachment exceeds size limit"));
    }
    let mut bytes = vec![];
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| err("Attachment transfer interrupted"))?
    {
        if bytes.len() + chunk.len() > MAX_DOCUMENT_BYTES + 16 {
            return Err(err("Attachment exceeds size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let key = if image {
        item.aeskey.as_deref().or(media.aes_key.as_deref())
    } else {
        media.aes_key.as_deref().or(item.aeskey.as_deref())
    };
    let bytes = if let Some(key) = key {
        decrypt(bytes, key)?
    } else {
        bytes
    };
    Ok(DownloadedAttachment {
        name: item.file_name.clone().unwrap_or_else(|| {
            if image {
                "微信图片.jpg".into()
            } else {
                "微信附件.bin".into()
            }
        }),
        bytes,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_accept_both_ilink_encodings() {
        let raw = [7u8; 16];
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        assert_eq!(parse_key(&b64).unwrap(), raw);
        let hex = "07070707070707070707070707070707";
        assert_eq!(
            parse_key(&base64::engine::general_purpose::STANDARD.encode(hex)).unwrap(),
            raw
        );
    }
    #[test]
    fn decrypt_roundtrip_and_bad_padding() {
        use aes::cipher::BlockEncrypt;
        let key = base64::engine::general_purpose::STANDARD.encode([7u8; 16]);
        let mut block = aes::cipher::Block::<aes::Aes128>::default();
        block[..8].copy_from_slice(b"contract");
        block[8..].fill(8);
        aes::Aes128::new_from_slice(&[7u8; 16])
            .unwrap()
            .encrypt_block(&mut block);
        assert_eq!(decrypt(block.to_vec(), &key).unwrap(), b"contract");
        assert!(decrypt(vec![0; 17], &key).is_err());
    }
}
