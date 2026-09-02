use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// AES-256-GCM encryption for data at rest.
/// Each shard file gets a unique nonce per write.
pub struct EncryptionEngine {
    cipher: Aes256Gcm,
}

/// Encrypted blob: nonce + ciphertext + tag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedBlob {
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

impl EncryptionEngine {
    /// Create from a 32-byte key
    pub fn new(key: &[u8; 32]) -> Result<Self> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| Error::Internal(format!("Failed to create cipher: {}", e)))?;
        Ok(Self { cipher })
    }

    /// Create from a hex-encoded key
    pub fn from_hex_key(hex_key: &str) -> Result<Self> {
        let key_bytes = hex::decode(hex_key)
            .map_err(|e| Error::Internal(format!("Invalid hex key: {}", e)))?;
        if key_bytes.len() != 32 {
            return Err(Error::Internal(format!(
                "Key must be 32 bytes, got {}",
                key_bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes);
        Self::new(&key)
    }

    /// Encrypt plaintext bytes
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedBlob> {
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| Error::Internal(format!("Encryption failed: {}", e)))?;

        Ok(EncryptedBlob {
            nonce: nonce_bytes.to_vec(),
            ciphertext,
        })
    }

    /// Decrypt an encrypted blob
    pub fn decrypt(&self, blob: &EncryptedBlob) -> Result<Vec<u8>> {
        let nonce = Nonce::from_slice(&blob.nonce);
        self.cipher
            .decrypt(nonce, blob.ciphertext.as_ref())
            .map_err(|e| Error::Internal(format!("Decryption failed: {}", e)))
    }
}

/// Generate a random 256-bit encryption key
pub fn generate_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = generate_key();
        let engine = EncryptionEngine::new(&key).unwrap();

        let plaintext = b"Hello, Muninn! This is sensitive data.";
        let encrypted = engine.encrypt(plaintext).unwrap();
        let decrypted = engine.decrypt(&encrypted).unwrap();

        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_different_nonces() {
        let key = generate_key();
        let engine = EncryptionEngine::new(&key).unwrap();

        let plaintext = b"Same plaintext";
        let enc1 = engine.encrypt(plaintext).unwrap();
        let enc2 = engine.encrypt(plaintext).unwrap();

        // Nonces should be different (probability of collision is negligible)
        assert_ne!(enc1.nonce, enc2.nonce);
        // Ciphertexts should be different due to different nonces
        assert_ne!(enc1.ciphertext, enc2.ciphertext);
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = generate_key();
        let key2 = generate_key();
        let engine1 = EncryptionEngine::new(&key1).unwrap();
        let engine2 = EncryptionEngine::new(&key2).unwrap();

        let encrypted = engine1.encrypt(b"secret").unwrap();
        assert!(engine2.decrypt(&encrypted).is_err());
    }

    #[test]
    fn test_hex_key() {
        let key = generate_key();
        let hex_key = hex::encode(key);
        let engine = EncryptionEngine::from_hex_key(&hex_key).unwrap();

        let encrypted = engine.encrypt(b"test").unwrap();
        let decrypted = engine.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, b"test");
    }
}
