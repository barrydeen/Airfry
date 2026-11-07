//! Key handling and BIP-340 schnorr helpers used by the relay, the store tests and the bench tool.

use crate::{CoreError, Event};
use secp256k1::schnorr::Signature;
use secp256k1::{Keypair, Message, Secp256k1, SecretKey, XOnlyPublicKey};
use sha2::{Digest, Sha256};

/// Verify a BIP-340 schnorr signature over a 32-byte message.
///
/// Uses the process-wide precomputed verification context: building a fresh `Secp256k1`
/// context costs ~1ms, which would dominate a per-event verification path.
pub fn verify_schnorr(pubkey: &[u8; 32], msg: &[u8; 32], sig: &[u8; 64]) -> Result<(), CoreError> {
    let pk = XOnlyPublicKey::from_slice(pubkey).map_err(|_| CoreError::BadSignature)?;
    let signature = Signature::from_slice(sig).map_err(|_| CoreError::BadSignature)?;
    let message = Message::from_digest(*msg);
    secp256k1::SECP256K1
        .verify_schnorr(&signature, &message, &pk)
        .map_err(|_| CoreError::BadSignature)
}

/// Verify a batch of `(pubkey, msg, sig)` triples, returning the index of the first failure.
///
/// BIP-340 batch verification is not exposed by `secp256k1` 0.29, so this simply loops; it
/// exists so callers have one place to swap in a batched implementation later.
pub fn verify_schnorr_batch(items: &[(&[u8; 32], &[u8; 32], &[u8; 64])]) -> Result<(), usize> {
    for (i, (pk, msg, sig)) in items.iter().enumerate() {
        if verify_schnorr(pk, msg, sig).is_err() {
            return Err(i);
        }
    }
    Ok(())
}

/// A secret key plus its x-only public key, with helpers to sign Nostr events.
#[derive(Clone)]
pub struct Keys {
    secret: SecretKey,
    pub public_key: [u8; 32],
}

impl Keys {
    /// Generate a fresh random keypair.
    pub fn generate() -> Keys {
        let secp = Secp256k1::new();
        let (secret, _) = secp.generate_keypair(&mut rand::thread_rng());
        Keys::from_secret(&secret)
    }

    /// Build from a raw 32-byte secret key.
    pub fn from_secret_bytes(bytes: &[u8; 32]) -> Result<Keys, CoreError> {
        let secret = SecretKey::from_slice(bytes).map_err(|_| CoreError::BadSignature)?;
        Ok(Keys::from_secret(&secret))
    }

    fn from_secret(secret: &SecretKey) -> Keys {
        let secp = Secp256k1::new();
        let kp = Keypair::from_secret_key(&secp, secret);
        let (xonly, _) = kp.x_only_public_key();
        Keys {
            secret: *secret,
            public_key: xonly.serialize(),
        }
    }

    /// Deterministic key derived from a label, useful for reproducible benchmarks.
    pub fn from_label(label: &str) -> Keys {
        let mut h = Sha256::new();
        h.update(b"nostr-core-keys");
        h.update(label.as_bytes());
        let d = h.finalize();
        let mut b = [0u8; 32];
        b.copy_from_slice(&d);
        Keys::from_secret_bytes(&b).expect("sha256 output is a valid secret key")
    }

    /// Public key as lowercase hex.
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.public_key)
    }

    /// Sign an arbitrary 32-byte digest.
    pub fn sign_digest(&self, digest: &[u8; 32]) -> [u8; 64] {
        let kp = Keypair::from_secret_key(secp256k1::SECP256K1, &self.secret);
        let msg = Message::from_digest(*digest);
        let sig = secp256k1::SECP256K1.sign_schnorr_no_aux_rand(&msg, &kp);
        *sig.as_ref()
    }

    /// Build and sign a complete event.
    pub fn sign_event(
        &self,
        created_at: u64,
        kind: u32,
        tags: Vec<Vec<String>>,
        content: &str,
    ) -> Event {
        let mut ev = Event {
            id: [0u8; 32],
            pubkey: self.public_key,
            created_at,
            kind,
            tags,
            content: content.to_string(),
            sig: [0u8; 64],
        };
        ev.id = ev.compute_id();
        ev.sig = self.sign_digest(&ev.id);
        ev
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_keys() {
        let a = Keys::from_label("alice");
        let b = Keys::from_label("alice");
        assert_eq!(a.public_key, b.public_key);
        let c = Keys::from_label("bob");
        assert_ne!(a.public_key, c.public_key);
    }

    #[test]
    fn verify_rejects_bad_sig() {
        let keys = Keys::generate();
        let ev = keys.sign_event(1, 1, vec![], "x");
        let mut sig = ev.sig;
        sig[0] ^= 0xff;
        assert!(verify_schnorr(&ev.pubkey, &ev.id, &sig).is_err());
    }

    #[test]
    fn batch_verify_reports_index() {
        let a = Keys::from_label("batch-a");
        let b = Keys::from_label("batch-b");
        let e1 = a.sign_event(1, 1, vec![], "one");
        let e2 = b.sign_event(2, 1, vec![], "two");
        let items = [(&e1.pubkey, &e1.id, &e1.sig), (&e2.pubkey, &e2.id, &e2.sig)];
        assert_eq!(verify_schnorr_batch(&items), Ok(()));
        let mut bad = e2.sig;
        bad[5] ^= 0x01;
        let items = [(&e1.pubkey, &e1.id, &e1.sig), (&e2.pubkey, &e2.id, &bad)];
        assert_eq!(verify_schnorr_batch(&items), Err(1));
    }
}
