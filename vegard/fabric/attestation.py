"""Node identity and signed outputs - cryptographic attestation

Every soil prediction published to AIP is cryptographically signed
by the drone node that produced it. Feeds into AIP's ZK proof chain.
"""

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import gnupg
import structlog
from pydantic import BaseModel

logger = structlog.get_logger()


class NodeIdentity(BaseModel):
    """Node identity with PGP keys"""

    entity_id: str
    public_key: str
    fingerprint: str
    region: str
    serial: str


class SignedPayload(BaseModel):
    """Signed data with verification metadata"""

    data_hash: str
    signature: str
    signer_fingerprint: str
    timestamp_ms: int
    valid: bool = False


@dataclass
class AttestationConfig:
    gpg_home: Path = Path.home() / ".syndar" / "gnupg"
    key_type: str = "RSA"
    key_length: int = 4096


class AttestationService:
    """PGP-based node attestation"""

    def __init__(self, config: Optional[AttestationConfig] = None):
        self.config = config or AttestationConfig()
        self.config.gpg_home.mkdir(parents=True, mode=0o700, exist_ok=True)
        self._gpg = gnupg.GPG(gnupghome=str(self.config.gpg_home))
        self._keys: dict[str, str] = {}  # entity_id -> fingerprint

    def generate_identity(self, region: str, serial: str) -> NodeIdentity:
        """Generate new node identity with PGP keypair"""
        entity_id = f"drone:{region}:{serial}"

        # Generate key
        key_input = self._gpg.gen_key_input(
            key_type=self.config.key_type,
            key_length=self.config.key_length,
            name_real=entity_id,
            name_comment="Vegard Drone Node",
            name_email=f"{entity_id}@syndar.local",
        )
        key = self._gpg.gen_key(key_input)

        # Export public key
        public_key = self._gpg.export_keys(key.fingerprint)

        # Store
        self._keys[entity_id] = key.fingerprint

        identity = NodeIdentity(
            entity_id=entity_id,
            public_key=public_key,
            fingerprint=key.fingerprint,
            region=region,
            serial=serial,
        )

        logger.info("Generated node identity", entity_id=entity_id, fingerprint=key.fingerprint)
        return identity

    def load_identity(self, entity_id: str) -> Optional[NodeIdentity]:
        """Load existing identity from GPG keyring"""
        keys = self._gpg.list_keys()
        for key in keys:
            uids = key.get("uids", [])
            for uid in uids:
                if entity_id in uid:
                    fingerprint = key["fingerprint"]
                    public_key = self._gpg.export_keys(fingerprint)

                    # Parse region/serial from entity_id
                    parts = entity_id.split(":")
                    region = parts[1] if len(parts) > 1 else "unknown"
                    serial = parts[2] if len(parts) > 2 else "unknown"

                    self._keys[entity_id] = fingerprint

                    return NodeIdentity(
                        entity_id=entity_id,
                        public_key=public_key,
                        fingerprint=fingerprint,
                        region=region,
                        serial=serial,
                    )
        return None

    def sign(self, entity_id: str, data: Union[bytes, str]) -> SignedPayload:
        """Sign data with node's private key"""
        fingerprint = self._keys.get(entity_id)
        if not fingerprint:
            # Try to load
            identity = self.load_identity(entity_id)
            if not identity:
                raise ValueError(f"No identity found for {entity_id}")
            fingerprint = identity.fingerprint

        # Hash data
        if isinstance(data, str):
            data = data.encode("utf-8")
        data_hash = hashlib.sha256(data).hexdigest()

        # Sign
        signed = self._gpg.sign(data, keyid=fingerprint, detach=True)

        return SignedPayload(
            data_hash=data_hash,
            signature=str(signed),
            signer_fingerprint=fingerprint,
            timestamp_ms=int(time.time() * 1000),
            valid=True,
        )

    def verify(
        self, data: Union[bytes, str], signature: str, signer_fingerprint: str
    ) -> bool:
        """Verify signature against data"""
        if isinstance(data, str):
            data = data.encode("utf-8")

        verified = self._gpg.verify(signature, data)
        if verified and verified.fingerprint == signer_fingerprint:
            return True
        return False

    def sign_soil_prediction(
        self, entity_id: str, prediction_data: dict
    ) -> SignedPayload:
        """Sign a soil prediction payload"""
        # Create canonical serialization
        canonical = self._canonicalize_prediction(prediction_data)
        return self.sign(entity_id, canonical)

    def verify_soil_prediction(
        self, prediction_data: dict, signature: SignedPayload
    ) -> bool:
        """Verify a signed soil prediction"""
        canonical = self._canonicalize_prediction(prediction_data)
        return self.verify(canonical, signature.signature, signature.signer_fingerprint)

    def _canonicalize_prediction(self, prediction_data: dict) -> str:
        """Create canonical string representation for signing"""
        # Sort keys for deterministic serialization
        import json

        return json.dumps(prediction_data, sort_keys=True, separators=(",", ":"))

    def get_public_key(self, entity_id: str) -> Optional[str]:
        """Get public key for a node"""
        fingerprint = self._keys.get(entity_id)
        if fingerprint:
            return self._gpg.export_keys(fingerprint)

        # Try loading
        identity = self.load_identity(entity_id)
        return identity.public_key if identity else None

    def import_public_key(self, public_key: str) -> str:
        """Import a peer's public key for verification"""
        import_result = self._gpg.import_keys(public_key)
        if import_result.count > 0:
            logger.info(
                "Imported public key",
                fingerprints=import_result.fingerprints,
            )
            return import_result.fingerprints[0]
        raise ValueError("Failed to import public key")

    def list_identities(self) -> list[NodeIdentity]:
        """List all known identities"""
        identities = []
        for entity_id, fingerprint in self._keys.items():
            identity = self.load_identity(entity_id)
            if identity:
                identities.append(identity)
        return identities
