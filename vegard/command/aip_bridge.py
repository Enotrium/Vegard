"""AIP Bridge - Clean Vegard→AIP data pipeline

POSTs structured SoilPrediction payloads to AIP's `/api/syndar/ingest`.
Translates Vegard entity model to AIP farm pipeline schema.
Zero circular coupling - AIP never imports Vegard.
"""

import asyncio
import json
from dataclasses import dataclass
from typing import Optional

import httpx
import structlog
from pydantic import BaseModel, Field
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from syndar.fabric.attestation import AttestationService, SignedPayload
from syndar.fabric.mesh import EntityState

logger = structlog.get_logger()


class AIPSoilPayload(BaseModel):
    """AIP-compatible soil prediction payload"""

    # AIP schema fields
    syndar_scan_id: str
    syndar_drone_id: str
    syndar_timestamp_ms: int
    field_id: str
    farm_id: Optional[str] = None

    # Soil data
    nitrogen_mg_kg: Optional[float] = None
    carbon_percent: Optional[float] = None
    phosphorus_mg_kg: Optional[float] = None
    potassium_mg_kg: Optional[float] = None
    moisture_percent: Optional[float] = None
    ph: Optional[float] = None

    # Contamination
    microplastic_detected: bool = False
    microplastic_types: list[str] = Field(default_factory=list)
    pfas_detected: bool = False
    heavy_metals_detected: bool = False

    # Derived scores
    land_value_score: float = 0.0
    remediation_priority: float = 0.0
    phytoremediation_suitability: float = 0.0

    # Provenance
    spectral_hash: str = ""
    model_version: str = ""
    signature: str = ""
    signer_fingerprint: str = ""

    # Geo data
    latitude: float = 0.0
    longitude: float = 0.0
    accuracy_m: float = 1.0


@dataclass
class AIPBridgeConfig:
    """AIP bridge configuration"""

    base_url: str = "http://localhost:3000"
    ingest_endpoint: str = "/api/syndar/ingest"
    api_key: Optional[str] = None
    timeout_s: float = 30.0
    max_retries: int = 3
    batch_size: int = 10
    batch_interval_s: float = 5.0


class AIPBridge:
    """Bridge from Vegard to AIP farm pipeline"""

    def __init__(
        self,
        config: AIPBridgeConfig,
        attestation: Optional[AttestationService] = None,
    ):
        self.config = config
        self.attestation = attestation
        self._client: Optional[httpx.AsyncClient] = None
        self._batch: list[AIPSoilPayload] = []
        self._batch_task: Optional[asyncio.Task] = None
        self._running = False
        self._stats = {
            "payloads_sent": 0,
            "payloads_failed": 0,
            "batches_sent": 0,
        }

    async def start(self) -> None:
        """Start AIP bridge"""
        self._running = True

        headers = {}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout_s,
            headers=headers,
        )

        # Start batch flush loop
        self._batch_task = asyncio.create_task(self._batch_flush_loop())

        logger.info("AIP bridge started", base_url=self.config.base_url)

    async def stop(self) -> None:
        """Stop AIP bridge and flush pending batches"""
        self._running = False

        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

        # Flush remaining batch
        if self._batch:
            await self._send_batch()

        if self._client:
            await self._client.aclose()

        logger.info("AIP bridge stopped")

    async def _batch_flush_loop(self) -> None:
        """Periodic batch flush"""
        while self._running:
            try:
                await asyncio.sleep(self.config.batch_interval_s)
                if len(self._batch) >= self.config.batch_size:
                    await self._send_batch()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Batch flush error")

    async def ingest_entity(self, entity: EntityState) -> bool:
        """Ingest single entity to AIP"""
        if not entity.soil:
            return False

        payload = self._convert_entity_to_aip(entity)

        # Add to batch for efficiency
        self._batch.append(payload)

        if len(self._batch) >= self.config.batch_size:
            await self._send_batch()

        return True

    async def ingest_soil_prediction(
        self,
        entity: EntityState,
        signed_payload: Optional[SignedPayload] = None,
    ) -> bool:
        """Ingest signed soil prediction to AIP"""
        payload = self._convert_entity_to_aip(entity)

        if signed_payload:
            payload.signature = signed_payload.signature
            payload.signer_fingerprint = signed_payload.signer_fingerprint

        return await self._send_single(payload)

    def _convert_entity_to_aip(self, entity: EntityState) -> AIPSoilPayload:
        """Convert Vegard entity to AIP payload"""
        soil = entity.soil

        # Map nutrients
        nitrogen = soil.nutrient_map.get("nitrogen") if soil else None
        carbon = soil.nutrient_map.get("carbon") if soil else None
        phosphorus = soil.nutrient_map.get("phosphorus") if soil else None
        potassium = soil.nutrient_map.get("potassium") if soil else None
        moisture = soil.nutrient_map.get("moisture") if soil else None
        ph = soil.nutrient_map.get("ph") if soil else None

        # Extract contaminants
        microplastic_types = []
        pfas_detected = False
        heavy_metals_detected = False

        if soil and soil.contaminants:
            for c in soil.contaminants:
                if c.type in ("PE", "PP", "PA", "PS", "PET"):
                    microplastic_types.append(c.type)
                elif c.type == "PFAS":
                    pfas_detected = True
                elif c.type in ("lead", "cadmium", "arsenic", "mercury"):
                    heavy_metals_detected = True

        return AIPSoilPayload(
            syndar_scan_id=soil.scan_id if soil else "unknown",
            syndar_drone_id=entity.entity_id,
            syndar_timestamp_ms=entity.timestamp_ms,
            field_id=soil.field_id if soil else "unknown",
            nitrogen_mg_kg=nitrogen,
            carbon_percent=carbon,
            phosphorus_mg_kg=phosphorus,
            potassium_mg_kg=potassium,
            moisture_percent=moisture,
            ph=ph,
            microplastic_detected=len(microplastic_types) > 0,
            microplastic_types=microplastic_types,
            pfas_detected=pfas_detected,
            heavy_metals_detected=heavy_metals_detected,
            land_value_score=soil.land_value_score if soil else 0.0,
            remediation_priority=soil.remediation_priority if soil else 0.0,
            phytoremediation_suitability=soil.phytoremediation_suitability if soil else 0.0,
            spectral_hash=soil.spectral_hash if soil else "",
            model_version=soil.model_version if soil else "",
            latitude=entity.position.lat,
            longitude=entity.position.lng,
            accuracy_m=entity.position.accuracy,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
    )
    async def _send_single(self, payload: AIPSoilPayload) -> bool:
        """Send single payload to AIP"""
        if not self._client:
            return False

        try:
            response = await self._client.post(
                self.config.ingest_endpoint,
                json=payload.model_dump(exclude_none=True),
            )
            response.raise_for_status()

            self._stats["payloads_sent"] += 1

            logger.debug(
                "Payload ingested to AIP",
                scan_id=payload.syndar_scan_id,
                field_id=payload.field_id,
            )
            return True

        except httpx.HTTPStatusError as e:
            self._stats["payloads_failed"] += 1
            logger.error(
                "AIP ingest failed",
                status_code=e.response.status_code,
                scan_id=payload.syndar_scan_id,
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError)),
    )
    async def _send_batch(self) -> bool:
        """Send batched payloads to AIP"""
        if not self._client or not self._batch:
            return False

        batch_to_send = self._batch[: self.config.batch_size]
        self._batch = self._batch[self.config.batch_size :]

        try:
            response = await self._client.post(
                f"{self.config.ingest_endpoint}/batch",
                json={
                    "payloads": [p.model_dump(exclude_none=True) for p in batch_to_send]
                },
            )
            response.raise_for_status()

            self._stats["batches_sent"] += 1
            self._stats["payloads_sent"] += len(batch_to_send)

            logger.info(
                "Batch ingested to AIP",
                count=len(batch_to_send),
                field_ids=[p.field_id for p in batch_to_send],
            )
            return True

        except httpx.HTTPStatusError as e:
            self._stats["payloads_failed"] += len(batch_to_send)
            logger.error(
                "AIP batch ingest failed",
                status_code=e.response.status_code,
                count=len(batch_to_send),
            )
            # Re-queue failed payloads
            self._batch = batch_to_send + self._batch
            raise

    async def verify_field_exists(self, field_id: str) -> bool:
        """Check if field exists in AIP"""
        if not self._client:
            return False

        try:
            response = await self._client.get(f"/api/farms/{field_id}")
            return response.status_code == 200
        except Exception:
            return False

    async def get_field_contract_status(self, field_id: str) -> Optional[dict]:
        """Get contract status for field from AIP"""
        if not self._client:
            return None

        try:
            response = await self._client.get(f"/api/contracts/field/{field_id}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None

    def get_stats(self) -> dict:
        """Get bridge statistics"""
        return {
            **self._stats,
            "pending_batch": len(self._batch),
            "connected": self._client is not None,
        }
