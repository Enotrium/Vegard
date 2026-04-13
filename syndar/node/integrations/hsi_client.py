"""HSIClient - Interface to Hyperspectral-Restruct CNN

HTTP client to api.py endpoint with retry logic,
payload serialization, mock for sandbox testing.

Dependency: Hyperspectral-Restruct running as sidecar
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import httpx
import structlog
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger()


class SoilPrediction(BaseModel):
    """Soil prediction from HSI model"""

    field_id: str
    scan_id: str
    nutrients: dict[str, float]
    land_value_score: float
    remediation_priority: float
    contamination_detected: bool
    contaminants: list[dict] = Field(default_factory=list)
    spectral_hash: str = ""
    model_version: str = ""
    capture_timestamp_ms: int = 0


class HSIModelInfo(BaseModel):
    """HSI model metadata"""

    version: str
    commit_hash: str
    supported_bands: tuple[int, int]
    accuracy_metrics: dict[str, float]


@dataclass
class HSIConfig:
    """HSI client configuration"""

    base_url: str = "http://localhost:8001"
    timeout_s: float = 30.0
    max_retries: int = 3
    api_version: str = "v1"


class MockHSIService:
    """Mock HSI service for sandbox testing"""

    def __init__(self):
        self._predictions: dict[str, SoilPrediction] = {}
        self._model_version = "mock-1.0.0"

    async def predict(self, cube_payload: dict) -> SoilPrediction:
        """Generate mock prediction"""
        import hashlib
        import random

        field_id = cube_payload.get("field_id", "unknown")

        # Deterministic pseudo-random based on field_id
        seed = int(hashlib.md5(field_id.encode()).hexdigest(), 16)
        random.seed(seed)

        prediction = SoilPrediction(
            field_id=field_id,
            scan_id=f"scan-{int(time.time() * 1000)}",
            nutrients={
                "nitrogen": random.uniform(0.5, 1.0),
                "carbon": random.uniform(1.5, 3.0),
                "phosphorus": random.uniform(0.3, 0.8),
                "potassium": random.uniform(0.4, 0.9),
                "moisture": random.uniform(0.2, 0.6),
                "ph": random.uniform(5.5, 7.5),
            },
            land_value_score=random.uniform(0.6, 0.95),
            remediation_priority=random.uniform(0.1, 0.5),
            contamination_detected=random.random() < 0.1,
            model_version=self._model_version,
            capture_timestamp_ms=cube_payload.get("capture_timestamp_ms", int(time.time() * 1000)),
        )

        if prediction.contamination_detected:
            prediction.contaminants = [
                {
                    "type": random.choice(["PE", "PP", "PFAS"]),
                    "concentration": random.uniform(0.001, 0.01),
                    "confidence": random.uniform(0.7, 0.95),
                }
            ]

        self._predictions[field_id] = prediction
        return prediction

    def get_model_info(self) -> HSIModelInfo:
        """Get mock model info"""
        return HSIModelInfo(
            version=self._model_version,
            commit_hash="mock-commit-123456",
            supported_bands=(400, 2500),
            accuracy_metrics={
                "nitrogen_rmse": 0.05,
                "carbon_rmse": 0.1,
                "microplastic_f1": 0.88,
            },
        )


class HSIClient:
    """Client for Hyperspectral-Restruct soil prediction API"""

    def __init__(self, config: Optional[HSIConfig] = None):
        self.config = config or HSIConfig()
        self._client: Optional[httpx.AsyncClient] = None
        self._mock_service = MockHSIService()
        self._last_prediction: Optional[SoilPrediction] = None
        self._use_mock = False

    async def start(self) -> None:
        """Start HSI client"""
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout_s,
        )

        # Try to connect, fallback to mock if unavailable
        try:
            await self._health_check()
            logger.info("HSI client connected", base_url=self.config.base_url)
        except Exception as e:
            logger.warning(
                "HSI service unavailable, using mock",
                base_url=self.config.base_url,
                error=str(e),
            )
            self._use_mock = True

    async def stop(self) -> None:
        """Stop HSI client"""
        if self._client:
            await self._client.aclose()

    async def _health_check(self) -> bool:
        """Check if HSI service is healthy"""
        if not self._client:
            return False

        try:
            response = await self._client.get("/health")
            return response.status_code == 200
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def predict(
        self, cube_payload: dict, timeout: Optional[float] = None
    ) -> SoilPrediction:
        """Get soil prediction from spectral cube"""
        if self._use_mock:
            return await self._mock_service.predict(cube_payload)

        if not self._client:
            raise RuntimeError("HSI client not started")

        try:
            response = await self._client.post(
                f"/api/{self.config.api_version}/predict",
                json=cube_payload,
                timeout=timeout or self.config.timeout_s,
            )
            response.raise_for_status()

            data = response.json()
            prediction = SoilPrediction(**data)
            self._last_prediction = prediction

            logger.info(
                "HSI prediction received",
                field_id=prediction.field_id,
                land_value=prediction.land_value_score,
            )

            return prediction

        except httpx.HTTPStatusError as e:
            logger.error(
                "HSI API error",
                status_code=e.response.status_code,
                response=e.response.text,
            )
            raise

    async def get_model_info(self) -> HSIModelInfo:
        """Get HSI model information"""
        if self._use_mock:
            return self._mock_service.get_model_info()

        if not self._client:
            raise RuntimeError("HSI client not started")

        try:
            response = await self._client.get(
                f"/api/{self.config.api_version}/model/info"
            )
            response.raise_for_status()
            return HSIModelInfo(**response.json())
        except Exception as e:
            logger.error("Failed to get model info", error=str(e))
            return self._mock_service.get_model_info()

    async def verify_spectral_hash(
        self, spectral_data: bytes, claimed_hash: str
    ) -> bool:
        """Verify spectral data hash for provenance"""
        import hashlib

        actual_hash = hashlib.sha256(spectral_data).hexdigest()
        return actual_hash == claimed_hash

    def get_last_prediction(self) -> Optional[SoilPrediction]:
        """Get last successful prediction"""
        return self._last_prediction

    def enable_mock(self) -> None:
        """Force use of mock service"""
        self._use_mock = True
        logger.info("HSI mock mode enabled")

    def disable_mock(self) -> None:
        """Try to use real service"""
        self._use_mock = False
        logger.info("HSI mock mode disabled")

    def is_mock(self) -> bool:
        """Check if using mock service"""
        return self._use_mock

    def get_stats(self) -> dict:
        """Get client statistics"""
        return {
            "base_url": self.config.base_url,
            "use_mock": self._use_mock,
            "has_last_prediction": self._last_prediction is not None,
            "model_version": (
                self._last_prediction.model_version if self._last_prediction else None
            ),
        }
