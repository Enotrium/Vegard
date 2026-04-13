"""SpectralBridge - Arthedain SNN → Hyperspectral-Restruct CNN adapter

Buffers spike stream into spectral cube frames, normalizes for HSI API,
returns structured SoilPrediction protobuf.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import numpy as np
import structlog
from pydantic import BaseModel

logger = structlog.get_logger()


class SpikeEvent(BaseModel):
    """Single spike event from Arthedain SNN"""

    neuron_id: int
    timestamp_ms: int
    weight: float


class SpectralFrame(BaseModel):
    """One frame of spectral data"""

    timestamp_ms: int
    band_data: dict[int, float]  # wavelength_nm -> intensity
    spatial_x: int
    spatial_y: int


class SpectralCube(BaseModel):
    """Complete spectral cube (x, y, λ)"""

    field_id: str
    capture_timestamp_ms: int
    dimensions: tuple[int, int, int]  # x, y, bands
    data: np.ndarray  # Shape: (x, y, bands)
    wavelengths_nm: list[int]
    spatial_resolution_m: float

    class Config:
        arbitrary_types_allowed = True


@dataclass
class BridgeConfig:
    """Spectral bridge configuration"""

    frame_buffer_size: int = 1000
    cube_width: int = 128
    cube_height: int = 128
    band_start_nm: int = 400
    band_end_nm: int = 2500
    band_count: int = 200
    exposure_ms: float = 10.0


class SpectralBridge:
    """Bridge between Arthedain SNN and Hyperspectral-Restruct CNN"""

    def __init__(self, config: Optional[BridgeConfig] = None):
        self.config = config or BridgeConfig()
        self._spike_buffer: deque[SpikeEvent] = deque(
            maxlen=self.config.frame_buffer_size
        )
        self._frame_buffer: deque[SpectralFrame] = deque(maxlen=100)
        self._current_cube: Optional[SpectralCube] = None
        self._cube_in_progress = False
        self._lock = asyncio.Lock()

    async def ingest_spike(self, spike: SpikeEvent) -> None:
        """Ingest spike from Arthedain SNN"""
        async with self._lock:
            self._spike_buffer.append(spike)

    async def ingest_spike_batch(self, spikes: list[SpikeEvent]) -> None:
        """Ingest batch of spikes"""
        async with self._lock:
            for spike in spikes:
                self._spike_buffer.append(spike)

    async def build_cube(
        self, field_id: str, duration_s: float = 30.0
    ) -> Optional[SpectralCube]:
        """Build spectral cube from buffered spikes"""
        async with self._lock:
            if self._cube_in_progress:
                logger.warning("Cube build already in progress")
                return None

            self._cube_in_progress = True

        try:
            logger.info(
                "Building spectral cube",
                field_id=field_id,
                duration=duration_s,
            )

            # Collect spikes for duration
            await asyncio.sleep(duration_s)

            async with self._lock:
                spikes = list(self._spike_buffer)
                self._spike_buffer.clear()

            # Convert spikes to spectral frames
            frames = self._spikes_to_frames(spikes)

            # Build cube from frames
            cube = self._frames_to_cube(field_id, frames)

            self._current_cube = cube

            logger.info(
                "Spectral cube built",
                field_id=field_id,
                dimensions=cube.dimensions,
            )

            return cube

        finally:
            async with self._lock:
                self._cube_in_progress = False

    def _spikes_to_frames(self, spikes: list[SpikeEvent]) -> list[SpectralFrame]:
        """Convert spike stream to spectral frames"""
        frames = []

        # Group spikes by time window
        window_ms = 100
        if not spikes:
            return frames

        min_ts = min(s.timestamp_ms for s in spikes)
        max_ts = max(s.timestamp_ms for s in spikes)

        for window_start in range(min_ts, max_ts, window_ms):
            window_end = window_start + window_ms
            window_spikes = [
                s for s in spikes if window_start <= s.timestamp_ms < window_end
            ]

            if not window_spikes:
                continue

            # Map spikes to spectral bands
            band_data = {}
            for spike in window_spikes:
                # Map neuron_id to wavelength
                wavelength = self._neuron_to_wavelength(spike.neuron_id)
                if wavelength not in band_data:
                    band_data[wavelength] = 0.0
                band_data[wavelength] += spike.weight

            # Normalize
            if band_data:
                max_val = max(band_data.values())
                band_data = {k: v / max_val for k, v in band_data.items()}

            frame = SpectralFrame(
                timestamp_ms=window_start,
                band_data=band_data,
                spatial_x=len(frames) % self.config.cube_width,
                spatial_y=len(frames) // self.config.cube_width,
            )
            frames.append(frame)

        return frames

    def _neuron_to_wavelength(self, neuron_id: int) -> int:
        """Map neuron ID to wavelength in nm"""
        # Linear mapping from neuron_id to wavelength range
        band_range = self.config.band_end_nm - self.config.band_start_nm
        ratio = (neuron_id % self.config.band_count) / self.config.band_count
        return int(self.config.band_start_nm + ratio * band_range)

    def _frames_to_cube(
        self, field_id: str, frames: list[SpectralFrame]
    ) -> SpectralCube:
        """Build 3D spectral cube from frames"""
        width = self.config.cube_width
        height = self.config.cube_height
        bands = self.config.band_count

        # Initialize cube array
        data = np.zeros((width, height, bands), dtype=np.float32)

        # Generate wavelength list
        wavelengths = [
            self.config.band_start_nm
            + i * (self.config.band_end_nm - self.config.band_start_nm) // bands
            for i in range(bands)
        ]

        # Fill cube from frames
        for frame in frames:
            x = min(frame.spatial_x, width - 1)
            y = min(frame.spatial_y, height - 1)

            for wavelength, intensity in frame.band_data.items():
                # Find closest band
                band_idx = min(
                    range(bands),
                    key=lambda i: abs(wavelengths[i] - wavelength),
                )
                data[x, y, band_idx] = intensity

        # Apply preprocessing (matching Hyperspectral-Restruct pipeline)
        data = self._preprocess_cube(data)

        return SpectralCube(
            field_id=field_id,
            capture_timestamp_ms=int(time.time() * 1000),
            dimensions=(width, height, bands),
            data=data,
            wavelengths_nm=wavelengths,
            spatial_resolution_m=0.5,
        )

    def _preprocess_cube(self, data: np.ndarray) -> np.ndarray:
        """Apply preprocessing pipeline"""
        # 1. Smoothing (Savitzky-Golay simplified)
        data = self._smooth_spectral(data)

        # 2. Continuum removal
        data = self._continuum_removal(data)

        # 3. Standard Normal Variate (SNV)
        data = self._snv_normalize(data)

        return data

    def _smooth_spectral(self, data: np.ndarray, window: int = 5) -> np.ndarray:
        """Apply simple moving average smoothing"""
        from scipy.ndimage import uniform_filter1d

        return uniform_filter1d(data, size=window, axis=2, mode="nearest")

    def _continuum_removal(self, data: np.ndarray) -> np.ndarray:
        """Remove continuum (simplified)"""
        # Divide each spectrum by its convex hull (approximated by max)
        max_vals = np.max(data, axis=2, keepdims=True)
        max_vals = np.where(max_vals == 0, 1, max_vals)
        return data / max_vals

    def _snv_normalize(self, data: np.ndarray) -> np.ndarray:
        """Standard Normal Variate normalization"""
        mean = np.mean(data, axis=2, keepdims=True)
        std = np.std(data, axis=2, keepdims=True)
        std = np.where(std == 0, 1, std)
        return (data - mean) / std

    async def get_current_cube(self) -> Optional[SpectralCube]:
        """Get current spectral cube if available"""
        async with self._lock:
            return self._current_cube

    def cube_to_api_payload(self, cube: SpectralCube) -> dict:
        """Convert cube to Hyperspectral-Restruct API payload"""
        import base64
        import json

        # Serialize numpy array
        data_bytes = cube.data.tobytes()
        data_b64 = base64.b64encode(data_bytes).decode("utf-8")

        return {
            "field_id": cube.field_id,
            "capture_timestamp_ms": cube.capture_timestamp_ms,
            "dimensions": cube.dimensions,
            "data_base64": data_b64,
            "wavelengths_nm": cube.wavelengths_nm,
            "spatial_resolution_m": cube.spatial_resolution_m,
            "dtype": str(cube.data.dtype),
        }

    def get_stats(self) -> dict:
        """Get bridge statistics"""
        return {
            "spike_buffer_size": len(self._spike_buffer),
            "frame_buffer_size": len(self._frame_buffer),
            "cube_in_progress": self._cube_in_progress,
            "current_cube_available": self._current_cube is not None,
        }
