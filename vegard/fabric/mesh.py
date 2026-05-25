"""Gossip protocol mesh - core coordination fabric

Entities broadcast state, neighbors gossip to fanout, stale entities timeout.
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Optional
from collections.abc import AsyncIterator

import numpy as np
import structlog
from pydantic import BaseModel, Field

from vegard.fabric.database import Database

# Lazy import to avoid circular dependency
try:
    from vegard.logging_config import get_logger, bind_context
    logger = get_logger(__name__)
except ImportError:
    logger = structlog.get_logger()
    bind_context = lambda **kwargs: None


class Position(BaseModel):
    lat: float
    lng: float
    alt: float = 0.0
    accuracy: float = 1.0


class Contaminant(BaseModel):
    """Contaminant detection result - matches proto definition"""
    type: str  # "PE", "PP", "PA", "PS", "PET", "PFAS", "glyphosate", "lead", "cadmium", etc.
    concentration: float = 0.0  # wt% or ppm depending on type
    confidence: float = 0.0  # 0.0-1.0
    location: Optional[Position] = None


class SoilPrediction(BaseModel):
    """Soil prediction from Hyperspectral-Restruct - matches proto definition"""
    field_id: str
    scan_id: str = ""  # Unique identifier for this scan

    # Core soil chemistry
    nutrients: dict[str, float] = Field(default_factory=dict)
    nutrient_map: dict[str, float] = Field(default_factory=dict)  # Convenience accessor

    # Contamination detection
    contaminants: list[Contaminant] = Field(default_factory=list)
    contamination_detected: bool = False

    # Derived intelligence
    land_value_score: float = 0.0  # 0.0-1.0 composite score
    remediation_priority: float = 0.0  # 0.0-1.0 urgency
    phytoremediation_suitability: float = 0.0  # 0.0-1.0 hemp suitability

    # Provenance and verification
    spectral_hash: str = ""  # SHA256 of source spectral cube
    drone_id: str = ""  # Source drone entity_id
    capture_timestamp_ms: int = 0
    center_position: Optional[Position] = None  # Center of scan area

    # Model metadata
    model_version: str = ""  # Hyperspectral-Restruct version
    model_commit: str = ""  # Git commit hash


class EntityState(BaseModel):
    """Core mesh entity - matches proto/entity.proto"""

    entity_id: str
    entity_type: str = "drone"
    position: Position
    soil: Optional[SoilPrediction] = None
    drift_score: float = 0.0
    drift_flag: bool = False
    battery_pct: float = 100.0
    task_id: Optional[str] = None
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    signature: str = ""
    model_version: str = ""


@dataclass
class MeshConfig:
    fanout: int = 3
    gossip_interval_ms: int = 1000
    max_entity_age_ms: int = 30000
    heartbeat_timeout_ms: int = 10000


@dataclass
class EntityStoreConfig:
    max_entity_age_ms: int = 30000
    heartbeat_timeout_ms: int = 10000


class EntityStore:
    """Thread-safe entity store with subscription support and database persistence"""

    def __init__(
        self, 
        config: Optional[EntityStoreConfig] = None, 
        database: Optional[Database] = None,
        stale_timeout_ms: Optional[int] = None,
    ):
        self.config = config or EntityStoreConfig()
        self._entities: dict[str, EntityState] = {}
        self._history: dict[str, list[EntityState]] = defaultdict(list)
        self._subscribers: list[Callable[[EntityState], None]] = []
        self._lock = asyncio.Lock()
        self._database = database
        self.stale_timeout_ms = (
            stale_timeout_ms 
            or getattr(self.config, "stale_timeout_ms", None)
            or getattr(self.config, "max_entity_age_ms", None)
            or 30000
        )

        # Performance optimizations: indexes
        self._type_index: dict[str, set[str]] = defaultdict(set)  # entity_type -> entity_ids
        self._cache: dict[str, tuple[list[EntityState], float]] = {}  # query -> (result, timestamp)
        self._cache_ttl = 1.0  # Cache TTL in seconds

    async def load_from_database(self) -> None:
        """Load entities from database on startup"""
        if not self._database:
            return

        try:
            entities_data = await self._database.list_entities()
            async with self._lock:
                for entity_data in entities_data:
                    # Reconstruct EntityState from database row
                    entity = EntityState(
                        entity_id=entity_data["entity_id"],
                        entity_type=entity_data["entity_type"],
                        position=Position(
                            lat=entity_data["lat"],
                            lng=entity_data["lng"],
                            alt=entity_data["alt"],
                            accuracy=entity_data.get("accuracy", 1.0),
                        ),
                        timestamp_ms=entity_data["timestamp_ms"],
                        drift_score=entity_data.get("drift_score", 0.0),
                        drift_flag=entity_data.get("drift_flag", False),
                        battery_pct=entity_data.get("battery_pct", 100.0),
                        task_id=entity_data.get("task_id"),
                    )
                    self._entities[entity.entity_id] = entity

            logger.info("Loaded entities from database", count=len(self._entities))
        except Exception as e:
            logger.error("Failed to load entities from database", error=str(e))

    async def update(self, entity: EntityState) -> None:
        """Update entity state and notify subscribers"""
        bind_context(entity_id=entity.entity_id, entity_type=entity.entity_type)
        
        async with self._lock:
            old = self._entities.get(entity.entity_id)
            self._entities[entity.entity_id] = entity
            self._history[entity.entity_id].append(entity)
            
            # Update type index
            if old and old.entity_type != entity.entity_type:
                self._type_index[old.entity_type].discard(entity.entity_id)
            self._type_index[entity.entity_type].add(entity.entity_id)

            # Trim old history
            cutoff = time.time() * 1000 - self.config.max_entity_age_ms
            self._history[entity.entity_id] = [
                e for e in self._history[entity.entity_id] if e.timestamp_ms > cutoff
            ]
            
            # Invalidate cache
            self._cache.clear()

        # Persist to database
        if self._database:
            await self._database.upsert_entity(entity)
            await self._database.record_entity_history(entity)
            logger.debug("Entity persisted to database", entity_id=entity.entity_id)

        if old is None or old.timestamp_ms < entity.timestamp_ms:
            await self._notify(entity)
            logger.debug("Entity updated and notified", entity_id=entity.entity_id)

    async def get(self, entity_id: str) -> Optional[EntityState]:
        async with self._lock:
            return self._entities.get(entity_id)

    async def get_all(self, entity_type: Optional[str] = None) -> list[EntityState]:
        async with self._lock:
            if entity_type:
                # Use type index for O(1) lookup instead of O(n) filtering
                entity_ids = self._type_index.get(entity_type, set())
                return [self._entities[eid] for eid in entity_ids if eid in self._entities]
            return list(self._entities.values())

    async def get_stats(self) -> dict:
        """Return summary statistics for the entity store"""
        async with self._lock:
            entity_count = len(self._entities)
            history_count = sum(len(history) for history in self._history.values())
            type_counts = {
                entity_type: len(entity_ids)
                for entity_type, entity_ids in self._type_index.items()
            }
            return {
                "entity_count": entity_count,
                "history_count": history_count,
                "type_counts": type_counts,
                "subscriber_count": len(self._subscribers),
                "cache_entries": len(self._cache),
            }

    async def get_history(
        self, entity_id: str, start_ms: int, end_ms: int
    ) -> list[EntityState]:
        async with self._lock:
            history = self._history.get(entity_id, [])
            return [e for e in history if start_ms <= e.timestamp_ms <= end_ms]

    async def remove_stale(self) -> list[str]:
        """Remove entities that haven't updated within timeout"""
        cutoff = time.time() * 1000 - self.config.heartbeat_timeout_ms
        removed = []
        async with self._lock:
            for entity_id, entity in list(self._entities.items()):
                if entity.timestamp_ms < cutoff:
                    del self._entities[entity_id]
                    # Clean up type index
                    self._type_index[entity.entity_type].discard(entity_id)
                    removed.append(entity_id)
            # Invalidate cache
            self._cache.clear()
        if removed:
            logger.info("Removed stale entities", count=len(removed), ids=removed)
        return removed

    def subscribe(self, callback: Callable[[EntityState], None]) -> None:
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[EntityState], None]) -> None:
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    async def _notify(self, entity: EntityState) -> None:
        for cb in self._subscribers:
            try:
                cb(entity)
            except Exception:
                logger.exception("Subscriber failed", entity_id=entity.entity_id)

    async def query_nearby(
        self, lat: float, lng: float, radius_m: float, entity_type: Optional[str] = None
    ) -> list[tuple[EntityState, float]]:
        """Query entities within radius (simple Euclidean for now)"""
        results = []
        entities = await self.get_all(entity_type)
        for e in entities:
            dist = ((e.position.lat - lat) ** 2 + (e.position.lng - lng) ** 2) ** 0.5
            # Rough conversion to meters (at equator)
            dist_m = dist * 111000
            if dist_m <= radius_m:
                results.append((e, dist_m))
        return sorted(results, key=lambda x: x[1])


class Mesh:
    """Gossip protocol mesh - core coordination fabric"""

    def __init__(
            self, config: MeshConfig = None, 
            database: Optional[Database] = None,
            transport: Optional[object] = None  # Placeholder for transport layer (e.g., gRPC, MQTT)
            ):
        self.config = config or MeshConfig()
        self.store = EntityStore(
            database=database,
            stale_timeout_ms=self.config.max_entity_age_ms,
        )
        self._peers: set[str] = set()
        self._gossip_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
        self._database = database
        self._transport = transport  # Transport layer for gossiping

    async def start(self) -> None:
        """Start mesh gossip and maintenance tasks"""
        self._running = True
        
        # Load entities from database
        await self.store.load_from_database()
        
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Mesh started", fanout=self.config.fanout)

    async def stop(self) -> None:
        """Stop mesh tasks"""
        self._running = False
        if self._gossip_task:
            self._gossip_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        logger.info("Mesh stopped")

    async def join(self, peer_address: str) -> None:
        """Add peer to mesh"""
        self._peers.add(peer_address)
        logger.info("Peer joined", peer=peer_address)

    async def leave(self, peer_address: str) -> None:
        """Remove peer from mesh"""
        self._peers.discard(peer_address)
        logger.info("Peer left", peer=peer_address)

    async def publish(self, entity: EntityState) -> None:
        """Publish entity state to mesh"""
        await self.store.update(entity)
        # Gossip to peers
        await self._gossip_entity(entity)

    async def _gossip_loop(self) -> None:
        """Periodic gossip of known entities"""
        while self._running:
            try:
                await asyncio.sleep(self.config.gossip_interval_ms / 1000)
                entities = await self.store.get_all()
                for entity in entities:
                    await self._gossip_entity(entity)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Gossip loop error")

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of stale entities"""
        while self._running:
            try:
                await asyncio.sleep(self.config.heartbeat_timeout_ms / 1000)
                await self.store.remove_stale()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Cleanup loop error")

    async def _gossip_entity(self, entity: EntityState) -> None:
        """Gossip entity to N random peers (fanout)"""
        if not self._transport:
            return

        try:
            # Serialize entity for transport
            payload = {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "position": {
                    "lat": entity.position.lat,
                    "lng": entity.position.lng,
                    "alt": entity.position.alt,
                },
                "timestamp_ms": entity.timestamp_ms,
                "drift_score": entity.drift_score,
                "drift_flag": entity.drift_flag,
                "battery_pct": entity.battery_pct,
                "task_id": entity.task_id,
            }

            # Include soil data if present
            if entity.soil:
                payload["soil"] = {
                    "field_id": entity.soil.field_id,
                    "scan_id": entity.soil.scan_id,
                    "land_value_score": entity.soil.land_value_score,
                    "contamination_detected": entity.soil.contamination_detected,
                    "nutrient_map": entity.soil.nutrient_map,
                }

            # Publish to mesh topic
            await self._transport.publish(
                f"mesh/entities/{entity.entity_id}",
                payload,
                protocol="grpc",
            )

            logger.debug(
                "Gossiped entity",
                entity_id=entity.entity_id,
                fanout=self.config.fanout,
            )
        except Exception as e:
            logger.error(
                "Gossip failed",
                entity_id=entity.entity_id,
                error=str(e),
            )

    def stream_entities(
        self, entity_type: Optional[str] = None
    ) -> AsyncIterator[EntityState]:
        """Stream entity updates"""
        queue: asyncio.Queue[EntityState] = asyncio.Queue()

        def on_update(entity: EntityState) -> None:
            if entity_type is None or entity.entity_type == entity_type:
                try:
                    queue.put_nowait(entity)
                except asyncio.QueueFull:
                    pass

        self.store.subscribe(on_update)

        async def generator() -> AsyncIterator[EntityState]:
            try:
                while True:
                    entity = await queue.get()
                    yield entity
            finally:
                self.store.unsubscribe(on_update)

        return generator()

    async def get_fused_picture(self) -> dict:
        """Get current fused operational picture"""
        drones = await self.store.get_all("drone")
        return {
            "timestamp_ms": int(time.time() * 1000),
            "drone_count": len(drones),
            "drones": [d.model_dump() for d in drones],
            "active_tasks": len([d for d in drones if d.task_id]),
            "drift_alerts": len([d for d in drones if d.drift_flag]),
        }
