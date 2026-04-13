"""Fused Field Picture - Materialized operational view

Time-series store of all entity states, GeoJSON export for AIP Mapbox/Deck.gl,
streaming API for real-time updates.
"""

import json
import time
from collections import defaultdict
from typing import Any, Optional

import structlog
from pydantic import BaseModel

from vegard.fabric.mesh import EntityState, Mesh

logger = structlog.get_logger()


class GeoJSONFeature(BaseModel):
    """GeoJSON Feature for drone/soil visualization"""

    type: str = "Feature"
    geometry: dict
    properties: dict


class GeoJSONCollection(BaseModel):
    """GeoJSON FeatureCollection"""

    type: str = "FeatureCollection"
    features: list[GeoJSONFeature]


class FOPState(BaseModel):
    """Fused Operational Picture state"""

    timestamp_ms: int
    drone_count: int
    active_tasks: int
    drift_alerts: int
    coverage_pct: float
    entities: list[EntityState]


class FusedFieldPicture:
    """Materialized projection of the state mesh"""

    def __init__(self, mesh: Mesh):
        self.mesh = mesh
        self._drone_history: dict[str, list[EntityState]] = defaultdict(list)
        self._soil_predictions: dict[str, list[dict]] = defaultdict(list)
        self._subscribers: list[callable] = []

        # Subscribe to mesh updates
        self.mesh.store.subscribe(self._on_entity_update)

    def _on_entity_update(self, entity: EntityState) -> None:
        """Handle entity update from mesh"""
        # Store in history
        self._drone_history[entity.entity_id].append(entity)

        # Trim old history (keep 24 hours)
        cutoff = time.time() * 1000 - 86400000
        self._drone_history[entity.entity_id] = [
            e for e in self._drone_history[entity.entity_id] if e.timestamp_ms > cutoff
        ]

        # Store soil prediction if present
        if entity.soil:
            self._soil_predictions[entity.soil.field_id].append(
                {
                    "drone_id": entity.entity_id,
                    "timestamp_ms": entity.timestamp_ms,
                    "prediction": entity.soil,
                }
            )

        # Notify subscribers
        for cb in self._subscribers:
            try:
                cb(entity)
            except Exception:
                logger.exception("FOP subscriber failed")

    def subscribe(self, callback: callable) -> None:
        """Subscribe to FOP updates"""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: callable) -> None:
        """Unsubscribe from FOP updates"""
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    async def get_current_state(self) -> FOPState:
        """Get current FOP state"""
        drones = await self.mesh.store.get_all("drone")

        # Calculate coverage
        fields_scanned = set()
        for drone in drones:
            if drone.soil:
                fields_scanned.add(drone.soil.field_id)

        return FOPState(
            timestamp_ms=int(time.time() * 1000),
            drone_count=len(drones),
            active_tasks=len([d for d in drones if d.task_id]),
            drift_alerts=len([d for d in drones if d.drift_flag]),
            coverage_pct=len(fields_scanned) / max(1, len(self._soil_predictions)) * 100,
            entities=drones,
        )

    async def export_geojson(
        self,
        entity_type: Optional[str] = "drone",
        include_tracks: bool = True,
        include_soil: bool = True,
    ) -> GeoJSONCollection:
        """Export FOP as GeoJSON for AIP Mapbox/Deck.gl"""
        features: list[GeoJSONFeature] = []

        entities = await self.mesh.store.get_all(entity_type)

        for entity in entities:
            # Current position point
            point_feature = GeoJSONFeature(
                geometry={
                    "type": "Point",
                    "coordinates": [
                        entity.position.lng,
                        entity.position.lat,
                        entity.position.alt,
                    ],
                },
                properties={
                    "entity_id": entity.entity_id,
                    "entity_type": entity.entity_type,
                    "battery_pct": entity.battery_pct,
                    "drift_score": entity.drift_score,
                    "drift_flag": entity.drift_flag,
                    "task_id": entity.task_id,
                    "timestamp_ms": entity.timestamp_ms,
                },
            )
            features.append(point_feature)

            # Flight track line
            if include_tracks and entity.entity_id in self._drone_history:
                history = self._drone_history[entity.entity_id]
                if len(history) > 1:
                    coordinates = [
                        [e.position.lng, e.position.lat, e.position.alt] for e in history
                    ]
                    track_feature = GeoJSONFeature(
                        geometry={
                            "type": "LineString",
                            "coordinates": coordinates,
                        },
                        properties={
                            "entity_id": entity.entity_id,
                            "track_type": "flight_path",
                            "point_count": len(coordinates),
                            "start_time_ms": history[0].timestamp_ms,
                            "end_time_ms": history[-1].timestamp_ms,
                        },
                    )
                    features.append(track_feature)

            # Soil prediction polygon
            if include_soil and entity.soil:
                # Create small polygon around scan location
                lat, lng = entity.position.lat, entity.position.lng
                delta = 0.001  # ~100m
                soil_polygon = GeoJSONFeature(
                    geometry={
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [lng - delta, lat - delta],
                                [lng + delta, lat - delta],
                                [lng + delta, lat + delta],
                                [lng - delta, lat + delta],
                                [lng - delta, lat - delta],
                            ]
                        ],
                    },
                    properties={
                        "entity_id": entity.entity_id,
                        "scan_type": "soil_prediction",
                        "field_id": entity.soil.field_id,
                        "land_value_score": entity.soil.land_value_score,
                        "contamination_detected": entity.soil.contamination_detected,
                        "nutrients": entity.soil.nutrients,
                    },
                )
                features.append(soil_polygon)

        return GeoJSONCollection(features=features)

    async def export_field_map(self, field_id: str) -> Optional[GeoJSONCollection]:
        """Export soil predictions for a specific field"""
        predictions = self._soil_predictions.get(field_id, [])
        if not predictions:
            return None

        features: list[GeoJSONFeature] = []

        for pred in predictions:
            soil = pred["prediction"]
            lat, lng = soil.center_position.lat, soil.center_position.lng

            feature = GeoJSONFeature(
                geometry={
                    "type": "Point",
                    "coordinates": [lng, lat],
                },
                properties={
                    "scan_id": soil.scan_id,
                    "drone_id": pred["drone_id"],
                    "timestamp_ms": pred["timestamp_ms"],
                    "land_value_score": soil.land_value_score,
                    "remediation_priority": soil.remediation_priority,
                    "nutrients": soil.nutrient_map,
                    "contaminants": [c.type for c in soil.contaminants],
                    "contamination_detected": soil.contamination_detected,
                },
            )
            features.append(feature)

        return GeoJSONCollection(features=features)

    async def get_entity_history(
        self, entity_id: str, hours: int = 24
    ) -> list[EntityState]:
        """Get entity history"""
        cutoff = time.time() * 1000 - hours * 3600 * 1000
        return [
            e for e in self._drone_history.get(entity_id, []) if e.timestamp_ms > cutoff
        ]

    async def get_field_coverage(self, field_id: str) -> dict:
        """Get scan coverage statistics for a field"""
        predictions = self._soil_predictions.get(field_id, [])

        if not predictions:
            return {
                "field_id": field_id,
                "scanned": False,
                "prediction_count": 0,
                "last_scan_ms": None,
                "unique_drones": 0,
            }

        unique_drones = set(p["drone_id"] for p in predictions)
        last_scan = max(p["timestamp_ms"] for p in predictions)

        return {
            "field_id": field_id,
            "scanned": True,
            "prediction_count": len(predictions),
            "last_scan_ms": last_scan,
            "unique_drones": len(unique_drones),
            "average_land_value": sum(
                p["prediction"].land_value_score for p in predictions
            )
            / len(predictions),
        }

    async def get_coverage_heatmap(
        self, region: Optional[str] = None
    ) -> list[dict]:
        """Get coverage heatmap data for Deck.gl"""
        heatmap = []

        for field_id, predictions in self._soil_predictions.items():
            if not predictions:
                continue

            # Calculate centroid
            avg_lat = sum(p["prediction"].center_position.lat for p in predictions) / len(
                predictions
            )
            avg_lng = sum(p["prediction"].center_position.lng for p in predictions) / len(
                predictions
            )

            # Coverage weight
            weight = len(predictions)

            heatmap.append(
                {
                    "field_id": field_id,
                    "position": [avg_lng, avg_lat],
                    "weight": weight,
                    "predictions": len(predictions),
                    "last_scan_ms": max(p["timestamp_ms"] for p in predictions),
                }
            )

        return heatmap

    def get_stats(self) -> dict:
        """Get FOP statistics"""
        return {
            "tracked_entities": len(self._drone_history),
            "fields_with_data": len(self._soil_predictions),
            "total_predictions": sum(len(p) for p in self._soil_predictions.values()),
            "subscribers": len(self._subscribers),
        }
