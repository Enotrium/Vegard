"""Database persistence layer for Syndar

Provides SQLite-based persistence for entities, tasks, and other data.
Designed to be swappable for PostgreSQL in production.
"""

import asyncio
import json
import sqlite3
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import structlog
from pydantic import BaseModel

logger = structlog.get_logger()


@dataclass
class DatabaseConfig:
    """Database configuration"""

    path: str = "syndar.db"
    enable_wal: bool = True
    connection_pool_size: int = 5


class Database:
    """SQLite database with async interface"""

    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self._local = asyncio.local()
        self._path = Path(self.config.path)
        self._initialized = False

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local connection"""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            conn = sqlite3.connect(self._path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._local.connection = conn
        return self._local.connection

    async def initialize(self) -> None:
        """Initialize database schema"""
        if self._initialized:
            return

        conn = self._get_connection()
        cursor = conn.cursor()

        # Enable WAL mode for better concurrency
        if self.config.enable_wal:
            cursor.execute("PRAGMA journal_mode=WAL")

        # Entities table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                entity_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                alt REAL DEFAULT 0.0,
                accuracy REAL DEFAULT 1.0,
                timestamp_ms INTEGER NOT NULL,
                drift_score REAL DEFAULT 0.0,
                drift_flag BOOLEAN DEFAULT FALSE,
                battery_pct REAL DEFAULT 100.0,
                task_id TEXT,
                soil_data TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )
        """)

        # Entity history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                alt REAL DEFAULT 0.0,
                timestamp_ms INTEGER NOT NULL,
                drift_score REAL DEFAULT 0.0,
                battery_pct REAL DEFAULT 100.0,
                recorded_at_ms INTEGER NOT NULL,
                FOREIGN KEY (entity_id) REFERENCES entities(entity_id)
            )
        """)

        # Tasks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                field_id TEXT NOT NULL,
                target_polygon TEXT NOT NULL,
                priority REAL NOT NULL,
                deadline_ms INTEGER NOT NULL,
                estimated_duration_s INTEGER NOT NULL,
                spectral_config TEXT,
                mission_id TEXT,
                requested_by TEXT,
                status TEXT DEFAULT 'pending',
                entity_id TEXT,
                assigned_at_ms INTEGER,
                completed_at_ms INTEGER,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )
        """)

        # Task bids table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_bids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                bid_cost REAL NOT NULL,
                estimated_arrival_s REAL NOT NULL,
                battery_at_completion_pct REAL NOT NULL,
                current_position TEXT,
                submitted_at_ms INTEGER NOT NULL,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            )
        """)

        # Soil predictions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS soil_predictions (
                scan_id TEXT PRIMARY KEY,
                field_id TEXT NOT NULL,
                drone_id TEXT NOT NULL,
                nutrients TEXT NOT NULL,
                contaminants TEXT,
                land_value_score REAL DEFAULT 0.0,
                remediation_priority REAL DEFAULT 0.0,
                phytoremediation_suitability REAL DEFAULT 0.0,
                spectral_hash TEXT,
                capture_timestamp_ms INTEGER NOT NULL,
                center_lat REAL,
                center_lng REAL,
                model_version TEXT,
                model_commit TEXT,
                created_at_ms INTEGER NOT NULL
            )
        """)

        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_timestamp ON entities(timestamp_ms)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_history_entity ON entity_history(entity_id, timestamp_ms)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tasks_field ON tasks(field_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_soil_field ON soil_predictions(field_id)")

        conn.commit()
        self._initialized = True

        logger.info("Database initialized", path=self.config.path)

    async def close(self) -> None:
        """Close database connections"""
        if hasattr(self._local, "connection") and self._local.connection:
            self._local.connection.close()
            self._local.connection = None

    async def upsert_entity(self, entity: BaseModel) -> None:
        """Insert or update entity"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now_ms = int(time.time() * 1000)
        
        # Serialize soil data if present
        soil_data = None
        if hasattr(entity, "soil") and entity.soil:
            soil_data = json.dumps(entity.soil.model_dump(), default=str)
        
        cursor.execute("""
            INSERT INTO entities (
                entity_id, entity_type, lat, lng, alt, accuracy, timestamp_ms,
                drift_score, drift_flag, battery_pct, task_id, soil_data,
                created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
                lat = excluded.lat,
                lng = excluded.lng,
                alt = excluded.alt,
                timestamp_ms = excluded.timestamp_ms,
                drift_score = excluded.drift_score,
                drift_flag = excluded.drift_flag,
                battery_pct = excluded.battery_pct,
                task_id = excluded.task_id,
                soil_data = excluded.soil_data,
                updated_at_ms = excluded.updated_at_ms
        """, (
            entity.entity_id,
            entity.entity_type,
            entity.position.lat,
            entity.position.lng,
            entity.position.alt,
            entity.position.accuracy,
            entity.timestamp_ms,
            entity.drift_score,
            entity.drift_flag,
            entity.battery_pct,
            entity.task_id,
            soil_data,
            now_ms,
            now_ms,
        ))
        
        conn.commit()

    async def get_entity(self, entity_id: str) -> Optional[dict]:
        """Get entity by ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM entities WHERE entity_id = ?", (entity_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None

    async def list_entities(self, entity_type: Optional[str] = None) -> list[dict]:
        """List all entities with optional type filter"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if entity_type:
            cursor.execute("SELECT * FROM entities WHERE entity_type = ?", (entity_type,))
        else:
            cursor.execute("SELECT * FROM entities")
        
        return [dict(row) for row in cursor.fetchall()]

    async def record_entity_history(self, entity: BaseModel) -> None:
        """Record entity state in history"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO entity_history (
                entity_id, lat, lng, alt, timestamp_ms, drift_score,
                battery_pct, recorded_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entity.entity_id,
            entity.position.lat,
            entity.position.lng,
            entity.position.alt,
            entity.timestamp_ms,
            entity.drift_score,
            entity.battery_pct,
            int(time.time() * 1000),
        ))
        
        conn.commit()

    async def get_entity_history(
        self, entity_id: str, hours: int = 24
    ) -> list[dict]:
        """Get entity history for specified time period"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff_ms = int(time.time() * 1000) - hours * 3600 * 1000
        
        cursor.execute("""
            SELECT * FROM entity_history
            WHERE entity_id = ? AND recorded_at_ms > ?
            ORDER BY recorded_at_ms DESC
        """, (entity_id, cutoff_ms))
        
        return [dict(row) for row in cursor.fetchall()]

    async def upsert_task(self, task: BaseModel) -> None:
        """Insert or update task"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now_ms = int(time.time() * 1000)
        
        # Serialize spectral config
        spectral_config = None
        if hasattr(task, "spectral") and task.spectral:
            spectral_config = json.dumps(task.spectral.model_dump(), default=str)
        
        # Serialize polygon
        target_polygon = json.dumps(task.target_polygon)
        
        cursor.execute("""
            INSERT INTO tasks (
                task_id, field_id, target_polygon, priority, deadline_ms,
                estimated_duration_s, spectral_config, mission_id, requested_by,
                status, entity_id, assigned_at_ms, completed_at_ms,
                created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                status = excluded.status,
                entity_id = excluded.entity_id,
                assigned_at_ms = excluded.assigned_at_ms,
                completed_at_ms = excluded.completed_at_ms,
                updated_at_ms = excluded.updated_at_ms
        """, (
            task.task_id,
            task.field_id,
            target_polygon,
            task.priority,
            task.deadline_ms,
            task.estimated_duration_s,
            spectral_config,
            task.mission_id,
            task.requested_by,
            task.status,
            task.entity_id,
            task.assigned_at_ms,
            task.completed_at_ms,
            now_ms,
            now_ms,
        ))
        
        conn.commit()

    async def get_task(self, task_id: str) -> Optional[dict]:
        """Get task by ID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None

    async def list_tasks(
        self, status: Optional[str] = None, field_id: Optional[str] = None
    ) -> list[dict]:
        """List tasks with optional filters"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM tasks WHERE 1=1"
        params = []
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        if field_id:
            query += " AND field_id = ?"
            params.append(field_id)
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    async def record_bid(self, bid: BaseModel) -> None:
        """Record task bid"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Serialize position
        position = None
        if hasattr(bid, "current_position") and bid.current_position:
            position = json.dumps(bid.current_position.model_dump(), default=str)
        
        cursor.execute("""
            INSERT INTO task_bids (
                task_id, entity_id, bid_cost, estimated_arrival_s,
                battery_at_completion_pct, current_position, submitted_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            bid.task_id,
            bid.entity_id,
            bid.bid_cost,
            bid.estimated_arrival_s,
            bid.battery_at_completion_pct,
            position,
            int(time.time() * 1000),
        ))
        
        conn.commit()

    async def get_task_bids(self, task_id: str) -> list[dict]:
        """Get all bids for a task"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM task_bids
            WHERE task_id = ?
            ORDER BY bid_cost ASC
        """, (task_id,))
        
        return [dict(row) for row in cursor.fetchall()]

    async def upsert_soil_prediction(self, prediction: BaseModel) -> None:
        """Insert or update soil prediction"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now_ms = int(time.time() * 1000)
        
        # Serialize nutrients and contaminants
        nutrients = json.dumps(prediction.nutrients, default=str)
        contaminants = json.dumps([c.model_dump() for c in prediction.contaminants], default=str)
        
        cursor.execute("""
            INSERT INTO soil_predictions (
                scan_id, field_id, drone_id, nutrients, contaminants,
                land_value_score, remediation_priority, phytoremediation_suitability,
                spectral_hash, capture_timestamp_ms, center_lat, center_lng,
                model_version, model_commit, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(scan_id) DO UPDATE SET
                land_value_score = excluded.land_value_score,
                remediation_priority = excluded.remediation_priority,
                phytoremediation_suitability = excluded.phytoremediation_suitability
        """, (
            prediction.scan_id,
            prediction.field_id,
            prediction.drone_id,
            nutrients,
            contaminants,
            prediction.land_value_score,
            prediction.remediation_priority,
            prediction.phytoremediation_suitability,
            prediction.spectral_hash,
            prediction.capture_timestamp_ms,
            prediction.center_position.lat if prediction.center_position else None,
            prediction.center_position.lng if prediction.center_position else None,
            prediction.model_version,
            prediction.model_commit,
            now_ms,
        ))
        
        conn.commit()

    async def get_soil_predictions(self, field_id: str) -> list[dict]:
        """Get soil predictions for a field"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM soil_predictions
            WHERE field_id = ?
            ORDER BY capture_timestamp_ms DESC
        """, (field_id,))
        
        return [dict(row) for row in cursor.fetchall()]

    async def cleanup_old_data(self, days: int = 30) -> int:
        """Clean up old data beyond retention period"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff_ms = int(time.time() * 1000) - days * 86400 * 1000
        
        # Clean up old entity history
        cursor.execute("""
            DELETE FROM entity_history
            WHERE recorded_at_ms < ?
        """, (cutoff_ms,))
        
        history_deleted = cursor.rowcount
        
        # Clean up old soil predictions (optional - may want to keep these)
        # cursor.execute("DELETE FROM soil_predictions WHERE created_at_ms < ?", (cutoff_ms,))
        
        conn.commit()
        
        logger.info("Cleaned up old data", days=days, history_deleted=history_deleted)
        return history_deleted

    async def get_stats(self) -> dict:
        """Get database statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute("SELECT COUNT(*) FROM entities")
        stats["entity_count"] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tasks")
        stats["task_count"] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM entity_history")
        stats["history_count"] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM soil_predictions")
        stats["soil_prediction_count"] = cursor.fetchone()[0]
        
        # Database size
        stats["database_size_bytes"] = self._path.stat().st_size if self._path.exists() else 0
        
        return stats


# Global database instance
_db: Optional[Database] = None


def get_database(config: Optional[DatabaseConfig] = None) -> Database:
    """Get global database instance"""
    global _db
    if _db is None:
        _db = Database(config)
    return _db
