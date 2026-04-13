"""Syndar REST + WebSocket API - Operator Interface

- GET /fop - GeoJSON field picture
- GET /entities - Active drones
- POST /tasks - Inject scan task
- GET /drift - Cross-node drift report
- WebSocket /stream - Real-time entity updates
"""

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Security, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

from syndar.auth import AuthManager, AuthConfig, get_auth_manager, set_auth_manager
from syndar.command.fop import FusedFieldPicture
from syndar.command.mission import MissionPlanner
from syndar.fabric.database import Database, DatabaseConfig
from syndar.fabric.drift_monitor import DriftMonitor
from syndar.fabric.mesh import Mesh

logger = structlog.get_logger()

security = HTTPBearer()


# Request/Response Models
class TaskRequest(BaseModel):
    """Task creation request"""
    field_id: str
    field_boundary: list[tuple[float, float]]
    spectral_config: dict
    priority: int = Field(default=5, ge=1, le=10)
    deadline_ms: Optional[int] = None


class TaskStatusResponse(BaseModel):
    """Task status response"""
    task_id: str
    status: str
    entity_id: Optional[str]
    progress_pct: float
    updated_at_ms: int


# Global state (injected on startup)
mesh: Optional[Mesh] = None
fop: Optional[FusedFieldPicture] = None
mission_planner: Optional[MissionPlanner] = None
drift_monitor: Optional[DriftMonitor] = None
database: Optional[Database] = None
auth_manager: Optional[AuthManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Syndar API starting")
    
    # Initialize database
    if database:
        await database.initialize()
    
    # Start components
    if mesh:
        await mesh.start()
    if mission_planner and mission_planner.task_allocator:
        await mission_planner.task_allocator.start()
    if drift_monitor:
        await drift_monitor.start()
    
    yield
    
    # Shutdown
    logger.info("Syndar API shutting down")
    if mesh:
        await mesh.stop()
    if drift_monitor:
        await drift_monitor.stop()
    if database:
        await database.close()


app = FastAPI(
    title="Syndar API",
    description="Autonomous Agricultural Intelligence Platform - Drone Fleet Coordination",
    version="0.1.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "mesh_active": mesh is not None and mesh._running,
        "drift_monitor_active": drift_monitor is not None,
    }


@app.get("/fop")
async def get_fused_picture(
    entity_type: Optional[str] = None,
    include_tracks: bool = True,
    include_soil: bool = True,
):
    """Get Fused Field Picture as GeoJSON"""
    if not fop:
        return JSONResponse(
            status_code=503,
            content={"error": "FOP not initialized"},
        )

    geojson = await fop.export_geojson(
        entity_type=entity_type,
        include_tracks=include_tracks,
        include_soil=include_soil,
    )

    return json.loads(geojson.model_dump_json())


@app.get("/fop/state")
async def get_fop_state():
    """Get FOP summary state"""
    if not fop:
        return JSONResponse(
            status_code=503,
            content={"error": "FOP not initialized"},
        )

    state = await fop.get_current_state()
    return json.loads(state.model_dump_json())


@app.get("/entities")
async def get_entities(
    entity_type: Optional[str] = None,
    near_lat: Optional[float] = None,
    near_lng: Optional[float] = None,
    radius_m: Optional[float] = None,
):
    """Get all active entities"""
    if not mesh:
        return JSONResponse(
            status_code=503,
            content={"error": "Mesh not initialized"},
        )

    if near_lat is not None and near_lng is not None and radius_m is not None:
        results = await mesh.store.query_nearby(
            near_lat, near_lng, radius_m, entity_type
        )
        entities = [e[0] for e in results]
    else:
        entities = await mesh.store.get_all(entity_type)

    return {
        "count": len(entities),
        "entities": [json.loads(e.model_dump_json()) for e in entities],
    }


@app.get("/entities/{entity_id}")
async def get_entity(entity_id: str):
    """Get specific entity by ID"""
    if not mesh:
        return JSONResponse(
            status_code=503,
            content={"error": "Mesh not initialized"},
        )

    entity = await mesh.store.get(entity_id)
    if not entity:
        return JSONResponse(
            status_code=404,
            content={"error": f"Entity {entity_id} not found"},
        )

    return json.loads(entity.model_dump_json())


@app.get("/entities/{entity_id}/history")
async def get_entity_history(
    entity_id: str,
    hours: int = Query(default=24, ge=1, le=168),
):
    """Get entity history"""
    if not fop:
        return JSONResponse(
            status_code=503,
            content={"error": "FOP not initialized"},
        )

    history = await fop.get_entity_history(entity_id, hours)
    return {
        "entity_id": entity_id,
        "hours": hours,
        "count": len(history),
        "history": [json.loads(e.model_dump_json()) for e in history],
    }


@app.post("/tasks")
async def create_task(task_request: dict):
    """Inject a new scan task"""
    if not mission_planner:
        raise HTTPException(status_code=503, detail="Mission planner not initialized")

    from syndar.fabric.task_allocator import TaskRequest

    try:
        task = TaskRequest(**task_request)
        # Publish to task allocator
        if mission_planner.task_allocator:
            await mission_planner.task_allocator.publish_task(task)

        return {
            "status": "published",
            "task_id": task.task_id,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid task request: {str(e)}")


@app.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """Get task status"""
    if not mission_planner or not mission_planner.task_allocator:
        raise HTTPException(status_code=503, detail="Task allocator not initialized")

    task = mission_planner.task_allocator.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    return {
        "task_id": task.task_id,
        "status": task.status,
        "entity_id": task.entity_id,
        "progress_pct": task.progress_pct,
        "updated_at_ms": task.updated_at_ms,
    }


@app.get("/tasks")
async def list_tasks(
    status: Optional[str] = None,
    field_id: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000),
):
    """List all tasks with optional filtering"""
    if not mission_planner or not mission_planner.task_allocator:
        raise HTTPException(status_code=503, detail="Task allocator not initialized")

    tasks = mission_planner.task_allocator.list_tasks(status=status, field_id=field_id, limit=limit)
    return {
        "count": len(tasks),
        "tasks": [json.loads(t.model_dump_json()) for t in tasks],
    }


@app.delete("/tasks/{task_id}")
async def cancel_task(task_id: str):
    """Cancel a task"""
    if not mission_planner or not mission_planner.task_allocator:
        raise HTTPException(status_code=503, detail="Task allocator not initialized")

    success = await mission_planner.task_allocator.cancel_task(task_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found or cannot be cancelled")

    return {"status": "cancelled", "task_id": task_id}


@app.get("/drift")
async def get_drift_report(
    field_id: Optional[str] = None,
    min_severity: str = "info",
):
    """Get cross-node drift report"""
    if not drift_monitor:
        return JSONResponse(
            status_code=503,
            content={"error": "Drift monitor not initialized"},
        )

    alerts = await drift_monitor.get_alerts(field_id, min_severity)
    return {
        "alerts_count": len(alerts),
        "alerts": [json.loads(a.model_dump_json()) for a in alerts],
    }


@app.get("/drift/correlations/{field_id}")
async def get_field_correlations(field_id: str):
    """Get drift correlations for a specific field"""
    if not drift_monitor:
        return JSONResponse(
            status_code=503,
            content={"error": "Drift monitor not initialized"},
        )

    correlations = await drift_monitor.get_correlations(field_id)
    return {
        "field_id": field_id,
        "correlations_count": len(correlations),
        "correlations": [json.loads(c.model_dump_json()) for c in correlations],
    }


@app.get("/fields/{field_id}/coverage")
async def get_field_coverage(field_id: str):
    """Get scan coverage for a field"""
    if not fop:
        return JSONResponse(
            status_code=503,
            content={"error": "FOP not initialized"},
        )

    coverage = await fop.get_field_coverage(field_id)
    return coverage


@app.get("/fields/{field_id}/map")
async def get_field_map(field_id: str):
    """Get soil prediction map for a field"""
    if not fop:
        return JSONResponse(
            status_code=503,
            content={"error": "FOP not initialized"},
        )

    field_map = await fop.export_field_map(field_id)
    if not field_map:
        return JSONResponse(
            status_code=404,
            content={"error": f"No data for field {field_id}"},
        )

    return json.loads(field_map.model_dump_json())


@app.get("/coverage/heatmap")
async def get_coverage_heatmap(region: Optional[str] = None):
    """Get coverage heatmap data"""
    if not fop:
        return JSONResponse(
            status_code=503,
            content={"error": "FOP not initialized"},
        )

    heatmap = await fop.get_coverage_heatmap(region)
    return {"heatmap": heatmap}


@app.get("/missions")
async def get_missions():
    """Get active missions"""
    if not mission_planner:
        return JSONResponse(
            status_code=503,
            content={"error": "Mission planner not initialized"},
        )

    missions = mission_planner.get_active_missions()
    return {"missions": missions, "count": len(missions)}


@app.get("/missions/{mission_id}")
async def get_mission(mission_id: str):
    """Get mission status"""
    if not mission_planner:
        return JSONResponse(
            status_code=503,
            content={"error": "Mission planner not initialized"},
        )

    status = await mission_planner.get_mission_status(mission_id)
    if not status:
        return JSONResponse(
            status_code=404,
            content={"error": f"Mission {mission_id} not found"},
        )

    return status


@app.get("/stats")
async def get_stats():
    """Get system statistics"""
    stats = {
        "mesh": mesh.store.get_stats() if mesh else None,
        "fop": fop.get_stats() if fop else None,
        "mission": mission_planner.get_stats() if mission_planner else None,
        "drift": drift_monitor.get_stats() if drift_monitor else None,
    }
    return stats


@app.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    """WebSocket for real-time entity stream"""
    await websocket.accept()

    if not mesh:
        await websocket.send_json({"error": "Mesh not initialized"})
        await websocket.close()
        return

    queue = asyncio.Queue()

    def on_entity_update(entity):
        try:
            queue.put_nowait(entity)
        except asyncio.QueueFull:
            pass

    mesh.store.subscribe(on_entity_update)

    try:
        while True:
            entity = await queue.get()
            await websocket.send_json(json.loads(entity.model_dump_json()))
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    finally:
        mesh.store.unsubscribe(on_entity_update)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> Optional[dict]:
    """Get current user from JWT token"""
    if not auth_manager or not auth_manager.config.enable_auth:
        return {"username": "anonymous", "role": "anonymous", "permissions": ["*"]}
    
    token = credentials.credentials
    user = auth_manager.verify_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    
    return {
        "username": user.username,
        "role": user.role,
        "permissions": user.permissions,
    }


def require_permission(permission: str):
    """Dependency factory for requiring specific permission"""
    def check_permission(current_user: dict = Depends(get_current_user)) -> dict:
        if not auth_manager or not auth_manager.config.enable_auth:
            return current_user
        
        if not auth_manager.check_permission(
            type('User', (), current_user)(), permission
        ):
            raise HTTPException(status_code=403, detail=f"Permission required: {permission}")
        
        return current_user
    
    return check_permission


def setup_api(
    mesh_instance: Mesh,
    fop_instance: FusedFieldPicture,
    mission_planner_instance: MissionPlanner,
    drift_monitor_instance: Optional[DriftMonitor] = None,
    database_instance: Optional[Database] = None,
    auth_manager_instance: Optional[AuthManager] = None,
):
    """Setup API with injected dependencies"""
    global mesh, fop, mission_planner, drift_monitor, database, auth_manager
    mesh = mesh_instance
    fop = fop_instance
    mission_planner = mission_planner_instance
    drift_monitor = drift_monitor_instance
    database = database_instance
    auth_manager = auth_manager_instance
    
    if auth_manager:
        set_auth_manager(auth_manager)
