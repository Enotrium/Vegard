"""Vegard REST + WebSocket API - Operator Interface

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
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from prometheus_fastapi_instrumentator import Instrumentator

from vegard.auth import AuthManager, AuthConfig, get_auth_manager, set_auth_manager
from vegard.command.fop import FusedFieldPicture
from vegard.command.mission import MissionPlanner
from vegard.fabric.database import Database, DatabaseConfig
from vegard.fabric.drift_monitor import DriftMonitor
from vegard.fabric.mesh import Mesh
from fastapi.responses import HTMLResponse

# Lazy import to avoid circular dependency
try:
    from vegard.logging_config import configure_logging, get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = structlog.get_logger()
    configure_logging = lambda **kwargs: None

security = HTTPBearer()
limiter = Limiter(key_func=get_remote_address)


# Request/Response Models
class TaskRequest(BaseModel):
    """Task creation request"""
    field_id: str = Field(..., min_length=1, max_length=100, description="Field identifier")
    field_boundary: list[tuple[float, float]] = Field(..., min_length=3, description="Polygon boundary coordinates")
    spectral_config: dict = Field(default_factory=dict, description="Spectral configuration")
    priority: int = Field(default=5, ge=1, le=10, description="Task priority (1-10)")
    deadline_ms: Optional[int] = Field(None, gt=0, description="Deadline in milliseconds since epoch")


class TaskStatusResponse(BaseModel):
    """Task status response"""
    task_id: str
    status: str
    entity_id: Optional[str]
    progress_pct: float = Field(..., ge=0, le=100)
    updated_at_ms: int


# Input sanitization utilities
def sanitize_string(input_str: str, max_length: int = 1000) -> str:
    """Sanitize string input to prevent injection attacks"""
    if not isinstance(input_str, str):
        return ""
    
    # Remove potentially dangerous characters
    sanitized = input_str.strip()
    
    # Limit length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
    
    return sanitized


def validate_lat_lng(lat: float, lng: float) -> bool:
    """Validate latitude and longitude values"""
    return -90 <= lat <= 90 and -180 <= lng <= 180


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
    configure_logging(level="INFO", json_output=True)
    logger.info("Vegard API starting")
    
    # Initialize database
    if database:
        await database.initialize()
        logger.info("Database initialized")
    
    # Start components
    if mesh:
        await mesh.start()
        logger.info("Mesh started")
    if mission_planner and mission_planner.task_allocator:
        await mission_planner.task_allocator.start()
        logger.info("Task allocator started")
    if drift_monitor:
        await drift_monitor.start()
        logger.info("Drift monitor started")
    
    yield
    
    # Shutdown
    logger.info("Vegard API shutting down")
    if mesh:
        await mesh.stop()
    if drift_monitor:
        await drift_monitor.stop()
    if database:
        await database.close()
        logger.info("Database closed")
    logger.info("Vegard API shutdown complete")


app = FastAPI(
    title="Vegard API",
    description="Autonomous Agricultural Intelligence Platform - Drone Fleet Coordination",
    version="0.1.0",
    lifespan=lifespan,
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Prometheus metrics instrumentation
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["System"])
@limiter.limit("100/minute")
async def health_check(request: Request) -> dict:
    """
    Health check endpoint
    
    Returns the current health status of the Vegard system.
    
    Returns:
        dict: Health status information including mesh and drift monitor status
    """
    return {
        "status": "healthy",
        "mesh_active": mesh is not None and mesh._running,
        "drift_monitor_active": drift_monitor is not None,
    }


@app.get("/")
async def dashboard():
    """Serve the web dashboard"""
    from pathlib import Path
    
    dashboard_path = Path(__file__).parent.parent / "web" / "dashboard.html"
    if dashboard_path.exists():
        return HTMLResponse(content=dashboard_path.read_text())
    return HTMLResponse(content="<h1>Vegard Dashboard</h1><p>Dashboard file not found</p>")


@app.get("/fop", tags=["Field Operations"])
@limiter.limit("60/minute")
async def get_fused_picture(
    request: Request,
    entity_type: Optional[str] = Query(None, description="Filter by entity type (e.g., 'drone', 'sensor')"),
    include_tracks: bool = Query(True, description="Include entity tracks in the response"),
    include_soil: bool = Query(True, description="Include soil prediction data"),
) -> dict:
    """
    Get Fused Field Picture as GeoJSON
    
    Returns a GeoJSON representation of the current operational state including
    entity positions, tracks, and soil predictions.
    
    Args:
        entity_type: Optional filter for specific entity types
        include_tracks: Whether to include entity movement tracks
        include_soil: Whether to include soil prediction data
    
    Returns:
        dict: GeoJSON FeatureCollection with entity and soil data
    """
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


@app.get("/entities", tags=["Entities"])
@limiter.limit("60/minute")
async def get_entities(
    request: Request,
    entity_type: Optional[str] = Query(None, description="Filter by entity type (e.g., 'drone', 'sensor')", min_length=1, max_length=50),
    near_lat: Optional[float] = Query(None, description="Latitude for nearby query", ge=-90, le=90),
    near_lng: Optional[float] = Query(None, description="Longitude for nearby query", ge=-180, le=180),
    radius_m: Optional[float] = Query(None, description="Radius in meters for nearby query", gt=0, le=100000),
) -> dict:
    """
    Get all active entities
    
    Returns a list of all active entities in the mesh, with optional filtering
    by type or location proximity.
    
    Args:
        entity_type: Optional filter for specific entity types
        near_lat: Latitude for proximity-based filtering
        near_lng: Longitude for proximity-based filtering
        radius_m: Search radius in meters for proximity-based filtering
    
    Returns:
        dict: Dictionary containing entity count and list of entities
    """
    if not mesh:
        return JSONResponse(
            status_code=503,
            content={"error": "Mesh not initialized"},
        )

    # Sanitize entity_type if provided
    if entity_type:
        entity_type = sanitize_string(entity_type, max_length=50)

    # Validate coordinates if provided
    if near_lat is not None and near_lng is not None and radius_m is not None:
        if not validate_lat_lng(near_lat, near_lng):
            raise HTTPException(status_code=400, detail="Invalid latitude or longitude values")
        
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


@app.post("/tasks", tags=["Tasks"])
@limiter.limit("10/minute")
async def create_task(request: Request, task_request: dict) -> dict:
    """
    Create a new scan task
    
    Injects a new scan task into the mission planner for allocation to drones.
    
    Args:
        task_request: Dictionary containing task details including field_id, field_boundary, spectral_config, priority, and optional deadline_ms
    
    Returns:
        dict: Created task information with task_id
    
    Raises:
        HTTPException: If mission planner is not initialized
    """
    if not mission_planner:
        raise HTTPException(status_code=503, detail="Mission planner not initialized")

    from vegard.fabric.task_allocator import TaskRequest

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


@app.get("/tasks/{task_id}", tags=["Tasks"])
async def get_task_status(task_id: str) -> dict:
    """
    Get task status by ID
    
    Returns the current status and progress of a specific task.
    
    Args:
        task_id: Unique identifier of the task
    
    Returns:
        dict: Task status information including current status, progress, and assignment
    
    Raises:
        HTTPException: If task is not found
    """
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


@app.get("/tasks", tags=["Tasks"])
async def list_tasks(
    status: Optional[str] = Query(None, description="Filter by task status (e.g., 'pending', 'assigned', 'complete')", min_length=1, max_length=50),
    field_id: Optional[str] = Query(None, description="Filter by field identifier", min_length=1, max_length=100),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict:
    """
    List all tasks with optional filtering
    
    Returns a list of all tasks with optional filtering by status or field.
    
    Args:
        status: Optional filter for task status
        field_id: Optional filter for field identifier
        limit: Maximum number of tasks to return
    
    Returns:
        dict: Dictionary containing task count and list of tasks
    """
    if not mission_planner or not mission_planner.task_allocator:
        raise HTTPException(status_code=503, detail="Task allocator not initialized")

    # Sanitize string inputs
    if status:
        status = sanitize_string(status, max_length=50)
    if field_id:
        field_id = sanitize_string(field_id, max_length=100)

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


@app.get("/drift", tags=["Drift Monitor"])
async def get_drift_report(
    field_id: Optional[str] = None,
    min_severity: str = "info",
) -> dict:
    """
    Get cross-node drift correlation report
    
    Returns a report of drift signals and their spatial/temporal correlations
    across nodes in the mesh.
    
    Args:
        field_id: Optional filter for specific field identifier
        min_severity: Minimum severity level for drift alerts (e.g., 'info', 'warning', 'error')
    
    Returns:
        dict: Drift report including alerts count and correlation data
    """
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


@app.get("/missions", tags=["Missions"])
async def get_missions() -> dict:
    """
    Get list of active missions
    
    Returns a list of all active missions managed by the mission planner.
    
    Returns:
        dict: Dictionary containing mission count and list of missions
    """
    if not mission_planner:
        return JSONResponse(
            status_code=503,
            content={"error": "Mission planner not initialized"},
        )

    missions = mission_planner.get_active_missions()
    return {"missions": missions, "count": len(missions)}


@app.get("/missions/{mission_id}", tags=["Missions"])
async def get_mission_status(mission_id: str) -> dict:
    """
    Get mission status by ID
    
    Returns the current status and progress of a specific mission.
    
    Args:
        mission_id: Unique identifier of the mission
    
    Returns:
        dict: Mission status information
    
    Raises:
        HTTPException: If mission is not found
    """
    # Sanitize mission_id
    mission_id = sanitize_string(mission_id, max_length=100)
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


@app.get("/stats", tags=["System"])
async def get_stats() -> dict:
    """
    Get system statistics
    
    Returns comprehensive statistics about the current system state including
    mesh, task allocator, and drift monitor metrics.
    
    Returns:
        dict: System statistics including entity counts, task counts, and component status
    """
    stats = {
        "mesh": await mesh.store.get_stats() if mesh else None,
        "fop": fop.get_stats() if fop else None,
        "mission": mission_planner.get_stats() if mission_planner else None,
        "drift": await drift_monitor.get_stats() if drift_monitor else None,
    }
    return stats


@app.websocket("/stream")
async def websocket_stream(websocket: WebSocket):
    """
    WebSocket for real-time entity stream
    
    Provides a real-time stream of entity updates using WebSocket protocol.
    Includes automatic reconnection support and error handling.
    
    The client should implement reconnection logic on disconnect.
    """
    try:
        await websocket.accept()
        
        if not mesh:
            await websocket.send_json({"error": "Mesh not initialized"})
            await websocket.close(code=1011, reason="Service unavailable")
            return

        queue = asyncio.Queue(maxsize=1000)
        ping_interval = 30  # Send ping every 30 seconds
        last_ping = asyncio.get_event_loop().time()

        def on_entity_update(entity):
            try:
                queue.put_nowait(entity)
            except asyncio.QueueFull:
                logger.warning("WebSocket queue full, dropping update")

        mesh.store.subscribe(on_entity_update)

        try:
            while True:
                # Wait for entity updates with timeout for ping
                try:
                    entity = await asyncio.wait_for(queue.get(), timeout=5.0)
                    await websocket.send_json(json.loads(entity.model_dump_json()))
                except asyncio.TimeoutError:
                    # Send periodic ping to keep connection alive
                    current_time = asyncio.get_event_loop().time()
                    if current_time - last_ping > ping_interval:
                        await websocket.send_json({"type": "ping", "timestamp": current_time})
                        last_ping = current_time
                        
        except WebSocketDisconnect as e:
            logger.info("WebSocket disconnected", code=e.code, reason=e.reason)
        except Exception as e:
            logger.error("WebSocket error", error=str(e))
            await websocket.close(code=1011, reason="Internal server error")
        finally:
            mesh.store.unsubscribe(on_entity_update)
            logger.info("WebSocket cleanup completed")
            
    except Exception as e:
        logger.error("WebSocket connection error", error=str(e))
        await websocket.close(code=1011, reason="Connection error")


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
