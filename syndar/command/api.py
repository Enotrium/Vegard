"""Syndar REST + WebSocket API - Operator Interface

- GET /fop - GeoJSON field picture
- GET /entities - Active drones
- POST /tasks - Inject scan task
- GET /drift - Cross-node drift report
- WebSocket /stream - Real-time entity updates
"""

import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import structlog
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from syndar.command.fop import FusedFieldPicture
from syndar.command.mission import MissionPlanner
from syndar.fabric.drift_monitor import DriftMonitor
from syndar.fabric.mesh import Mesh

logger = structlog.get_logger()


# Global state (injected on startup)
mesh: Optional[Mesh] = None
fop: Optional[FusedFieldPicture] = None
mission_planner: Optional[MissionPlanner] = None
drift_monitor: Optional[DriftMonitor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Syndar API starting")
    if mesh:
        await mesh.start()
    if drift_monitor:
        await drift_monitor.start()
    yield
    # Shutdown
    logger.info("Syndar API shutting down")
    if mesh:
        await mesh.stop()
    if drift_monitor:
        await drift_monitor.stop()


app = FastAPI(
    title="Syndar API",
    description="Autonomous Agricultural Intelligence Platform - Drone Fleet Coordination",
    version="0.1.0",
    lifespan=lifespan,
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
        return JSONResponse(
            status_code=503,
            content={"error": "Mission planner not initialized"},
        )

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
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid task request: {str(e)}"},
        )


@app.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """Get task status"""
    if not mission_planner or not mission_planner.task_allocator:
        return JSONResponse(
            status_code=503,
            content={"error": "Task allocator not initialized"},
        )

    # TODO: Query task status from allocator
    return {
        "task_id": task_id,
        "status": "unknown",
    }


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


def setup_api(
    mesh_instance: Mesh,
    fop_instance: FusedFieldPicture,
    mission_planner_instance: MissionPlanner,
    drift_monitor_instance: Optional[DriftMonitor] = None,
):
    """Setup API with injected dependencies"""
    global mesh, fop, mission_planner, drift_monitor
    mesh = mesh_instance
    fop = fop_instance
    mission_planner = mission_planner_instance
    drift_monitor = drift_monitor_instance
