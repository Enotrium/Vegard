"""Mock AIP Server - Minimal FastAPI for testing Syndar integration

Implements /api/syndar/ingest and related endpoints for
end-to-end testing without full AIP deployment.
"""

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = structlog.get_logger()

app = FastAPI(title="Mock AIP Server", version="0.1.0")

# Storage
_received_payloads: list[dict] = []
_field_status: dict[str, dict] = defaultdict(
    lambda: {
        "scanned": False,
        "prediction_count": 0,
        "last_scan_ms": None,
    }
)


class SoilIngestPayload(BaseModel):
    """AIP soil prediction payload"""

    syndar_scan_id: str
    syndar_drone_id: str
    syndar_timestamp_ms: int
    field_id: str
    farm_id: Optional[str] = None
    nitrogen_mg_kg: Optional[float] = None
    carbon_percent: Optional[float] = None
    phosphorus_mg_kg: Optional[float] = None
    potassium_mg_kg: Optional[float] = None
    moisture_percent: Optional[float] = None
    ph: Optional[float] = None
    microplastic_detected: bool = False
    microplastic_types: list[str] = Field(default_factory=list)
    pfas_detected: bool = False
    heavy_metals_detected: bool = False
    land_value_score: float = 0.0
    remediation_priority: float = 0.0
    phytoremediation_suitability: float = 0.0
    spectral_hash: str = ""
    model_version: str = ""
    signature: str = ""
    signer_fingerprint: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    accuracy_m: float = 1.0


class BatchIngestPayload(BaseModel):
    """Batch ingest payload"""

    payloads: list[SoilIngestPayload]


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "received_payloads": len(_received_payloads),
        "fields_with_data": len(_field_status),
    }


@app.get("/api/syndar/health")
async def syndar_health():
    """Syndar-specific health check"""
    return {
        "ingest_endpoint_active": True,
        "received_count": len(_received_payloads),
        "last_received_ms": (
            _received_payloads[-1].get("_received_at_ms") if _received_payloads else None
        ),
    }


@app.post("/api/syndar/ingest")
async def ingest(payload: SoilIngestPayload, request: Request):
    """Ingest single soil prediction"""
    received_at = int(time.time() * 1000)

    # Store payload
    payload_dict = payload.model_dump()
    payload_dict["_received_at_ms"] = received_at
    payload_dict["_source_ip"] = request.client.host if request.client else None

    _received_payloads.append(payload_dict)

    # Update field status
    _field_status[payload.field_id]["scanned"] = True
    _field_status[payload.field_id]["prediction_count"] += 1
    _field_status[payload.field_id]["last_scan_ms"] = payload.syndar_timestamp_ms

    logger.info(
        "Payload ingested",
        scan_id=payload.syndar_scan_id,
        field_id=payload.field_id,
        drone_id=payload.syndar_drone_id,
        land_value=payload.land_value_score,
    )

    return {
        "status": "accepted",
        "scan_id": payload.syndar_scan_id,
        "aip_record_id": f"aip-{received_at}",
        "received_at_ms": received_at,
    }


@app.post("/api/syndar/ingest/batch")
async def ingest_batch(payload: BatchIngestPayload, request: Request):
    """Ingest batch of soil predictions"""
    received_at = int(time.time() * 1000)

    accepted = 0
    rejected = 0

    for p in payload.payloads:
        try:
            payload_dict = p.model_dump()
            payload_dict["_received_at_ms"] = received_at
            _received_payloads.append(payload_dict)

            _field_status[p.field_id]["scanned"] = True
            _field_status[p.field_id]["prediction_count"] += 1
            _field_status[p.field_id]["last_scan_ms"] = p.syndar_timestamp_ms

            accepted += 1
        except Exception as e:
            logger.error("Batch payload rejected", error=str(e))
            rejected += 1

    logger.info(
        "Batch ingested",
        accepted=accepted,
        rejected=rejected,
        total=len(payload.payloads),
    )

    return {
        "status": "completed",
        "accepted_count": accepted,
        "rejected_count": rejected,
        "processed_at_ms": received_at,
    }


@app.get("/api/syndar/fields/{field_id}/status")
async def get_field_status(field_id: str):
    """Get field scan status"""
    status = _field_status.get(field_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Field {field_id} not found")

    return {
        "field_id": field_id,
        **status,
        "aip_contract_status": "pending",
    }


@app.get("/api/syndar/payloads")
async def list_payloads(
    field_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """List received payloads (for debugging)"""
    payloads = _received_payloads

    if field_id:
        payloads = [p for p in payloads if p.get("field_id") == field_id]

    total = len(payloads)
    payloads = payloads[offset : offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "payloads": payloads,
    }


@app.get("/api/syndar/stats")
async def get_stats():
    """Get ingestion statistics"""
    return {
        "total_payloads": len(_received_payloads),
        "fields_with_data": len(_field_status),
        "payloads_per_field": {
            field_id: status["prediction_count"]
            for field_id, status in _field_status.items()
        },
        "contamination_detected": sum(
            1
            for p in _received_payloads
            if p.get("microplastic_detected") or p.get("pfas_detected")
        ),
        "high_value_fields": sum(
            1
            for status in _field_status.values()
            if status.get("prediction_count", 0) > 5
        ),
    }


@app.post("/api/syndar/reset")
async def reset():
    """Reset all stored data (for testing)"""
    global _received_payloads, _field_status
    _received_payloads = []
    _field_status = defaultdict(
        lambda: {
            "scanned": False,
            "prediction_count": 0,
            "last_scan_ms": None,
        }
    )
    return {"status": "reset"}


@app.post("/api/syndar/export")
async def export_data(output_path: Optional[str] = None):
    """Export all received data to file"""
    if not output_path:
        output_path = f"sandbox/data/aip_export_{int(time.time())}.json"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    export_data = {
        "exported_at_ms": int(time.time() * 1000),
        "payload_count": len(_received_payloads),
        "field_count": len(_field_status),
        "payloads": _received_payloads,
        "field_status": dict(_field_status),
    }

    with open(output_path, "w") as f:
        json.dump(export_data, f, indent=2, default=str)

    logger.info("Data exported", path=str(output_path), count=len(_received_payloads))

    return {
        "status": "exported",
        "path": str(output_path),
        "payload_count": len(_received_payloads),
    }


def main():
    """Run mock AIP server"""
    import argparse

    parser = argparse.ArgumentParser(description="Mock AIP Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument("--reload", action="store_true")

    args = parser.parse_args()

    logger.info("Starting mock AIP server", host=args.host, port=args.port)
    uvicorn.run(
        "sandbox.mock_aip_server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
