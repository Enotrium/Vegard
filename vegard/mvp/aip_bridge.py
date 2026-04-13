"""SimpleAIPBridge - POST to AIP with retry

MVP: Just send results downstream. Add real crypto later.
"""

import time
import requests
from typing import Optional


class SimpleAIPBridge:
    """Minimal AIP bridge - POST with retry"""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.stats = {
            "sent": 0,
            "failed": 0,
            "retries": 0
        }
    
    def send(self, result: dict) -> dict:
        """Send result to AIP
        
        Args:
            result: Scan result dict from NodeAgent
            
        Returns:
            {"success": bool, "scan_id": str, "error": str|None}
        """
        prediction = result.get("prediction", {})
        position = result.get("position", {})
        
        # Build AIP payload
        payload = {
            "vegard_drone_id": result.get("drone_id"),
            "vegard_timestamp_ms": result.get("timestamp_ms"),
            "vegard_task_id": result.get("task_id"),
            "field_id": result.get("field_id"),
            "latitude": position.get("lat", 0.0),
            "longitude": position.get("lng", 0.0),
            "altitude_m": position.get("alt", 0.0),
            "land_value_score": prediction.get("land_value_score", 0.0),
            "remediation_priority": prediction.get("remediation_priority", 0.0),
            "nutrients": prediction.get("nutrients", {}),
            "contamination_detected": prediction.get("contamination_detected", False),
            "contaminants": prediction.get("contaminants", []),
            "spectral_hash": prediction.get("spectral_hash", ""),
            "model_version": prediction.get("model_version", ""),
            "drift_e": result.get("drift_e", 0.0),
            "drift_flag": result.get("drift_flag", False),
            "signature": "stub-signature",  # MVP: Add PGP later
        }
        
        # Try to send with exponential backoff
        for attempt in range(3):
            try:
                headers = {}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"
                
                resp = requests.post(
                    f"{self.base_url}/api/vegard/ingest",
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                resp.raise_for_status()
                
                self.stats["sent"] += 1
                
                return {
                    "success": True,
                    "scan_id": result.get("task_id"),
                    "aip_response": resp.json() if resp.content else None
                }
                
            except requests.exceptions.RequestException as e:
                if attempt == 2:  # Last attempt
                    self.stats["failed"] += 1
                    return {
                        "success": False,
                        "scan_id": result.get("task_id"),
                        "error": str(e)
                    }
                
                self.stats["retries"] += 1
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
        
        return {"success": False, "scan_id": result.get("task_id"), "error": "exhausted retries"}
    
    def send_batch(self, results: list[dict]) -> dict:
        """Send batch of results"""
        sent = 0
        failed = 0
        
        for result in results:
            r = self.send(result)
            if r["success"]:
                sent += 1
            else:
                failed += 1
        
        return {
            "success": failed == 0,
            "sent": sent,
            "failed": failed
        }
    
    def check_health(self) -> bool:
        """Check if AIP is reachable"""
        try:
            resp = requests.get(f"{self.base_url}/health", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False
    
    def get_stats(self) -> dict:
        """Get bridge statistics"""
        return self.stats.copy()
