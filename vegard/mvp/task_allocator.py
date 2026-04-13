"""Simple task allocation - nearest available drone

MVP: No auctions, no preemption. Just: who is closest and idle?
"""

import math
from typing import Optional
from vegard.mvp.mesh import SimpleMesh, DroneState


class Task:
    """Simple task definition"""
    
    def __init__(self, task_id: str, field_id: str, lat: float, lng: float, priority: float = 0.5):
        self.id = task_id
        self.field_id = field_id
        self.location = {"lat": lat, "lng": lng}
        self.priority = priority
        self.assigned_to: Optional[str] = None
        self.status: str = "pending"  # pending, assigned, completed, failed


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate distance in meters between two lat/lng points"""
    R = 6371000  # Earth radius in meters
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    
    a = (math.sin(delta_phi / 2) ** 2 + 
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def assign_task_simple(task: Task, mesh: SimpleMesh) -> Optional[DroneState]:
    """Assign task to nearest available drone
    
    Args:
        task: Task to assign
        mesh: SimpleMesh instance with drone states
    
    Returns:
        DroneState of assigned drone, or None if no drones available
    """
    available = mesh.get_available()
    
    if not available:
        return None
    
    # Find nearest
    def distance(drone: DroneState) -> float:
        return haversine_distance(
            drone.position.lat, drone.position.lng,
            task.location["lat"], task.location["lng"]
        )
    
    nearest = min(available, key=distance)
    
    # Check if too far (> 50km)
    if distance(nearest) > 50000:
        return None
    
    return nearest


class SimpleTaskAllocator:
    """Simple sequential task allocator"""
    
    def __init__(self, mesh: SimpleMesh):
        self.mesh = mesh
        self.pending_tasks: list[Task] = []
        self.active_tasks: dict[str, Task] = {}  # task_id -> Task
        self.completed_tasks: list[Task] = []
    
    def add_task(self, task: Task) -> bool:
        """Add task to pending queue"""
        self.pending_tasks.append(task)
        return True
    
    def process_pending(self) -> list[tuple[Task, DroneState]]:
        """Process pending tasks, assign to available drones"""
        assignments = []
        still_pending = []
        
        for task in self.pending_tasks:
            drone = assign_task_simple(task, self.mesh)
            
            if drone:
                # Assign
                task.assigned_to = drone.drone_id
                task.status = "assigned"
                self.active_tasks[task.id] = task
                assignments.append((task, drone))
                
                # Update drone state
                from vegard.mvp.mesh import Position
                drone.status = "assigned"
                drone.current_task_id = task.id
                self.mesh.update(drone.drone_id, drone)
            else:
                still_pending.append(task)
        
        self.pending_tasks = still_pending
        return assignments
    
    def complete_task(self, task_id: str, success: bool = True) -> None:
        """Mark task as completed"""
        task = self.active_tasks.pop(task_id, None)
        if task:
            task.status = "completed" if success else "failed"
            self.completed_tasks.append(task)
    
    def get_stats(self) -> dict:
        """Get allocator stats"""
        return {
            "pending": len(self.pending_tasks),
            "active": len(self.active_tasks),
            "completed": len(self.completed_tasks),
        }
