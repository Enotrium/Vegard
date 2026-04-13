""" - Map Syndar to patterns

Implements anduril.taskmanager.v1 patterns for Syndar compatibility.
This allows Syndar to interface with Lattice infrastructure if needed,
while keeping internal implementation simple.

Key patterns from buf.build/anduril/lattice-sdk:
- ListenAsAgent: Agents listen for tasks (not poll)
- TaskAssignment: Bidirectional streaming task control
- EntityState: Similar to Syndar's DroneState but Lattice-formatted
"""

from dataclasses import dataclass
from typing import Optional, Callable, Iterator
from enum import Enum


class TaskStatus(Enum):
    """Anduril taskmanager.v1.TaskStatus equivalent"""
    TASK_STATUS_UNSPECIFIED = 0
    TASK_STATUS_PENDING = 1
    TASK_STATUS_ASSIGNED = 2
    TASK_STATUS_IN_PROGRESS = 3
    TASK_STATUS_COMPLETED = 4
    TASK_STATUS_FAILED = 5
    TASK_STATUS_CANCELLED = 6


@dataclass
class AndurilTask:
    """Maps to anduril.taskmanager.v1.Task"""
    task_id: str
    task_type: str  # "syndar/field_scan"
    entity_id: str  # drone assigned to
    status: TaskStatus
    
    # Syndar-specific payload in Anduril parameters field
    parameters: dict  # {"field_id": ..., "location": ..., "priority": ...}
    
    created_at_ms: int
    updated_at_ms: int
    
    def to_syndar_task(self):
        """Convert to Syndar MVP Task"""
        from syndar.mvp.task_allocator import Task
        params = self.parameters
        return Task(
            task_id=self.task_id,
            field_id=params.get("field_id", "unknown"),
            lat=params.get("location", {}).get("lat", 0.0),
            lng=params.get("location", {}).get("lng", 0.0),
            priority=params.get("priority", 0.5)
        )


@dataclass  
class AndurilEntityState:
    """Maps to anduril.entitymanager.v1.Entity"""
    entity_id: str
    entity_type: str  # "drone"
    
    # Position (GeoPoint in Anduril)
    latitude: float
    longitude: float
    altitude_m: float
    
    # Syndar-specific in ontology
    battery_pct: float
    status: str  # idle, scanning, etc.
    current_task_id: Optional[str]
    
    timestamp_ms: int
    
    def to_syndar_state(self):
        """Convert to Syndar DroneState"""
        from syndar.mvp.mesh import DroneState, Position
        return DroneState(
            drone_id=self.entity_id,
            position=Position(
                lat=self.latitude,
                lng=self.longitude,
                alt=self.altitude_m
            ),
            status=self.status,
            current_task_id=self.current_task_id,
            battery_pct=self.battery_pct
        )


class AndurilTaskStream:
    """
    Implements anduril.taskmanager.v1.ListenAsAgent pattern
    
    Instead of polling, agents (drones) open a bidirectional stream
    and receive tasks as they are assigned. This is more efficient
    than the simple polling in MVP.
    """
    
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self._task_queue: list[AndurilTask] = []
        self._status_callbacks: list[Callable[[AndurilTask], None]] = []
        self._running = False
    
    def listen(self) -> Iterator[AndurilTask]:
        """
        anduril.taskmanager.v1.ListenAsAgent RPC
        
        Yields tasks as they are assigned to this agent.
        This is a blocking generator - caller iterates forever.
        """
        self._running = True
        
        while self._running:
            # Check for new tasks
            if self._task_queue:
                task = self._task_queue.pop(0)
                task.status = TaskStatus.ASSIGNED
                task.entity_id = self.agent_id
                yield task
            
            # In real implementation, this would block on gRPC stream
            import time
            time.sleep(0.1)
    
    def submit_task_status(self, task: AndurilTask) -> None:
        """
        anduril.taskmanager.v1.UpdateTaskStatus
        
        Agent reports task status back to task manager.
        """
        for cb in self._status_callbacks:
            cb(task)
    
    def assign_task(self, task: AndurilTask) -> None:
        """Called by task manager to assign task to this agent"""
        self._task_queue.append(task)
    
    def stop(self) -> None:
        """Stop listening"""
        self._running = False


class AndurilTaskManager:
    """
    Implements anduril.taskmanager.v1.TaskManager
    
    Central coordinator that assigns tasks to agents.
    """
    
    def __init__(self):
        self._agents: dict[str, AndurilTaskStream] = {}  # agent_id -> stream
        self._tasks: dict[str, AndurilTask] = {}  # task_id -> task
    
    def register_agent(self, agent_id: str) -> AndurilTaskStream:
        """Register a drone agent"""
        stream = AndurilTaskStream(agent_id)
        self._agents[agent_id] = stream
        return stream
    
    def create_task(self, parameters: dict) -> AndurilTask:
        """Create a new task"""
        import time
        task_id = f"task-{int(time.time() * 1000)}"
        
        task = AndurilTask(
            task_id=task_id,
            task_type="syndar/field_scan",
            entity_id="",  # Not assigned yet
            status=TaskStatus.TASK_STATUS_PENDING,
            parameters=parameters,
            created_at_ms=int(time.time() * 1000),
            updated_at_ms=int(time.time() * 1000)
        )
        
        self._tasks[task_id] = task
        return task
    
    def assign_to_agent(self, task_id: str, agent_id: str) -> bool:
        """
        Assign task to specific agent.
        Uses Anduril pattern: task manager pushes to agent stream.
        """
        task = self._tasks.get(task_id)
        agent = self._agents.get(agent_id)
        
        if not task or not agent:
            return False
        
        task.entity_id = agent_id
        task.status = TaskStatus.TASK_STATUS_ASSIGNED
        agent.assign_task(task)
        return True


class SyndarAndurilBridge:
    """
    Bridge between Syndar MVP and Anduril Lattice patterns.
    
    Allows Syndar to use Anduril-compatible task management
    while keeping internal implementation simple.
    """
    
    def __init__(self, task_manager: AndurilTaskManager):
        self.task_manager = task_manager
        self._syndar_allocator = None  # Will be set
    
    def enable_for_allocator(self, allocator):
        """Connect to Syndar SimpleTaskAllocator"""
        self._syndar_allocator = allocator
        
        # Subscribe to allocator to assign via Anduril pattern
        # (In real code, this would wire up the streams)
    
    def create_syndar_task(self, task: "Task") -> AndurilTask:
        """Convert Syndar Task to Anduril format and register"""
        return self.task_manager.create_task({
            "field_id": task.field_id,
            "location": {"lat": task.location["lat"], "lng": task.location["lng"]},
            "priority": task.priority
        })
    
    def start_agent_listener(self, agent_id: str, on_task: Callable[["Task"], None]):
        """
        Start Anduril-style agent listener for a drone.
        
        When tasks arrive, converts to Syndar format and calls on_task.
        """
        stream = self.task_manager.register_agent(agent_id)
        
        def listen_loop():
            for anduril_task in stream.listen():
                syndar_task = anduril_task.to_syndar_task()
                on_task(syndar_task)
        
        import threading
        threading.Thread(target=listen_loop, daemon=True).start()
        
        return stream


# Integration example in docstring
"""
Usage with Syndar MVP:

    # Create Anduril-style task manager
    anduril_tm = AndurilTaskManager()
    
    # Create Syndar components
    mesh = SimpleMesh()
    allocator = SimpleTaskAllocator(mesh)
    
    # Bridge them
    bridge = SyndarAndurilBridge(anduril_tm)
    bridge.enable_for_allocator(allocator)
    
    # For each drone, start Anduril-style listener
    def on_task_assigned(task):
        print(f"Drone received task: {task.id}")
        # Execute...
    
    bridge.start_agent_listener("drone-001", on_task_assigned)
    
    # Create tasks through Anduril API
    anduril_task = bridge.create_syndar_task(
        Task("task-1", "field-1", 39.0, -77.0)
    )
    
    # Assign via Anduril
    anduril_tm.assign_to_agent(anduril_task.task_id, "drone-001")
    # Drone receives via its listener stream
"""
