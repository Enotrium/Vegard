"""Vegard MVP — Minimal viable implementations

Simple working versions of core components.
Use these to get end-to-end system running in hours, not days.
"""

from vegard.mvp.mesh import SimpleMesh
from vegard.mvp.task_allocator import SimpleTaskAllocator, assign_task_simple
from vegard.mvp.node_agent import SimpleNodeAgent
from vegard.mvp.aip_bridge import SimpleAIPBridge
from vegard.mvp.drift_monitor import SimpleDriftMonitor

__all__ = [
    "SimpleMesh",
    "SimpleTaskAllocator", 
    "assign_task_simple",
    "SimpleNodeAgent",
    "SimpleAIPBridge",
    "SimpleDriftMonitor",
]
