"""Syndar MVP — Minimal viable implementations

Simple working versions of core components.
Use these to get end-to-end system running in hours, not days.
"""

from syndar.mvp.mesh import SimpleMesh
from syndar.mvp.task_allocator import SimpleTaskAllocator, assign_task_simple
from syndar.mvp.node_agent import SimpleNodeAgent
from syndar.mvp.aip_bridge import SimpleAIPBridge
from syndar.mvp.drift_monitor import SimpleDriftMonitor

__all__ = [
    "SimpleMesh",
    "SimpleTaskAllocator", 
    "assign_task_simple",
    "SimpleNodeAgent",
    "SimpleAIPBridge",
    "SimpleDriftMonitor",
]
