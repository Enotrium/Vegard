"""Fabric Layer - State Mesh + Coordination Services

Analogous to Anduril Lattice Mesh:
- State Mesh: Gossip protocol entity pub/sub
- Task Allocator: Auction-based field assignment
- Drift Monitor: Cross-node concept drift detection
- Attestation: Node identity, signed soil outputs
- Transport Bus: gRPC + Protobuf / MQTT mesh
"""

from syndar.fabric.mesh import Mesh, EntityStore
from syndar.fabric.task_allocator import TaskAllocator
from syndar.fabric.drift_monitor import DriftMonitor
from syndar.fabric.attestation import AttestationService
from syndar.fabric.transport import TransportBus

__all__ = [
    "Mesh",
    "EntityStore",
    "TaskAllocator",
    "DriftMonitor",
    "AttestationService",
    "TransportBus",
]
