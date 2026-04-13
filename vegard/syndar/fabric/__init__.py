"""Fabric Layer - State Mesh + Coordination Services

Analogous to Anduril Lattice Mesh:
- State Mesh: Gossip protocol entity pub/sub
- Task Allocator: Auction-based field assignment
- Drift Monitor: Cross-node concept drift detection
- Attestation: Node identity, signed soil outputs
- Transport Bus: gRPC + Protobuf / MQTT mesh
"""

from vegard.fabric.mesh import Mesh, EntityStore
from vegard.fabric.task_allocator import TaskAllocator
from vegard.fabric.drift_monitor import DriftMonitor
from vegard.fabric.attestation import AttestationService
from vegard.fabric.transport import TransportBus

__all__ = [
    "Mesh",
    "EntityStore",
    "TaskAllocator",
    "DriftMonitor",
    "AttestationService",
    "TransportBus",
]
