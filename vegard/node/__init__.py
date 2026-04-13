"""Node Layer - Edge Intelligence Bridge

The bridge between a single drone's onboard intelligence and the Vegard fabric.
This is the only module Vegard adds to the Arthedain execution environment.

Components:
- NodeAgent: Receives tasks, executes scans, publishes results
- SpectralBridge: Arthedain SNN → Hyperspectral-Restruct CNN adapter
- ArthedainClient: Interface to edge SNN learning algorithm
- HSIClient: Interface to soil prediction model
"""

from syndar.node.node_agent import NodeAgent
from syndar.node.spectral_bridge import SpectralBridge

__all__ = ["NodeAgent", "SpectralBridge"]
