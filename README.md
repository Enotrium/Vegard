# Syndar

**Autonomous Agricultural Intelligence Platform — Drone Fleet Coordination & Soil Intelligence Fabric**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Syndar is the autonomous systems coordination layer for the Enotrium agricultural stack. Analogous to Anduril Lattice for defense, Syndar coordinates fleets of UAVs carrying hyperspectral sensors, fuses their data into a shared operational picture, and routes soil intelligence to the AIP supply chain platform.

## Quick Start

```bash
# Install
pip install -e .
pip install -e ../arthedain  # Optional: edge learning dependency

# Start Syndar API
python -m syndar.command.api

# Simulate a fleet of 4 drones
python sandbox/simulate_fleet.py --drones 4 --duration 60

# Test AIP integration
python -m sandbox.mock_aip_server --port 3000 &
python sandbox/test_aip_bridge.py --aip-url http://localhost:3000
```

## Architecture

Syndar is structured in three layers:

### Layer 3 — Node (Edge Intelligence)
- **NodeAgent**: Drone ↔ fabric bridge, executes scan tasks
- **SpectralBridge**: Arthedain SNN → Hyperspectral-Restruct CNN adapter
- **Integrations**: Arthedain client, HSI API client with graceful mock fallbacks

### Layer 2 — Fabric (Coordination)
- **Mesh**: Gossip protocol state mesh (Anduril Lattice pattern)
- **TaskAllocator**: Auction-based tasking with preemption
- **DriftMonitor**: Cross-node drift correlation (key differentiator)
- **Attestation**: PGP-signed soil outputs for AIP ZK chain
- **Transport**: gRPC (drone↔node) + MQTT (cloud↔AIP)

### Layer 1 — Command (Operational Picture)
- **FusedFieldPicture**: Materialized view, GeoJSON export for Mapbox
- **MissionPlanner**: Goal-to-task conversion, priority scoring
- **AIPBridge**: Clean POST to `/api/syndar/ingest`, zero circular coupling
- **API**: REST + WebSocket operator interface

## Key Features

- **Spatial Drift Correlation**: Detects contamination events by correlating Arthedain E(t) signals across drones
- **Auction-Based Tasking**: Distributed task allocation without central coordination
- **Attestation**: PGP-signed predictions for supply chain provenance
- **Multi-Protocol Transport**: gRPC for performance, MQTT for compatibility
- **Sandbox Mode**: Full simulation without live hardware

## Repository Structure

```
EnotriumSyndicate/Syndar/
├── syndar/
│   ├── node/              # Edge intelligence bridge
│   ├── fabric/            # Coordination services
│   ├── command/           # Operational picture + AIP integration
│   └── proto/             # Protocol Buffer definitions
├── configs/               # YAML configurations + test polygons
├── sandbox/               # Simulators + mock services
├── hardware/              # FPGA interface spec
├── tests/                 # Test suite
├── pyproject.toml         # Package configuration
└── CLAUDE.md              # Full architecture documentation
```

## Dependencies

- Python 3.11+
- gRPC, Protobuf, MQTT
- FastAPI, WebSockets
- NumPy, Shapely
- gnupg (for attestation)
- arthedain (optional, local editable install)

## Documentation

- Full architecture: [CLAUDE.md](CLAUDE.md)
- FPGA interface: [hardware/fpga_interface.md](hardware/fpga_interface.md)
- API reference: `GET /docs` when running API server

## The Enotrium Stack

| Repo | Role |
|------|------|
| `arthedain` | Edge SNN learning algorithm |
| `Hyperspectral-Restruct` | 3D CNN soil prediction model |
| **Syndar** | **Fleet coordination (this repo)** |
| `AIP` | Supply chain command center |
| `Icarus` | Drone hardware platform (planned) |

## License

MIT — Enotrium Syndicate
 Syndar