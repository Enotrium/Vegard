# FPGA Interface Specification

## NodeAgent ↔ Artix-7 Interface

This document specifies the hardware interface between the Syndar NodeAgent and the Xilinx Artix-7 FPGA running the Arthedain SNN.

---

## Overview

**Target Platform:** Xilinx Artix-7 (XC7A100T or XC7A200T)  
**Development Board:** Digilent Nexys A7 or custom carrier  
**Power Budget:** ~2.5 mW at 10 MHz  
**Data Format:** INT8/INT16 fixed-point

---

## Physical Interface

### Option 1: USB-UART Bridge (Development)

For development and testing:
- **Interface:** USB 2.0 Full Speed (12 Mbps)
- **Protocol:** Custom framing over UART
- **Voltage:** 3.3V TTL
- **Baud Rate:** 921600 (configurable)

### Option 2: SPI (Production)

For production deployment:
- **Interface:** SPI Mode 0 (CPOL=0, CPHA=0)
- **Clock:** Up to 25 MHz
- **Signals:**
  - MOSI (Master Out Slave In)
  - MISO (Master In Slave Out)
  - SCK (Serial Clock)
  - CS_N (Chip Select, Active Low)
  - INT_N (Interrupt, Active Low)
- **Voltage:** 3.3V or 1.8V (configurable)

### Option 3: Ethernet (Future)

For high-bandwidth applications:
- **Interface:** RMII or RGMII
- **Speed:** 100 Mbps or 1 Gbps
- **Protocol:** Custom UDP framing

---

## Protocol Layer

### Message Framing

All messages use a fixed 8-byte header + variable payload:

```
Byte 0:     Sync Byte 0x55
Byte 1:     Sync Byte 0xAA
Byte 2-3:   Message Type (uint16, little-endian)
Byte 4-7:   Payload Length (uint32, little-endian)
Byte 8-N:   Payload (variable length)
Byte N+1-2: CRC-16 CCITT (little-endian)
```

### Message Types

#### Control Messages (0x0000-0x00FF)

| Type | Code | Direction | Description |
|------|------|-----------|-------------|
| NOP | 0x0000 | Bidirectional | No operation / heartbeat |
| RESET | 0x0001 | Host→FPGA | Soft reset SNN state |
| CONFIG | 0x0002 | Host→FPGA | Configure SNN parameters |
| STATUS_REQ | 0x0010 | Host→FPGA | Request status |
| STATUS_RSP | 0x0011 | FPGA→Host | Status response |
| ERROR | 0x001F | FPGA→Host | Error notification |

#### Data Messages (0x0100-0x01FF)

| Type | Code | Direction | Description |
|------|------|-----------|-------------|
| SPIKE_IN | 0x0100 | Host→FPGA | Input spike data |
| TRACE_OUT | 0x0101 | FPGA→Host | Eligibility trace output |
| WEIGHT_UPDATE | 0x0102 | FPGA→Host | Weight update notification |
| DRIFT_ALERT | 0x0110 | FPGA→Host | Drift threshold exceeded |

---

## SNN Configuration

### Network Architecture

The Arthedain SNN on FPGA implements:

```
Input Layer:    200 neurons (spectral bands)
Hidden Layer:   64 LIF neurons (recurrent)
Output Layer:   10 readout neurons

Total Weights:  ~14K parameters
Memory:         ~28KB (INT16 weights)
```

### Configuration Parameters

Configure via CONFIG message (0x0002):

```c
struct SNNConfig {
    uint16_t version;           // Config format version
    uint16_t input_neurons;     // Input layer size
    uint16_t hidden_neurons;    // Hidden layer size
    uint16_t output_neurons;    // Output layer size
    
    // Timing parameters (microseconds)
    uint32_t tau_fast_us;       // Fast timescale (~100000)
    uint32_t tau_slow_us;       // Slow timescale (~700000)
    
    // Learning rates (Q8.8 fixed point)
    uint16_t eta_q8_8;          // Learning rate η
    
    // Thresholds (Q0.16 fixed point)
    uint16_t drift_threshold_q0_16;
    
    // Spike threshold (mV, Q8.8)
    uint16_t v_thresh_q8_8;
    
    // Reset potential (mV, Q8.8)
    uint16_t v_reset_q8_8;
};
```

---

## Data Formats

### Input Spike Format (SPIKE_IN)

```c
struct SpikePacket {
    uint32_t timestamp_us;      // Microsecond timestamp
    uint16_t neuron_id;         // Source neuron (0-199)
    int16_t weight_q8_8;        // Spike weight (Q8.8)
};
```

### Eligibility Trace Format (TRACE_OUT)

```c
struct TracePacket {
    uint32_t timestamp_us;
    uint16_t neuron_id;
    int16_t e_fast_q8_8;        // Fast trace (Q8.8)
    int16_t e_slow_q8_8;        // Slow trace (Q8.8)
    int16_t combined_e_q8_8;    // E(t) = α·e_fast + β·e_slow
};
```

### Weight Update Format (WEIGHT_UPDATE)

```c
struct WeightUpdatePacket {
    uint32_t timestamp_us;
    uint16_t pre_neuron_id;     // Presynaptic neuron
    uint16_t post_neuron_id;    // Postsynaptic neuron
    int16_t delta_w_q8_8;       // Weight change (Q8.8)
    int16_t new_w_q8_8;         // New weight value
};
```

---

## Timing Constraints

### Latency Requirements

| Operation | Target | Maximum |
|-----------|--------|---------|
| Spike to trace update | 10 µs | 100 µs |
| Trace read latency | 100 µs | 1 ms |
| Config application | 1 ms | 10 ms |
| Full reset | 10 ms | 100 ms |

### Throughput

- **Spike Input:** Up to 10,000 spikes/second
- **Trace Output:** 64 traces every 10 ms (6.4 kHz)
- **Weight Updates:** Burst up to 1000 updates/ms

---

## Power Management

### Active Mode
- Clock: 10 MHz
- Power: 2.5 mW
- All features active

### Idle Mode
- Clock: 1 MHz (gated)
- Power: 0.5 mW
- Spike detection active
- Trace accumulation paused

### Sleep Mode
- Clock: Stopped
- Power: <0.1 mW
- Wakeup on SPI activity or external interrupt

---

## Integration Example

### Python Interface

```python
from syndar.hardware.fpga import FPGAInterface

# Initialize interface
fpga = FPGAInterface(
    interface="spi",
    device="/dev/spidev0.0",
    clock_speed_hz=10_000_000
)

# Configure SNN
fpga.configure_snn(
    input_neurons=200,
    hidden_neurons=64,
    tau_fast_us=100_000,
    tau_slow_us=700_000,
    eta=0.01,
    drift_threshold=0.5
)

# Send spike
fpga.send_spike(
    neuron_id=42,
    weight=1.0,
    timestamp_us=int(time.time() * 1_000_000)
)

# Read traces
traces = fpga.read_traces()
for trace in traces:
    print(f"Neuron {trace.neuron_id}: E={trace.combined_e:.4f}")

# Check drift
if fpga.drift_exceeded():
    print("Drift threshold exceeded!")
```

### C++ Driver (STM32/Linux)

```cpp
#include "syndar_fpga.h"

int main() {
    // Initialize SPI
    SyndarFPGA fpga;
    fpga.init("/dev/spidev0.0", 10000000);
    
    // Reset and configure
    fpga.reset();
    fpga.configure({
        .input_neurons = 200,
        .hidden_neurons = 64,
        .tau_fast_us = 100000,
        .tau_slow_us = 700000,
        .eta = 0x0003,  // 0.01 in Q8.8
        .drift_threshold = 0x8000  // 0.5 in Q0.16
    });
    
    // Main loop
    while (true) {
        // Read traces
        auto traces = fpga.read_traces();
        
        // Process drift
        for (const auto& trace : traces) {
            if (trace.combined_e > 0.5) {
                // Report to Syndar fabric
                report_drift(trace);
            }
        }
        
        usleep(10000);  // 10ms
    }
    
    return 0;
}
```

---

## Verification & Testing

### Built-in Self Test (BIST)

The FPGA implements:
1. **Memory Test:** Read/write all BRAM
2. **Logic Test:** Verify LIF neuron computation
3. **Connectivity Test:** Verify all I/O pins
4. **Trace Test:** Verify eligibility trace accumulation

### Test Mode

Enable via CONFIG message with `test_mode = 1`:
- Generates synthetic spikes
- Verifies trace accumulation against golden model
- Reports pass/fail status

---

## References

1. Arthedain Architecture: `../arthedain/docs/architecture.md`
2. Xilinx Artix-7 Datasheet: DS181
3. LIF Neuron Model: Dayan & Abbott, Theoretical Neuroscience
4. Online Learning: Senn et al., "Learning through perturbations"

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 0.1.0 | 2026-04-13 | Initial specification |
