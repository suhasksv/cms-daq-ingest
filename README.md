# CMS-DAQ-Ingest: Zero-Allocation UDP Event Building in Rust

An ultra-high-throughput, lock-free, deterministic hash-routing architecture written in Rust. Designed to resolve the critical "Out-of-Order Event Building" bottleneck in High-Energy Physics (HEP) Data Acquisition (DAQ) pipelines, such as the Compact Muon Solenoid (CMS) experiment at the Large Hadron Collider (CERN).

This architecture demonstrates mechanical sympathy—software designed in harmony with modern CPU cache hierarchies, memory bus topologies, and weak memory consistency models—to achieve massive ingest rates without operating system thread contention or heap allocations.

## The Architectural Challenge

In experimental high-energy physics, collision events generate fragmented data across multiple spatially separated detector subsystems. These fragments are transmitted as asynchronous, out-of-order UDP packets that must be reassembled in real-time before being passed to the High-Level Trigger (HLT) farm.

The Bottleneck

Traditional ingestion architectures distribute packets to worker threads using round-robin scheduling or dynamic thread pools protected by shared state. Under high data velocities, this introduces major issues:

Cache-Line Bouncing: Thread-shared queues and coordinate maps force CPU cores to continuously broadcast cache-invalidation signals across the interconnect bus, stalling execution.

Mutex Contention: Thread synchronization primitives (Mutexes, semaphores) force the operating system kernel to repeatedly park and wake threads, destroying context locality.

Non-Deterministic Latency: Dynamic heap allocations during packet deserialization cause unpredictable garbage collection sweeps or allocator lock-ups under heavy load, leading to kernel socket buffer overflows (ENOBUFS) and permanent packet loss.

## The Solution: Deterministic Hash-Routing & "Fast-Peeking"

This framework implements a lock-free pipeline that guarantees single-core execution locality for related packet fragments, completely eliminating thread cross-talk and heap allocations.

![Fig1](https://github.com/suhasksv/cms-daq-ingest/blob/master/fig1.png)
![Fig1-1](https://github.com/suhasksv/cms-daq-ingest/blob/master/fig1-1.png)

1. "Fast-Peek" Shallow Packet Inspection

Instead of parsing the entire incoming payload inside the critical network ingestion loop, the main thread extracts only the 4-byte Event ID directly from its raw byte offsets in the UDP socket buffer. Struct deserialization is completely deferred to the workers.

2. Modulo-Based Multi-Core Work Distribution

Using the extracted Event ID, a modulo hash distributes packet fragments deterministically:

$$\text{Worker Index} = \text{Event ID} \pmod W$$

Where $W$ is the number of active worker threads. This guarantees that all fragmented packets belonging to a specific physics event land in the exact same worker's queue. Each worker maintains its own thread-local, un-shared event assembly map. Because no state is shared, zero locks, mutexes, or atomic synchronization blocks are used during assembly.

3. Pre-Allocated, Lock-Free Ring Buffers

We replaced the standard library's unbounded channels (std::mpsc) with bounded, lock-free ring buffers (crossbeam-channel) pre-allocated with a capacity of 100,000 packets per worker. This ensures that memory is requested from the OS once during booting and never allocated or deallocated during hot ingestion.

📊 Benchmarks & Hardware Performance

The ingestion pipeline was profiled under unthrottled loopback stress tests to isolate software execution efficiency from physical transceiver limits.

![Fig2](https://github.com/suhasksv/cms-daq-ingest/blob/master/fig3.png)

Intel Core i5 (Dual-Core Baseline): Achieved hardware saturation at ~84,300 events/sec. High scheduling competition on 2 cores led to context-switch storms and thermal throttling, proving the pipeline extracts maximum performance from the hardware.

Apple M1 Pro (10-Core ARM64): Achieved a sustained throughput of ~184,000 events/sec, peaking at ~202,819 events/sec. Since the architecture is completely lock-free, performance scaled linearly with core availability and the 200 GB/s memory bus.

Telemetry Scaling vs. Formula 1 Telemetry

To test scalability, we expanded the payload schema to 26 bytes to include spatial data coordinates:

$$\text{Payload} = \text{Timestamp (8B)} + \text{Event ID (4B)} + \text{Detector ID (2B)} + \text{Energy (4B)} + \text{X Position (4B)} + \text{Y Position (4B)}$$

At a sustained throughput of 202,819 events per second, this equals 1,216,914 parameters processed per second. This performance successfully beats the real-time telemetry footprint of a Formula 1 racing car (~1.1 million parameters/sec) on a single consumer-grade laptop.

📈 Lock-Free Metric Collection & Observability

Observability is handled outside the critical path to prevent print statements from blocking the pipeline.

![Fig3](https://github.com/suhasksv/cms-daq-ingest/blob/master/fig2.png)

Worker threads increment global counter metrics using lock-free Relaxed Atomics (std::sync::atomic::Ordering::Relaxed). On ARM64, these compile down to basic, single-cycle CPU instructions. A separate, asynchronous telemetry dashboard thread wakes up every second, reads the atomic offsets, clears the terminal using ANSI escape codes, and renders live operational status.

## How to Run the Benchmark

Prerequisites

Make sure you have the Rust Toolchain (Cargo) installed.

1. Compile the Workspace

Compile the entire workspace in release mode to enable aggressive compiler optimizations:
```
git clone [https://github.com/suhasksv/cms-daq-ingest.git](https://github.com/suhasksv/cms-daq-ingest.git)
cd cms-daq-ingest
cargo build --release
```

2. Start the Telemetry Ingestion Engine (The Sink)

The ingestion engine expands the operating system's network socket buffer, allocates the lock-free queues, and launches the live telemetry dashboard.
```
# Run the engine
cargo run --release --package daq_cms_exp_2 --bin daq_cms_exp_2
```

3. Launch the Hardware Mock Generator (The Firehose)

In a separate terminal tab, launch the generator to blast unthrottled 26-byte physics event payloads:
```
# Run the generator
cargo run --release --package daq_cms_exp --bin daq_cms_exp
```

Watch the live dashboard update in real-time. Once complete, you will find fully-decoded, high-speed telemetry records saved inside worker_X_sink.csv.

📝 License
This project is dual-licensed under the MIT License and the Apache License (Version 2.0). See the MIT LICENSE and LICENSE-APACHE files for details.
