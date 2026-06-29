use std::net::{UdpSocket, SocketAddr};
use std::thread;
use crossbeam_channel::{bounded, Sender};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::fs::File;
use std::io::{BufWriter, Write};
use socket2::{Socket, Domain, Type, Protocol};
use std::sync::atomic::{AtomicUsize, Ordering};

const LISTEN_ADDRESS: &str = "127.0.0.1:8080";
const WORKER_COUNT: usize = 4;
const QUEUE_CAPACITY: usize = 100_000;

// Global Atomic Counters for the Live Dashboard
static TOTAL_EVENTS: AtomicUsize = AtomicUsize::new(0);
static LAST_LATENCY_US: AtomicUsize = AtomicUsize::new(0);

// The expanded 26-byte struct
#[derive(Debug)]
struct ParticleEvent {
    timestamp: u64,
    event_id: u32,
    detector_id: u16,
    energy: f32,
    x_pos: f32,
    y_pos: f32,
}

impl ParticleEvent {
    /// Deserializes the fixed 26-byte array back into our Rust struct
    fn from_bytes(bytes: &[u8; 26]) -> Self {
        let timestamp = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let event_id = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let detector_id = u16::from_be_bytes(bytes[12..14].try_into().unwrap());
        let energy = f32::from_bits(u32::from_be_bytes(bytes[14..18].try_into().unwrap()));
        let x_pos = f32::from_bits(u32::from_be_bytes(bytes[18..22].try_into().unwrap()));
        let y_pos = f32::from_bits(u32::from_be_bytes(bytes[22..26].try_into().unwrap()));

        Self {
            timestamp,
            event_id,
            detector_id,
            energy,
            x_pos,
            y_pos,
        }
    }
}

fn get_microseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_micros() as u64
}

fn main() -> std::io::Result<()> {
    // 1. Bind to the exact port using socket2 for OS buffer expansion
    let addr: SocketAddr = LISTEN_ADDRESS.parse().unwrap();
    let raw_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

    // Expand the OS socket receive buffer to ~32MB
    if let Err(e) = raw_socket.set_recv_buffer_size(32 * 1024 * 1024) {
        eprintln!("Warning: Failed to set full 32MB recv buffer: {}", e);
    }

    raw_socket.bind(&addr.into())?;
    let socket: UdpSocket = raw_socket.into();

    // --- LIVE TELEMETRY DASHBOARD THREAD ---
    thread::spawn(|| {
        let mut last_events = 0;
        loop {
            thread::sleep(Duration::from_secs(1));

            let current_events = TOTAL_EVENTS.load(Ordering::Relaxed);
            let latency = LAST_LATENCY_US.load(Ordering::Relaxed);
            let eps = current_events - last_events;
            last_events = current_events;

            print!("\x1B[2J\x1B[1;1H");
            println!("=====================================================");
            println!(" 🚀 CMS DAQ INGESTION: LIVE TELEMETRY DASHBOARD 🚀 ");
            println!("=====================================================");
            println!("  Status:           🟢 ONLINE & LOCK-FREE");
            println!("  Payload Size:     26 Bytes");
            println!("  Workers:          {}", WORKER_COUNT);
            println!("  Total Events:     {}", current_events);
            println!("  Throughput:       {} events/sec", eps);
            println!("  Latest Latency:   {} µs", latency);
            println!("=====================================================\n");
        }
    });

    // 2. Setup the Worker Pool and Channels (Now expects 26 bytes)
    let mut worker_channels: Vec<Sender<[u8; 26]>> = Vec::new();

    for i in 0..WORKER_COUNT {
        let (sender, receiver) = bounded::<[u8; 26]>(QUEUE_CAPACITY);
        worker_channels.push(sender);

        thread::spawn(move || {
            let mut processed_count = 0;
            let file_name = format!("worker_{}_sink.csv", i);
            let file = File::create(&file_name).expect("Failed to create sink file");

            // Mega-Batched Disk I/O (8MB)
            let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

            // WRITE ALL 6 HEADERS
            writeln!(writer, "timestamp_us,event_id,detector_id,energy_gev,x_pos,y_pos").unwrap();

            while let Ok(buffer) = receiver.recv() {
                let event = ParticleEvent::from_bytes(&buffer);
                processed_count += 1;

                TOTAL_EVENTS.fetch_add(1, Ordering::Relaxed);

                // WRITE ALL 6 PARAMETERS
                writeln!(
                    writer,
                    "{},{},{},{:.6},{:.2},{:.2}",
                    event.timestamp, event.event_id, event.detector_id, event.energy, event.x_pos, event.y_pos
                ).expect("Failed to write to buffer");

                if processed_count % 1000 == 0 {
                    let now = get_microseconds();
                    let latency_us = now.saturating_sub(event.timestamp);

                    LAST_LATENCY_US.store(latency_us as usize, Ordering::Relaxed);
                    writer.flush().unwrap();
                }
            }
        });
    }

    // 3. The I/O Tight Loop (Main Thread)
    let mut buffer = [0u8; 26]; // Now expects 26 bytes

    loop {
        match socket.recv_from(&mut buffer) {
            Ok((size, _src)) => {
                // Ensure we receive exactly 26 bytes
                if size == 26 {
                    // FAST PEEK: Event ID is still at bytes 8-11!
                    let event_id_bytes: [u8; 4] = buffer[8..12].try_into().unwrap();
                    let event_id = u32::from_be_bytes(event_id_bytes);

                    let worker_index = (event_id % WORKER_COUNT as u32) as usize;

                    if let Err(e) = worker_channels[worker_index].send(buffer) {
                        eprintln!("Failed to send data to worker {}: {}", worker_index, e);
                    }
                } else {
                    eprintln!("Received malformed packet of size: {}", size);
                }
            }
            Err(e) => eprintln!("Failed to read from socket: {}", e),
        }
    }
}