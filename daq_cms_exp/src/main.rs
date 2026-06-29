use std::net::UdpSocket;
use std::time::{SystemTime, UNIX_EPOCH};
// use std::{thread, time::Duration};

const TARGET_ADDR: &str = "127.0.0.1:8080";
// const BROADCAST_INTERVAL_MS: u64 = 1;

struct ParticleEvent {
    timestamp: u64,   // 8 bytes
    event_id: u32,    // 4 bytes
    detector_id: u16, // 2 bytes
    energy: f32,      // 4 bytes
    x_pos: f32,       // 4 bytes
    y_pos: f32,       // 4 bytes
}

impl ParticleEvent {
    fn to_bytes(&self) -> [u8; 26] {
        // Corrected to 26 bytes
        let mut buffer = [0u8; 26];

        buffer[0..8].copy_from_slice(&self.timestamp.to_be_bytes());
        buffer[8..12].copy_from_slice(&self.event_id.to_be_bytes());
        buffer[12..14].copy_from_slice(&self.detector_id.to_be_bytes());
        buffer[14..18].copy_from_slice(&self.energy.to_bits().to_be_bytes());
        buffer[18..22].copy_from_slice(&self.x_pos.to_bits().to_be_bytes());
        buffer[22..26].copy_from_slice(&self.y_pos.to_bits().to_be_bytes());

        buffer
    }
}

fn get_microseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_micros() as u64
}

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    println!("DAQ Mock Generator booted successfully.");
    println!("Streaming telemetry firehose to {}...", TARGET_ADDR);

    let mut event_counter: u32 = 0;

    let mut seed_energy = 45.32f32;

    loop {
        event_counter += 1;

        seed_energy = (seed_energy + 0.15) % 150.0;
        let simulated_detector = (event_counter % 4) as u16;

        let event = ParticleEvent {
            timestamp: get_microseconds(),
            event_id: event_counter,
            detector_id: simulated_detector,
            energy: seed_energy,
            x_pos: (event_counter % 100) as f32 * 1.5,
            y_pos: (event_counter % 100) as f32 * 2.5,
        };

        let payload = event.to_bytes();

        if let Err(e) = socket.send_to(&payload, TARGET_ADDR) {
            eprintln!("Failed to broadcast packet [Event ID: {}]: {}", event_counter, e);
        }

        if event_counter % 1000 == 0 {
            println!(
                "Broadcasting -> Sent: {} packets | Current Payload Size: {} bytes",
                event_counter,
                payload.len()
            );
        }

        // thread::sleep(Duration::from_millis(BROADCAST_INTERVAL_MS));
    }
}
