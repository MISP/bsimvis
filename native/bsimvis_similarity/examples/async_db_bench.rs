use std::env;
use std::time::{Duration, Instant};

#[path = "../src/async_db.rs"]
#[allow(dead_code)]
mod async_db;

use async_db::run_bounded_units_with_delay;

fn arg_usize(index: usize, default: usize) -> usize {
    env::args()
        .nth(index)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let units = arg_usize(1, 266_750);
    let max_in_flight = arg_usize(2, 64);
    let delay_micros = arg_usize(3, 100);
    let delay = Duration::from_micros(delay_micros as u64);

    let serial_start = Instant::now();
    for _ in 0..units {
        tokio::time::sleep(delay).await;
    }
    let serial_seconds = serial_start.elapsed().as_secs_f64();

    let bounded_start = Instant::now();
    let stats = run_bounded_units_with_delay(units, max_in_flight, delay).await;
    let bounded_seconds = bounded_start.elapsed().as_secs_f64();

    println!(
        "{{\"units\":{},\"max_in_flight\":{},\"delay_micros\":{},\"serial_seconds\":{:.6},\"bounded_seconds\":{:.6},\"speedup\":{:.6},\"observed_peak_in_flight\":{},\"completed_units\":{}}}",
        units,
        max_in_flight,
        delay_micros,
        serial_seconds,
        bounded_seconds,
        serial_seconds / bounded_seconds,
        stats.observed_peak_in_flight,
        stats.completed_units
    );
}
