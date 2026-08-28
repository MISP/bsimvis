use std::env;
use std::time::{Duration, Instant};

#[path = "../src/async_db.rs"]
#[allow(dead_code)]
mod async_db;

use async_db::run_three_stage_pipeline;

fn arg_usize(index: usize, default: usize) -> usize {
    env::args()
        .nth(index)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let chunks = arg_usize(1, 15);
    let read_limit = arg_usize(2, 4);
    let write_limit = arg_usize(3, 2);
    let read_micros = arg_usize(4, 3_000);
    let transform_micros = arg_usize(5, 1_000);
    let write_micros = arg_usize(6, 7_000);
    let read_delay = Duration::from_micros(read_micros as u64);
    let transform_delay = Duration::from_micros(transform_micros as u64);
    let write_delay = Duration::from_micros(write_micros as u64);

    let serial_start = Instant::now();
    for _ in 0..chunks {
        tokio::time::sleep(read_delay).await;
        tokio::time::sleep(transform_delay).await;
        tokio::time::sleep(write_delay).await;
    }
    let serial_seconds = serial_start.elapsed().as_secs_f64();

    let pipeline_start = Instant::now();
    let stats = run_three_stage_pipeline(
        chunks,
        read_limit,
        write_limit,
        read_delay,
        transform_delay,
        write_delay,
    )
    .await;
    let pipeline_seconds = pipeline_start.elapsed().as_secs_f64();

    println!(
        "{{\"chunks\":{},\"read_limit\":{},\"write_limit\":{},\"read_micros\":{},\"transform_micros\":{},\"write_micros\":{},\"serial_seconds\":{:.6},\"pipeline_seconds\":{:.6},\"speedup\":{:.6},\"observed_peak_reads\":{},\"observed_peak_writes\":{},\"completed_chunks\":{}}}",
        chunks,
        read_limit,
        write_limit,
        read_micros,
        transform_micros,
        write_micros,
        serial_seconds,
        pipeline_seconds,
        serial_seconds / pipeline_seconds,
        stats.observed_peak_reads,
        stats.observed_peak_writes,
        stats.completed_chunks
    );
}
