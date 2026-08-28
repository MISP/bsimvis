use std::sync::mpsc;

use bytemuck::{Pod, Zeroable};
use wgpu::util::DeviceExt;

use super::{Algorithm, SparseVector};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Pair {
    left: u32,
    right: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Parameters {
    pair_count: u32,
    algorithm: u32,
    _padding: [u32; 2],
}

fn buffer(device: &wgpu::Device, label: &str, contents: &[u8]) -> wgpu::Buffer {
    device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some(label),
        contents,
        usage: wgpu::BufferUsages::STORAGE,
    })
}

pub struct Scorer {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    offsets: wgpu::Buffer,
    features: wgpu::Buffer,
    values: wgpu::Buffer,
    norms: wgpu::Buffer,
    totals: wgpu::Buffer,
    resident_bytes: usize,
}

impl Scorer {
    pub fn new(vectors: &[SparseVector]) -> Result<Self, String> {
        let instance = wgpu::Instance::default();
        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            ..Default::default()
        }))
        .map_err(|error| error.to_string())?;
        let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
            label: Some("bsimvis-similarity"),
            ..Default::default()
        }))
        .map_err(|error| error.to_string())?;

        let mut offsets = Vec::with_capacity(vectors.len() + 1);
        let mut feature_ids = Vec::new();
        let mut values = Vec::new();
        let mut norms = Vec::with_capacity(vectors.len());
        let mut totals = Vec::with_capacity(vectors.len());
        offsets.push(0_u32);
        for vector in vectors {
            for &(feature_id, value) in &vector.features {
                feature_ids.push(feature_id);
                values.push(value as f32);
            }
            offsets.push(feature_ids.len() as u32);
            norms.push(vector.norm as f32);
            totals.push(vector.total as f32);
        }
        let resident_bytes = offsets.len() * size_of::<u32>()
            + feature_ids.len() * size_of::<u32>()
            + values.len() * size_of::<f32>()
            + norms.len() * size_of::<f32>()
            + totals.len() * size_of::<f32>();
        let offsets = buffer(&device, "offsets", bytemuck::cast_slice(&offsets));
        let features = buffer(&device, "features", bytemuck::cast_slice(&feature_ids));
        let values = buffer(&device, "values", bytemuck::cast_slice(&values));
        let norms = buffer(&device, "norms", bytemuck::cast_slice(&norms));
        let totals = buffer(&device, "totals", bytemuck::cast_slice(&totals));
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("sparse-pair-scorer"),
            source: wgpu::ShaderSource::Wgsl(include_str!("similarity.wgsl").into()),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("sparse-pair-scorer"),
            layout: None,
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        Ok(Self {
            device,
            queue,
            pipeline,
            offsets,
            features,
            values,
            norms,
            totals,
            resident_bytes,
        })
    }

    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub fn score_pairs(
        &self,
        pairs: &[(usize, usize)],
        algorithm: Algorithm,
    ) -> Result<Vec<f64>, String> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let packed_pairs = pairs
            .iter()
            .map(|&(left, right)| Pair {
                left: left as u32,
                right: right as u32,
            })
            .collect::<Vec<_>>();
        let parameters = Parameters {
            pair_count: pairs.len() as u32,
            algorithm: match algorithm {
                Algorithm::Cosine => 0,
                Algorithm::Jaccard => 1,
            },
            _padding: [0; 2],
        };
        let pairs_buffer = buffer(&self.device, "pairs", bytemuck::cast_slice(&packed_pairs));
        let parameters_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("parameters"),
                contents: bytemuck::bytes_of(&parameters),
                usage: wgpu::BufferUsages::UNIFORM,
            });
        let output_bytes = (pairs.len() * size_of::<f32>()) as u64;
        let output_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("scores"),
            size: output_bytes,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("score-readback"),
            size: output_bytes,
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });
        let layout = self.pipeline.get_bind_group_layout(0);
        let buffers = [
            &self.offsets,
            &self.features,
            &self.values,
            &pairs_buffer,
            &self.norms,
            &self.totals,
            &output_buffer,
            &parameters_buffer,
        ];
        let entries = buffers
            .iter()
            .enumerate()
            .map(|(binding, buffer)| wgpu::BindGroupEntry {
                binding: binding as u32,
                resource: buffer.as_entire_binding(),
            })
            .collect::<Vec<_>>();
        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("sparse-pair-bindings"),
            layout: &layout,
            entries: &entries,
        });
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("sparse-pair-commands"),
            });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor::default());
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            let workgroups = (pairs.len() as u32).div_ceil(64);
            let workgroups_x = workgroups.min(65_535);
            pass.dispatch_workgroups(workgroups_x, workgroups.div_ceil(workgroups_x), 1);
        }
        encoder.copy_buffer_to_buffer(&output_buffer, 0, &readback_buffer, 0, output_bytes);
        self.queue.submit([encoder.finish()]);
        let (sender, receiver) = mpsc::channel();
        readback_buffer
            .slice(..)
            .map_async(wgpu::MapMode::Read, move |result| {
                let _ = sender.send(result);
            });
        self.device
            .poll(wgpu::PollType::wait_indefinitely())
            .map_err(|error| error.to_string())?;
        receiver
            .recv()
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let mapped = readback_buffer
            .slice(..)
            .get_mapped_range()
            .map_err(|error| error.to_string())?;
        let scores = bytemuck::cast_slice::<u8, f32>(&mapped)
            .iter()
            .map(|&score| score as f64)
            .collect();
        drop(mapped);
        readback_buffer.unmap();
        Ok(scores)
    }
}

pub fn score_all_pairs_checksum(
    scorer: &Scorer,
    vector_count: usize,
    algorithm: Algorithm,
) -> Result<(usize, f64), String> {
    let pair_count = vector_count * vector_count.saturating_sub(1) / 2;
    let mut pairs = Vec::with_capacity(pair_count);
    for left in 0..vector_count {
        for right in (left + 1)..vector_count {
            pairs.push((left, right));
        }
    }
    let checksum = scorer.score_pairs(&pairs, algorithm)?.into_iter().sum();
    Ok((pair_count, checksum))
}
