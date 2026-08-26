struct Parameters {
    pair_count: u32,
    algorithm: u32,
    padding_a: u32,
    padding_b: u32,
}

@group(0) @binding(0) var<storage, read> offsets: array<u32>;
@group(0) @binding(1) var<storage, read> feature_ids: array<u32>;
@group(0) @binding(2) var<storage, read> values: array<f32>;
@group(0) @binding(3) var<storage, read> pairs: array<vec2<u32>>;
@group(0) @binding(4) var<storage, read> norms: array<f32>;
@group(0) @binding(5) var<storage, read> totals: array<f32>;
@group(0) @binding(6) var<storage, read_write> scores: array<f32>;
@group(0) @binding(7) var<uniform> parameters: Parameters;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) invocation: vec3<u32>) {
    // The x dispatch dimension is capped at 65,535 workgroups on Metal.
    let pair_index = invocation.y * (65535u * 64u) + invocation.x;
    if pair_index >= parameters.pair_count {
        return;
    }
    let pair = pairs[pair_index];
    var left = offsets[pair.x];
    let left_end = offsets[pair.x + 1u];
    var right = offsets[pair.y];
    let right_end = offsets[pair.y + 1u];
    var accumulated = 0.0;
    while left < left_end && right < right_end {
        let left_feature = feature_ids[left];
        let right_feature = feature_ids[right];
        if left_feature == right_feature {
            if parameters.algorithm == 0u {
                accumulated += values[left] * values[right];
            } else {
                accumulated += min(values[left], values[right]);
            }
            left += 1u;
            right += 1u;
        } else if left_feature < right_feature {
            left += 1u;
        } else {
            right += 1u;
        }
    }
    if parameters.algorithm == 0u {
        let denominator = norms[pair.x] * norms[pair.y];
        scores[pair_index] = select(0.0, accumulated / denominator, denominator > 0.0);
    } else {
        let union_value = totals[pair.x] + totals[pair.y] - accumulated;
        scores[pair_index] = select(0.0, accumulated / union_value, union_value > 0.0);
    }
}
