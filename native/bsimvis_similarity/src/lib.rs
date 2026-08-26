use std::collections::{HashMap, HashSet};
#[cfg(feature = "gpu")]
use std::sync::Mutex;
use std::sync::OnceLock;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

#[cfg(feature = "async-db")]
#[cfg(feature = "gpu")]
mod gpu;

#[derive(Clone)]
struct SparseVector {
    features: Vec<(u32, f64)>,
    norm: f64,
    total: f64,
}

#[derive(Clone, Copy)]
enum Algorithm {
    Cosine,
    Jaccard,
}

impl Algorithm {
    fn parse(value: &str) -> PyResult<Self> {
        match value {
            "unweighted_cosine" => Ok(Self::Cosine),
            "jaccard" => Ok(Self::Jaccard),
            _ => Err(PyValueError::new_err(format!(
                "Unsupported algorithm: {value}"
            ))),
        }
    }
}

fn score(left: &SparseVector, right: &SparseVector, algorithm: Algorithm) -> f64 {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut dot = 0.0;
    let mut intersection = 0.0;

    while left_index < left.features.len() && right_index < right.features.len() {
        let (left_feature, left_value) = left.features[left_index];
        let (right_feature, right_value) = right.features[right_index];
        if left_feature == right_feature {
            match algorithm {
                Algorithm::Cosine => dot += left_value * right_value,
                Algorithm::Jaccard => intersection += left_value.min(right_value),
            }
            left_index += 1;
            right_index += 1;
        } else if left_feature < right_feature {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }

    match algorithm {
        Algorithm::Cosine if left.norm > 0.0 && right.norm > 0.0 => dot / (left.norm * right.norm),
        Algorithm::Cosine => 0.0,
        Algorithm::Jaccard => {
            let union = left.total + right.total - intersection;
            if union > 0.0 {
                intersection / union
            } else {
                0.0
            }
        }
    }
}

fn select_candidates(
    vectors: &[SparseVector],
    target: usize,
    candidate_indices: &[usize],
    algorithm: Algorithm,
    top_k: usize,
    min_score: f64,
) -> Vec<(usize, f64)> {
    let mut candidates = Vec::new();
    for &candidate in candidate_indices {
        if candidate == target {
            continue;
        }
        let candidate_score = score(&vectors[target], &vectors[candidate], algorithm);
        if candidate_score > 0.0 && candidate_score >= min_score {
            candidates.push((candidate, candidate_score));
        }
    }
    candidates.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates.truncate(top_k);
    candidates
}

fn quantile(sorted_scores: &[f64], probability: f64) -> f64 {
    if sorted_scores.is_empty() {
        return 0.0;
    }
    let position = probability * sorted_scores.len().saturating_sub(1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted_scores[lower]
    } else {
        let fraction = position - lower as f64;
        sorted_scores[lower] * (1.0 - fraction) + sorted_scores[upper] * fraction
    }
}

type TargetAnalytics = (
    Vec<(usize, f64)>,
    Vec<(usize, f64)>,
    usize,
    f64,
    f64,
    f64,
    f64,
    Vec<f64>,
);

type SummaryEdges = (Vec<(usize, f64)>, Vec<(usize, f64)>);

fn indexed_summary_edges(summaries: &[SummaryEdges]) -> PyResult<HashMap<(usize, usize), i64>> {
    let function_count = summaries.len();
    let mut edges = HashMap::new();
    for (target, (nearest, distant)) in summaries.iter().enumerate() {
        for &(candidate, score) in nearest.iter().chain(distant.iter()) {
            if candidate >= function_count {
                return Err(PyValueError::new_err(
                    "summary candidate index is out of range",
                ));
            }
            if candidate == target {
                continue;
            }
            if !score.is_finite() {
                return Err(PyValueError::new_err("summary score must be finite"));
            }
            let pair = (target.min(candidate), target.max(candidate));
            let quantized = (score * 10_000.0).round_ties_even() as i64;
            if let Some(existing) = edges.insert(pair, quantized) {
                if existing != quantized {
                    return Err(PyValueError::new_err(format!(
                        "conflicting compact edge score: {pair:?}"
                    )));
                }
            }
        }
    }
    Ok(edges)
}

#[pyfunction]
fn compact_edge_delta_from_summaries_native(
    existing_summaries: Vec<SummaryEdges>,
    updated_summaries: Vec<SummaryEdges>,
) -> PyResult<(
    Vec<(usize, usize, f64)>,
    Vec<(usize, usize, f64)>,
    usize,
    usize,
)> {
    let existing = indexed_summary_edges(&existing_summaries)?;
    let updated = indexed_summary_edges(&updated_summaries)?;
    let mut additions = updated
        .iter()
        .filter_map(|(&(left, right), &score)| {
            (existing.get(&(left, right)) != Some(&score)).then_some((
                left,
                right,
                score as f64 / 10_000.0,
            ))
        })
        .collect::<Vec<_>>();
    let mut removals = existing
        .iter()
        .filter_map(|(&(left, right), &score)| {
            (updated.get(&(left, right)) != Some(&score)).then_some((
                left,
                right,
                score as f64 / 10_000.0,
            ))
        })
        .collect::<Vec<_>>();
    additions.sort_unstable_by_key(|&(left, right, _)| (left, right));
    removals.sort_unstable_by_key(|&(left, right, _)| (left, right));
    Ok((additions, removals, existing.len(), updated.len()))
}

fn summarize_candidates(
    vectors: &[SparseVector],
    target: usize,
    candidate_indices: &[usize],
    algorithm: Algorithm,
    nearest_k: usize,
    distant_k: usize,
) -> TargetAnalytics {
    let mut candidates = candidate_indices
        .iter()
        .filter(|&&candidate| candidate != target)
        .map(|&candidate| {
            (
                candidate,
                score(&vectors[target], &vectors[candidate], algorithm),
            )
        })
        .collect::<Vec<_>>();
    let count = candidates.len();
    let mean = if count > 0 {
        candidates.iter().map(|(_, value)| value).sum::<f64>() / count as f64
    } else {
        0.0
    };
    let variance = if count > 0 {
        candidates
            .iter()
            .map(|(_, value)| {
                let delta = value - mean;
                delta * delta
            })
            .sum::<f64>()
            / count as f64
    } else {
        0.0
    };
    let mut sorted_scores = candidates
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    sorted_scores.sort_unstable_by(f64::total_cmp);
    let minimum = sorted_scores.first().copied().unwrap_or(0.0);
    let maximum = sorted_scores.last().copied().unwrap_or(0.0);
    let quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
        .iter()
        .map(|&probability| quantile(&sorted_scores, probability))
        .collect();

    let mut nearest = candidates.clone();
    nearest.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    nearest.truncate(nearest_k);
    candidates.sort_unstable_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates.truncate(distant_k);
    (
        nearest, candidates, count, mean, variance, minimum, maximum, quantiles,
    )
}

fn frequency_candidates(
    vectors: &[SparseVector],
    postings: &[Vec<(usize, f64)>],
    target: usize,
    eligible: &[bool],
    max_candidates: usize,
    max_posting_fraction: f64,
) -> Vec<usize> {
    let mut support = HashMap::<usize, f64>::new();
    let population = vectors.len() as f64;
    for &(feature_id, _) in &vectors[target].features {
        let posting = &postings[feature_id as usize];
        if posting.len() as f64 / population > max_posting_fraction {
            continue;
        }
        let weight = ((population + 1.0) / (posting.len() as f64 + 1.0)).ln() + 1.0;
        for &(candidate, _) in posting {
            if candidate != target && eligible[candidate] {
                *support.entry(candidate).or_insert(0.0) += weight;
            }
        }
    }
    let mut ranked = support.into_iter().collect::<Vec<_>>();
    if ranked.is_empty() {
        ranked.extend(
            eligible
                .iter()
                .enumerate()
                .filter(|&(candidate, allowed)| candidate != target && *allowed)
                .map(|(candidate, _)| (candidate, 0.0)),
        );
    }
    ranked.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked.truncate(max_candidates);
    ranked.into_iter().map(|(candidate, _)| candidate).collect()
}

fn select_inverted_candidates(
    vectors: &[SparseVector],
    postings: &[Vec<(usize, f64)>],
    target: usize,
    eligible: &[bool],
    algorithm: Algorithm,
    max_posting_fraction: f64,
    top_k: usize,
    min_score: f64,
) -> (Vec<(usize, f64)>, usize) {
    let mut accumulators = HashMap::<usize, f64>::new();
    let population = vectors.len() as f64;
    for &(feature_id, target_value) in &vectors[target].features {
        let posting = &postings[feature_id as usize];
        if posting.len() as f64 / population > max_posting_fraction {
            continue;
        }
        for &(candidate, candidate_value) in posting {
            if candidate == target || !eligible[candidate] {
                continue;
            }
            let contribution = match algorithm {
                Algorithm::Cosine => target_value * candidate_value,
                Algorithm::Jaccard => target_value.min(candidate_value),
            };
            *accumulators.entry(candidate).or_insert(0.0) += contribution;
        }
    }
    let candidate_count = accumulators.len();
    let mut selected = accumulators
        .into_iter()
        .filter_map(|(candidate, accumulated)| {
            let candidate_score = match algorithm {
                Algorithm::Cosine
                    if vectors[target].norm > 0.0 && vectors[candidate].norm > 0.0 =>
                {
                    accumulated / (vectors[target].norm * vectors[candidate].norm)
                }
                Algorithm::Cosine => 0.0,
                Algorithm::Jaccard => {
                    let union = vectors[target].total + vectors[candidate].total - accumulated;
                    if union > 0.0 {
                        accumulated / union
                    } else {
                        0.0
                    }
                }
            };
            (candidate_score > 0.0 && candidate_score >= min_score)
                .then_some((candidate, candidate_score))
        })
        .collect::<Vec<_>>();
    selected.sort_unstable_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    selected.truncate(top_k);
    (selected, candidate_count)
}

fn build_pool(workers: usize) -> PyResult<rayon::ThreadPool> {
    if workers == 0 {
        return Err(PyValueError::new_err("workers must be positive"));
    }
    ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyclass]
struct ExactScorer {
    vectors: Vec<SparseVector>,
    postings: Vec<Vec<(usize, f64)>>,
    #[cfg(feature = "gpu")]
    gpu: Mutex<Option<gpu::Scorer>>,
}

#[pymethods]
impl ExactScorer {
    #[new]
    fn new(vectors: Vec<Vec<(String, f64)>>) -> PyResult<Self> {
        let mut feature_ids = HashMap::new();
        let mut next_feature_id = 0_u32;
        let mut packed_vectors = Vec::with_capacity(vectors.len());

        for vector in vectors {
            let mut packed = Vec::with_capacity(vector.len());
            let mut norm_squared = 0.0;
            let mut total = 0.0;
            for (feature, value) in vector {
                if !value.is_finite() || value < 0.0 {
                    return Err(PyValueError::new_err(
                        "feature weights must be finite and non-negative",
                    ));
                }
                let feature_id = *feature_ids.entry(feature).or_insert_with(|| {
                    let assigned = next_feature_id;
                    next_feature_id += 1;
                    assigned
                });
                packed.push((feature_id, value));
                norm_squared += value * value;
                total += value;
            }
            packed.sort_unstable_by_key(|(feature_id, _)| *feature_id);
            packed_vectors.push(SparseVector {
                features: packed,
                norm: norm_squared.sqrt(),
                total,
            });
        }

        let mut postings = vec![Vec::new(); next_feature_id as usize];
        for (vector_index, vector) in packed_vectors.iter().enumerate() {
            for &(feature_id, value) in &vector.features {
                postings[feature_id as usize].push((vector_index, value));
            }
        }

        Ok(Self {
            vectors: packed_vectors,
            postings,
            #[cfg(feature = "gpu")]
            gpu: Mutex::new(None),
        })
    }

    fn vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn score_pairs(
        &self,
        py: Python<'_>,
        pairs: Vec<(usize, usize)>,
        algorithm: &str,
        workers: usize,
    ) -> PyResult<Vec<f64>> {
        let algorithm = Algorithm::parse(algorithm)?;
        for &(left, right) in &pairs {
            if left >= self.vectors.len() || right >= self.vectors.len() {
                return Err(PyValueError::new_err("pair index is out of range"));
            }
        }
        let pool = build_pool(workers)?;
        let vectors = &self.vectors;
        Ok(py.allow_threads(|| {
            pool.install(|| {
                pairs
                    .par_iter()
                    .map(|&(left, right)| score(&vectors[left], &vectors[right], algorithm))
                    .collect()
            })
        }))
    }

    #[cfg(feature = "gpu")]
    fn score_pairs_wgpu(
        &self,
        py: Python<'_>,
        pairs: Vec<(usize, usize)>,
        algorithm: &str,
    ) -> PyResult<Vec<f64>> {
        let algorithm = Algorithm::parse(algorithm)?;
        for &(left, right) in &pairs {
            if left >= self.vectors.len() || right >= self.vectors.len() {
                return Err(PyValueError::new_err("pair index is out of range"));
            }
        }
        let mut gpu = self
            .gpu
            .lock()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        if gpu.is_none() {
            *gpu = Some(gpu::Scorer::new(&self.vectors).map_err(PyValueError::new_err)?);
        }
        py.allow_threads(|| gpu.as_ref().unwrap().score_pairs(&pairs, algorithm))
            .map_err(PyValueError::new_err)
    }

    fn score_all_pairs_checksum(
        &self,
        py: Python<'_>,
        algorithm: &str,
        workers: usize,
    ) -> PyResult<(usize, f64)> {
        let algorithm = Algorithm::parse(algorithm)?;
        let pool = build_pool(workers)?;
        let vectors = &self.vectors;
        let row_sums = py.allow_threads(|| {
            pool.install(|| {
                (0..vectors.len())
                    .into_par_iter()
                    .map(|left| {
                        let mut checksum = 0.0;
                        for right in (left + 1)..vectors.len() {
                            checksum += score(&vectors[left], &vectors[right], algorithm);
                        }
                        checksum
                    })
                    .collect::<Vec<_>>()
            })
        });
        let pair_count = self.vectors.len() * self.vectors.len().saturating_sub(1) / 2;
        Ok((pair_count, row_sums.into_iter().sum()))
    }

    #[cfg(feature = "gpu")]
    fn score_all_pairs_wgpu_checksum(
        &self,
        py: Python<'_>,
        algorithm: &str,
    ) -> PyResult<(usize, f64)> {
        let algorithm = Algorithm::parse(algorithm)?;
        let mut gpu = self
            .gpu
            .lock()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        if gpu.is_none() {
            *gpu = Some(gpu::Scorer::new(&self.vectors).map_err(PyValueError::new_err)?);
        }
        py.allow_threads(|| {
            gpu::score_all_pairs_checksum(gpu.as_ref().unwrap(), self.vectors.len(), algorithm)
        })
        .map_err(PyValueError::new_err)
    }

    #[cfg(feature = "gpu")]
    fn select_target_block_wgpu(
        &self,
        py: Python<'_>,
        target_indices: Vec<usize>,
        candidate_indices: Vec<usize>,
        algorithm: &str,
        workers: usize,
        top_k: usize,
        min_score: f64,
        score_margin: f64,
    ) -> PyResult<Vec<Vec<(usize, f64)>>> {
        let algorithm = Algorithm::parse(algorithm)?;
        let pool = build_pool(workers)?;
        if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
            return Err(PyValueError::new_err(
                "min_score must be finite and between 0 and 1",
            ));
        }
        if !score_margin.is_finite() || score_margin < 0.0 {
            return Err(PyValueError::new_err(
                "score_margin must be finite and non-negative",
            ));
        }
        if target_indices
            .iter()
            .chain(candidate_indices.iter())
            .any(|&index| index >= self.vectors.len())
        {
            return Err(PyValueError::new_err("vector index is out of range"));
        }
        let pairs = target_indices
            .iter()
            .flat_map(|&target| {
                candidate_indices
                    .iter()
                    .filter(move |&&candidate| candidate != target)
                    .map(move |&candidate| (target, candidate))
            })
            .collect::<Vec<_>>();
        let mut gpu = self
            .gpu
            .lock()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        if gpu.is_none() {
            *gpu = Some(gpu::Scorer::new(&self.vectors).map_err(PyValueError::new_err)?);
        }
        let scores = py
            .allow_threads(|| gpu.as_ref().unwrap().score_pairs(&pairs, algorithm))
            .map_err(PyValueError::new_err)?;

        let mut offsets = Vec::with_capacity(target_indices.len() + 1);
        offsets.push(0);
        for &target in &target_indices {
            let count = candidate_indices
                .iter()
                .filter(|&&candidate| candidate != target)
                .count();
            offsets.push(offsets.last().unwrap() + count);
        }
        let vectors = &self.vectors;
        Ok(py.allow_threads(|| {
            pool.install(|| {
                target_indices
                    .par_iter()
                    .enumerate()
                    .map(|(target_offset, &target)| {
                        let target_scores =
                            &scores[offsets[target_offset]..offsets[target_offset + 1]];
                        let mut score_index = 0;
                        let mut ranked = Vec::new();
                        for &candidate in &candidate_indices {
                            if candidate == target {
                                continue;
                            }
                            let candidate_score = target_scores[score_index];
                            score_index += 1;
                            if candidate_score >= (min_score - score_margin).max(0.0) {
                                ranked.push((candidate, candidate_score));
                            }
                        }
                        ranked.sort_unstable_by(|left, right| {
                            right
                                .1
                                .total_cmp(&left.1)
                                .then_with(|| left.0.cmp(&right.0))
                        });
                        if top_k == 0 || ranked.is_empty() {
                            return Vec::new();
                        }
                        let boundary = ranked[top_k.min(ranked.len()) - 1].1;
                        let shortlist = ranked
                            .into_iter()
                            .take_while(|(_, candidate_score)| {
                                *candidate_score >= boundary - score_margin
                            })
                            .map(|(candidate, _)| candidate)
                            .collect::<Vec<_>>();
                        select_candidates(vectors, target, &shortlist, algorithm, top_k, min_score)
                    })
                    .collect()
            })
        }))
    }

    #[cfg(feature = "gpu")]
    fn wgpu_resident_bytes(&self) -> PyResult<usize> {
        let gpu = self
            .gpu
            .lock()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(gpu.as_ref().map(gpu::Scorer::resident_bytes).unwrap_or(0))
    }

    #[cfg(feature = "gpu")]
    fn clear_wgpu_resident_buffers(&self) -> PyResult<()> {
        let mut gpu = self
            .gpu
            .lock()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        *gpu = None;
        Ok(())
    }

    fn select_target_block(
        &self,
        py: Python<'_>,
        target_indices: Vec<usize>,
        candidate_indices: Vec<usize>,
        algorithm: &str,
        workers: usize,
        top_k: usize,
        min_score: f64,
    ) -> PyResult<Vec<Vec<(usize, f64)>>> {
        let algorithm = Algorithm::parse(algorithm)?;
        if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
            return Err(PyValueError::new_err(
                "min_score must be finite and between 0 and 1",
            ));
        }
        if target_indices
            .iter()
            .chain(candidate_indices.iter())
            .any(|&index| index >= self.vectors.len())
        {
            return Err(PyValueError::new_err("vector index is out of range"));
        }
        let pool = build_pool(workers)?;
        let vectors = &self.vectors;
        Ok(py.allow_threads(|| {
            pool.install(|| {
                target_indices
                    .par_iter()
                    .map(|&target| {
                        select_candidates(
                            vectors,
                            target,
                            &candidate_indices,
                            algorithm,
                            top_k,
                            min_score,
                        )
                    })
                    .collect()
            })
        }))
    }

    fn summarize_target_block(
        &self,
        py: Python<'_>,
        target_indices: Vec<usize>,
        candidate_indices: Vec<usize>,
        algorithm: &str,
        workers: usize,
        nearest_k: usize,
        distant_k: usize,
    ) -> PyResult<Vec<TargetAnalytics>> {
        let algorithm = Algorithm::parse(algorithm)?;
        if target_indices
            .iter()
            .chain(candidate_indices.iter())
            .any(|&index| index >= self.vectors.len())
        {
            return Err(PyValueError::new_err("vector index is out of range"));
        }
        let pool = build_pool(workers)?;
        let vectors = &self.vectors;
        Ok(py.allow_threads(|| {
            pool.install(|| {
                target_indices
                    .par_iter()
                    .map(|&target| {
                        summarize_candidates(
                            vectors,
                            target,
                            &candidate_indices,
                            algorithm,
                            nearest_k,
                            distant_k,
                        )
                    })
                    .collect()
            })
        }))
    }

    fn select_frequency_target_block(
        &self,
        py: Python<'_>,
        target_indices: Vec<usize>,
        candidate_indices: Vec<usize>,
        algorithm: &str,
        workers: usize,
        candidate_limit: usize,
        max_posting_fraction: f64,
        top_k: usize,
        min_score: f64,
    ) -> PyResult<Vec<(Vec<(usize, f64)>, usize)>> {
        let algorithm = Algorithm::parse(algorithm)?;
        if !max_posting_fraction.is_finite() || !(0.0..=1.0).contains(&max_posting_fraction) {
            return Err(PyValueError::new_err(
                "max_posting_fraction must be between 0 and 1",
            ));
        }
        if target_indices
            .iter()
            .chain(candidate_indices.iter())
            .any(|&index| index >= self.vectors.len())
        {
            return Err(PyValueError::new_err("vector index is out of range"));
        }
        let mut eligible = vec![false; self.vectors.len()];
        for &candidate in &candidate_indices {
            eligible[candidate] = true;
        }
        let pool = build_pool(workers)?;
        let vectors = &self.vectors;
        let postings = &self.postings;
        Ok(py.allow_threads(|| {
            pool.install(|| {
                target_indices
                    .par_iter()
                    .map(|&target| {
                        let candidates = frequency_candidates(
                            vectors,
                            postings,
                            target,
                            &eligible,
                            candidate_limit,
                            max_posting_fraction,
                        );
                        let candidate_count = candidates.len();
                        (
                            select_candidates(
                                vectors,
                                target,
                                &candidates,
                                algorithm,
                                top_k,
                                min_score,
                            ),
                            candidate_count,
                        )
                    })
                    .collect()
            })
        }))
    }

    fn select_inverted_target_block(
        &self,
        py: Python<'_>,
        target_indices: Vec<usize>,
        candidate_indices: Vec<usize>,
        algorithm: &str,
        workers: usize,
        max_posting_fraction: f64,
        top_k: usize,
        min_score: f64,
    ) -> PyResult<Vec<(Vec<(usize, f64)>, usize)>> {
        let algorithm = Algorithm::parse(algorithm)?;
        if !max_posting_fraction.is_finite() || !(0.0..=1.0).contains(&max_posting_fraction) {
            return Err(PyValueError::new_err(
                "max_posting_fraction must be between 0 and 1",
            ));
        }
        if target_indices
            .iter()
            .chain(candidate_indices.iter())
            .any(|&index| index >= self.vectors.len())
        {
            return Err(PyValueError::new_err("vector index is out of range"));
        }
        let mut eligible = vec![false; self.vectors.len()];
        for &candidate in &candidate_indices {
            eligible[candidate] = true;
        }
        let pool = build_pool(workers)?;
        let vectors = &self.vectors;
        let postings = &self.postings;
        Ok(py.allow_threads(|| {
            pool.install(|| {
                target_indices
                    .par_iter()
                    .map(|&target| {
                        select_inverted_candidates(
                            vectors,
                            postings,
                            target,
                            &eligible,
                            algorithm,
                            max_posting_fraction,
                            top_k,
                            min_score,
                        )
                    })
                    .collect()
            })
        }))
    }
}

#[pymodule]
fn bsimvis_similarity_native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExactScorer>()?;
    module.add_function(wrap_pyfunction!(
        compact_edge_delta_from_summaries_native,
        module
    )?)?;
    #[cfg(feature = "gpu")]
    module.add_function(wrap_pyfunction!(wgpu_adapter_info, module)?)?;
    Ok(())
}

#[cfg(feature = "gpu")]
#[pyfunction]
fn wgpu_adapter_info() -> PyResult<HashMap<String, String>> {
    let instance = wgpu::Instance::default();
    let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
        power_preference: wgpu::PowerPreference::HighPerformance,
        ..Default::default()
    }))
    .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let info = adapter.get_info();
    Ok(HashMap::from([
        ("name".to_string(), info.name),
        ("backend".to_string(), format!("{:?}", info.backend)),
        ("device_type".to_string(), format!("{:?}", info.device_type)),
        ("driver".to_string(), info.driver),
        ("driver_info".to_string(), info.driver_info),
    ]))
}

#[cfg(test)]
mod tests {
    use super::{Algorithm, SparseVector, score, select_candidates, summarize_candidates};

    fn vector(features: &[(u32, f64)]) -> SparseVector {
        SparseVector {
            features: features.to_vec(),
            norm: features
                .iter()
                .map(|(_, value)| value * value)
                .sum::<f64>()
                .sqrt(),
            total: features.iter().map(|(_, value)| value).sum(),
        }
    }

    #[test]
    fn exact_scores_match_reference_values() {
        let left = vector(&[(1, 1.0), (2, 2.0)]);
        let right = vector(&[(1, 3.0), (3, 4.0)]);
        let cosine = score(&left, &right, Algorithm::Cosine);
        let jaccard = score(&left, &right, Algorithm::Jaccard);
        assert!((cosine - 3.0 / (5.0 * 5.0_f64.sqrt())).abs() < 1e-12);
        assert!((jaccard - 1.0 / 9.0).abs() < 1e-12);
    }

    #[test]
    fn disjoint_vectors_score_zero() {
        let left = vector(&[(1, 1.0)]);
        let right = vector(&[(2, 1.0)]);
        assert_eq!(score(&left, &right, Algorithm::Cosine), 0.0);
        assert_eq!(score(&left, &right, Algorithm::Jaccard), 0.0);
    }

    #[test]
    fn top_k_uses_global_scores_and_deterministic_ties() {
        let vectors = vec![
            vector(&[(1, 1.0)]),
            vector(&[(1, 1.0)]),
            vector(&[(1, 1.0)]),
            vector(&[(2, 1.0)]),
        ];
        assert_eq!(
            select_candidates(&vectors, 0, &[0, 1, 2, 3], Algorithm::Cosine, 1, 0.0),
            vec![(1, 1.0)]
        );
    }

    #[test]
    fn candidate_filter_is_respected() {
        let vectors = vec![
            vector(&[(1, 1.0)]),
            vector(&[(1, 1.0)]),
            vector(&[(1, 0.5), (2, 0.5)]),
        ];
        assert_eq!(
            select_candidates(&vectors, 0, &[0, 2], Algorithm::Cosine, 10, 0.0),
            vec![(2, 1.0 / 2.0_f64.sqrt())]
        );
    }

    #[test]
    fn analytics_retain_nearest_distant_and_distribution() {
        let vectors = vec![
            vector(&[(1, 1.0)]),
            vector(&[(1, 1.0)]),
            vector(&[(1, 0.5), (2, 0.5)]),
            vector(&[(3, 1.0)]),
        ];
        let (nearest, distant, count, mean, variance, minimum, maximum, quantiles) =
            summarize_candidates(&vectors, 0, &[0, 1, 2, 3], Algorithm::Cosine, 1, 1);
        assert_eq!(nearest, vec![(1, 1.0)]);
        assert_eq!(distant, vec![(3, 0.0)]);
        assert_eq!(count, 3);
        assert!(mean > 0.5 && mean < 0.6);
        assert!(variance > 0.0);
        assert_eq!(minimum, 0.0);
        assert_eq!(maximum, 1.0);
        assert_eq!(quantiles.len(), 5);
    }
}
