# How to benchmark a worktree against the baseline

For agents. You changed something for performance and need a defensible number
comparing your branch to what it forked from. This is the procedure, and the
traps that make a benchmark lie.

The short version: **isolate the phase you changed, use a corpus big enough to
show the effect, give each side the defaults it actually ships with, and never
benchmark against production.**

---

## 0. Rules that decide whether the number means anything

1. **Never run a writing benchmark against a live instance.** `benchmark_sim.py`
   and `bsimvis-bench` create and delete collections. Point them at an isolated
   worktree stack only. Read-only probes against production are fine (see §4).
2. **Same data both sides.** Different ingests produce different corpora and the
   comparison is void.
3. **Each side gets its own shipped defaults.** If your change alters a config
   default (cache budget, batch size), do *not* give the baseline your new value
   — it may not be affordable under the old representation. Benchmark what each
   side would actually run with.
4. **Change exactly one thing.** If the harness itself needs a fix to run at all,
   apply that fix to *both* sides.
5. **Report the phase, not the wall clock.** `grand_total` is usually dominated
   by ingestion. See §3.
6. **State the corpus.** A speedup that only appears above some corpus size is a
   property of the pair (change, corpus), not of the change.

## 1. Bring up an isolated stack

From inside the worktree:

```bash
./scripts/wt-setup.sh          # allocates its own ports, writes .env
grep PORT .env                 # APP_PORT / REDIS_PORT / KVROCKS_PORT
```

Ports are offset per worktree so several can run at once. Tear down with
`./scripts/wt-teardown.sh`, or `tmux kill-session -t bsimvis-<worktree>`.

## 2. Run the pipeline benchmark

```bash
APP_PORT=<port> DISPLAY= uv run python -m bsimvis.cli.bsimvis_bench \
    --dir data/bench --collection bench_new --clear \
    --save /tmp/new.json
```

`DISPLAY=` keeps matplotlib headless. `--save` writes structured per-phase
metrics; `--compare <baseline.json>` prints a diff against a previous run.

Useful flags: `--limit N` (fewer binaries), `--skip-write` (discovery only, no
similarity persistence), `--algo`, `--top-k`, `--min-score`, `--min-features`.

### Getting the baseline side

Two options. Prefer (b) — it is less setup and removes stack-to-stack variance.

**(a) Second worktree.** `git worktree add ../baseline <base-commit>`, run
`wt-setup.sh` there too (it gets different ports), benchmark both. Note that a
worktree-isolated agent session cannot run git against another worktree; do it
from the main checkout or from that worktree's own session.

**(b) Swap the changed file in place.** When the change is confined to a few
files, keep one stack and swap them:

```bash
cp bsimvis/app/services/foo.py /tmp/foo_NEW.py
git show <base-commit>:bsimvis/app/services/foo.py > bsimvis/app/services/foo.py
./scripts/wt-setup.sh                      # RESTART: workers cache imported code
# ... run benchmark, --save /tmp/base.json ...
cp /tmp/foo_NEW.py bsimvis/app/services/foo.py
./scripts/wt-setup.sh
```

**You must restart the stack after swapping** — the app and workers hold the
imported module, so an un-restarted stack silently benchmarks the old code twice.

## 3. Read the result correctly

`--save` output has `grand_total`, `stats`, and `sub_tasks`. **Read `sub_tasks`.**
A real example from the discovery-vectorisation change on `data/bench`:

| sub_task | baseline | new |
|---|---|---|
| `enrich_features` | 81.70 s | 75.74 s |
| `idx_features` | 1.62 s | 1.15 s |
| `idx_functions` | 0.99 s | 1.18 s |
| **`build_sim`** | **1.478 s** | **1.250 s** |
| `index_sim` | 0.018 s | 0.020 s |
| `grand_total` | 86.26 s | 79.80 s |

The change touched discovery only, i.e. `build_sim`: 1.18x. The 7.5% difference
in `grand_total` is mostly `enrich_features`, which the change never touches —
quoting it would have been dishonest in both directions. Always check `stats`
matches across runs (`func_similarities` identical) or you compared two different
workloads.

## 4. Library-level A/B without a stack

For a change inside one service, timing the function directly is sharper than a
pipeline run, and can be done read-only against a real collection:

```python
import importlib.util
spec = importlib.util.spec_from_file_location("base_mod", "/tmp/foo_BASE.py")
base = importlib.util.module_from_spec(spec); spec.loader.exec_module(base)
# now time base.Service()._method() vs the current one, same client, same inputs
```

Build both objects with `Cls.__new__(Cls)` and set only the attributes the method
needs, to skip heavy `__init__` side effects. Only issue read commands
(`ZCARD`/`ZRANGE`/`ZSCORE`/`GET`) if the target is a live instance.

**Sample targets the way production does.** This one bit us: sampling targets at
random across a 1.5M-function collection showed **1.08x**, because random targets
share almost no features and a posting-list cache has nothing to reuse. A real
`build_batch` processes all functions **of one binary**, where feature overlap is
large. Draw targets from `<coll>:idx:file:functions:<md5>` to reproduce that.

## 5. Corpus size decides what you can even measure

`data/bench` is 5 binaries / 1,179 functions. Measured profile:

| | `data/bench` | production `full_arbor` |
|---|---|---|
| functions | 1,179 | 1,518,960 |
| largest posting list | 632 (53.6% of corpus) | 948,734 (62.8%) |
| posting pairs scanned per function | 1,820 mean | 1,907,043 mean |

The *shape* is representative — the largest feature covers about half the corpus
in both. The *scale* is 1,000x apart. Any change whose benefit grows with posting
list length is therefore invisible on `data/bench`; it can only prove
**no regression and identical output**, which is still worth running.

If your change is scale-dependent, say so with numbers rather than extrapolating
a small-corpus result. Either ingest a larger corpus (see `$CORPUS_ROOT`,
180 binaries) or do the read-only A/B of §4 against a real collection.

### Synthetic scaling sweep

The cheapest way to find *where* a change starts paying: drive the changed
function against an in-memory store (`FakeRedis` in
`scripts/test_discover_equivalence.py`) over corpora of increasing size. No
stack, no network, seconds to run. The discovery change measured:

| corpus | pairs/target | baseline | new | speedup |
|---|---|---|---|---|
| 1,000 | 1,894 | 0.06 s | 0.05 s | 1.11x |
| 5,000 | 9,342 | 0.32 s | 0.08 s | 3.96x |
| 20,000 | 37,615 | 1.85 s | 0.24 s | 7.70x |
| 60,000 | 113,259 | 6.93 s | 0.84 s | 8.23x |
| 150,000 | 282,339 | 20.81 s | 2.55 s | 8.17x |

The 1.11x at corpus 1,000 independently reproduced the 1.18x measured on
`data/bench` — when two methods agree at the overlapping point, both are more
trustworthy.

**Use a realistic number of targets.** The same sweep with 25 targets per corpus
instead of 400 gave 1.1–1.7x, because a per-target setup cost had nothing to
amortise against. Match how the production caller batches work, or you will
measure your harness instead of your change.

## 6. Known traps

- **`data/bench` JSONs carry a `batch_uuid`.** `upload_file_data` defaults to
  `enqueue=False` for batch-tagged uploads, so jobs park forever waiting on a
  `batch_finalize` the benchmark never sends. `bsimvis_bench` now sets
  `enqueue: True` explicitly. Symptom: pipelines `pending`, queue empty, workers
  idle, `[!] Timeout waiting for job`.
- **`RESULT: PASS` from `wt-test.sh` is not sufficient** — check `Failed : 0`
  separately, and diff any failures against the known-failing set.
- **Cold vs warm caches.** First run pays fetch cost the second does not. Run
  each side cold, or each side warm, and say which.
- **Don't reuse a collection name across runs** without `--clear`; leftover
  similarities skew `stats` and make the second run look faster.
- **kvrocks `dbsize` reads 0** even with data present (lazy scan). Use `SCARD` /
  `ZCARD` on a known key to confirm ingestion.

## 7. Reporting

State: base commit, corpus and its size, which phase, each side's relevant
defaults, cold/warm, and the run-to-run spread. If the benchmark cannot resolve
the effect, say that instead of quoting the noise. A change can be worth keeping
on measured no-regression plus a mechanism argument — but then the mechanism
argument is what you are claiming, and it must be labelled as such.
