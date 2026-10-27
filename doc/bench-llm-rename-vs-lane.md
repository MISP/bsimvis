# AI rename vs N-way lane naming: speed and precision

## Summary

**The model call was never slow. AI rename was slow because it and the lane namer
asked Ollama for different context sizes, and every change of `num_ctx` reloads
the model.**

- Lane naming pinned `num_ctx` to 2560. AI rename (and every other LLM call in
  `llm_service.py`) passed no size, which loads qwen3.8's whole 262144 context.
- With both running, every rename paid a ~29 s reload. Under the app's own
  4-thread lane pool the rename never ran: 12 of 12 timed out at 300 s.
- On a quiet, warm server the existing rename call takes ~1.2 s median.
- Lane-style distilled evidence does not help function naming. Precision is flat
  across prompt variants that send code (25–27/66), and drops without code (20/66).
- Fix: one `num_ctx` (config `llm.num_ctx`, default 65536) and `keep_alive=1h` on
  every chat call. Rename under lane load goes from 300 s timeouts to 4.3 s median.

## Setup

- Model: `qwen3.8:27b` Q4_K_M, Ollama 0.33.1, one remote GPU.
- Truth set: 33 N-way rows from the same tables the lane-name eval uses, where the
  row name came from a cluster (a real symbol of an unstripped sibling) and every
  member in the row is stripped (`FUN_*`). Ghidra `caseD_*` labels dropped.
- 9 of the 33 rows carry a Ghidra FunctionID comment in their code
  (`/* Library Function - Single Match memmove */`). That is fair evidence: an
  analyst sees it too. I scored those rows in, and noted them.
- Grading: 2 right / 1 close / 0 wrong. Token overlap first, then by hand where the
  tokens mislead (`xorshift` for Mirai's `rand_next` is right, `create_socket` for
  `attack_udp_plain` is wrong).
- Timings come from Ollama's own response fields (`load_duration`,
  `prompt_eval_count`, `eval_count`), so load, prefill and generation are split.

Some truth names are suspect. `write` and `do_dlvsym` get the same answer
(`atexit`, `free`) from every variant, which points at a mixed cluster, not a
bad namer. I left them in; they cost every variant the same.

## Why lane naming looked fast

| | lane naming | AI rename (before) |
|:--|:--|:--|
| prompt | distilled evidence, ≤2600 chars | up to 3 × 6000 chars of decompiled code |
| `num_ctx` | 2560 | unset: 262144 |
| `num_predict` | 20, stop at newline | -1 |
| `keep_alive` | 1h | default 5 min |
| cache | 30 days in Redis, by content key | none |
| concurrency | 4 server threads | 2 browser workers |

Only one row of that table matters for speed. Measured VRAM by context size:

| `num_ctx` | VRAM |
|:--|--:|
| 2560 | 17.3 GB |
| 32768 | 17.5 GB |
| 65536 | 17.7 GB |
| 262144 (unset) | 51.2 GB |

Ollama keys a loaded model on its options. A request with a different `num_ctx`
waits for the loaded instance to drain, unloads it and loads a new one:
13–16 s on an idle server. While the lane namer keeps sending 2560 requests, the
drain never comes.

The other differences turned out not to matter. Rename replies were 2–6 tokens
even with `num_predict: -1`, and prefill of an 8k-token prompt is ~4 s.

## Finding 1: a rename under lane load never runs

Ran AI rename through the real `/api/notes/rename/suggest` route while
`llm_service.name_lane` ran in 4 threads, the way `_LANE_POOL` runs it. Same 12
rows, cold server each time.

| code | rename median | rename max | timeouts | lanes named meanwhile |
|:--|--:|--:|--:|--:|
| dev HEAD | 300 s | 300 s | 12 / 12 | 8111 |
| one `num_ctx` | 4.3 s | 9.5 s | 0 / 12 | 46 |

The first run of the baseline bench hit the same thing by accident: another
lane-eval session was sending 2560 requests, and from the 8th row on every rename
paid a 28–31 s `load_duration`. Median 31.4 s per row, 830 s for 33 rows.

**Any LLM feature with its own `num_ctx` evicts every other one.** Before the fix
there were four sizes in the app: unset (most calls), 2560 (lane), 4096 (the lane
namer's think retry, so one retry reloaded mid-run), 8192 (block explain).

## Finding 2: precision vs speed per prompt variant

Quiet server, sequential, same 33 rows. `first` is the first call after a cold
start or a context switch.

| variant | what the model sees | score | wrong | median | p90 | first | prompt tokens (median) |
|:--|:--|--:|--:|--:|--:|--:|--:|
| `base` | current prompt, 3 functions × 6000 chars | 26/66 | 18 | 1.16 s | 4.46 s | 16.2 s | 609 |
| `base_capped` | same, `num_predict` 24, stop at newline | 25/66 | 18 | 1.23 s | 3.72 s | 13.2 s | 609 |
| `one_fn` | 1 function × 2400 chars | 25/66 | 17 | 1.64 s | 2.09 s | 1.6 s | 224 |
| `lane` | lane evidence only | 20/66 | 20 | 1.75 s | 1.96 s | 2.0 s | 371 |
| `hybrid` | lane evidence + 1 function × 3000 chars | 25/66 | 19 | 2.00 s | 2.68 s | 2.0 s | 591 |

±3 points is noise on a set this size. All variants that send code score the
same. `lane` loses 6: it drops the code and keeps only callee names, strings and
findings, which is enough to say what a group does but not what one function is
called.

Two smaller reads:

- `one_fn` matches `base` with a third of the prompt. It halves p90 because the
  8k-token rows are gone. Not worth changing on its own; the reload dwarfs it.
- The lane evidence builder drops every line starting with `/*`. That throws away
  the FunctionID comment, the best name evidence there is. Worth checking on the
  lane eval.

## Finding 3: the lane namer gains nothing from ctx 2560

Lane-name workload (107 lanes, first-try prompt), cold server each time:

| `num_ctx` | threads | wall | median per lane |
|:--|--:|--:|--:|
| 2560 | 1 | 140 s | 1.23 s |
| 32768 | 1 | 142 s | 1.24 s |
| 2560 | 4 | 125 s | 4.32 s |
| 32768 | 4 | 124 s | 4.30 s |

Same speed. 32 of 107 names changed between the two sizes at temperature 0. On
the 63 hand-labelled lanes, 17 changed: 32k better on 4, worse on 2, the rest
equally right or wrong. Noise.

Four threads buy 11% on lanes on this server. On the rename `hybrid` variant
they halved wall time (67 s -> 34 s), so the gain depends on prompt size and on
the server's parallel slots.

## The fix

`llm_service.Client` subclasses `ollama.Client`. Its `chat` sets `num_ctx` from
`llm.num_ctx` (default 65536) and `keep_alive="1h"` on every call, whatever the
caller passed. `llm_chat_service` and `analysis_orchestrator` import it instead
of `ollama.Client`. The per-call sizes in `name_lane` and `explain_block_change`
are gone.

65536 rather than 32768: chat sessions with tool results used to get 262k for
free, and 65536 costs 0.2 GB more than 32768.

`_selfcheck` asserts that a caller's own `num_ctx` never reaches Ollama.

## Not done

- Caching rename suggestions by row content, the way lanes are cached. Rename is
  an explicit click, so a repeat ask is rare.
- Raising the browser's 2 rename workers to 4. Gain depends on the server.
- Keeping FunctionID comments in the lane evidence. Needs a lane-eval rerun.

## Rerunning

The harness is local and gitignored, under `data/rename_eval/` in the worktree
(it reads row fids from my corpus): `dump_rows.py` builds the truth rows,
`bench.py VARIANT OUT` runs one variant, `grade.py` scores, `contend.py` runs
rename under lane load (point `PYTHONPATH` at another checkout for the old code),
`lane_ctx.py CTX PAR OUT` runs the lane workload at one context size.

To see the reload on any Ollama, load one size and ask for another:

```bash
curl -s $OLLAMA/api/chat -d '{"model":"qwen3.8:27b","messages":[{"role":"user","content":"ok"}],"stream":false,"options":{"num_ctx":2560}}' | jq .load_duration
curl -s $OLLAMA/api/chat -d '{"model":"qwen3.8:27b","messages":[{"role":"user","content":"ok"}],"stream":false}' | jq .load_duration
curl -s $OLLAMA/api/ps | jq '.models[] | {context_length, size_vram}'
```
