# Findings: N-way group evidence and region coherence

Answers to `handoff_nway_evidence_regions.md`, measured 2026-11-18.

## Harness

- Model qwen3.8:27b on Ollama 11435, `num_ctx` 65536 for every run. The harness read the main DB
  in-process; no test stack was started.
- **The truth set has shrunk.** Only 39 of the 63 truth lanes still match by id. The cluster
  `t_d47e837fdfb7` grew to 437 files and lost all 24 of its labelled lanes, and 13 of the 39 lanes
  have a different row count now. The scores below are out of 78 (39 lanes × 2), graded in
  `grades.py`.
- **The evidence text was not deterministic.** The top-k of "Library calls", "Named callees" and
  "Called from" followed Redis set order, so 36 of 71 lanes got different text on identical data.
  Ties are now sorted by name (`_top`).
- **LLM noise.** On identical text, a rerun of the baseline flips 10 of 115 names; the salient
  variant flips 1 of 115.
- Harness files and dumps (gitignored): `.claude/worktrees/nway-evidence-regions/data/lane_eval/`.

| Round | Score /78 | Wrong | Names changed vs det |
|---|---|---|---|
| base0 (code before this branch) | 51 | 4 | n/a |
| det (deterministic ties), two runs | 49 / 49 | 5 / 4 | 10 (rerun) |
| ATTR (per-string attribution) | 49 | 5 | 18 |
| SAL v1 (salient snippet) | 45 | 7 | 62 |
| **SAL2 (final salient snippet), two runs** | **57 / 57** | **2 / 2** | 59 |
| RANK (no generic findings in read order) | not named | | |

## Q1: one function's strings name the whole group

- 97% of distinct strings (2217 of 2282) are held by exactly one of the functions read. In 23 of
  the 72 lanes with strings, only one read function has any; in 62, every string has a single
  holder.
- A word-match proxy finds that 13 of 72 lanes (18%) take a name word that only a one-function
  string contains. About half of those names are right (telnet credentials, STUN).
- Live, on 13 named groups with 20 sampled members each: 0 to 7 members hold any string, and each
  evidence string is held by at most 2 of the 20.
- **Per-string attribution is no win** (49 vs 49). Almost every string has one holder, so it only
  relabels the line as "seen once". That code was dropped.
- Leaving `yara:unknown` and generic botnet findings out of the read order changes the functions
  read in 27 of 115 lanes and the text in 24. Those lanes were not named, so this is unmeasured.
  The experiment was removed from the code.

## Q2: the first 20 lines of a function as the snippet

**The salient snippet wins: +8 over det at the same 900-character budget.** Its wrong set {7, 23}
is a subset of det's {1, 2, 5, 7, 23}.

`_salient` keeps:

- the signature;
- calls, each callee at most twice;
- string literals;
- XOR lines;
- loop heads.

It drops `__` compiler helpers and Ghidra macros (`CARRY4`, `CONCAT44`, ...). It comments
`socket(2,T,P)` with the socket type and protocol, also through stripped wrappers such as
`FUN_x(2,3,0x11)`. A function with fewer than 6 such lines (pure compute) keeps its first 20 lines.
SAL v1 lost because it dropped the bodies of XOR decoders; the fallback and the XOR lines fixed
that.

Lane 4 is now graded 1, not 0: its function 1 is the raw-socket SYN scanner, which the old label
missed.

Checked by hand:

- `788a4f..:00013db4`: the old snippet was 18 `rand()` lines. The salient one shows
  `socket(2,2,0) /* UDP datagram socket */`, bind and connect, the `send` loop and the SNQUERY /
  SSDP payloads. Its stripped sibling `FUN_0000cc64` decodes through the wrapper.
- The F5 exploit `cbb5b5..:004059a0` is short, so both snippets are the same.

## Q3: libc in regions

No code. What was found:

- **The handoff's picture is reversed on live XorDDoS** (`b0eeb81a94a5`). 78 anchors of its 4
  named regions were labelled from decompiled code: 85 to 95% are glibc (r1 17/20, r2 ~17/20,
  r3 19/20, r4 ~15/18). These are libc regions that pulled in 1 to 3 malware functions.
  "XOR and RC4 encryption utilities" comes from capa `encoding:xor` / `encryption:rc4` tags that
  sit on glibc `malloc` and `memcpy`.
- **"Any copy with a library tag makes the row library."**
  - On local `more_mirai_yara` it changes 1, 34 and 37 rows (about 2%), and 28 of the 28 rows
    checked are libc or runtime (`abort`, `clone`, `perror`, `fseeko64`, `mmap`, ...). A few
    match only through an ambiguous FID.
  - On the live XorDDoS anchors it does nothing: they carry no `fid:` or `boilerplate:` tags.
- **A function cluster named after a libc symbol** (`getrlimit`, `__stdio_wcommit`, ...) covers
  about 8% of anchors, with high precision.
- **Best signal found:** the i386 syscall pattern `(*(code *)in_GS_OFFSET[4])()`, with errno at
  `+0x1c`, or `swi(0x80)`. It is in about half the libc functions and in none of the malware
  ones. It needs decompiled code, while `row_regions` sees only function metadata, so it would
  have to become a tag at ingest.
- The Go `morestack` prologue was not measured, and region coherence was not re-checked.

## Q4: capability probes (live, old vs new evidence)

The probes read the group's first 6 anchors, not `_annotate`'s pick.

| Probe | Old | New |
|---|---|---|
| UDP flood (`7566.../lane:0000111100000000`) | C0001, C0021, no ATT&CK | Adds T1498.001, quoted from the `send` loop |
| libc (`b0ee.../region:r2`) | No ids (correct). Name "memory copy and runtime initialization" | No ids. Name "C library memory and lock routines" |
| XorDDoS installer (`b0ee.../lane:111110000`) | C0051, T1057 | Same. Both miss cron and rc.d: the installer function is not in the first 6 anchors. The stored live record has T1037.004 and T1053.003. |
| Killer (`2185.../lane:0000001110`) | Its ids | Same ids plus C0019. The kill quote is now `FID_conflict:setpgid(iVar1,9)` instead of the "Killed process" string. |

**CAPS_V is not bumped.** The new evidence changes capability answers (probes 1 and 4), so a bump
is warranted. It is left for the capability redesign (Plan G slice C3, `CAPS_V` 5), which changes
the prompt anyway.

## What landed (`nway_group_service.py`)

- `_salient` replaces the first-20-lines snippet. `LANE_PROMPT_V` goes from 10 to 11, so cached
  names are redone.
- `_top` sorts ties in the call lists by name, so the evidence text is deterministic.
- `_lane_evidence(l)` returns `(text, ev)`, with `ev = {tags, strings, calls, items}`. There is
  one item per function read:
  `{"fid": "md5:addr", "strings": [...], "calls": [...], "findings": [...], "snippet": "..."}`.
  - `strings` holds every string of the function, from the string table and code literals,
    4 characters or more, before cross-function dedupe.
  - `calls` holds the named external callees, then the named internal ones.
  - A string's holders are the items that list it.
  - The text is rendered from the items and is unchanged apart from tie order.
  - `_lane_caps` receives the items. Capability suggestions in Plan G cite them as pointers.
- Checks: `_salient` asserts in `demo()`, and two `nway evidence:` checks in `test_nway_diff`.

## Not done

- Strip `FID_conflict:` from snippets.
- A library row rule ("any copy tagged library") behind `test_nway_roles`.
- A syscall-pattern tag at ingest for static libc.
- Measure the RANK read order.
- Re-check region coherence after any library rule.
- Rebuild the truth set for the 24 lanes lost to the 437-file cluster.
