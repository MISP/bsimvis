# Authoring guide for rule agents

Stack: http://localhost:5602/api (yara-editor worktree). Collection `more_mirai_yara`.
Client: `tools/yc.py` (`import sys; sys.path.insert(0, "<dir>/tools"); import yc`).
Groups: `families.json` (`yc.groups()`); UPX originals carry no strings -> write on the unpacked
sibling (`upx_pairs`), the packed original is a separate `packer` concern (skip).

## Hard rules
- Never read `data/kvrocks/`, `hs_err_pid*.log`. Do not edit repo source code (only files under
  `docs/reports/yara-more-mirai/`). Do not call mutating endpoints other than `POST /api/yara/rules`
  (save a rule). Never `/yara/apply`, `/yara/sweep`, tag writes, bulk `/api/function/features`.
- Scratch in your own dir under `/home/thomas/.claude/jobs/70baf563/tmp/<agent>/`. Large nway calls
  (all files) take minutes: use subsets. Print small. Use `python3 -I` for scripts that read data.
- No browser dialogs. If an API call fails 3x, record it and move on.

## Workflow per group
1. `yc.suggest([T], [N])` (T = the group, N = everything else classified; `yc.rest`) -> ranked strings
   and constants; also look at `defining_strings` in families.json and the strings API
   (`/api/strings/search?q=`, `/api/strings/functions?id=`) for config tables, command grammar, banners,
   credential lists, URLs/IPs/domains, model lists, error text, file paths.
2. Draft rules. Test each with `yc.test(rule, [T], [N])` where N is split in labelled sibling sets
   (e.g. `{"label":"expected overlap: parent family", ...}` and `{"label":"unrelated", ...}`) so the
   per-group hit table shows WHERE false hits fall. Iterate until recall on the group is high (>=0.85
   of non-packed, non-stripped members) and hits outside siblings are ~0.
3. Add byte seeds when strings are weak: `yc.hitmap(rule, ...)` shows which function clusters your
   strings sit in; `yc.seed_cluster(cluster_id)` returns per-architecture stable windows (`??` = varies).
   Combine arch windows with `any of ($b_*)`. Check with `/yara/test`.
4. `POST /api/yara/lint` your final text; fix warnings (meta, ELF magic `uint32(0) == 0x464c457f`,
   `filesize <`, >=2 independent anchors, no string <6 chars, hex >=6 fixed bytes).
5. Save each final rule with `yc.save(rule, note)`; write all rules of the agent to
   `rules/<agent>.yar`; write one `reports/<group_id>.md` per group (see below).

## Rule quality bar
- Original: do not copy Elastic/other rules. Describe the group's own markers.
- Distinguish kinds in `meta: kind` = `family` (whole family), `variant` (a fork), `capability`.
  Precision is judged against *siblings*: a variant rule that also hits its parent family is fine,
  hits on unrelated families are bugs. A low raw precision may only mean a better cluster exists.
  Say so in meta/notes instead of weakening the rule.
- Prefer: unique config strings, command grammar, banners, XOR-table constants (encoded bytes are
  matchable directly; note key), exploit/URL templates; per-arch function windows as support only.
- Avoid: libc/uclibc/musl strings, generic paths (/proc/net/tcp), short words.
- Condition shape: ELF magic, filesize bound, `N of ($anchor*)` plus an alternative path via byte
  windows when strings are weak. Naming: `Linux_Botnet_<Family>_<Variant>` for family/variant,
  `Linux_Capability_<Name>` for capability. Unique names (check `GET /api/yara/rules`).

## Required meta (all of it, real values)
```
description  = "one sentence: what it identifies and how"
author       = "BSimVis YARA workbench (Claude), human review pending"
date         = "2026-10-28"
kind         = "family" | "variant" | "capability"
family       = "mirai"            // gafgyt, tsunami, miner, other...
variant      = "boatnet"          // omit for family-wide
capability   = "..."              // for capability rules, or the capabilities a family rule implies
capabilities = "udp_flood,telnet_scanner,killer"   // from the analysis
confidence   = "high|medium|low"
reference_md5 = "md5 of 1-3 sample files (unpacked ones)"
sample_count  = "n files in the group"
tested        = "recall X/Y on group, Z hits in siblings, W unrelated (2026-10-28)"
arch_note     = "arch-independent (strings) | per-arch byte windows for A,B"
note          = "caveats: expected overlap with ..., packed originals not covered"
```

## reports/<group_id>.md
Evidence (strings, offsets/functions, clusters), what the family does (capabilities seen in code/strings,
no speculation without marking it), the rules and their measured P/R by sibling set, known gaps.
Final message to the orchestrator: <=10 lines: rules saved (names), recall/hits per group, open gaps.
