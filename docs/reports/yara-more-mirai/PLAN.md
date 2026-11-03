# YARA families and capabilities for `more_mirai_yara`

Goal: original, documented YARA rules that identify the families, sub-families,
variants and capabilities in the collection (447 Linux IoT-botnet ELF files), built
through the `/api/yara/*` workbench of the `worktree-yara-editor` stack (port 5602).

## Principles

- Two rule kinds, kept apart in `meta: kind`:
  - `family` / `variant`: classify a file. Evidence = strings, config tables, layout.
  - `capability`: a behaviour (UDP flood, telnet scanner, killer, SSDP, anti-VM...) tied
    to bytes/strings of the functions that implement it. Shared across variants on purpose.
- Precision is judged against *siblings*, not the whole collection. A variant rule that
  also hits its parent family is right; one that hits unrelated families is wrong.
  Every report lists "expected overlap" separately from "unexpected hits".
- Cross-architecture: the same source built for ARM/MIPS/x86/... shares strings and
  XOR-table constants, but not code bytes. Byte seeds are per-architecture, OR-ed.
- Function clusters are per-architecture. A capability that spans families is found by
  its strings/constants and by its function clusters in each architecture.

## API added for this work (this branch)

| Endpoint | Purpose |
|:--|:--|
| `POST /api/yara/hitmap` | rule -> function clusters its hits land in, per group; marks family markers vs shared capabilities |
| `POST /api/yara/seed/cluster` | byte windows aligned across a function cluster / N-way row (`fids`) per architecture, with `??` for differing bytes and coverage |

Client for scripts: `tools/yc.py`.

## Phases

1. **Cartography** -> `families.json`, `families.md` (file -> family/variant groups).
2. **Capability survey** -> `capabilities.json` (behaviours, their strings, clusters).
3. **Authoring** (one sub-agent per group): rules in `rules/*.yar`, evidence in `reports/`.
4. **Cross-evaluation**: every rule against every group (matrix), fix noisy ones.
5. **Pack**: `pack/more_mirai.yar`, README with the matrix, and the registry copies.
6. **UI**: hit map and cluster seeds in `/yara`.
