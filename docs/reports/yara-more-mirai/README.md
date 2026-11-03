# YARA pack for `more_mirai_yara` (2026-10-28/29)

77 rules in `pack/more_mirai.yar` (compiles as one unit): 10 family, 31 variant, 36 capability.
All were built and measured through the workbench API on the yara-editor stack and saved in
its rule registry (`GET /api/yara/rules`). **Human review pending** (meta `author` says so).

| File | What |
|:--|:--|
| `pack/more_mirai.yar` | all rules, one file (`tools/pack.py`) |
| `rules/A..H.yar` | per author agent: A boatnet, B p_killermaps/xnxn, C demons/nuclear/frosted..., D flylegit/kushnet/cling..., E tsunami/misc, F non-Mirai, G DDoS+C2+XOR tables, H scanner/killer/persist/exploit |
| `families.json` / `families.md` | 33 groups over the 447 files (regrouped after authoring; `families.v1.json` is the first cut) |
| `capabilities.json` / `.md` | 36 catalogued capabilities with evidence and function clusters |
| `matrix.md` / `matrix.json` | every rule x every group (`tools/matrix.py`) |
| `reports/*.md` | evidence and measured P/R per group and per capability |

## Reading the numbers
- Family/variant rules: recall is on non-packed members; hits outside their group were 0 for
  almost every rule. Exceptions are by design (Boatnet_Netis inside Boatnet; Flylegit also hits
  the four misfiled flylegit builds, now regrouped).
- Capability rules hit several families on purpose (see `matrix.md`). Their "false hits" against
  the catalogue holder list are mostly real implementers the holder list undercounts (Ghidra
  merges adjacent strings; UPX copies). Weak by their own reports: `WatchdogHandle`,
  `C2GoogleDnsResolver`, `C2SelfRepBanner` (single anchor), `DdosGreIcmpFlood` / `DdosDnsFlood`
  (ARM prologue windows only).
- Some "capability" rules are in practice single-family (AntiPtrace -> frosted, SshPythonSpread ->
  tsunami): they describe a behaviour but have no second implementer in this collection.

## Not covered
UPX originals (82 files, no strings; match on the unpacked sibling in `families.json` `upx_pairs`),
24 unclassified files, SOCKS proxy / reverse shell / wipe (no evidence found), per-arch byte
windows for most families (string-first rules were already complete).

## Workbench additions (code)
`POST /api/yara/hitmap` (rule -> function clusters, UI "Clusters" tab), `POST /api/yara/seed/cluster`
(aligned byte windows for a cluster or N-way row), `POST /api/yara/lint`.
