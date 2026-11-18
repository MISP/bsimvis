# data/capabilities/catalog.json

A trimmed id -> name catalog of MBC and MITRE ATT&CK, read by
`bsimvis/app/services/capability_service.py`. It keeps only:

- MBC malware behaviours (B*, E*, F*) and micro-behaviours (C*): name, objective,
  and whether it is a micro-behaviour. Methods (B0029.002, ...) are left out.
- ATT&CK Enterprise techniques and sub-techniques that are neither revoked nor
  deprecated: name and tactics.

## Sources

| Catalog | Repository | Commit | Date | Licence |
|---|---|---|---|---|
| MBC | https://github.com/MBCProject/mbc-stix2.1 (`mbc/mbc.json`) | `add8ab4d523afe1de82e261d34ca378ddeffb1bf` | 2025-01-27 | Apache License 2.0 |
| ATT&CK Enterprise | https://github.com/mitre/cti (`enterprise-attack/enterprise-attack.json`) | `e79c7c53d13c08cc21ba3ad77d4f8f49bf64ff07` | 2026-11-05 | ATT&CK terms of use (below) |

Fetched 2026-11-18.

## Rebuild

```bash
curl -sLo mbc.json https://raw.githubusercontent.com/MBCProject/mbc-stix2.1/<commit>/mbc/mbc.json
curl -sLo enterprise-attack.json https://raw.githubusercontent.com/mitre/cti/<commit>/enterprise-attack/enterprise-attack.json
uv run python -I scripts/trim_capability_catalog.py mbc.json enterprise-attack.json data/capabilities/catalog.json
uv run python -I bsimvis/app/services/capability_service.py   # every curated id must still exist
```

A newer ATT&CK release can retire an id the curated list in `capability_service.py`
uses (T1562 was split into T1685 and others in the 2026 release). The self-check
names it.

## Notices

MBC (Malware Behavior Catalog), MBCProject: licensed under the Apache License,
Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0). The repository's
LICENSE.txt is the plain Apache 2.0 text and names no separate copyright holder.

ATT&CK®: "© 2026 The MITRE Corporation. This work is reproduced and distributed with
the permission of The MITRE Corporation."

The MITRE Corporation (MITRE) hereby grants you a non-exclusive, royalty-free license
to use ATT&CK® for research, development, and commercial purposes. Any copy you make
for such purposes is authorized provided that you reproduce MITRE's copyright
designation and this license in any such copy.

MITRE does not claim ATT&CK enumerates all possibilities for the types of actions and
behaviors documented as part of its adversary model and framework of techniques. Using
the information contained within ATT&CK to address or cover full categories of
techniques will not guarantee full defensive coverage as there may be undisclosed
techniques or variations on existing techniques not documented by ATT&CK.

ALL DOCUMENTS AND THE INFORMATION CONTAINED THEREIN ARE PROVIDED ON AN "AS IS" BASIS
AND THE CONTRIBUTOR, THE ORGANIZATION HE/SHE REPRESENTS OR IS SPONSORED BY (IF ANY),
THE MITRE CORPORATION, ITS BOARD OF TRUSTEES, OFFICERS, AGENTS, AND EMPLOYEES,
DISCLAIM ALL WARRANTIES, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO ANY WARRANTY
THAT THE USE OF THE INFORMATION THEREIN WILL NOT INFRINGE ANY RIGHTS OR ANY IMPLIED
WARRANTIES OF MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
