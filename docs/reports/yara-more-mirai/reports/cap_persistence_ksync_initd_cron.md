# cap_persistence_ksync_initd_cron

Rule `Linux_Capability_PersistKsync` (kind capability, confidence high). MITRE ATT&CK T1037,T1053.003,T1543.

## What it detects
Writes /etc/init.d/S99ksync, /etc/cron.d/ksync, /data/local/.ksync.sh and an @reboot cron line.

## Evidence
- `string` "/etc/init.d/S99ksync" (6 files)
- `string` "/etc/cron.d/ksync" (6 files)
- `string` "/data/local/.ksync.sh" (6 files)
- `string` "boot hook .ksync.sh" (6 files)
- `string` "update-rc.d S99ksync defaults 2>/dev/null || chkconfig --add S99ksync 2>/dev/null || insse" (6 files)

Catalogue holders: 6. Spread in the catalogue: {"MD5MD5": 6}. Spread of the rule: MD5MD5-named ksync family (6) and the kushnet rival_killer sample (shares the boot-hook set).

## Rule
Boot persistence via /etc/init.d/S99ksync, /etc/cron.d/ksync, /data/local/.ksync.sh and update-rc.d: any 2 of 4. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 6, fn 0 (recall 1.0).
- hits outside the holder list: 1 (rival_killer 1). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 0.8571.

## Cluster landing (POST /api/yara/hitmap)
- no hit lands in a named or clustered function (strings sit outside clustered code)

Implemented in: string-only: no function cluster resolves (the paths live in unnamed init_persistence functions).

## Gaps
The one hit in rival_killer is expected: kushnet reuses the ksync boot hooks.
