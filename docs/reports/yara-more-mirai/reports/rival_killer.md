# rival_killer

Verdict: coherent; the family names itself kushnet. Rule `Linux_Botnet_Mirai_Kushnet` (variant, high).

## Evidence
All 7 files (ARM x3, MIPS BE, PowerPC, m68k, sh4) share, with no hit outside the group: `kushnet_boot_v1`, `kushnet_`, `/kushnet.`, `libkys.so`, `killed rival pid=%d exe=%s`, `killed pid=%d %s exe=%s`, `removed persist path=%s`, `purged file=%s`, `/tmp/.c2_redirect`, `c2_redirect`, `com.google.firestick.youtube`, `/bin/mushi`, `/usr/bin/mushi`, `kworker/1:1`, `systemd-`.
Six of seven add the boot-hook set (`.ksync.sh`, `/etc/cron.d/ksync`, `/etc/init.d/S99ksync`, `install-recovery.sh`, `/data/local/userinit.sh`, `KUSHNET_GUARD`, `FBI OPEN UP`, `apk_tmsg.queue`). The m68k and sh4 members, which the earlier note said join on weak similarity, in fact carry the kushnet string set (at least the firestick and rival strings), so the placement is sound.

## What it does
Kills rival processes by exe path, removes other persistence, masquerades as a kworker/systemd thread, installs itself through cron.d, init.d, rc.local, install-recovery and an Android-style userinit script, and keeps a runtime C2 redirect file. "mushi" appears as a path, linking it to the mushi group.

## Rule
ELF, under 3 MB, 3 of 9 kushnet / libkys / rival-kill / c2_redirect / firestick / install-recovery strings. 7/7 recall, 0 hits in 429 other files. An existing rule `kushnet_google_firestick` overlaps by design.

## Gaps
No byte seeds (not needed). The group label should be renamed kushnet.
