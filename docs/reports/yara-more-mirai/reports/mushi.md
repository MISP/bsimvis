# mushi

Rule: `Linux_Botnet_Mirai_Mushi` (kind variant, confidence medium; 2 samples, MIPS-LE).

## Evidence
- Persistence: `/etc/init.d/mushi_%ld`, `/etc/systemd/system/mushi%ld.service` (`Description=The service of legends`), a generated init script with LSB header (`Provides: my_service`) and `systemctl daemon-reload && systemctl enable %s`; `nohup /bin/bash /tmp/self_restart.sh` watchdog.
- Killer: `Killed process %d (command line: %s) because it matched kill string "%s" in cmdline_kill_strings.` and the `path_kill_strings` twin; deletes statically linked executables, `.sh` files and just-created files.
- Binary protocol parser (`Packet Type: %u`, `Number of Key-Value Pairs`), HTTP downloader (`GET /bins/%s`), the word `hexakisoctahedron`, test credentials `1234abc`, `228818`.
- Embedded exploit strings: a Digest `dslf-config` DeviceUpgrade POST and a `/shell?cd+/tmp;wget` chain pointing at 178.215.238.13.

## DEMONS relation
Only `wrgnd08_dlob_dir815` and `wrgn49_dlob_dir600b` are shared. The other DEMONS model names, the credentials and the `<<<` line are absent. Decision: separate fork. The Mirai ancestry is weak; the code looks mostly bespoke, so "Mirai-derived" follows the group assignment and is not proven.

## Rule P/R
2/2 on group; 0 hits in demons (18) and 0 in 397 others.

## Gaps
Two samples, one architecture. No byte windows added; the strings are plentiful and unique.
