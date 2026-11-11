# static_bot_started

Rules: `Linux_Botnet_StaticBot_BotStarted` (family, medium), `Linux_Botnet_StaticBot_UpdaterDesktop` (variant, low).

## Coherence check: the group was two bots
Only 42a7f107 (ARM?) and 6e834265 share the "BOT STARTED" strings. The MIPS-LE member 6e05a46a ("no clear marker beyond static linking") is a different binary:

- 42a7f107 and 6e834265: `BOT STARTED\n`, `CNC_HOST`/`CNC_PORT` env, `wget ... -O /tmp/.bot || curl`, `ping_addr=127.0.0.1%0a`, `Action=Update&User=admin&Pass=admin&Command=`, `miniigd`/SOAP, SSH client strings (`SSH-2.0-OpenSSH_8.9p1`, group14 kex, brute force with "assword"), HTTP `CONNECT` proxy handling, persistence by cron (`* * * * * root`), OpenWrt `rc.common` `START=99`, inittab `::respawn:`, `/etc/rc{2,3,5}.d`, a systemd unit `Description=Network Manager` with `Restart=always`, `systemctl enable`.
- 6e05a46a: `updater.desktop` and `~/.profile` autostart (`(nohup %s >/dev/null 2>&1 &)`), `/proc/self/oom_score_adj` = -1000, daemonise error strings, `pong %s`, option parser keys `psize=`, `srcport=`, `gport=` (flood parameters). It shares only glibc with the other two.

## Rules and measurement
- BotStarted: 3 of 4 core strings + 2 of 6 persistence/exploit strings. Recall 2/2 coherent members.
- UpdaterDesktop: 2 of 4 autostart strings + 2 of 4 command strings. Recall 1/1.
- Together 3/3 on the group; 0 hits in zbot_static, static_sk_bot, and the other ~400 files.

## Gaps
Variant rule is a single sample; its family identity is unknown. Capabilities are read from strings, not from tracing code.
