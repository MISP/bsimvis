# tsun_net_series

Rule: `Linux_Botnet_Tsunami_NetSeries` (variant, medium). Recall 12/12, 0 hits in tsun_holocaust, 0 in 394 unrelated files.

## Evidence
Coherent. All 12 files share the Source-engine query blob (`TSource Engine Query` plus its escaped hex twin), the key `d4mQasDSH6`, the long `lXfYC7...` table and the RPC/resolver libc tail. Eleven also carry:
- flood names `OVH-STORM`, `HIPER-OVH`, `OVH-PACKET`, `HYDRA-KILL`, `KILLALLV3`, `HOME-DOWN`, `DOMINATE` ("DOMINATE Flooding %s for %d seconds.", function `SendDOMINATE`)
- raw-socket errors ("Failed opening raw socket.", "Failed setting raw headers mode.")
- the HTTP flood templates, including `/cdn-cgi/l/chk_captcha CFKILL/1.1`
- an sshpass spreader: `sshpass -p '%s' ssh ... 'cd; python udp.py %s %s %s'`, with `ssh_server_ip` and `Hax0r@1337`
- the netstat scanner formats (`%*d: %255s %255s %x`, `/proc/%d/fd`)

The twelfth (7b70aceca81d038bcf859ea5a28f9fd9) is a trimmed build with only the key tables and the method list `STDHEX NFODROP OVHKILL XMAS CRUSH STOMP`; it is covered by the second condition path (`d4mQasDSH6` + TSource + two of the method names).

## Capabilities
IRC-driven Tsunami/Kaiten-style bot (`processCmd` dispatches; command arguments are not visible as strings): UDP/TCP/raw-IP floods, HTTP floods with Cloudflare-captcha path abuse, and a spread path that logs into hosts over SSH and runs a python UDP script. The "three string flavours" in families.md all share the first-listed anchors, so one rule covers the series.

## Measured
- target 12/12; siblings (tsun_holocaust, 9) 0; unrelated 0 (403 of 411 scanned, 8 unreadable).
- `dayzddos.co` note and `y8rtyu...` key are shared with Holocaust and were left out of the anchors on purpose.

## Gaps
Arch-independent; no byte seeds needed. Variant names (net, unet, cock...) cannot be split by this rule.
