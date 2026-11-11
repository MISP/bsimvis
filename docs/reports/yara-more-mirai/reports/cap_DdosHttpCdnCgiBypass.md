# Linux_Capability_DdosHttpCdnCgiBypass

Capability: http_flood (http_flood,cloudflare_bypass). MITRE ATT&CK: see rule meta. Rule: `rules/G.yar`.

## What the rule matches

HTTP flood with Cloudflare captcha-path bypass: '/cdn-cgi/l/chk_captcha' request template, or ARM httpattack/SendHTTP prologues.

## Evidence

### ddos_http_flood_cloudflare_bypass (28 holders)

GET/POST floods against /cdn-cgi/l/chk_captcha, Cookie/User-Agent builders, attack_app_http, httpattack.

- [string] `'%s /cdn-cgi/l/chk_captcha HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s\r\nConnection: close\r\n\r\n'` in 18 files
- [string] `'%s /cdn-cgi/l/chk_captcha CFKILL/1.1\r\nHost: %s\r\nUser-Agent: %s\r\nConnection: close\r\n\r\n'` in 11 files
- [string] `' HTTP/1.1\r\nUser-Agent: '` in 22 files
- [string] `'\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Ge'` in 21 files
- [string] `'Cookie: '` in 23 files

## Spread (groups of every file the rule hits)

tsun_net_series:11, tsun_holocaust:10, boatnet_xor22:6, nuclear:2

## Implemented in (function clusters the hits land in, `POST /api/yara/hitmap`)

- FUN_00008d3c[1073887716] hit=8/8 (ARM)
- SendHTTPCloudflare[1073886354] hit=4/4 (ARM,MIPS,MIPS)
- httpattack[1073885976] hit=4/4 (ARM,MIPS,MIPS)
- sendHTTPtwo[1073889201] hit=3/3 (MIPS,MIPS,PowerPC)
- sendHTTPtwo[1073889010] hit=2/2 (x86)

## Measured precision and recall

Truth set: holder files from `capabilities.json` (`ddos_http_flood_cloudflare_bypass`); negatives are every other classified file; the `/api/yara/test` raw byte scan skips about 8 unreadable files.

- tp 26, fn 2, fp 3, precision 0.8966, recall 0.9286.
- The 3 hits outside the holder list also contain the cdn-cgi template.

## Known gaps

2 holders (91e42ffd, a1aff109) are not matched; their HTTP flood has no cdn-cgi template and no ARM window.
