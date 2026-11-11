# bottx

Rule: `Linux_Botnet_BotTx_HttpToken` (kind family, low confidence: two samples, identity unknown).

## Coherence check
Coherent: the two MIPS builds (BE, LE; ~1100 functions) share 11+ strings at 2/2 and the same functions around them (`FUN_0041b62c` and `FUN_0041b8d0` in the BE build reference `bot-auth-v1` and `bot-tx-v1`).

## Evidence
- Channel names `bot-auth-v1`, `bot-tx-v1`, `bot-rx-v1`.
- HTTP form protocol: `method=getConfiguration`, `method=getEndpoints&token=`, `token=%s&sid=%s&nonce=%u&seq=%u`, `token=%s&guid=%s`, `token=%s&sid=%s&x=%u`, with a Firefox 78 user agent and `curl/8.0` for JSON.
- `POST /client` and `GET /info.json` with `User-Agent: CitizenFX/1` (FiveM server query protocol), so it probes or poses as FiveM game servers.
- JSON-RPC `eth_call` with `"to":"0x%s","data":"%s"`: reads a contract to get C2 endpoints (blockchain-hosted endpoint list); this is my reading of the string, not traced to a call site.
- Hygiene: `/system/bin/toybox`, `ANDROID_DATA`, `/system/xbin`, process `comm`/`cmdline`/`maps` checks and `reason=root|tmp|var_run|var_tmp|dev|blocklist` strings, `oom_score_adj`. Looks like a loader that refuses to run from certain locations or under certain process names.

Not Mirai-derived.

## Rule and measurement
2 of 3 channel names + 2 of 4 protocol strings + 1 of 4 extras (`reason=`, `eth_call`, `CitizenFX/1`). 2/2, 0 hits in the 413 other files.

## Gaps
No code-level confirmation of the eth_call use; the family name is unknown.
