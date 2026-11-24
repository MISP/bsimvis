# tsun_holocaust

Rule: `Linux_Botnet_Tsunami_Holocaust` (variant, medium). Recall 9/9, 0 hits in tsun_net_series, 1 hit outside the group.

## Evidence
Coherent. Anchors: the obfuscated key `xmiwdio MOQWI"K (**(D8u9289d2892e9 NIOWJDiojwioWAJM DKLAM)`, `9u123448u124au814d4x10`, the ANSI banner `\e[36mIHolocaust` (printed with a literal backslash), and the attack table: `OVHCRUSH OVHFREEZE OVH-NULL OVH-KILL CPU-RAPE HOME HOME-SEC HOME-XV/XZ/X12 UDPBV2 V-HEX V-STD V-VSE NFONULL NFO-DROP COD-CRASH FORTNITE LDAP MOLLY-NFO SLAP-OVH SERVER CRAWLER GAME UDP-KILL UDP-BP VMAAT`. Also present (not used as anchors): `dayzddos.co runs you ...` note, netis banner "Self Rep Fucking NeTiS...", TSource Engine Query, `/cdn-cgi/l/chk_captcha`, escaped-hex key tables.

The mips and mpsl members have no extracted strings but still match; only the strings API is empty for them.

## Capabilities
Tsunami-style IRC bot with a large game-server/OVH flood menu (Source query, Fortnite, COD, LDAP reflection), Cloudflare-captcha HTTP flood, netis-router spread banner. Capabilities beyond names not verified in code.

## Measured
- target 9/9; sibling tsun_net_series 0/12; unrelated 1/394.
- The one outside hit is `9fa3e4cb407b4a15244191292efac441` ("sh4", filed under xor_wbyy). It matches every Holocaust anchor, so it is almost certainly a mis-grouped Holocaust SuperH build. Suggest moving it.

## Gaps
Overlap with net series is real (shared key tables, dayzddos note); the rule discriminates only on the Holocaust-only strings above.
