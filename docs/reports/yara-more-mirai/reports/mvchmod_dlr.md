# mvchmod_dlr

10 files, plain architecture names, compact (127-239 functions). The only non-libc strings shared by all ten are two self-relocation shell templates: `mv %s /%s/%s && chmod 777 /%s/%s >/dev/null` and `mkdir /%s/ && >/%s/%s && cd /%s/ >/dev/null`. 0 hits in the other classified files except one xor_wbyy member. ClamAV Trojan.Mirai-9854559.

## Rule
`Linux_Botnet_Mirai_MvChmodDlr` (variant, high): both strings. 10/10, 1 hit in xor_wbyy (same dropper), 0 unrelated.

## Gaps
Capabilities beyond the dropper not analysed (no attack or scanner strings in the shared set). Both anchors are dropper text; no byte-window path was needed for recall.
