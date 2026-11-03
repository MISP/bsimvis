# securecore

Rule: `Linux_Botnet_Mirai_SecureCore` (variant, medium).

## Evidence
`A_2026_SECURE_CORE_BOT_KEY!` in both files (MIPS-LE: `mipsel` f3f8af38... and b1676efd...), 0 hits elsewhere in 413 files. No other string in the group is unique to it, so the rule has one anchor. The key suggests encrypted C2 but this was not verified in code.

## Rule P/R
2/2 on group; 0 unrelated.

## Gaps
Single string anchor and two samples: it identifies this build, not a code family. A recompiled build with a changed key is missed. Byte seeds were not tried: one architecture, two files.
