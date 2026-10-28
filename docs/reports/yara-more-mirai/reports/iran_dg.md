# iran_dg

Verdict: NOT one coherent group. It is a loose MIPS collection of static-libc builds. Two rules written for the parts the data supports; the rest has no marker.

## Evidence
Strings: after removing libc text (RPC, resolver, xdr) every member has 36-59 strings. File-level non-library code similarity (bin_sim score_code) splits the 13 files into five blocks:

| Block | Files | Similarity inside | To the others |
|:---|:---|:---|:---|
| iran SigCgt | iran.mips, iran.mipsel (05d802..), 32817e.., 4854ea.. | 0.92-1.00 | 0.51-0.67 |
| dg | dg.mips, dg.mipsel | 1.00 | 0.51-0.69 |
| flylegit MIPS | boatnet.mips, ce750fab.., iran.mipsel (df1b7b..) | 1.00 | 0.62-0.75 |
| wife | wife.mpsl (UPX, no strings), 4d9127.. unpacked | n/a | 0.52-0.75 |
| bot | bot.mips, bot.mpsl | 1.00 | 0.29-0.35 |

- The flylegit MIPS block carries the t.me/flylegit banner and `udpplain`: it belongs to the `flylegit` group.
- The iran SigCgt block has `SigCgt:`, `VmSize:`, `/proc/%d/task/%d/children`, `/proc/%s/fd/0`, `/bin/login`: a process-inspection routine.
- bot.* has `condi2 %s:%d`, `top1hbt.<arch>` and a hand-rolled HTTP client (`User-Agent: Update v1.0`). It shares almost no code with the rest; it was placed here by ClamAV signature only.
- dg.* and the wife pair have no strings beyond libc. Byte seeds from the three non-libc looking core functions turned out to be libc (pthread) and hit 7 unrelated files, so they were dropped.

## Rules
- `Linux_Botnet_Mirai_Iran_ProcScan`: 4 of 6 process strings. 4/4 on the iran SigCgt block, 0 hits elsewhere (423 files). Recall on the whole group is 4/13 by design.
- `Linux_Botnet_Mirai_Condi2_Top1hbt`: `condi2 %s:%d` or 2 of the top1hbt names / HTTP request. 2/2 on bot.*, 0 elsewhere.
- The flylegit MIPS trio is matched by `Linux_Botnet_Mirai_Flylegit` (see flylegit report).

## Gaps
No family rule: the iran, dg, flylegit-MIPS and wife blocks share only libc and about 0.5-0.75 code similarity. dg.* (2) and wife (2, one UPX) have no marker. A proper split of the group into iran, dg, flylegit-MIPS, condi2 and wife is recommended.
