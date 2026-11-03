# xor_wbyy

16 files, 10 distinct arches. Mirai variant whose table is encoded with a different key; no decode was done, the encoded fragments are matched as they sit in the binary.

## Evidence
10 files have a string table; the same ~35 fragments (`wbYyC107:6;6:`, `kf{bhjpl`wbkfgqlm`, `bgnjmwfof`ln`, `wfomfwbgnjm`, `wpdljmdlm`, `L{kotPD;`, `P1eDrMEp`, `woItsal5`, `rae44232`) appear in all 10 and in no other group (0 negatives). Also reads `/proc/self/exe` and `/proc/*/maps`. ClamAV Malware.Mirai-9950761.

## Rule
`Linux_Botnet_Mirai_XorWbyy` (variant, medium): 3 of 9 fragments. 13/16 on group, 0 in siblings, 0 unrelated. (13 > 10 stringed: the raw-byte scan also finds the fragments in 3 files the string API lists no table for.)

## Gaps
Missed: 2 sh4 files and `azsxd.mpsl`, the stripped members the grouping flagged as uncertain. One member also carries the mvchmod_dlr dropper commands.
