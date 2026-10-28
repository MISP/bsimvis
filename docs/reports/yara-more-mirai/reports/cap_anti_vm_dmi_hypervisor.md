# cap_anti_vm_dmi_hypervisor

Rule `Linux_Capability_AntiVmDmi` (kind capability, confidence high). MITRE ATT&CK T1497.001.

## What it detects
Reads /sys/class/dmi/id/product_name and compares to 'VMware'; od_* family also checks /proc/driver/vmware, /sys/hypervisor/type, /proc/sys/qemu_version, 'sandbox'.

## Evidence
- `string` "/sys/class/dmi/id/product_name" (32 files)
- `string` "VMware" (32 files)
- `string` "/proc/driver/vmware" (10 files)
- `string` "/sys/hypervisor/type" (10 files)
- `string` "/proc/sys/qemu_version" (10 files)
- `string` "/etc/vmware" (10 files)
- `string` "sandbox" (10 files)
- `string` "/sys/class/dmi/id/board_vendor" (10 files)

Catalogue holders: 32. Spread in the catalogue: {"MD5.unpacked": 10, "MD5MD5": 9, "xnxnxnxnxnxnxnxn": 8, "xnxnxnxnxnxnxnxnsh2x": 2, "xnxnxnxnxnxnxnxnloon": 2, "MD5MD5.elf": 1}. Spread of the rule: xnxn (all arch variants), od_ kit (10), unpacked MD5MD5 files.

## Rule
Anti-VM check: reads /sys/class/dmi/id/product_name and compares to VMware; the od_ build adds /proc/driver/vmware, /sys/hypervisor/type, /proc/sys/qemu_version. Targets are the capability's holder files (not one variant); negatives are every other classified file. Architecture: arch-independent (strings).

## Measured
- vs catalogue holders: tp 32, fn 0 (recall 1.0).
- hits outside the holder list: 0 (none). Judged by tp+fp truth with the raw byte scan: the string index undercounts holders (merged adjacent strings, UPX copies), so hits in sibling families that carry the same bytes are real holders, not false positives. Catalogue-only precision: 1.0.

## Cluster landing (POST /api/yara/hitmap)
- cluster 1073841746 `FUN_00400568` (12 members), 12 hit files, AARCH64, Loongarch, MIPS, SuperH4, x86, via $v1, $v2
- cluster 1073861917 `FUN_ram_00010446` (6 members), 6 hit files, PowerPC, RISCV, via $v1, $v2
- cluster 1073834024 `evade_detect` (4 members), 4 hit files, MIPS, via $v1, $v2, $v3, $v4, $v5
- cluster 1073853705 `evade_detect` (4 members), 4 hit files, ARM, via $v1, $v2, $v3, $v4, $v5
- cluster 1073851564 `FUN_08049550` (2 members), 2 hit files, x86, via $v1, $v2
- cluster 1073837878 `evade_detect` (2 members), 2 hit files, AARCH64, via $v1, $v2, $v3, $v4, $v5

Implemented in: evade_detect 1073853705 (ARM), 1073834024 (MIPS), 1073837878 (AARCH64) for the full check; FUN_00400568 1073841746 (AARCH64, Loongarch, MIPS, SuperH4, x86), FUN_ram_00010446 (PowerPC, RISCV), FUN_800005d2 (m68k) for the short one.

## Gaps
The condition needs product_name + VMware only; the extra od_ strings make it stronger but are not required.
