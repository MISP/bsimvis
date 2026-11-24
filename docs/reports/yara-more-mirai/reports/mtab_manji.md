# mtab_manji

Rule: `Linux_Botnet_Other_MountFlagsHelper` (variant, low). Recall 3/4, 0 hits outside.

## Group is weakly coherent
Three files (a27302b0 manji.mpsl, a8c66689 nerv.mpsl, 5991b6c6 unnamed) share one block: `/proc/mounts`, `/etc/mtab`, `nosuid noexec nodev noatime nodiratime`, plus `/proc/%d/cmdline`. The nerv build adds an HTTP flood client (Host/User-Agent/Cookie, Mozilla UA). The fourth (8ace49d1 mipsel) is a different bot: `dvrHelper`, a systemd `bot.service` unit, `@reboot` crontab, `.bashrc` and /etc/passwd persistence. It shares only libc strings, so it does not belong.

The mount block looks like a shared helper (the kind of code that remounts or hides filesystems) rather than a family signature, so the rule's meaning is "same code lineage as the manji/nerv builds", not a named family.

## Measured
target 3/4, 0 of 411 unrelated. Requires all seven strings, which fire only on these three.

## Gaps
No byte seeds attempted: the three files are MIPS-LE only and the strings are already exact. Suggest moving 8ace49d1 out of the group.
