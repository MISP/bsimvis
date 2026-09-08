# Vendored rulesets

**No third-party rule is tracked in git.** `house/` is the only ruleset in the
repo, because those rules were written here and exist nowhere else. Everything
else described below is fetched from a pinned upstream commit by
`scripts/fetch_yara_rules.sh`, which `install.sh` runs. Run it after a fresh
clone, and after any pull that lands this change -- git deletes the previously
vendored copies from your working tree, and until you re-fetch the `yara` module
scans with `house/` alone. Re-vendor by bumping a commit in the script, never by
copying files back in.

`SKIP_ELASTIC=1 scripts/fetch_yara_rules.sh` leaves out the one ruleset with a
non-OSI licence; see the `elastic/` section for the trade.

## backdoor/ certificate/ downloader/ exploit/ infostealer/ pua/ ransomware/ rootkit/ trojan/ virus/

Source: https://github.com/reversinglabs/reversinglabs-yara-rules
Commit: e0a0be54aa1e11ccfd6854e4f19e9476f328fd84 (2025-11-03)
License: MIT (see LICENSE in this directory)

Update by bumping `RL_COMMIT` in `scripts/fetch_yara_rules.sh` (and the commit
above) and re-running it; it replaces these directories wholesale.

## botnet/ anomaly/

Source: https://github.com/Neo23x0/signature-base (yara/crime_mirai.yar,
yara/apt_vpnfilter.yar, yara/gen_elf_file_anomalies.yar)
Commit: e737ebd96c27a52ee99485d4d3e02e9c256d1d3a (2026-08-03)
License: Detection Rule License 1.1 -- permissive (use/copy/modify/distribute),
requires author attribution be retained in the rule's `meta.author`, which it
already is. Full text: https://github.com/Neo23x0/signature-base/blob/master/LICENSE

RL's ruleset has no Mirai/IoT-botnet coverage at all (Windows ransomware/RAT
focus); this supplements it with the Linux/embedded rules that do. Unlike the
RL rules these did not originally carry `meta.category`/`meta.malware` --
those two fields were added by hand to each rule copied in here so they tag
onto the same `yara:<category>:<family>:<rule_name>` scheme instead of
collapsing into `yara:unknown:unknown:*`. No string or condition logic was
touched.

Those additions now live in `scripts/yara_meta.patch`, which the fetch script
applies after copying the three files in. The patch carries the added `meta`
lines and nothing else -- no upstream rule text -- and applies with zero
context, so bumping `SB_COMMIT` without refreshing it fails loudly rather than
silently dropping the fields. Regenerate it with `diff -U0` against the new
upstream copies.

## elastic/

Source: https://github.com/elastic/protections-artifacts (yara/rules/Linux_*.yar
and yara/rules/Multi_*.yar -- 273 files; the Windows/MacOS rules are not
copied in)
Commit: 04edb141ad41aae8e0dc6bd4ee58054d15c14bbb (2026-08-11)
License: **Elastic License 2.0** (see LICENSE-elastic-v2 in this directory).
Permits use, copy, distribute and derivative works. Its one relevant
limitation: you may not provide the software to third parties as a hosted or
managed service giving them access to a substantial set of its functionality.
Running these rules inside a self-hosted BSimVis is fine; shipping BSimVis as a
multi-tenant SaaS whose value is this ruleset is the case to check first. This
is a stricter licence than everything else vendored here -- delete this
directory if that is not a trade you want to carry.

Why: the ELF/IoT-botnet coverage in the other rulesets is thin. On a 259-sample
multi-architecture botnet corpus the pre-existing set tagged 19 samples; adding
these took it to 142. See doc/yara-botnet-coverage.md.

Copied verbatim, unlike the signature-base rules above -- these carry no
`meta.category`/`meta.malware` either, but they do carry
`threat_name = "Linux.Trojan.Mirai"`, and `tag_taxonomy._match_tag()` reads the
category and family off that field instead. So a re-vendor is a plain wipe and
re-copy of this directory with no hand-editing, which is what
`scripts/fetch_yara_rules.sh` does -- bump `EL_COMMIT` there (and the commit
above) and re-run it.

Rules were also evaluated from Yara-Rules/rules (`malware/MALW_*` ELF set),
ditekshen/detection and ESET/malware-ioc. All three added **zero** detections
on that corpus over the above, so none are vendored -- which also avoids
Yara-Rules' GPLv2. Do not re-add them without measuring first.

## ELF_Mirai.yara -- removed, no fetchable source

A single Mirai rule from YARAhub (author `NDA0E`, CC BY 4.0, uuid
`386c1b6c-c5f9-4a9c-a83f-1f940f4d2c2e`). It used to sit untracked at the top
level of this directory. YARAhub publishes no pinnable git commit, so the fetch
script cannot reproduce it and it is simply gone. Mirai stays covered by the
`elastic/` `Linux_Trojan_Mirai*` rules and by `house/`. To put it back, pull it
from https://yaraify.abuse.ch/ by that uuid and drop it in -- anything outside
`house/` is untracked, so it will not be committed.

## house/

Not vendored from anywhere -- rules written in-house against specific
samples in this bsimvis instance's own corpus. Never wiped by a re-vendor of
the folders above; nothing outside this directory should be either. Each
rule documents which sample it was built against in its own `meta` block.

`Linux.Botnet.MultiArchGeneric.yara` is the exception to "specific samples":
every vendored family rule detects ELF botnets by code bytes, so a rule built
from an x86 build never fires on the MIPS/SH/PPC build of the same family, and
the corpus above is 79% non-x86. Those five rules key on recompilation-surviving
behaviour instead and carry the set from 142/259 to 227/259.
