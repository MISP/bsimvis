# Vendored rulesets

## backdoor/ certificate/ downloader/ exploit/ infostealer/ pua/ ransomware/ rootkit/ trojan/ virus/

Source: https://github.com/reversinglabs/reversinglabs-yara-rules
Commit: e0a0be54aa1e11ccfd6854e4f19e9476f328fd84 (2025-11-03)
License: MIT (see LICENSE in this directory)

Update by re-cloning the source repo and replacing these directories' contents
(except this file and LICENSE), then bumping the commit above.

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

## house/

Not vendored from anywhere -- rules written in-house against specific
samples in this bsimvis instance's own corpus. Never wiped by a re-vendor of
the folders above; nothing outside this directory should be either. Each
rule documents which sample it was built against in its own `meta` block.
