#!/usr/bin/env bash
# Fetch the third-party YARA rulesets into data/yara_rules/.
#
# No third-party rule is tracked in git. Only house/ is, because those rules
# were written here and exist nowhere else. Everything else is fetched from a
# pinned upstream commit; the meta.category/meta.malware fields that botnet/ and
# anomaly/ need are re-applied from yara_meta.patch, which holds those added
# lines and nothing else. See data/yara_rules/VENDORED_FROM.md.
#
# Not having run this is not fatal: yara_service.py treats a thin ruleset the
# same as a missing one -- fewer tags, no failed jobs.
set -euo pipefail

RULES_DIR="${YARA_RULES_DIR:-$(cd "$(dirname "$0")/.." && pwd)/data/yara_rules}"

RL_REPO=https://github.com/reversinglabs/reversinglabs-yara-rules
RL_COMMIT=e0a0be54aa1e11ccfd6854e4f19e9476f328fd84
RL_DIRS=(backdoor certificate downloader exploit infostealer pua ransomware rootkit trojan virus)

EL_REPO=https://github.com/elastic/protections-artifacts
EL_COMMIT=04edb141ad41aae8e0dc6bd4ee58054d15c14bbb

SB_REPO=https://github.com/Neo23x0/signature-base
SB_COMMIT=e737ebd96c27a52ee99485d4d3e02e9c256d1d3a
META_PATCH="$(cd "$(dirname "$0")" && pwd)/yara_meta.patch"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# ponytail: blob:none + checkout <sha> beats --depth 1, which cannot fetch a
# pinned commit once it is no longer the branch tip.
clone() {  # repo commit dest
    git clone --filter=blob:none --no-checkout -q "$1" "$3"
    git -C "$3" checkout -q "$2"
}

mkdir -p "$RULES_DIR"

echo "ReversingLabs $RL_COMMIT -> $RULES_DIR (MIT)"
clone "$RL_REPO" "$RL_COMMIT" "$tmp/rl"
for d in "${RL_DIRS[@]}"; do
    rm -rf "${RULES_DIR:?}/$d"
    cp -r "$tmp/rl/yara/$d" "$RULES_DIR/$d"
done
cp "$tmp/rl/LICENSE" "$RULES_DIR/LICENSE"

# signature-base supplies the Linux/IoT botnet coverage the RL set has none of.
# These three are the only fetched files that are not verbatim: each rule needs
# meta.category/meta.malware so it tags as yara:<category>:<family>:<rule> rather
# than collapsing into yara:unknown:unknown:*. yara_meta.patch adds exactly those
# lines -- it applies with zero context, so it fails loudly if SB_COMMIT moves.
echo "Neo23x0/signature-base $SB_COMMIT -> $RULES_DIR (DRL 1.1)"
clone "$SB_REPO" "$SB_COMMIT" "$tmp/sb"
rm -rf "${RULES_DIR:?}/botnet" "${RULES_DIR:?}/anomaly"
mkdir -p "$RULES_DIR/botnet" "$RULES_DIR/anomaly"
cp "$tmp/sb/yara/crime_mirai.yar" "$tmp/sb/yara/apt_vpnfilter.yar" "$RULES_DIR/botnet/"
cp "$tmp/sb/yara/gen_elf_file_anomalies.yar" "$RULES_DIR/anomaly/"
cp "$tmp/sb/LICENSE" "$RULES_DIR/LICENSE-neo23x0-DRL-1.1"
patch -p1 --forward -d "$RULES_DIR" < "$META_PATCH"

# Elastic License 2.0 is the strictest licence here: self-hosting is fine, but
# it forbids offering a substantial set of its functionality as a hosted
# multi-tenant service. SKIP_ELASTIC=1 leaves it out -- costs roughly half the
# ELF/botnet coverage (see doc/yara-botnet-coverage.md).
if [ "${SKIP_ELASTIC:-0}" = 1 ]; then
    echo "elastic/ skipped (SKIP_ELASTIC=1)"
else
    echo "Elastic protections-artifacts $EL_COMMIT -> $RULES_DIR/elastic (Elastic License 2.0)"
    clone "$EL_REPO" "$EL_COMMIT" "$tmp/el"
    rm -rf "${RULES_DIR:?}/elastic"
    mkdir -p "$RULES_DIR/elastic"
    cp "$tmp"/el/yara/rules/Linux_*.yar "$tmp"/el/yara/rules/Multi_*.yar "$RULES_DIR/elastic/"
    cp "$tmp/el/LICENSE.txt" "$RULES_DIR/LICENSE-elastic-v2"
fi

echo "rule files now: $(find "$RULES_DIR" \( -name '*.yar' -o -name '*.yara' \) | wc -l)"
