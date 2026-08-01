#!/usr/bin/env bash
# Build the BSim benchmark corpus: 5 open-source C projects, cross-compiled to
# 6 targets x 3 optimisation levels x {dynamic, static} linking.
#
# Everything is fetched from upstream at a pinned version + sha256
# (scripts/bench/corpus/sources.txt), and every binary is produced by a direct
# compiler invocation -- no autotools, no cmake, no container. That is the whole
# reproducibility argument: the same command line runs on any machine with the
# Debian/Ubuntu cross toolchains installed (see doc/bench_corpus.md).
#
# Binaries are NOT stripped: the symbol table is the benchmark's ground truth.
# Statically linked variants pull in the whole libc, which is deliberate -- it is
# where standard-library boilerplate (the thing feature weighting is supposed to
# suppress) enters the corpus.
#
# Usage:
#   scripts/bench/corpus/build_corpus.sh [--out DIR] [--tier quick|full] [--jobs N]
#
#   quick : linux-x64 + linux-arm64 + win-x64, O0/O2, dyn/static   (~60 binaries)
#   full  : all 6 targets, O0/O2/Os, dyn/static                    (~180 binaries)

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${CORPUS_ROOT:-$HOME/data/bsim-bench-corpus}"
TIER="full"
JOBS="$(nproc)"

while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT="$2"; shift 2 ;;
        --tier) TIER="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

SRC="$OUT/src"
BIN="$OUT/bin"
LOG="$OUT/build.log"
mkdir -p "$SRC" "$BIN"
: > "$LOG"

# --- targets ----------------------------------------------------------------
# name|compiler prefix|os
ALL_TARGETS=(
    "linux-x64|x86_64-linux-gnu-|linux"
    "linux-arm64|aarch64-linux-gnu-|linux"
    "linux-ppc64le|powerpc64le-linux-gnu-|linux"
    "linux-riscv64|riscv64-linux-gnu-|linux"
    "win-x64|x86_64-w64-mingw32-|windows"
    "win-x32|i686-w64-mingw32-|windows"
)
QUICK_TARGETS=(
    "linux-x64|x86_64-linux-gnu-|linux"
    "linux-arm64|aarch64-linux-gnu-|linux"
    "win-x64|x86_64-w64-mingw32-|windows"
)

if [ "$TIER" = "quick" ]; then
    TARGETS=("${QUICK_TARGETS[@]}")
    OPTS=(O0 O2)
else
    TARGETS=("${ALL_TARGETS[@]}")
    OPTS=(O0 O2 Os)
fi
LINKS=(dyn static)

# --- fetch ------------------------------------------------------------------
fetch() {
    local name ver sha url archive
    while read -r name ver sha url; do
        [ -z "${name:-}" ] && continue
        case "$name" in \#*) continue ;; esac
        archive="$SRC/$(basename "$url")"
        if [ ! -f "$archive" ]; then
            echo "[fetch] $name $ver"
            curl -fsSL -o "$archive.part" "$url" || { echo "  FAILED download $url" >&2; return 1; }
            mv "$archive.part" "$archive"
        fi
        echo "$sha  $archive" | sha256sum -c --quiet - || {
            echo "  CHECKSUM MISMATCH for $name -- refusing to build" >&2; return 1; }
        local marker="$SRC/.extracted-$name-$ver"
        if [ ! -f "$marker" ]; then
            echo "[extract] $name $ver"
            case "$archive" in
                *.zip) unzip -q -o "$archive" -d "$SRC" ;;
                *.tar.gz|*.tgz) tar xzf "$archive" -C "$SRC" ;;
                *.tar.bz2) tar xjf "$archive" -C "$SRC" ;;
                *.tar.xz) tar xJf "$archive" -C "$SRC" ;;
            esac || return 1
            touch "$marker"
        fi
    done < "$HERE/sources.txt"
}

# --- per-project recipes ----------------------------------------------------
# Each recipe is one compiler invocation over a fixed source list. Called with:
#   $1 CC   $2 CFLAGS   $3 LDFLAGS   $4 output path   $5 target os
# ponytail: one exec per binary, no per-object caching. Compiling is minutes;
# the Ghidra ingest downstream is hours. Not worth a build system.

D_SQLITE="$SRC/sqlite-amalgamation-3450300"
D_ZLIB="$SRC/zlib-1.3.1"
D_LUA="$SRC/lua-5.4.6"
D_ZSTD="$SRC/zstd-1.5.6"
D_MBED="$SRC/mbedtls-mbedtls-3.6.0"

build_sqlite() {
    local cc="$1" cflags="$2" ldflags="$3" out="$4" os="$5" extra=""
    [ "$os" = linux ] && extra="-lm -lpthread -ldl"
    $cc $cflags -I"$D_SQLITE" \
        -DSQLITE_OMIT_LOAD_EXTENSION=1 -DSQLITE_THREADSAFE=0 \
        "$D_SQLITE/sqlite3.c" "$D_SQLITE/shell.c" -o "$out" $ldflags $extra
}

build_zlib() {
    local cc="$1" cflags="$2" ldflags="$3" out="$4" os="$5"
    local srcs
    srcs=$(ls "$D_ZLIB"/*.c | grep -v -E 'example|minigzip|infcover|fuzz')
    $cc $cflags -I"$D_ZLIB" -DHAVE_UNISTD_H \
        $srcs "$D_ZLIB/test/minigzip.c" -o "$out" $ldflags
}

build_lua() {
    local cc="$1" cflags="$2" ldflags="$3" out="$4" os="$5" extra=""
    [ "$os" = linux ] && extra="-DLUA_USE_POSIX"
    local srcs
    srcs=$(ls "$D_LUA"/src/*.c | grep -v -E 'onelua|luac\.c')
    $cc $cflags -I"$D_LUA/src" $extra $srcs -o "$out" $ldflags -lm
}

build_zstd() {
    local cc="$1" cflags="$2" ldflags="$3" out="$4" os="$5"
    local srcs
    srcs=$(ls "$D_ZSTD"/lib/common/*.c "$D_ZSTD"/lib/compress/*.c \
              "$D_ZSTD"/lib/decompress/*.c "$D_ZSTD"/lib/dictBuilder/*.c \
              "$D_ZSTD"/programs/*.c 2>/dev/null)
    $cc $cflags -I"$D_ZSTD/lib" -I"$D_ZSTD/lib/common" -I"$D_ZSTD/lib/compress" \
        -I"$D_ZSTD/lib/dictBuilder" -I"$D_ZSTD/programs" \
        -DZSTD_DISABLE_ASM=1 -DZSTD_MULTITHREAD=0 -DZSTD_NOBENCH=1 -DZSTD_NODICT=1 \
        -DBACKTRACE_ENABLE=0 \
        $srcs -o "$out" $ldflags -lm
}

build_mbedtls() {
    local cc="$1" cflags="$2" ldflags="$3" out="$4" os="$5" extra=""
    [ "$os" = windows ] && extra="-lws2_32 -lbcrypt"
    local srcs
    srcs=$(ls "$D_MBED"/library/*.c)
    $cc $cflags -I"$D_MBED/include" -I"$D_MBED/library" \
        $srcs "$D_MBED/programs/test/selftest.c" -o "$out" $ldflags $extra
}

PROJECTS=(sqlite zlib lua zstd mbedtls)

# --- build matrix -----------------------------------------------------------
build_one() {
    local project="$1" tname="$2" prefix="$3" os="$4" opt="$5" link="$6"
    local cc="${prefix}gcc"
    command -v "$cc" >/dev/null || { echo "SKIP $project $tname: no $cc" >> "$LOG"; return 0; }

    local ext="" ldflags="" cflags="-$opt -g0 -fno-stack-protector"
    [ "$os" = windows ] && ext=".exe"
    [ "$link" = static ] && ldflags="-static"

    local dir="$BIN/$project"
    mkdir -p "$dir"
    local out="$dir/${project}-${tname}-${opt}-${link}${ext}"
    [ -f "$out" ] && { echo "HAVE $out" >> "$LOG"; return 0; }

    if "build_$project" "$cc" "$cflags" "$ldflags" "$out" "$os" >> "$LOG" 2>&1; then
        echo "OK   $out" | tee -a "$LOG"
    else
        rm -f "$out"
        echo "FAIL $project $tname $opt $link (see $LOG)" | tee -a "$LOG"
    fi
}
export -f build_one build_sqlite build_zlib build_lua build_zstd build_mbedtls
export BIN LOG D_SQLITE D_ZLIB D_LUA D_ZSTD D_MBED

echo "corpus root : $OUT"
echo "tier        : $TIER   targets=${#TARGETS[@]} opts=${OPTS[*]} links=${LINKS[*]}"
fetch || exit 1

for project in "${PROJECTS[@]}"; do
    for t in "${TARGETS[@]}"; do
        IFS='|' read -r tname prefix os <<< "$t"
        for opt in "${OPTS[@]}"; do
            for link in "${LINKS[@]}"; do
                echo "$project|$tname|$prefix|$os|$opt|$link"
            done
        done
    done
done | xargs -P "$JOBS" -I{} bash -c 'IFS="|" read -r p t pre o opt link <<< "{}"; build_one "$p" "$t" "$pre" "$o" "$opt" "$link"'

echo
echo "built: $(find "$BIN" -type f \( -name '*.exe' -o ! -name '*.*' \) | wc -l) binaries, $(du -sh "$BIN" | cut -f1)"
echo "failures: $(grep -c '^FAIL' "$LOG")  (details: $LOG)"
echo "next: scripts/bench/corpus/manifest.py --out $OUT"
