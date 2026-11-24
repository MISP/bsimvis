# Boilerplate name audit: what the name rules missed

The boilerplate rules tagged 47,971 of 95,260 named functions in a mirai/IoT malware collection. They missed 42,413 more that are plainly not custom code: Go standard library, uClibc sun-RPC and linuxthreads, glibc resolver internals, libgcc unwinder helpers. They also tagged 344 functions they should not have: every `entry`.

The rules now tag the 42,413 and no longer tag `entry`. **After the change, 361 named functions are left untagged in the Go files, and they are all custom code.**

This report covers what I pulled, how I decided each name was safe to add, what I rejected, and where the method stops working. `boilerplate:` is a free namespace, so the new families sit beside `runtime:` as `library:`.

## Scope and numbers

Collection size, at the time of the audit:

```
functions                                   ~201.7k
carry neither boilerplate: nor fid: tag      154,354
  of those, unnamed (FUN_xxxx)               ~107.8k
  of those, with a real name                  46,550   (5,870 distinct names)
```

The audit ran on the 46,550. A `FUN_` name carries nothing a name rule can use.

Result of replaying the new rules over every named function in the collection (95,260, file by file, since the gates are per file):

| Family tag | Newly tagged |
|---|---:|
| `boilerplate:library:go:stdlib` | 23,330 |
| `boilerplate:runtime:go:runtime` | 11,858 |
| `boilerplate:runtime:libc:rpc` | 3,292 |
| `boilerplate:runtime:libc:support` | 2,017 |
| `boilerplate:runtime:libc:thread` | 1,267 |
| `boilerplate:runtime:libgcc:unwind` | 254 |
| `boilerplate:runtime:libc:resolver` | 240 |
| `boilerplate:library:openssl:*` | 67 |
| `boilerplate:runtime:libgcc:support` | 42 |
| `boilerplate:runtime:gcc:support` | 24 |
| `boilerplate:runtime:elf:init` / `fini` | 22 |
| **Total** | **42,413** (5,144 distinct names) |

322 of the 42,413 already carried a `fid:` tag, so the two sources agree there. The one loss in the replay is `entry`: 344 functions, no other name.

## How I found candidates

Pulled every function that has neither tag. `exclude_func_tag` matches hierarchically, so one `boilerplate` covers the whole namespace:

```bash
curl -s "$BSIMVIS/api/function/search?collection=$COLL&limit=5000&offset=$OFF\
&exclude_func_tag=boilerplate&exclude_func_tag=fid"
```

`limit=5000` is the practical page size. Page with `offset` until it reaches `total`. Each record carries what the rest of the method needs: `function_name`, `instruction_count`, `callers`, `callees`, `file_tags`, `language_id`.

Then I grouped by name and counted distinct files per name. The top of that list is two populations mixed together: real libc (`memset` in 60 files, `xdr_callhdr` in 58) and the bots' own code (`attack_udp_plain`, `table_init`, `killer_init`). Separating them is the whole job, and a name's frequency does not do it: `main` is in 51 files.

I did not use the `origin:lib:*` tags as evidence. Mirai functions carry them (`processCmd`, `table_init`, `attack_*`, and `main` in 38 of 51 files), which is the old `fid:` bug. I used them only to build the first candidate list.

## How I checked a name was safe

Four checks, in the order I ran them. A name went in only if it passed all four.

**1. Callers and callees are libc.** For each name I collected every non-`FUN_` caller and callee across all its untagged instances. A libc-internal name is called by other libc names and by nothing from the bot.

| Name | Files | Callers |
|---|---:|---|
| `enqueue` | 108 | `pthread_cond_wait`, `pthread_rwlock_wrlock`, `sem_wait`, `sem_timedwait` |
| `restart` | 38 | `__pthread_lock`, `__pthread_manager`, `pthread_cancel`, `pthread_cond_signal` |
| `strip_whitespace` | 11 | `_nss_netgroup_parseline` |
| `same_address` | 11 | `resolv_conf_matches` |
| `search_list_add__` | 11 | `__resolv_conf_load` |
| `dl_cleanup` | 10 | `__uClibc_fini` |
| `frame_downheap` | 8 | `_Unwind_Find_FDE` |
| `_INIT_0` | 10 | calls `__register_frame_info` |

`enqueue`, `restart` and `suspend` are the names I was most worried about, because a bot could name its own queue `enqueue`. In 113 untagged `enqueue` instances, every caller that is not a `FUN_` is a `pthread_*` or `sem_*` function.

This check does not work for thin libc wrappers. `close`, `socket`, `send` are called by `attack_*` and `killer_*` in every bot, because bots use the libc API. For those the caller is the bot and the function is still libc. I used a different check there (next).

**2. Size against the name's own median.** Per name, I flagged any untagged instance larger than `max(3 x median, median + 80)` bytes, for names with 3 or more instances. `instruction_count` in the index is `getNumAddresses()`, so it is a byte count, not an instruction count. A libc `strcpy` is tens of bytes; a bot function that took the name would be far larger.

30 of 7,843 untagged instances of the proposed names were flagged. All 30 were libc variants:

- `fork` (704 bytes against a median of 38), `fcntl`, `ioctl`, `memcpy`, `socket`, `wcrtomb` in the MIPS pair `117a13cd` / `1c78aba4`. Same sizes in the little- and big-endian build, so one static glibc, with cancellation wrappers inlined.
- `malloc` at 2,149 bytes in five x86_64 files, identical size: glibc malloc.
- `internal_getent` at ~1,504 bytes in five MIPS files: glibc nss.

Two I read in the decompiler because the size or the caller looked wrong:

- `strcpy` in `88e17e55` (sparc): 908 bytes against a median of 61. The decompiler header reads `Library Function - Single Match, Library: uclibc 0.9.30.1`. It is uClibc's word-aligned `strcpy`.
- `telldir` in `65ffd431` (arm): 8 bytes, `return *(long *)(dirp + 0x10)`, called by `killer_init` and `locker`. It is uClibc's `telldir`. The bot calls it; it did not write it.

**3. Malware-shaped callers, for names that are not wrappers.** I ran the same search for callers or callees matching `attack_*`, `killer_*`, `scanner_*`, `flood_*`, `Send*`, `table_*`, `util_*`, `processCmd`. Outside the thin wrappers, only `telldir` hit, and that is the case above. After the replay, none of the 5,144 newly tagged names matches that pattern.

**4. Replay on the real collection before merging.** Ran the new `boilerplate_tags_for_names` over each file's full name list (the gates are per file) and compared with the tags already stored. This gave the table above, the 344 `entry` losses, and a list of every name the new rules touch. I read that list rather than trust the counts.

## Names I rejected

The audit's second job is what it left out. These looked like candidates and are not boilerplate:

| Names | Why not |
|---|---|
| `attack_*`, `flood_*`, `Send*`, `killer_*`, `scanner_*`, `table_*`, `util_*`, `rand_next/init/str/cmwc`, `print`, `printi`, `prints`, `processCmd` | The bots' own code. Present in 10 to 14 files each because the source is shared, which is the opposite of boilerplate. |
| `_memcpy`, `_strlen`, `_strcpy`, `_atoi` and other single-underscore helpers | Bot helpers. The existing comment in `boilerplate_tag_service.py` already says so. |
| `sha256_transform`, `sha256_*`, `chacha20_*` | Callers are the bot's own `sec_sha256_buf`. Crypto the malware runs is what an analyst wants to see. |
| Go `main.*`, third-party Go packages | Custom code or not in the standard library. The Go rule requires a standard-library root. |
| `type:.eq.main.*` | The compiler's equality helper for a bot type. Generic in form, but the type is custom. |

## `entry` is removed

**`entry` is Ghidra's name for whatever sits at the entry point, so a name rule cannot say what it is.** The rule tagged all 344 as `boilerplate:runtime:elf:startup`.

- 149 of the 344 are in files tagged `packer:upx`. There the entry point is the UPX unpacker stub.
- In unpacked files, `entry` is at most 1,008 bytes (ARM uClibc, with `__uClibc_init` inlined). In UPX files it runs up to 2,841 bytes. Thirteen are over 1,000 bytes. The x86_64 one in `20532b27` is 2,701 bytes with a 3.5 KB stack frame and one callee, `FUN_00110df8`: a decompression loop, not a CRT stub.
- Size alone does not separate them. Most packed `entry` functions are tiny (4 to 300 bytes), because the stub is mostly a jump. A ceiling would have kept the small ones tagged and dropped the big ones, on a rule that is wrong in kind.

I first proposed a 1,500-byte ceiling. It was dropped: the name is too general to tag at any size. `_start` and the `__libc_*` startup names stay; they name something specific.

## The gate, and why the soft version is safe

Plain names (`read`, `open`, `xdr_callhdr`) also belong to application code. They were tagged only in files that already show a static libc: at least 25 functions matching a reserved-namespace prefix (`__stdio_`, `_dl_`, `__pthread_`, and so on).

That misses small uClibc statics. 103 files had plain libc names that were in the list but untagged, 2,987 functions in all, because they show 3 to 24 prefixes.

The gate is now: 25 prefixes, **or** 3 prefixes plus one of `__uClibc_main`, `__uClibc_init`, `__libc_start_main`, `__libc_csu_init` in the file. A file with a libc startup symbol and three libc-namespace functions links libc, and `read` in it is libc's `read`. On the collection this covers about 1,570 of the 2,987; the remainder have too few prefixes and no startup symbol, and I left them.

Two details that keep the gate honest:

- `__i686.get_pc_thunk.*`, `vpaes_*`, `bsaes_*` are tagged but do not count toward the gate. Any gcc x86 build has the thunks, and they say nothing about libc.
- The plain-name check still applies to clone suffixes (`.isra.0`, `.part.0`, `.constprop.0`), which are stripped before lookup.

## New branches under `boilerplate:`

| Tag | What it holds | Gate |
|---|---|---|
| `runtime:libc:rpc` | sun-RPC / XDR: `xdr_*`, `svc_*`, `clnt*`, `pmap_*`, `authnone_*` | libc gate |
| `runtime:libc:thread` | linuxthreads / NPTL internals: `pthread_*` internals, `wait_node_*`, `enqueue`, `restart`, `setxid_*` | libc gate |
| `runtime:libc:resolver` | glibc resolver and nss internals: `conf_decrement`, `maybe_init`, `internal_getent` | libc gate |
| `runtime:libc:support` | uClibc odds, syscall wrappers (`mkstemp`, `openpty`, `seteuid`, `reboot`), the plain glibc list | libc gate |
| `runtime:libgcc:unwind` | DWARF unwinder helpers: `fde_*`, `uw_*`, `btree_*`, `execute_cfa_program_*` | libc gate |
| `runtime:libgcc:support` | integer division, ctors helpers, ARM 64-bit atomics | libc gate |
| `runtime:elf:init` / `fini`, `runtime:gcc:support` | `_INIT_0`, `_FINI_0`, `_PREINIT_0`, `call_weak_fn`, `__i686.get_pc_thunk.*` | none |
| `library:openssl:*` | `BIO_printf`, `bn_*_comba*`, `vpaes_*`, `md5_block_asm_data_order` | none, exact names |
| `runtime:go:runtime` | `runtime.*`, `internal/runtime/*`, `internal/abi.*`, Go asm stubs (`gogo`, `memeqbody`) | Go gate |
| `library:go:stdlib` | every other standard-library package, and `type:.eq.<stdlib type>` | Go gate |

The Go gate is 10 or more functions named `runtime.*` in the file. A C clone such as `sort.part.0` has the same shape as a Go name, so a standard-library root alone is not enough.

Go was the largest single source: 8 Go files, ~4k functions each, 35,188 of the 42,413. The residue shows the rule stops in the right place. **361 named functions are left untagged in those files: 162 in the `vortax_server/Methods` package, 62 `main.*`, 110 `type:.eq.*` helpers for custom types, 27 misc.** None of the 8 files has a `main.*` function tagged.

OpenSSL is exact names only. A static OpenSSL is library code, but a prefix such as `EVP_` or `CRYPTO_` would also catch a bot's dynamic-link thunk for the import, and hide a call the analyst wants to see.

## What this does not cover

- One collection, heavy on uClibc and glibc IoT builds, plus 8 Go files. musl, newlib, MSVC and Mach-O runtimes were not audited. The exact-name tables cover what this collection contains and nothing more.
- Thin wrappers such as `system`, `execve`, `fork`, `kill`, `ptrace` are tagged boilerplate when the file links libc. The wrapper's body is libc's. The call sites in the bot's own functions are not tagged and stay visible in `callers`. A filter on the boilerplate axis hides the wrapper, not who called it.
- Third-party Go packages (`github.com/...`) are not tagged. They are libraries, and the same idea would extend to them, but there is no closed list to check against.
- The rules run at analysis time (`ghidra_service` and the scan path). A stored collection needs a re-analysis or a backfill to pick them up. `uv run python scripts/backfill_boilerplate_tags.py --collection <name> --dry-run` replays the rules over the stored names (no Ghidra) and prints the added and removed counts per family; drop `--dry-run` to write. Compare the dry-run against the 42,413 / 344 above before the real run. Limits: sim-level `func_tags` and cluster `tag_distribution` keep the old tags until a rebuild; a file's `boilerplate:runtime` tag stays even if its only runtime function was `entry`; run it with no upload or analysis job active on the collection.

## Reproducing

1. Pull the untagged named functions with the call above, and group by `function_name`.
2. For each candidate, collect `callers` and `callees` names across all instances, and compute the median of `instruction_count`.
3. Flag instances above `max(3 x median, median + 80)` and read them. Flag callers matching the bot-name pattern.
4. Replay `boilerplate_tags_for_names` per file over the stored names and read the diff.

`uv run python -m bsimvis.app.services.boilerplate_tag_service` runs the self-check for every rule above, including `entry` staying untagged and each gate refusing a small file.
