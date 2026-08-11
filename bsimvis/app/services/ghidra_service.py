import hashlib
import json
import logging
import os
import re
import tempfile
import time
import uuid
from pathlib import Path
from collections import Counter

import pyghidra
from pyghidra.launcher import HeadlessPyGhidraLauncher
import tomllib

from bsimvis.app.services import tag_taxonomy
from bsimvis.app.services.unpack_service import FILE_SCOPE_TAG_PREFIXES

GHIDRA_DECOMP_MAX_TIMEOUT = 10
DEFAULT_CONFIG_NAME = "bsimvis_config.toml"


class GhidraService:
    def __init__(self, config=None):
        if config is None:
            config = self._load_default_config()
        self.config = config
        self._launcher = None

    def _load_default_config(self):
        try:
            config_path = Path(DEFAULT_CONFIG_NAME)
            if config_path.exists():
                with open(config_path, "rb") as f:
                    return tomllib.load(f)
            else:
                example_path = Path("bsimvis_config.toml.example")
                if example_path.exists():
                    with open(example_path, "rb") as f:
                        return tomllib.load(f)
        except Exception as e:
            logging.warning(f"Failed to load default config: {e}")
        return {}

    def ensure_launcher(
        self, verbose=False, max_ram_percent=60.0, max_heap_mb=None, jvm_args=None
    ):
        if not self._launcher:
            try:
                from pyghidra.launcher import get_launcher

                self._launcher = get_launcher()
                if self._launcher:
                    return self._launcher
            except:
                pass

            logging.info("[i] Starting Ghidra JVM")
            self._launcher = HeadlessPyGhidraLauncher(verbose=verbose)
            # MaxRAMPercentage is a share of *host* RAM applied per JVM, so N
            # workers authorize N x that share. Prefer an absolute cap.
            if max_heap_mb:
                self._launcher.add_vmargs(f"-Xmx{int(max_heap_mb)}m")
            else:
                self._launcher.add_vmargs(f"-XX:MaxRAMPercentage={max_ram_percent}")
            if jvm_args:
                for arg in jvm_args:
                    self._launcher.add_vmargs(arg)
            self._launcher.start()
        return self._launcher

    def _function_id_hash(self, func, program):
        """Deterministic per-function hash for exact-matching small functions.

        Primary: Ghidra FunctionID full-hash (masks relocatable operands, so it
        is stable across binaries). Fallback: sha1 of the mnemonic+operand-type
        sequence when FID declines — it refuses functions below its shingle
        floor, which are exactly the tiniest ones we still want to match.
        Returns None only if the function has no instructions.
        """
        try:
            from ghidra.feature.fid.service import FidService

            quad = FidService().hashFunction(func)
            if quad is not None:
                return format(quad.getFullHash() & 0xFFFFFFFFFFFFFFFF, "016x")
        except Exception:
            pass

        # ponytail: fallback keys on mnemonic + operand *types* only, not operand
        # values — loose for pathological tiny funcs. Tighten with operand-value
        # masking if false 100% matches show up.
        try:
            listing = program.getListing()
            parts = []
            for instr in listing.getInstructions(func.getBody(), True):
                ops = ",".join(
                    str(instr.getOperandType(i)) for i in range(instr.getNumOperands())
                )
                parts.append(f"{instr.getMnemonicString()}|{ops}")
            if not parts:
                return None
            return "f" + hashlib.sha1("\n".join(parts).encode()).hexdigest()[:15]
        except Exception:
            return None

    def get_token_type(self, clazz):
        if clazz == "ClangVariableToken":
            return "variable"
        if clazz == "ClangFuncNameToken":
            return "func_call"
        if clazz == "ClangTypeToken":
            return "type"
        if clazz == "ClangOpToken":
            return "op"
        if clazz == "ClangFieldToken":
            return "field"
        if clazz == "ClangSyntaxToken":
            return "syntax"
        return "text"

    def build_semantic_source(self, markup, program=None):
        c_lines = []
        c_tokens = []
        addr_to_line = {}
        addr_to_token_idx = {}
        line_to_token_idx = {}
        line_to_addr = {}
        seq_to_token_idx = {}

        current_line_text = []
        token_counter = 0

        if not markup:
            return (
                c_lines,
                c_tokens,
                addr_to_line,
                addr_to_token_idx,
                line_to_token_idx,
                line_to_addr,
                seq_to_token_idx,
            )

        def walk_tokens(node):
            nonlocal token_counter

            num_children = node.numChildren() if hasattr(node, "numChildren") else 0
            if num_children > 0:
                for i in range(num_children):
                    walk_tokens(node.Child(i))
                return

            clazz = node.getClass().getSimpleName()
            line_idx = len(c_lines)

            if clazz == "ClangBreak":
                c_lines.append("".join(current_line_text))
                current_line_text[:] = []

                indent_level = node.getIndent()
                if indent_level > 0:
                    indent_text = "  " * indent_level
                    token_obj = {
                        "t": indent_text,
                        "type": None,
                        "line": line_idx + 1,
                        "addr": None,
                        "pcode_time": None,
                        "seq": None,
                    }
                    current_line_text.append(indent_text)
                    c_tokens.append(token_obj)
                    token_counter += 1
                return

            token_text = node.getText() if hasattr(node, "getText") else None
            if not token_text:
                return

            current_line_text.append(token_text)
            addr = node.getMinAddress()
            hex_addr = str(addr).split(":")[-1] if addr else None

            pcode_time = None
            seq_str = None
            p_op = node.getPcodeOp() if hasattr(node, "getPcodeOp") else None
            if p_op:
                seq_num = p_op.getSeqnum()
                pcode_time = seq_num.getTime()
                seq_str = seq_num.toString()
                seq_to_token_idx.setdefault(seq_str, []).append(token_counter)

            target_addr = None
            target_name = None
            is_external = False

            if clazz == "ClangFuncNameToken" and program:
                try:
                    from ghidra.app.decompiler import DecompilerUtils

                    called_func = DecompilerUtils.getFunction(program, node)
                    if called_func:
                        target_name = called_func.getName()
                        is_external = called_func.isExternal() or called_func.isThunk()
                        target_entry = called_func.getEntryPoint()
                        if target_entry:
                            target_addr = str(target_entry).split(":")[-1]
                except Exception:
                    pass

            token_obj = {
                "t": token_text,
                "type": self.get_token_type(clazz),
                "line": line_idx,
                "addr": hex_addr,
                "pcode_time": pcode_time,
                "seq": seq_str,
            }
            if target_addr:
                token_obj["target_addr"] = target_addr
            if target_name:
                token_obj["target_name"] = target_name
            if is_external:
                token_obj["is_external"] = is_external

            c_tokens.append(token_obj)
            line_to_token_idx.setdefault(line_idx, []).append(token_counter)

            if hex_addr:
                if hex_addr not in addr_to_line:
                    addr_to_line[hex_addr] = set()
                addr_to_line[hex_addr].add(line_idx)
                addr_to_token_idx.setdefault(hex_addr, []).append(token_counter)
                l_addr = line_to_addr.setdefault(line_idx, [])
                if hex_addr not in l_addr:
                    l_addr.append(hex_addr)

            token_counter += 1

        walk_tokens(markup)
        if current_line_text:
            c_lines.append("".join(current_line_text))

        addr_to_line = {k: list(v) for k, v in addr_to_line.items()}
        return (
            c_lines,
            c_tokens,
            addr_to_line,
            addr_to_token_idx,
            line_to_token_idx,
            line_to_addr,
            seq_to_token_idx,
        )

    def extract_bsim_features(
        self,
        decomp_results,
        decomp_interface,
        func,
        monitor,
        language,
        addr_to_line,
        addr_to_token_idx,
        seq_to_token_idx,
    ):
        from ghidra.app.decompiler.signature import (
            VarnodeSignature,
            CopySignature,
            BlockSignature,
        )

        times = {"pcode": 0.0, "sigs": 0.0, "loop": 0.0}
        bsim_meta = []
        bsim_raw = []

        hfunction = decomp_results.getHighFunction()
        if not hfunction:
            return bsim_meta, bsim_raw, [], times

        ts_pcode = time.time()
        seq_to_pcode = {}
        seq_to_pcode_full = {}
        addr_to_pcodes = {}
        block_addr_to_pcode_dump = {}
        addr_to_block_start = {}

        for block in hfunction.getBasicBlocks():
            start_hex = str(block.getStart()).split(":")[-1]
            this_block_ops = {}
            op_iter = block.getIterator()
            while op_iter.hasNext():
                op = op_iter.next()
                s_num = op.getSeqnum()
                s_str = s_num.toString()
                op_str = op.toString()
                instr_hex = str(s_num.getTarget()).split(":")[-1]

                seq_to_pcode[s_num] = op
                seq_to_pcode_full[s_str] = op_str
                this_block_ops[s_str] = op_str

                if instr_hex not in addr_to_pcodes:
                    addr_to_pcodes[instr_hex] = {}
                addr_to_pcodes[instr_hex][s_str] = op_str
                addr_to_block_start[instr_hex] = start_hex

            block_addr_to_pcode_dump[start_hex] = this_block_ops
        times["pcode"] = time.time() - ts_pcode

        ts_sigs = time.time()
        signatures = decomp_interface.debugSignatures(func, 10, monitor)
        times["sigs"] = time.time() - ts_sigs

        if not signatures:
            return bsim_meta, bsim_raw, [], times

        ts_loop = time.time()
        for i in range(signatures.size()):
            sig = signatures.get(i)
            feature_hash = hex(sig.hash & 0xFFFFFFFF)[2:]

            feature_data = {
                "hash": feature_hash,
                "type": "UNKNOWN",
                "pcode_op": None,
                "previous_pcode_op": None,
                "previous_seq": None,
                "line_idx": [],
                "seq_to_token_idx": [],
                "addr_to_token_idx": [],
                "addr": None,
                "seq_time": None,
                "seq": None,
                "block_index": None,
                "pcode_block": [],
            }

            target_seq = None
            prev_target_seq = None
            hex_addr = None

            if isinstance(sig, VarnodeSignature):
                target_seq = sig.seqNum
                feature_data["type"] = "DATA_FLOW"
            elif sig.getClass().getSimpleName() == "CopySignature":
                feature_data["type"] = "COPY_SIG"
                feature_data["block_index"] = sig.index
            elif isinstance(sig, BlockSignature):
                feature_data["block_index"] = sig.index
                if not getattr(sig, "opSeq", None):
                    feature_data["type"] = "CONTROL_FLOW"
                    if hasattr(sig, "blockSeq") and sig.blockSeq:
                        hex_addr = str(sig.blockSeq).split(":")[-1]
                elif sig.previousOpSeq is None:
                    feature_data["type"] = "COMBINED"
                    target_seq = sig.opSeq
                else:
                    feature_data["type"] = "DUAL_FLOW"
                    target_seq = sig.opSeq
                    prev_target_seq = sig.previousOpSeq

            if target_seq:
                feature_data["seq"] = target_seq.toString()
                feature_data["seq_time"] = target_seq.getTime()
                p_op = seq_to_pcode.get(target_seq)
                if p_op:
                    feature_data["pcode_op"] = p_op.getMnemonic()
                    feature_data["pcode_op_full"] = p_op.toString()
                if prev_target_seq:
                    feature_data["previous_seq"] = prev_target_seq.toString()
                    prev_p_op = seq_to_pcode.get(prev_target_seq)
                    if prev_p_op:
                        feature_data["previous_pcode_op"] = prev_p_op.getMnemonic()
                hex_addr = str(target_seq.getTarget()).split(":")[-1]

            if hex_addr:
                feature_data["addr"] = hex_addr
                if feature_data["seq"] and feature_data["seq"] in seq_to_pcode_full:
                    feature_data["pcode_block"] = {
                        feature_data["seq"]: seq_to_pcode_full[feature_data["seq"]]
                    }
                elif hex_addr in addr_to_pcodes:
                    feature_data["pcode_block"] = addr_to_pcodes[hex_addr]
                elif hex_addr in addr_to_block_start:
                    parent_start = addr_to_block_start[hex_addr]
                    feature_data["pcode_block"] = block_addr_to_pcode_dump.get(
                        parent_start, {}
                    )

                feature_data["line_idx"] = addr_to_line.get(hex_addr, [])
                feature_data["seq_to_token_idx"] = seq_to_token_idx.get(
                    feature_data["seq"], []
                )
                feature_data["addr_to_token_idx"] = addr_to_token_idx.get(hex_addr, [])

            bsim_meta.append(feature_data)
            bsim_raw.append(feature_hash)

        times["loop"] = time.time() - ts_loop
        tf_counts = Counter(bsim_raw)
        sorted_tf = sorted(tf_counts.items(), key=lambda x: (-x[1], x[0]))
        bsim_tf = [{"hash": k, "tf": v} for k, v in sorted_tf]
        return bsim_meta, bsim_raw, bsim_tf, times

    def get_bsim_data(self, program, options=None):
        generator = self.stream_bsim_data(program, options, chunk_size=999999)
        file_metadata = next(generator)
        functions = []
        for chunk in generator:
            functions.extend(chunk)
        return {"file_metadata": file_metadata, "functions": functions}

    def _extract_fid_tags_for_function(
        self, func, program, fid_query_service=None, fid_service=None
    ):
        fid_tags = set()

        # 1. Parse Bookmarks generated by Ghidra's Function ID analyzer
        try:
            bm_mgr = program.getBookmarkManager()
            for bm in bm_mgr.getBookmarks(func.getEntryPoint()):
                bm_text = bm.getComment() or ""
                cat = bm.getCategory() or ""
                if "Function ID" in cat:
                    m = re.search(r"Library:\s*([^\r\n]+)", bm_text, re.IGNORECASE)
                    if m:
                        raw_val = m.group(1).strip()
                        parts = raw_val.split()
                        if parts:
                            lib_name = parts[0].strip("*/; \t")
                            lib_ver = None
                            if len(parts) > 1:
                                val_part = parts[1].strip("*/; \t")
                                if val_part.lower() not in (
                                    "binaries",
                                    "functions",
                                    "matches",
                                    "match",
                                ):
                                    lib_ver = val_part
                            if lib_name:
                                func_name = func.getName()
                                # A `FUN_` name is Ghidra's placeholder, not a
                                # library symbol, so it identifies nothing worth
                                # putting in the tag.
                                fid_tags.add(
                                    tag_taxonomy.origin_tag(
                                        "lib",
                                        lib_name,
                                        lib_ver,
                                        (
                                            None
                                            if func_name.startswith("FUN_")
                                            else func_name
                                        ),
                                    )
                                )
        except Exception:
            pass

        # 2. Query FidQueryService directly via hashes
        if fid_service and fid_query_service:
            try:
                # Apply Ghidra's default Instruction Count Threshold (10)
                instr_count = 0
                instr_iter = program.getListing().getInstructions(func.getBody(), True)
                while instr_iter.hasNext():
                    instr_iter.next()
                    instr_count += 1

                if instr_count < 10:
                    return list(fid_tags)

                hash_quad = fid_service.hashFunction(func)
                if hash_quad is not None:
                    records = list(
                        fid_query_service.findFunctionsByFullHash(
                            hash_quad.getFullHash()
                        )
                    )
                    if not records:
                        records = list(
                            fid_query_service.findFunctionsBySpecificHash(
                                hash_quad.getSpecificHash()
                            )
                        )

                    is_multiple_match = len(records) > 5
                    lib_to_names = {}
                    for r in records:
                        lib = fid_query_service.getLibraryForFunction(r)
                        if lib:
                            lib_name = lib.getLibraryFamilyName()
                            lib_ver = lib.getLibraryVersion()
                            if lib_name:
                                key = (lib_name, lib_ver)
                                if key not in lib_to_names:
                                    lib_to_names[key] = set()
                                lib_to_names[key].add(r.getName())

                    for (lib_name, lib_ver), names in lib_to_names.items():
                        func_name = (
                            "ambiguous"
                            if is_multiple_match or len(names) > 1
                            else list(names)[0]
                        )
                        fid_tags.add(
                            tag_taxonomy.origin_tag("lib", lib_name, lib_ver, func_name)
                        )
            except Exception as e:
                logging.debug(f"FID query failed for {func.getName()}: {e}")

        return list(fid_tags)

    def stream_bsim_data(
        self, program, options=None, chunk_size=100, job_service=None, job_id=None
    ):
        from ghidra.app.decompiler import DecompInterface, DecompileOptions
        from ghidra.util.task import ConsoleTaskMonitor

        options = options or {}
        monitor = ConsoleTaskMonitor()
        now_unix = int(time.time() * 1000)

        batch_uuid = options.get("batch_uuid")
        batch_name = options.get("batch_name", "Ghidra Batch")
        tags = options.get("tags", [])
        # `container:apk`, `packer:upx` & co. describe the upload, not the code:
        # copying them onto every function made them look like library evidence
        # and gave the binary-similarity tag split a `container:apk` category.
        func_scope_tags = [
            t for t in tags if not str(t).startswith(FILE_SCOPE_TAG_PREFIXES)
        ]
        related_md5 = options.get("related_md5", [])
        min_func_len = options.get("min_func_len", 10)
        batch_order = options.get("batch_order", 0)

        file_md5 = program.getExecutableMD5() or "00000000000000000000000000000000"
        file_name = program.getName()
        lang_id = str(program.getLanguageID())
        language = program.getLanguage()
        file_id = f"{file_md5}:#{file_md5}"

        # Extract PE/ELF/Mach-O metadata from Ghidra
        file_format = {}
        try:
            file_format["Executable Format"] = str(program.getExecutableFormat())
            if hasattr(program, "getMetadata"):
                meta_map = program.getMetadata()
                for key in meta_map.keySet():
                    file_format[str(key)] = str(meta_map.get(key))
        except Exception:
            pass

        file_metadata = {
            "entry_date": now_unix,
            "file_date": int(program.getCreationDate().getTime()),
            "file_md5": file_md5,
            "file_name": file_name,
            "batch_uuid": batch_uuid,
            "batch_name": batch_name,
            "batch_order": batch_order,
            "tags": tags,
            "related_md5": related_md5,
            "language_id": lang_id,
            "file_id": file_id,
            "file_format": file_format,
        }

        # Yield file_metadata first as the initial item
        yield file_metadata

        symbol_table = program.getSymbolTable()
        decomp_opts = DecompileOptions()
        decomp_interface = DecompInterface()
        decomp_interface.setOptions(decomp_opts)
        decomp_interface.setSignatureSettings(0x4D)

        if not decomp_interface.openProgram(program):
            logging.error(f"[-] Decompiler failed to initialize for {file_name}")
            return

        decompiler_id = f"{decomp_interface.getMajorVersion()}.{decomp_interface.getMinorVersion()}:{decomp_interface.getCompilerSpec().getLanguage()}:{hex(decomp_interface.getSignatureSettings())}"

        # Count total eligible functions first
        all_funcs = list(program.getFunctionManager().getFunctions(True))
        eligible_funcs = [
            f
            for f in all_funcs
            if not (f.isExternal() or f.isThunk())
            and f.getBody().getNumAddresses() >= min_func_len
        ]
        total_funcs = len(eligible_funcs)

        if job_service and job_id:
            job_service.add_log(
                job_id, f"Found {total_funcs} functions to decompile and analyze."
            )

        # Build full call graph in one reference-manager pass (avoids N×getCalledFunctions calls)
        func_manager = program.getFunctionManager()
        ref_manager = program.getReferenceManager()
        eligible_entries = {
            str(f.getEntryPoint()).split(":")[-1] for f in eligible_funcs
        }
        callees_graph = {
            e: [] for e in eligible_entries
        }  # entry_str -> [callee_info, ...]
        callers_graph = {
            e: [] for e in eligible_entries
        }  # entry_str -> [caller_info, ...]

        ref_iter = ref_manager.getReferenceIterator(program.getMinAddress())
        while ref_iter.hasNext():
            ref = ref_iter.next()
            if not ref.getReferenceType().isCall():
                continue

            caller_func = func_manager.getFunctionContaining(ref.getFromAddress())
            if caller_func is None:
                continue
            caller_entry = str(caller_func.getEntryPoint()).split(":")[-1]

            callee_func = func_manager.getFunctionAt(ref.getToAddress())
            if callee_func is not None:
                callee_entry = str(callee_func.getEntryPoint()).split(":")[-1]
                callee_info = {
                    "name": callee_func.getName(),
                    "entrypoint": callee_entry,
                    "is_external": callee_func.isExternal() or callee_func.isThunk(),
                }
                caller_info = {
                    "name": caller_func.getName(),
                    "entrypoint": caller_entry,
                    "is_external": caller_func.isExternal() or caller_func.isThunk(),
                }
                # Dedup using sets per entry
                if caller_entry in callees_graph:
                    callees_graph[caller_entry].append(callee_info)
                if callee_entry in callers_graph:
                    callers_graph[callee_entry].append(caller_info)
            else:
                # Unresolved destination — mark as external
                sym = program.getSymbolTable().getPrimarySymbol(ref.getToAddress())
                callee_name = sym.getName() if sym else str(ref.getToAddress())
                callee_info = {
                    "name": callee_name,
                    "entrypoint": None,
                    "is_external": True,
                }
                if caller_entry in callees_graph:
                    callees_graph[caller_entry].append(callee_info)

        # Dedup each list (multiple CALL sites to the same target within one function)
        def _dedup(lst, key_fn):
            seen = set()
            result = []
            for item in lst:
                k = key_fn(item)
                if k not in seen:
                    seen.add(k)
                    result.append(item)
            return result

        for entry in callees_graph:
            callees_graph[entry] = _dedup(
                callees_graph[entry],
                lambda x: (x["entrypoint"], x["name"], x["is_external"]),
            )
        for entry in callers_graph:
            callers_graph[entry] = _dedup(
                callers_graph[entry],
                lambda x: (x["entrypoint"], x["name"], x["is_external"]),
            )

        skip_function_id = bool(options.get("skip_function_id"))

        fid_service = None
        fid_query_service = None
        if not skip_function_id:
            try:
                from ghidra.feature.fid.service import FidService

                fid_service = FidService()
                fid_query_service = fid_service.openFidQueryService(language, False)
            except Exception as e:
                logging.debug(f"FID service unavailable: {e}")

        chunk = []
        decompiled_count = 0

        for func in eligible_funcs:
            decompiled_count += 1
            if (
                job_service
                and job_id
                and (decompiled_count % 10 == 0 or decompiled_count == total_funcs)
            ):
                pct = int(
                    (decompiled_count / max(1, total_funcs)) * 80
                )  # Reserve 80-100% for chunk streaming/indexing
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Decompiling and analyzing functions: {decompiled_count}/{total_funcs}",
                )

            func_name = func.getName()
            entry_point = func.getEntryPoint()
            entry_str = str(entry_point).split(":")[-1]
            full_id = f"{file_id}::{func_name}:@{entry_str}"
            call_conv = func.getCallingConventionName() or "unknown"
            return_type = func.getReturnType().getName()
            namespace = (
                func.getParentNamespace().getName(True)
                if not func.getParentNamespace().isGlobal()
                else ""
            )
            parameters = [p.getDataType().getName() for p in func.getParameters()]

            func_tags = list(func_scope_tags)
            fid_tags = (
                []
                if skip_function_id
                else self._extract_fid_tags_for_function(
                    func, program, fid_query_service, fid_service
                )
            )
            for ft in fid_tags:
                if ft not in func_tags:
                    func_tags.append(ft)

            # Insert Capa tags based on function address
            addr_hex = hex(func.getEntryPoint().getOffset())
            capa_tags = options.get("capa_tags", {})
            for ctag in capa_tags.get(addr_hex, []):
                if ctag not in func_tags:
                    func_tags.append(ctag)

            entry_symbols = symbol_table.getSymbols(entry_point)
            labels = [s.getName() for s in entry_symbols]
            if not labels:
                labels = [func.getName()]

            decomp_results = decomp_interface.decompileFunction(
                func, GHIDRA_DECOMP_MAX_TIMEOUT, monitor
            )
            (
                c_lines,
                c_tokens,
                addr_to_line,
                addr_to_token_idx,
                line_to_token_idx,
                line_to_addr,
                seq_to_token_idx,
            ) = ([], [], {}, {}, {}, {}, {})
            bsim_meta, bsim_raw, bsim_tf = [], [], []

            if decomp_results.decompileCompleted():
                # The listing signature above is the program DB one, which stays
                # "undefined FUN_xxx()" for every function with a DEFAULT signature
                # source. The decompiler's own prototype is what the C body was
                # printed from, so prefer it and keep the DB values as fallback.
                try:
                    proto = decomp_results.getHighFunction().getFunctionPrototype()
                    return_type = proto.getReturnType().getName()
                    parameters = [
                        proto.getParam(i).getDataType().getName()
                        for i in range(proto.getNumParams())
                    ]
                except Exception:
                    pass

                markup = decomp_results.getCCodeMarkup()
                (
                    c_lines,
                    c_tokens,
                    addr_to_line,
                    addr_to_token_idx,
                    line_to_token_idx,
                    line_to_addr,
                    seq_to_token_idx,
                ) = self.build_semantic_source(markup, program)
                bsim_meta, bsim_raw, bsim_tf, ext_times = self.extract_bsim_features(
                    decomp_results,
                    decomp_interface,
                    func,
                    monitor,
                    language,
                    addr_to_line,
                    addr_to_token_idx,
                    seq_to_token_idx,
                )

            callees_list = callees_graph.get(entry_str, [])
            callers_list = callers_graph.get(entry_str, [])

            chunk.append(
                {
                    "function_metadata": {
                        "type": "function",
                        "function_name": func_name,
                        "calling_convention": call_conv,
                        "decompiler_id": decompiler_id,
                        "entry_date": now_unix,
                        "file_date": file_metadata["file_date"],
                        "file_md5": file_md5,
                        "file_name": file_name,
                        "full_id": full_id,
                        "batch_uuid": batch_uuid,
                        "batch_name": batch_name,
                        "tags": func_tags,
                        "instruction_count": func.getBody().getNumAddresses(),
                        "is_thunk": func.isThunk(),
                        "labels": labels,
                        "language_id": lang_id,
                        "return_type": return_type,
                        "namespace": namespace,
                        "parameters": parameters,
                        "entrypoint_address": entry_str,
                        "function_id_hash": self._function_id_hash(func, program),
                        "bsim_features_count": len(bsim_raw),
                        "bsim_unique_features_count": len(bsim_tf),
                        "callees": callees_list,
                        "callers": callers_list,
                    },
                    "function_source": {
                        "c_lines": c_lines,
                        "c_tokens": c_tokens,
                        "addr_to_line": addr_to_line,
                        "addr_to_token": addr_to_token_idx,
                        "seq_to_token": seq_to_token_idx,
                        "line_to_token": line_to_token_idx,
                        "line_to_addr": line_to_addr,
                    },
                    "function_features": {
                        "bsim_features_meta": bsim_meta,
                        "bsim_features_raw": bsim_raw,
                        "bsim_features_tf": bsim_tf,
                        "bsim_features_count": len(bsim_raw),
                        "bsim_unique_features_count": len(bsim_tf),
                    },
                }
            )

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

        if fid_query_service:
            try:
                fid_query_service.close()
            except Exception:
                pass

        decomp_interface.dispose()

    def run_profile_analysis(
        self, program, profile_name, force_reanalysis=False, disable_function_id=False
    ):
        from ghidra.app.plugin.core.analysis import AutoAnalysisManager
        from ghidra.util.task import ConsoleTaskMonitor

        # Initialize manager first to populate options
        mgr = AutoAnalysisManager.getAnalysisManager(program)
        monitor = ConsoleTaskMonitor()

        profile = self.config.get("profiles", {}).get(profile_name)
        if not profile:
            logging.warning(
                f"Profile '{profile_name}' not found. Using default analysis."
            )
            tx_id = program.startTransaction("Default Analysis")
            try:
                if disable_function_id:
                    options = program.getOptions("Analyzers")
                    if options.contains("Function ID"):
                        options.setBoolean("Function ID", False)
                if force_reanalysis:
                    mgr.reAnalyzeAll(None)
                mgr.startAnalysis(monitor)
            finally:
                program.endTransaction(tx_id, True)
            mgr.waitForAnalysis(None, monitor)
            return

        if profile.get("no_analysis", False):
            logging.info(f"Profile '{profile_name}' active: Skipping auto-analysis.")
            return

        logging.info(f"Applying Profile: {profile_name}")

        tx_id = program.startTransaction(f"Apply Profile: {profile_name}")
        try:
            options = program.getOptions("Analyzers")
            analyzer_settings = profile.get("analyzers", {})

            for name, enabled in analyzer_settings.items():
                if options.contains(name):
                    options.setBoolean(name, enabled)
                else:
                    logging.warning(f"Analyzer '{name}' not found.")

            if disable_function_id:
                if options.contains("Function ID"):
                    options.setBoolean("Function ID", False)
            elif options.contains("Function ID.Always Apply FID Labels"):
                options.setBoolean("Function ID.Always Apply FID Labels", True)

            if force_reanalysis:
                mgr.reAnalyzeAll(None)
            mgr.startAnalysis(monitor)
        finally:
            program.endTransaction(tx_id, True)

        mgr.waitForAnalysis(None, monitor)

    def analyze_file(self, file_path, options=None):
        from ghidra.base.project import GhidraProject

        options = options or {}
        target_path = Path(file_path).resolve()

        with tempfile.TemporaryDirectory(prefix="bsim_") as temp_dir:
            project = GhidraProject.createProject(temp_dir, "TempGhidraProject", False)
            try:
                if options.get("processor"):
                    from ghidra.program.model.lang import LanguageID, CompilerSpecID
                    from ghidra.program.util import DefaultLanguageService

                    lang_service = DefaultLanguageService.getLanguageService()
                    lang_id = LanguageID(options.get("processor"))
                    lang = lang_service.getLanguage(lang_id)
                    if options.get("cspec"):
                        cspec_id = CompilerSpecID(options.get("cspec"))
                        cspec = lang.getCompilerSpecByID(cspec_id)
                    else:
                        cspec = lang.getDefaultCompilerSpec()
                    logging.info(
                        f"[i] Importing {target_path.name} with forced language: {lang_id}"
                    )
                    program = project.importProgram(target_path, lang, cspec)
                else:
                    program = project.importProgram(target_path)

                self.run_profile_analysis(
                    program, options.get("profile", "fast"), force_reanalysis=True
                )
                data = self.get_bsim_data(program, options)
                return data
            except Exception as e:
                logging.error(f"[!] Analysis failed for file : {target_path.name}: {e}")
                raise
            finally:
                # close() releases every program importProgram() registered, ends
                # their transactions, and disposes the project's LocalFileSystem --
                # which is what stops its "File System Listener" thread. Do not
                # release the program first: that consumer is already gone by then
                # and close() would throw "unknown consumer". See worker.py.
                project.close()

    def analyze_project(self, project_path, options=None):
        from ghidra.base.project import GhidraProject

        options = options or {}
        target_path = Path(project_path).resolve()

        project = GhidraProject.openProject(target_path.parent, target_path.stem)
        all_data = []
        try:
            root_folder = project.getProjectData().getRootFolder()
            files = root_folder.getFiles()
            for file in files:
                from ghidra.util.task import ConsoleTaskMonitor

                program = file.getDomainObject(
                    project, True, False, ConsoleTaskMonitor()
                )
                try:
                    self.run_profile_analysis(
                        program, options.get("profile", "fast"), force_reanalysis=False
                    )
                    data = self.get_bsim_data(program, options)
                    all_data.append(data)
                finally:
                    if program:
                        program.release(project)
            return all_data
        except Exception as e:
            logging.error(f"[!] Analysis failed for project : {target_path.name}: {e}")
            raise
        finally:
            project.close()


ghidra_service = GhidraService()
