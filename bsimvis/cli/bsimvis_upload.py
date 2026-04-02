import redis

import tomllib, json, uuid

import pyghidra
from pyghidra.launcher import PyGhidraLauncher
from pyghidra.launcher import HeadlessPyGhidraLauncher

import time, logging, argparse, os, tempfile
from pathlib import Path
from collections import Counter
import concurrent.futures, threading

from tqdm import tqdm
from bsimvis.app.services.redis_client import get_redis, init_redis
from bsimvis.app.services.index_service import save_file, save_function

DEFAULT_CONFIG_NAME = "bsimvis_config.toml"
DEFAULT_BATCH_NAME = "Ghidra Batch"
DEFAULT_GHIDRA_PROJECT_NAME = "TempGhidraProject"
import time
import uuid
import datetime
import logging

GHIDRA_DECOMP_MAX_TIMEOUT = 10


def upload_bsim_data(data, args, config):
    if not data or not data.get("functions"):
        return

    # 1. Grab file-level metadata
    file_meta = data.get("file_metadata", {})
    file_md5 = file_meta.get("file_md5", "unknown_md5")

    functions_data = data.get("functions", [])

    num_files = 1
    num_functions = len(functions_data)
    timestamp = int(time.time() * 1000)
    batch_uuid = file_meta.get("batch_uuid", "unknown_batch_uuid")
    batch_name = file_meta.get("batch_name", "unknown_batch_name")

    for h, host in enumerate(args.hosts):
        dest = host.split(":")[0]
        port = int(host.split(":")[1]) if ":" in host else 6379
        try:
            # Connect to Redis
            r = redis.Redis(host=dest, port=port, decode_responses=True)
            pipe = r.pipeline()

            # --- Global Batch Metadata ---
            pipe.sadd("global:batches", batch_uuid)
            global_batch_key = f"global:batch:{batch_uuid}"
            initial_global_batch = {
                "name": batch_name,
                "batch_uuid": batch_uuid,
                "batch_id": global_batch_key,
                "created_at": timestamp,
                "last_updated": timestamp,
                "collections": {},
            }
            if not r.exists(global_batch_key):
                r.json().set(global_batch_key, "$", initial_global_batch)
            pipe.json().set(global_batch_key, '$["last_updated"]', timestamp)

            for c, collection in enumerate(args.collections):
                pipe.sadd("global:collections", collection)
                pipe.json().set(
                    global_batch_key, f'$["collections"]["{collection}"]', True
                )

                # --- Collection Stats ---
                coll_meta_key = f"global:collection:{collection}:meta"
                pipe.hincrby(coll_meta_key, "total_files", num_files)
                pipe.hincrby(coll_meta_key, "total_functions", num_functions)
                pipe.hset(coll_meta_key, "last_updated", timestamp)

                # --- Collection Batch Metadata ---
                batch_key = f"{collection}:batch:{batch_uuid}"
                initial_batch_data = {
                    "name": batch_name,
                    "batch_uuid": batch_uuid,
                    "batch_id": batch_key,
                    "created_at": timestamp,
                    "last_updated": timestamp,
                    "total_files": 0,
                    "total_functions": 0,
                    "collection": collection,
                }
                if not r.exists(batch_key):
                    r.json().set(batch_key, "$", initial_batch_data)
                pipe.json().numincrby(batch_key, '$["total_files"]', num_files)
                pipe.json().numincrby(batch_key, '$["total_functions"]', num_functions)
                pipe.json().set(batch_key, '$["last_updated"]', timestamp)
                # Keep fields as requested
                pipe.json().set(batch_key, f'$["collections"]["{collection}"]', True)

                # --- 1. File Metadata ---
                file_meta_key = f"{collection}:file:{file_md5}:meta"
                coll_file_meta = dict(file_meta)
                coll_file_meta["collection"] = collection
                coll_file_meta["type"] = "file"
                coll_file_meta["file_id"] = f"{collection}:file:{file_md5}"
                pipe.json().set(file_meta_key, "$", coll_file_meta)
                # Populate secondary index
                save_file(pipe, collection, file_md5, coll_file_meta)

                # --- 2. Function Data ---
                for func_data in functions_data:
                    func_meta = dict(func_data.get("function_metadata", {}))
                    func_meta["collection"] = collection
                    func_features = func_data.get("function_features", {})
                    func_source = func_data.get("function_source", {})

                    full_id = func_meta.get("full_id", "")
                    addr = (
                        full_id.split(":@")[-1] if ":@" in full_id else "unknown_addr"
                    )

                    base_func_key = f"{collection}:function:{file_md5}:{addr}"
                    func_meta["function_id"] = base_func_key

                    pipe.json().set(f"{base_func_key}:meta", "$", func_meta)
                    pipe.json().set(f"{base_func_key}:source", "$", func_source)
                    # Populate secondary index
                    save_function(pipe, collection, file_md5, addr, func_meta)

                    vec_meta = func_features.get("bsim_features_meta", [])
                    pipe.json().set(f"{base_func_key}:vec:meta", "$", vec_meta)

                    vec_raw = func_features.get("bsim_features_raw", [])
                    pipe.json().set(f"{base_func_key}:vec:raw", "$", vec_raw)
                    # Add to batch-to-functions mapping SET for instant status reporting
                    pipe.sadd(
                        f"{collection}:batch:{batch_uuid}:functions", base_func_key
                    )

                    vec_tf_list = func_features.get("bsim_features_tf", [])
                    if vec_tf_list:
                        zset_mapping = {
                            item["hash"]: item["tf"] for item in vec_tf_list
                        }
                        pipe.zadd(f"{base_func_key}:vec:tf", zset_mapping)

            # Execute the entire pipeline batch at once
            t_pipe_start = time.time()
            results = pipe.execute()
            t_pipe_end = time.time()
            logging.info(
                f"[+] Pushed {len(results)} keys to Redis host {host} in {t_pipe_end - t_pipe_start:.3f}s"
            )

        except Exception as e:
            logging.error(f"[!] Failed to upload to Redis host {host}: {e}")


def get_token_type(clazz):
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


def build_semantic_source(markup):
    c_lines = []
    c_tokens = []
    addr_to_line = {}
    addr_to_token_idx = {}
    line_to_token_idx = {}
    line_to_addr = {}
    # New mapping for precise feature-to-token alignment
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

        # Handle Newlines
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

        # Handle Tokens
        token_text = node.getText() if hasattr(node, "getText") else None
        if not token_text:
            return

        current_line_text.append(token_text)

        addr = node.getMinAddress()
        hex_addr = str(addr).split(":")[-1] if addr else None

        # --- PRECISE SEQUENCE LOGIC ---
        pcode_time = None
        seq_str = None

        # Check if this token is linked to a specific P-Code Operation
        p_op = node.getPcodeOp() if hasattr(node, "getPcodeOp") else None
        if p_op:
            seq_num = p_op.getSeqnum()
            pcode_time = seq_num.getTime()
            seq_str = seq_num.toString()

            # Map the sequence string to this specific token counter
            seq_to_token_idx.setdefault(seq_str, []).append(token_counter)

        token_obj = {
            "t": token_text,
            "type": get_token_type(clazz),
            "line": line_idx,
            "addr": hex_addr,
            "pcode_time": pcode_time,
            "seq": seq_str,
        }
        c_tokens.append(token_obj)

        # Structural mappings
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

    # Handle the final line if it didn't end with a ClangBreak
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
    decomp_results,
    decomp_interface,
    func,
    monitor,
    language,
    addr_to_line,
    addr_to_token_idx,
    seq_to_token_idx,
):
    import time
    from ghidra.app.decompiler.signature import (
        VarnodeSignature,
        CopySignature,
        BlockSignature,
    )

    times = {"pcode": 0.0, "sigs": 0.0, "loop": 0.0}

    bsim_meta = []
    bsim_raw = []

    # Get the tokens from the caller's context (we know they are in decomp_results markup)
    # But since we can't change the signature, we'll build a quick local map of P-Code to tokens
    # by looking at the C-markup again via the decomp_results
    markup = decomp_results.getCCodeMarkup()
    pcode_to_tidx = {}

    # We need the tokens we just built in get_bsim_data.
    # Since we can't pass them in, we recreate a local seq_num map from the HighFunction
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
    # Assuming signatures is a List<SignatureRecord>
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
        # --- Java Logic Port ---

        # 1. VarnodeSignature (DATA_FLOW)
        if isinstance(sig, VarnodeSignature):
            target_seq = sig.seqNum
            feature_data["type"] = "DATA_FLOW"
            # Note: Java also handles sig.vn here if needed

        # 2. CopySignature (COPY_SIG) - Checking by class name if type not imported
        elif sig.getClass().getSimpleName() == "CopySignature":
            feature_data["type"] = "COPY_SIG"
            feature_data["block_index"] = sig.index
            # Java creates a dummy sequence at the start of the block
            # basicBlockStart = hfunction.getBasicBlocks().get(sig.index).getStart()
            # In Python, we'll wait to resolve the address via the block index if needed

        # 3. BlockSignature (CONTROL_FLOW, COMBINED, or DUAL_FLOW)
        elif isinstance(sig, BlockSignature):
            feature_data["block_index"] = sig.index

            if not getattr(sig, "opSeq", None):
                # Pure control-flow feature
                feature_data["type"] = "CONTROL_FLOW"
                # Java: seq = new SequenceNumber(sig.blockSeq, 0)
                # We'll use the blockSeq (Address) as the target
                if hasattr(sig, "blockSeq") and sig.blockSeq:
                    hex_addr = str(sig.blockSeq).split(":")[-1]

            elif sig.previousOpSeq is None:
                # First root op mixed with control-flow
                feature_data["type"] = "COMBINED"
                target_seq = sig.opSeq

            else:
                # Two consecutive root ops mixed together
                feature_data["type"] = "DUAL_FLOW"
                target_seq = sig.opSeq
                prev_target_seq = sig.previousOpSeq

        # --- Data Extraction & Mapping ---

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

            # Tiered fallback for pcode-block (Seq -> Addr -> Block)
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

            # Map Previous P-Code (for DUAL_FLOW)

            # Map UI/Token indices
            feature_data["line_idx"] = addr_to_line.get(hex_addr, [])
            feature_data["seq_to_token_idx"] = seq_to_token_idx.get(
                feature_data["seq"], []
            )
            feature_data["addr_to_token_idx"] = addr_to_token_idx.get(hex_addr, [])

            if feature_data["seq"]:
                feature_data["seq_to_token_idx"] = seq_to_token_idx.get(
                    feature_data["seq"], []
                )

        bsim_meta.append(feature_data)
        bsim_raw.append(feature_hash)

    times["loop"] = time.time() - ts_loop

    # Finalize TF (Term Frequency)
    tf_counts = Counter(bsim_raw)
    sorted_tf = sorted(tf_counts.items(), key=lambda x: (-x[1], x[0]))
    bsim_tf = [{"hash": k, "tf": v} for k, v in sorted_tf]

    return bsim_meta, bsim_raw, bsim_tf, times


def get_bsim_data(program, args, config, batch_order):
    import java.lang.StringBuffer

    from ghidra.app.decompiler import DecompInterface, DecompileOptions
    from ghidra.util.task import ConsoleTaskMonitor
    import uuid, logging

    monitor = ConsoleTaskMonitor()
    now_unix = int(time.time() * 1000)

    batch_uuid = args.batch_uuid
    batch_name = args.batch_name
    tags = args.tags

    file_md5 = program.getExecutableMD5() or "00000000000000000000000000000000"
    file_name = program.getName()
    lang_id = str(program.getLanguageID())
    language = program.getLanguage()
    file_id = f"{file_md5}:#{file_md5}"

    file_metadata = {
        "entry_date": now_unix,
        "file_date": int(program.getCreationDate().getTime()),
        "file_md5": file_md5,
        "file_name": file_name,
        "batch_uuid": batch_uuid,
        "batch_name": batch_name,
        "batch_order": batch_order,
        "tags": tags,
        "language_id": lang_id,
        "file_id": file_id,
    }

    symbol_table = program.getSymbolTable()

    # Setup Decompiler
    decomp_opts = DecompileOptions()
    decomp_interface = DecompInterface()
    decomp_interface.setOptions(decomp_opts)

    # 0x4d sets the BSim feature generation flags
    decomp_interface.setSignatureSettings(0x4D)

    if not decomp_interface.openProgram(program):
        logging.error(f"[-] Decompiler failed to initialize for {file_name}")
        return {}

    decompiler_id = f"{decomp_interface.getMajorVersion()}.{decomp_interface.getMinorVersion()}:{decomp_interface.getCompilerSpec().getLanguage()}:{hex(decomp_interface.getSignatureSettings())}"

    functions = program.getFunctionManager().getFunctions(True)
    all_function_data = []

    total_decomp_time = 0.0
    total_semantic_time = 0.0
    total_extract_time = 0.0
    total_ext_pcode_time = 0.0
    total_ext_sigs_time = 0.0
    total_ext_loop_time = 0.0
    total_json_time = 0.0

    for func in functions:
        if func.isExternal() or func.isThunk():
            continue

        func_name = func.getName()
        entry_point = func.getEntryPoint()
        entry_str = str(entry_point).split(":")[-1]
        full_id = f"{file_id}::{func_name}:@{entry_str}"

        call_conv = func.getCallingConventionName() or "unknown"
        return_type = func.getReturnType().getName()

        entry_symbols = symbol_table.getSymbols(entry_point)
        labels = [s.getName() for s in entry_symbols]

        # Ensure the primary function name is included even if it's the only one
        if not labels:
            labels = [func.getName()]

        # ---------------------------------------------------------
        # BSim Feature Extraction via debugSignatures
        # ---------------------------------------------------------
        bsim_meta = {}
        bsim_raw = []

        # Generate the signatures (10 second timeout per function)
        t0 = time.time()
        decomp_results = decomp_interface.decompileFunction(
            func, GHIDRA_DECOMP_MAX_TIMEOUT, monitor
        )
        total_decomp_time += time.time() - t0

        t1 = time.time()
        if decomp_results.decompileCompleted():
            markup = decomp_results.getCCodeMarkup()

            t_sem = time.time()
            (
                c_lines,
                c_tokens,
                addr_to_line,
                addr_to_token_idx,
                line_to_token_idx,
                line_to_addr,
                seq_to_token_idx,
            ) = build_semantic_source(markup)
            total_semantic_time += time.time() - t_sem

            t_ext = time.time()
            bsim_meta, bsim_raw, bsim_tf, ext_times = extract_bsim_features(
                decomp_results,
                decomp_interface,
                func,
                monitor,
                language,
                addr_to_line,
                addr_to_token_idx,
                seq_to_token_idx,
            )
            total_extract_time += time.time() - t_ext
            total_ext_pcode_time += ext_times["pcode"]
            total_ext_sigs_time += ext_times["sigs"]
            total_ext_loop_time += ext_times["loop"]

        t2 = time.time()
        func_meta = {
            "type": "function",
            "function_name": func.getName(),
            "calling_convention": call_conv,
            "decompiler_id": decompiler_id,
            "entry_date": now_unix,
            "file_date": file_metadata["file_date"],
            "file_md5": file_md5,
            "file_name": file_name,
            "full_id": full_id,
            "batch_uuid": batch_uuid,
            "batch_name": batch_name,
            "tags": tags,
            "instruction_count": func.getBody().getNumAddresses(),
            "is_thunk": func.isThunk(),
            "labels": labels,
            "language_id": lang_id,
            "return_type": return_type,
            "entrypoint_address": entry_str,
            "bsim_features_count": len(bsim_raw),
            "bsim_unique_features_count": len(bsim_tf),
        }

        bsim_features = {
            "bsim_features_meta": bsim_meta,
            "bsim_features_raw": bsim_raw,
            "bsim_features_tf": bsim_tf,
            "bsim_features_count": len(bsim_raw),
            "bsim_unique_features_count": len(bsim_tf),
        }

        func_source = {
            "c_lines": c_lines,
            "c_tokens": c_tokens,
            "addr_to_line": addr_to_line,  # {addr: line_idx}
            "addr_to_token": addr_to_token_idx,  # {addr: [token_idxs]}
            "seq_to_token": seq_to_token_idx,  # {addr: [token_idxs]}
            "line_to_token": line_to_token_idx,
            "line_to_addr": line_to_addr,
        }

        all_function_data.append(
            {
                "function_metadata": func_meta,
                "function_source": func_source,
                "function_features": bsim_features,
            }
        )
        total_json_time += time.time() - t2

        if len(bsim_raw) != len(bsim_meta):
            logging.warning("Non matching features between bsim_meta and bsim_raw")

    decomp_interface.dispose()

    logging.info(
        f"[i] {file_name} - Decomp: {total_decomp_time:.3f}s | "
        f"Semantic: {total_semantic_time:.3f}s | "
        f"Extract Total: {total_extract_time:.3f}s "
        f"(Pcode: {total_ext_pcode_time:.3f}s | Sigs: {total_ext_sigs_time:.3f}s | Loop: {total_ext_loop_time:.3f}s) | "
        f"JSON: {total_json_time:.3f}s"
    )

    return {"file_metadata": file_metadata, "functions": all_function_data}


def run_profile_analysis(program, profile_name, config):
    """Configures and runs Ghidra analyzers based on TOML profile."""
    from ghidra.app.plugin.core.analysis import AutoAnalysisManager
    from ghidra.util.task import ConsoleTaskMonitor

    profile = config.get("profiles", {}).get(profile_name)
    if not profile:
        logging.error(f"Profile '{profile_name}' not found. Using defaults.")
        raise Exception(f"Profile '{profile_name}' not found. Using defaults.")

    if profile.get("no_analysis", False):
        logging.info(f"Profile '{profile_name}' active: Skipping auto-analysis.")
        return

    logging.info(f"Applying Profile: {profile_name}")

    options = program.getOptions("Analyzers")
    analyzer_settings = profile.get("analyzers", {})

    for name, enabled in analyzer_settings.items():
        if options.contains(name):
            options.setBoolean(name, enabled)
            logging.debug(f"Analyzer '{name}' enabled : {enabled}")
        else:
            logging.warning(f"Analyzer '{name}' not found.")

    mgr = AutoAnalysisManager.getAnalysisManager(program)
    mgr.reAnalyzeAll(None)
    mgr.startAnalysis(ConsoleTaskMonitor())


def process_target(target, args, config, batch_order) -> int:

    from ghidra.base.project import GhidraProject
    from ghidra.util.exception import NotFoundException
    from java.io import IOException
    from ghidra.app.plugin.core.analysis import PdbAnalyzer
    from ghidra.app.plugin.core.analysis import PdbUniversalAnalyzer

    target_path = Path(target).resolve()

    # CASE 1: Existing Ghidra Project
    if target_path.suffix == ".gpr":
        project = GhidraProject.openProject(target_path.parent, target_path.stem)

        try:
            t0 = time.time()
            root_folder = project.getProjectData().getRootFolder()
            files = root_folder.getFiles()
            for file in tqdm(
                files, desc=f"Proj: {target_path.stem}", unit="bin", leave=False
            ):
                # getImmutableDomainObject(Object consumer, int version, TaskMonitor monitor)
                program = file.getImmutableDomainObject(project, -1, None)
                try:
                    run_profile_analysis(program, args.profile, config)

                    t_analysis = time.time()
                    data = get_bsim_data(program, args, config, batch_order)

                    t_get = time.time()
                    upload_bsim_data(data, args, config)

                    t_upload = time.time()
                    logging.info(
                        f"[+] Job {batch_order} finished for project file : {file.getName()} in {t_upload - t0:.3f}s (Analysis: {t_analysis - t0:.3f}s, Data: {t_get - t_analysis:.3f}s, Upload: {t_upload - t_get:.3f}s)"
                    )
                finally:
                    if program:
                        program.release(project)
            return 1
        except Exception as e:
            logging.error(
                f"[!] Job {batch_order} failed for project : {target_path.name}: {e}"
            )
            return 0
        finally:
            project.close()

    # CASE 2: Raw Binary (ELF, PE, Mach-O, etc.)
    else:
        with tempfile.TemporaryDirectory(prefix="bsim_") as temp_dir:

            project = GhidraProject.createProject(
                temp_dir, DEFAULT_GHIDRA_PROJECT_NAME, False
            )

            try:
                t0 = time.time()
                if args.processor:
                    from ghidra.program.model.lang import LanguageID, CompilerSpecID
                    from ghidra.program.util import DefaultLanguageService

                    lang_service = DefaultLanguageService.getLanguageService()
                    lang_id = LanguageID(args.processor)
                    lang = lang_service.getLanguage(lang_id)

                    if args.cspec:
                        cspec_id = CompilerSpecID(args.cspec)
                        cspec = lang.getCompilerSpecByID(cspec_id)
                    else:
                        cspec = lang.getDefaultCompilerSpec()

                    logging.info(f"[i] Importing {target_path.name} with forced language: {lang_id}")
                    program = project.importProgram(target_path, lang, cspec)
                else:
                    program = project.importProgram(target_path, readOnly=True)

                run_profile_analysis(program, args.profile, config)

                t_analysis = time.time()
                data = get_bsim_data(program, args, config, batch_order)

                t_get = time.time()
                upload_bsim_data(data, args, config)

                t_upload = time.time()
                logging.info(
                    f"[+] Job {batch_order} finished for file : {target_path.name} in {t_upload - t0:.3f}s (Analysis: {t_analysis - t0:.3f}s, Data: {t_get - t_analysis:.3f}s, Upload: {t_upload - t_get:.3f}s)"
                )
                return 1

            except Exception as e:
                logging.error(
                    f"[!] Job {batch_order} failed for file : {target_path.name}: {e}"
                )
                return 0
            finally:
                if "program" in locals() and program:
                    program.release(project)


def worker(target, args, config, batch_order):
    """Thread entry point."""
    logging.info(f"[+] Job {batch_order} started for {target}")
    result = process_target(target, args, config, batch_order)

    return result


def run_upload(host, port, args):
    if host:
        init_redis(host, port)

    # Ensure hosts list exists
    if not hasattr(args, "hosts") or not args.hosts:
        if host:
            args.hosts = [f"{host}:{port}"]
        else:
            args.hosts = ["localhost:6666"]

    if args.verbose == 0:
        level = logging.WARNING
    elif args.verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logging.basicConfig(level=level, force=True)

    # Map back to what main(args) expects
    main(args)


def main(args):

    print(f"[i] Starting Ghidra JVM")
    launcher = HeadlessPyGhidraLauncher(verbose=args.verbose_analysis)

    launcher.add_vmargs(f"-XX:MaxRAMPercentage={args.max_ram_percent}")

    if args.print_flags:
        launcher.add_vmargs("-XX:+PrintFlagsFinal")

    if args.jvm_args:
        for jvm_arg in args.jvm_args:
            logging.info("Adding JVM arg {jvm_arg}")
            launcher.add_vmargs(jvm_arg)

    launcher.start()

    logging.info(f"[i] Loading config {args.config}")
    config = load_config(args.config)

    if len(args.collections) == 0:
        args.collections = ["main"]

    if not args.batch_uuid:
        args.batch_uuid = str(uuid.uuid4())

    logging.info(f"[i] Processing targets using profile: {args.profile}")
    print(
        f"[i] Uploading to collections {args.collections} on hosts {args.hosts} with batch uuid {args.batch_uuid}"
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_target = {
            executor.submit(worker, target, args, config, batch_order): target
            for batch_order, target in enumerate(args.targets)
        }

        success_count = 0
        total = len(args.targets)

        # Progress bar setup
        # unit="bin" makes it say "10bin/s"
        with tqdm(
            total=total, desc="Analyzing", unit="bin", dynamic_ncols=True
        ) as pbar:
            for future in concurrent.futures.as_completed(future_to_target):
                target_name = future_to_target[future]
                try:
                    result = future.result()
                    if result == 1:
                        success_count += 1
                except Exception as e:
                    # tqdm.write ensures the progress bar stays at the bottom
                    # while the error message is printed above it
                    pbar.write(f"[!] Exception in job for {target_name}: {e}")

                pbar.update(1)

        rate = (success_count / total * 100) if total > 0 else 0
        print(f"[i] Success rate : {rate:.2f}% ({success_count}/{total})")


def load_config(path=DEFAULT_CONFIG_NAME):
    with open(path, "rb") as f:
        return tomllib.load(f)


def cli_main():
    start = time.time()

    parser = argparse.ArgumentParser(prog="BSimVis", description="...", epilog="...")

    parser.add_argument(
        "targets",
        nargs="+",
        help="Path to Ghidra project (.gpr), a specific binary, or a directory/*",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        default=0,
        help="Increase output verbosity (e.g., -v, -vv, -vvv)",
        action="count",
    )

    parser.add_argument(
        "-H",
        "--host",
        dest="hosts",
        action="append",
        metavar="HOST",
        required=True,
        default=[],
        help="Host address (can be specified multiple times)",
    )

    parser.add_argument(
        "-n",
        "--threads",
        type=int,
        default=1,
        help="Number of threads to use (default: 1)",
    )

    parser.add_argument(
        "-t",
        "--tag",
        dest="tags",
        action="append",
        metavar="TAG",
        default=[],
        help="Tag to filter by (can be specified multiple times)",
    )

    parser.add_argument(
        "-c",
        "--collection",
        dest="collections",
        action="append",
        metavar="NAME",
        default=[],
        help="Collections to include (default: 'main' if none provided)",
    )

    parser.add_argument(
        "-C",
        "--config",
        dest="config",
        default=DEFAULT_CONFIG_NAME,
        metavar="FILE",
        help="Config file",
    )

    decomp_args = parser.add_argument_group("Decompilation options")

    decomp_args.add_argument(
        "--va",
        "--verbose-analysis",
        dest="verbose_analysis",
        help="Verbose logging for analysis step.",
        action="store_true",
        default=False,
    )
    # decomp_args.add_argument('-d', '--decompilers', dest="decompilers", help='Number of parallel decompilers', type=int,default=1)
    decomp_args.add_argument("--temp-dir", metavar="DIR", default=None)
    decomp_args.add_argument(
        "-p",
        "--profile",
        dest="profile",
        default="fast",
        help="Profile for ghidra analysis options",
    )
    decomp_args.add_argument(
        "--min-func-len",
        help="Minimum function length to be considered",
        type=int,
        default=10,
    )

    ghidra_import = parser.add_argument_group("Ghidra Import Options")
    ghidra_import.add_argument(
        "--processor",
        dest="processor",
        help="Force a specific Ghidra Language ID (e.g., 'x86:LE:64:default')",
        default=None,
    )
    ghidra_import.add_argument(
        "--cspec",
        dest="cspec",
        help="Force a specific Ghidra Compiler Spec ID (e.g., 'gcc')",
        default=None,
    )

    jvm_options = parser.add_argument_group("JVM Options")
    jvm_options.add_argument(
        "--max-ram-percent", help="Set JVM Max Ram %% of host RAM", default=60.0
    )
    jvm_options.add_argument(
        "--print-flags",
        help="Print JVM flags at start",
        action="store_true",
        default=False,
    )
    jvm_options.add_argument(
        "--jvm-args", nargs="?", help="JVM args to add at start", default=None
    )

    batch_options = parser.add_argument_group("Batch Options")
    batch_options.add_argument("--batch-uuid", help="Batch uuid", default=None)
    batch_options.add_argument(
        "--batch-name", help="Batch name", default=DEFAULT_BATCH_NAME
    )

    args = parser.parse_args()

    if args.verbose == 0:
        level = logging.WARNING
    elif args.verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logging.basicConfig(level=level)

    main(args)

    end = time.time()

    print(f"[i] Total time : {end - start:.6f} seconds")


if __name__ == "__main__":
    cli_main()
