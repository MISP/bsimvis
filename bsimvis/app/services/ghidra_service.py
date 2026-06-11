import json
import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from collections import Counter

import pyghidra
from pyghidra.launcher import HeadlessPyGhidraLauncher
import tomllib

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
        except Exception as e:
            logging.warning(f"Failed to load default config: {e}")
        return {}

    def ensure_launcher(self, verbose=False, max_ram_percent=60.0, jvm_args=None):
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
            self._launcher.add_vmargs(f"-XX:MaxRAMPercentage={max_ram_percent}")
            if jvm_args:
                for arg in jvm_args:
                    self._launcher.add_vmargs(arg)
            self._launcher.start()
        return self._launcher

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
        from ghidra.app.decompiler import DecompInterface, DecompileOptions
        from ghidra.util.task import ConsoleTaskMonitor

        options = options or {}
        monitor = ConsoleTaskMonitor()
        now_unix = int(time.time() * 1000)

        batch_uuid = options.get("batch_uuid")
        batch_name = options.get("batch_name", "Ghidra Batch")
        tags = options.get("tags", [])
        min_func_len = options.get("min_func_len", 10)
        batch_order = options.get("batch_order", 0)

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
        decomp_opts = DecompileOptions()
        decomp_interface = DecompInterface()
        decomp_interface.setOptions(decomp_opts)
        decomp_interface.setSignatureSettings(0x4D)

        if not decomp_interface.openProgram(program):
            logging.error(f"[-] Decompiler failed to initialize for {file_name}")
            return {}

        decompiler_id = f"{decomp_interface.getMajorVersion()}.{decomp_interface.getMinorVersion()}:{decomp_interface.getCompilerSpec().getLanguage()}:{hex(decomp_interface.getSignatureSettings())}"
        functions = program.getFunctionManager().getFunctions(True)
        all_function_data = []

        for func in functions:
            if func.isExternal() or func.isThunk():
                continue
            if func.getBody().getNumAddresses() < min_func_len:
                continue

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

            callees_list = []
            try:
                for callee in func.getCalledFunctions(monitor):
                    if callee:
                        callee_entry = callee.getEntryPoint()
                        callee_entry_str = (
                            str(callee_entry).split(":")[-1] if callee_entry else None
                        )
                        callees_list.append(
                            {
                                "name": callee.getName(),
                                "entrypoint": callee_entry_str,
                                "is_external": callee.isExternal() or callee.isThunk(),
                            }
                        )
            except Exception:
                pass

            callers_list = []
            try:
                for caller in func.getCallingFunctions(monitor):
                    if caller:
                        caller_entry = caller.getEntryPoint()
                        caller_entry_str = (
                            str(caller_entry).split(":")[-1] if caller_entry else None
                        )
                        callers_list.append(
                            {
                                "name": caller.getName(),
                                "entrypoint": caller_entry_str,
                                "is_external": caller.isExternal() or caller.isThunk(),
                            }
                        )
            except Exception:
                pass

            all_function_data.append(
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
                        "tags": tags,
                        "instruction_count": func.getBody().getNumAddresses(),
                        "is_thunk": func.isThunk(),
                        "labels": labels,
                        "language_id": lang_id,
                        "return_type": return_type,
                        "namespace": namespace,
                        "parameters": parameters,
                        "entrypoint_address": entry_str,
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

        decomp_interface.dispose()
        return {"file_metadata": file_metadata, "functions": all_function_data}

    def run_profile_analysis(self, program, profile_name, force_reanalysis=False):
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
                if "program" in locals() and program:
                    program.release(project)

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
