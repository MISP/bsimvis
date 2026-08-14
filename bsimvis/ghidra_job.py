"""Run a single GHIDRA_ANALYZE job in its own process.

Ghidra runs an embedded JVM via pyghidra, and it crashes: 27 hs_err_pid*.log
dumps sit in the repo root. In-process, one of those crashes took the whole
worker down with it, along with whatever else that worker was doing.

Moving analysis out here buys two things:

  1. A JVM crash kills this child, not the worker. worker.py retries it and
     flags the file unanalyzed rather than dying.
  2. The worker itself no longer loads a JVM at all. That floor was ~1.3-2.4 GB
     of a 3 GB per-worker cgroup cap, which every job carried whether it
     touched Ghidra or not -- enrich_features died repeatedly while holding a
     JVM it never used, with only ~0.6 GB of headroom to work in.

Streaming already goes over HTTP to the app rather than through the worker's
memory, so this process needs nothing back from its parent but an exit code.

    python -m bsimvis.ghidra_job --job-id <id>
"""

import json
import logging
import os
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from bsimvis.app.services.redis_client import get_queue_redis, get_redis, get_raw_redis
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.lua_manager import lua_manager
from bsimvis.app.services.ghidra_service import ghidra_service
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.metadata_service import staged_metadata


def _peak_rss():
    """This process's peak RSS in bytes, from the kernel's own counter."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


class GhidraAnalyzer:
    """Owns the JVM for exactly one job, then exits."""

    def __init__(self, name="ghidra-child"):
        self.name = name
        self.id = name
        self.r_queue = get_queue_redis()
        self.r_data = get_redis()
        self.r_raw = get_raw_redis()
        self.job_service = JobService()
        lua_manager.init_app()
        max_heap_mb = config_service.get("ghidra.max_heap_mb", 1536)
        jvm_args = list(config_service.get("ghidra.jvm_args", []) or [])
        # Crash dumps landed in the repo root; 27 of them accumulated there.
        # Send them somewhere findable and cleanable as a batch.
        log_dir = os.getenv("LOG_DIR") or os.path.join(os.getcwd(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        jvm_args.append(f"-XX:ErrorFile={os.path.join(log_dir, 'hs_err_pid%p.log')}")
        ghidra_service.ensure_launcher(max_heap_mb=max_heap_mb, jvm_args=jvm_args)

    def _collect_project_files(self, folder):
        """Recursively collect all DomainFiles from a Ghidra project folder."""
        files = list(folder.getFiles())
        for sub in folder.getFolders():
            files.extend(self._collect_project_files(sub))
        return files

    def _post_chunk(
        self,
        chunk,
        idx,
        is_final,
        collection,
        file_md5,
        file_meta,
        skip_sim,
        payload,
        hosts,
        job_id,
        splice_into_parent=True,
    ):
        import requests

        chunk_payload = {
            "collection": collection,
            "file_md5": file_md5,
            "chunk_index": idx,
            "is_final": is_final,
            "skip_sim": skip_sim,
            "file_metadata": file_meta if idx == 0 else None,
            "functions": chunk,
        }
        if splice_into_parent:
            chunk_payload["parent_job_id"] = job_id

        for opt in ["top_k", "min_score", "min_features", "algo"]:
            if opt in payload:
                chunk_payload[opt] = payload[opt]
        for api_host in hosts:
            url = f"http://{api_host}/api/file/upload_chunk"
            resp = requests.post(url, json=chunk_payload, timeout=300)
            resp.raise_for_status()
            return resp.json()

    def _capa_tags_for_program(self, capa_json_path, program):
        """capa's JSON -> `(tags by entry point hex, rule ids by entry point, doc)`.

        Two things sit between a capa match and a Ghidra function, and getting
        either wrong yields an empty capa axis rather than an error:

        * capa and Ghidra load a PIE image at different bases (0x2000000 against
          Ghidra's 0x100000 for a static-pie ELF), so a capa address means
          nothing until it is re-expressed as an offset from the base capa
          recorded and re-based onto Ghidra's.
        * most rules match at instruction or basic-block scope, so the address
          lands inside a function body rather than on it. Tags hang off entry
          points downstream, so every hit is resolved through the function
          containing it -- keying on the raw address would drop every rule that
          is not function-scoped.

        The second and third returns are the provenance half: which capa rule
        put a tag on which function, and capa's own document so the rule rows
        (authors, ATT&CK/MBC, the rule YAML) can be recorded. Both come out of
        the same resolution pass, so a function can never name a rule that fired
        on a different function.
        """
        if not os.path.exists(capa_json_path) or not os.path.getsize(capa_json_path):
            raise RuntimeError("capa wrote no output")

        from bsimvis.app.services.tag_provenance import capa_rule_id
        from bsimvis.app.services.tag_taxonomy import capa_rule_hits

        with open(capa_json_path) as f:
            cdata = json.load(f)
        capa_base, hits = capa_rule_hits(cdata)

        rebase = program.getImageBase().getOffset() - capa_base
        func_manager = program.getFunctionManager()
        space = program.getAddressFactory().getDefaultAddressSpace()

        tags_by_addr, rules_by_addr = {}, {}
        for virtual_addr, ctags in hits.items():
            try:
                func = func_manager.getFunctionContaining(
                    space.getAddress(virtual_addr + rebase)
                )
            except Exception:
                # capa seeing a section Ghidra never loaded is not a reason to
                # drop the hits that do land.
                continue
            if func:
                entry = func.getEntryPoint()
                tags_by_addr.setdefault(hex(entry.getOffset()), set()).update(ctags)
                # The rule id is the namespace the `capa:` tag was built from,
                # so it is recoverable from the tag; the `mitre:`/`mbc:` tags
                # riding along came from those same rules.
                rules_by_addr.setdefault(str(entry).split(":")[-1], set()).update(
                    capa_rule_id(t.split("capa:", 1)[1].replace(":", "/"))
                    for t in ctags
                    if t.startswith("capa:")
                )
        return tags_by_addr, rules_by_addr, cdata

    def _yara_tags_for_program(self, matches, program, extra_tags=None):
        """yara-python matches -> `{function entry point hex: {yara tag, ...}}`.

        A YARA match offset is a raw file offset -- the scan reads the file on
        disk, not a loaded image -- so it needs no rebase constant the way a
        capa virtual address does. What it needs instead is Ghidra's own file
        layout: `Memory.locateAddressesForFileOffset` is Ghidra's built-in
        answer to "which loaded address did this file byte end up at", and
        handles every section/segment gap itself, including the file offset
        landing in something Ghidra never mapped (an empty result, not an
        error). From there this is the same fan-out `_capa_tags_for_program`
        does: an offset can land inside several functions' worth of matched
        strings, and each resolves through the function containing it.

        Unlike capa, though, most YARA offsets are *not* code. A rule matches
        string literals, which live in `.rodata` and are inside no function
        body at all, so `getFunctionContaining` alone drops the common case on
        the floor. Those fall through to the functions that reference the
        data (`_funcs_referencing`). Whatever still resolves to nothing is not
        lost either -- the caller tags the file with every matched rule.
        """
        from bsimvis.app.services.tag_taxonomy import yara_rule_hits

        # `stream_bsim_data` looks these up by `hex(entry point)`.
        return self._funcs_by_offset(
            yara_rule_hits(matches, extra_tags),
            program,
            lambda f: hex(f.getEntryPoint().getOffset()),
        )

    def _funcs_by_offset(self, hits, program, key):
        """`{file offset: set}` -> `{key(function): union of the sets it covers}`.

        Shared by the two things a YARA match feeds: the tags a function ends up
        carrying, and the rule ids that put them there. One resolution pass, so
        a function's provenance can never name a different function's rule.
        """
        func_manager = program.getFunctionManager()
        memory = program.getMemory()

        out = {}
        for file_offset, values in hits.items():
            try:
                addrs = memory.locateAddressesForFileOffset(file_offset)
            except Exception:
                continue
            for addr in addrs:
                func = func_manager.getFunctionContaining(addr)
                funcs = [func] if func else self._funcs_referencing(addr, program)
                for f in funcs:
                    out.setdefault(key(f), set()).update(values)
        return out

    def _funcs_referencing(self, addr, program):
        """Functions with a reference to the data item containing `addr`.

        A YARA string is usually a substring of the literal it matched, so the
        match address lands in the *middle* of the data item while every xref
        points at the item's start -- asking for references to `addr` itself
        finds nothing. `getDataContaining` walks back to the start first,
        which is the whole reason this works on real `.rodata` hits.

        One hop only: a function that references the string, not its callers.
        A string reached through a table or a computed offset has no direct
        reference to follow and stays a file-level tag.
        """
        func_manager = program.getFunctionManager()
        try:
            data = program.getListing().getDataContaining(addr)
            start = data.getMinAddress() if data else addr
            refs = program.getReferenceManager().getReferencesTo(start)
        except Exception:
            return []
        funcs = []
        for ref in refs:
            func = func_manager.getFunctionContaining(ref.getFromAddress())
            if func:
                funcs.append(func)
        return funcs

    def _stream_program_chunks(
        self, program, payload, hosts, job_id, splice_into_parent=True
    ):
        collection = payload.get("collection", "main")
        skip_sim = payload.get("skip_sim", False)

        # Initialize stream generator with job context for real-time progress
        generator = ghidra_service.stream_bsim_data(
            program,
            payload,
            chunk_size=100,
            job_service=self.job_service,
            job_id=job_id,
        )
        file_meta = next(generator)

        # Merge CLI-provided metadata (upload --metadata) into the file metadata so
        # it gets stored just like the `metadata propagate` path. The raw-upload
        # route forwards it as file_metadata_extra; without this the streaming
        # analysis path silently drops it.
        extra_meta = payload.get("file_metadata_extra")
        if extra_meta:
            if isinstance(extra_meta, str):
                extra_meta = json.loads(extra_meta)
            file_meta.update(extra_meta)
            if "file_name" in extra_meta:
                file_meta["file_name"] = extra_meta["file_name"]

        file_md5 = file_meta.get("file_md5")

        # Look-ahead: send each chunk immediately without buffering all of them.
        # Holding one chunk behind lets us detect the final chunk on arrival of the next.
        idx = 0
        prev_chunk = None
        for chunk in generator:
            # Cancellation was only ever checked before a job started; a long
            # decompilation ignored it entirely. Chunk boundaries are the natural
            # place to notice.
            if self.job_service.is_cancelled(job_id):
                raise RuntimeError(f"Job {job_id} cancelled during streaming")
            if prev_chunk is not None:
                self._post_chunk(
                    prev_chunk,
                    idx - 1,
                    False,
                    collection,
                    file_md5,
                    file_meta,
                    skip_sim,
                    payload,
                    hosts,
                    job_id,
                    splice_into_parent=splice_into_parent,
                )
                self.job_service.update_progress(job_id, 80, f"Uploaded chunk {idx}")
            prev_chunk = chunk
            idx += 1

        # Send the final chunk (or empty sentinel when the program has no functions)
        final_chunk = prev_chunk if prev_chunk is not None else []
        final_idx = max(idx - 1, 0)
        res = self._post_chunk(
            final_chunk,
            final_idx,
            True,
            collection,
            file_md5,
            file_meta,
            skip_sim,
            payload,
            hosts,
            job_id,
            splice_into_parent=splice_into_parent,
        )
        self.job_service.update_progress(job_id, 100, f"Uploaded chunk {idx}/{idx}")

        pipeline_id = res.get("pipeline_id") if isinstance(res, dict) else None
        return pipeline_id

    def run(self, payload, job_id):
        """Body moved verbatim from Worker._dispatch's GHIDRA_ANALYZE branch."""
        collection = payload.get("collection", "main")
        raw_file_id = payload.get("raw_file_id")
        file_md5 = payload.get("file_md5")

        # 1. Fetch raw binary from Kvrocks
        raw_bytes = self.r_raw.get(raw_file_id)
        if not raw_bytes:
            self.job_service.add_log(
                job_id, f"Error: Raw file {raw_file_id} not found."
            )
            return False

        temp_dir = None
        temp_path = None
        try:
            # 2. Save to temp file with original name to preserve name in Ghidra/DB
            orig_name = payload.get("file_name", "unknown")
            orig_name = os.path.basename(orig_name)
            if not orig_name:
                orig_name = "unknown"

            temp_dir = tempfile.mkdtemp(prefix="bsim_worker_")
            temp_path = os.path.join(temp_dir, orig_name)
            with open(temp_path, "wb") as f:
                f.write(raw_bytes)

            import subprocess
            from bsimvis.app.services.unpack_service import (
                CAPA_E_INVALID_FILE_OS,
                capa_failure_reason,
                capa_fallback_os,
                capa_path,
            )

            skip_capa = bool(payload.get("skip_capa"))
            skip_yara = bool(payload.get("skip_yara"))
            skip_rulezet = bool(payload.get("skip_rulezet"))
            skip_function_id = bool(payload.get("skip_function_id"))
            capa = None if skip_capa else capa_path()
            capa_proc = None
            capa_out = None
            capa_err = None
            capa_json_path = os.path.join(temp_dir, "capa.json")
            capa_err_path = os.path.join(temp_dir, "capa.err")
            if skip_capa:
                self.job_service.add_log(job_id, "capa skipped by request.")
            elif not capa:
                # Otherwise an install with no capa binary looks exactly like a
                # sample with no capabilities.
                self.job_service.add_log(
                    job_id, "capa not installed; skipping capability tags."
                )
            if capa and not temp_path.endswith(".gpr.zip"):
                capa_out = open(capa_json_path, "w")
                # capa says on stderr *why* it refused a sample -- unsupported
                # architecture, undetectable OS. Discarding that was why an
                # unanalyzable sample and a clean one looked identical.
                capa_err = open(capa_err_path, "w")
                # No --os here on purpose: capa's own detection is better than
                # anything guessable from the header, and it is the only thing
                # that recognises an Android ELF. The fallback below runs only
                # if capa says it could not tell.
                capa_proc = subprocess.Popen(
                    [capa, "-j", temp_path],
                    stdout=capa_out,
                    stderr=capa_err,
                )

            # 3. Run Analysis & Stream Chunks directly to API
            app_host = os.getenv("APP_HOST", "localhost")
            app_port = os.getenv("APP_PORT", "5000")
            fallback_host = f"{app_host}:{app_port}"

            # Check environment variables first, then fallback to config
            hosts = (
                fallback_host
                if os.getenv("APP_PORT")
                else config_service.get("bsimvis.host", fallback_host)
            )
            if isinstance(hosts, str):
                hosts = [hosts]

            # We will analyze using stream_bsim_data
            if temp_path.endswith(".gpr.zip"):
                self.job_service.add_log(
                    job_id, f"Extracting Ghidra project archive {orig_name}..."
                )
                import zipfile

                with zipfile.ZipFile(temp_path, "r") as zip_ref:
                    zip_ref.extractall(temp_dir)

                # Find .gpr file
                gpr_path = None
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith(".gpr"):
                            gpr_path = os.path.join(root, file)
                            break
                    if gpr_path:
                        break

                if not gpr_path:
                    self.job_service.add_log(
                        job_id, "Error: No .gpr file found in archive."
                    )
                    return False

                self.job_service.add_log(
                    job_id, f"Starting Ghidra project analysis for {orig_name}..."
                )
                from ghidra.base.project import GhidraProject

                project = GhidraProject.openProject(
                    Path(gpr_path).parent, Path(gpr_path).stem
                )
                pipeline_ids = []
                # A project holds many programs but the upload carried one CSV
                # row, keyed by the .gpr.zip's md5. Its facts still apply to
                # every program in it; its `file_name` does not -- forcing it
                # would store all of them under the archive's name.
                if isinstance(payload.get("file_metadata_extra"), dict):
                    payload["file_metadata_extra"].pop("file_name", None)
                try:
                    root_folder = project.getProjectData().getRootFolder()
                    files = self._collect_project_files(root_folder)
                    for file in files:
                        from ghidra.util.task import ConsoleTaskMonitor

                        program = file.getDomainObject(
                            project, True, False, ConsoleTaskMonitor()
                        )
                        try:
                            binary_md5 = program.getExecutableMD5()
                            if binary_md5:
                                # Check if already exists in collection
                                if self.r_data.sismember(
                                    f"{collection}:all_files",
                                    f"{collection}:file:{binary_md5}",
                                ):
                                    self.job_service.add_log(
                                        job_id,
                                        f"Skipping {program.getName()} (MD5 {binary_md5} already exists in collection).",
                                    )
                                    continue

                            ghidra_service.run_profile_analysis(
                                program,
                                payload.get("profile", "fast"),
                                force_reanalysis=False,
                                disable_function_id=skip_function_id,
                            )
                            # The programs inside a project never pass through
                            # the upload route, so this is the only place their
                            # own staged CSV row can be picked up.
                            prog_payload = payload
                            own_meta = staged_metadata(
                                payload.get("batch_uuid"), binary_md5, r=self.r_data
                            )
                            if own_meta:
                                prog_payload = dict(payload)
                                prog_payload["file_metadata_extra"] = {
                                    **(payload.get("file_metadata_extra") or {}),
                                    **own_meta,
                                }
                            pipe_id = self._stream_program_chunks(
                                program,
                                prog_payload,
                                hosts,
                                job_id,
                                splice_into_parent=True,
                            )
                            if pipe_id:
                                pipeline_ids.append(pipe_id)
                        finally:
                            if program:
                                program.release(project)

                    if pipeline_ids:
                        group_id = self.job_service.create_group(
                            pipeline_ids, enqueue=False
                        )
                        parent_pipeline_id = self.r_queue.hget(
                            f"job:{job_id}", "parent_id"
                        )
                        if parent_pipeline_id:
                            parent_pipeline_id = (
                                parent_pipeline_id.decode()
                                if isinstance(parent_pipeline_id, bytes)
                                else parent_pipeline_id
                            )
                            if self.job_service.splice_tasks(
                                parent_pipeline_id, job_id, [group_id]
                            ):
                                self.job_service.add_log(
                                    parent_pipeline_id,
                                    f"Spliced child pipelines group {group_id} into pipeline.",
                                )
                finally:
                    project.close()
            else:
                self.job_service.add_log(
                    job_id, f"Starting Ghidra analysis for {orig_name}..."
                )
                # For a single file
                from ghidra.base.project import GhidraProject

                with tempfile.TemporaryDirectory(prefix="bsim_") as project_temp_dir:
                    project = GhidraProject.createProject(
                        project_temp_dir, "TempGhidraProject", False
                    )
                    try:
                        if payload.get("processor"):
                            from ghidra.program.model.lang import (
                                LanguageID,
                                CompilerSpecID,
                            )
                            from ghidra.program.util import DefaultLanguageService

                            lang_service = DefaultLanguageService.getLanguageService()
                            lang_id = LanguageID(payload.get("processor"))
                            lang = lang_service.getLanguage(lang_id)
                            if payload.get("cspec"):
                                cspec_id = CompilerSpecID(payload.get("cspec"))
                                cspec = lang.getCompilerSpecByID(cspec_id)
                            else:
                                cspec = lang.getDefaultCompilerSpec()
                            program = project.importProgram(
                                Path(temp_path), lang, cspec
                            )
                        else:
                            program = project.importProgram(Path(temp_path))

                        ghidra_service.run_profile_analysis(
                            program,
                            payload.get("profile", "fast"),
                            force_reanalysis=True,
                            disable_function_id=skip_function_id,
                        )

                        capa_tags_by_addr = {}
                        if capa_proc:
                            capa_proc.wait()
                            for handle in (capa_out, capa_err):
                                if handle:
                                    handle.close()
                            returncode = capa_proc.returncode

                            # capa refusing to guess the OS is recoverable for an
                            # ELF, and this is the only sample it costs a second
                            # pass -- the first one ran alongside the Ghidra
                            # analysis and is already paid for.
                            fallback_os = (
                                capa_fallback_os(raw_bytes)
                                if returncode == CAPA_E_INVALID_FILE_OS
                                else None
                            )
                            if fallback_os:
                                self.job_service.add_log(
                                    job_id,
                                    f"capa could not detect the OS; retrying as "
                                    f"{fallback_os}.",
                                )
                                with (
                                    open(capa_json_path, "w") as out,
                                    open(capa_err_path, "w") as err,
                                ):
                                    returncode = subprocess.call(
                                        [capa, "-j", "--os", fallback_os, temp_path],
                                        stdout=out,
                                        stderr=err,
                                    )

                            if returncode:
                                self.job_service.add_log(
                                    job_id,
                                    "capa skipped this sample: "
                                    + capa_failure_reason(capa_err_path, returncode),
                                )
                            else:
                                try:
                                    (
                                        capa_tags_by_addr,
                                        capa_rules_by_addr,
                                        capa_doc,
                                    ) = self._capa_tags_for_program(
                                        capa_json_path, program
                                    )
                                    # Same deal as the YARA path below: the rule
                                    # metadata (authors, ATT&CK/MBC, the rule
                                    # YAML) exists only in capa's document, and
                                    # capa rules are not checked out locally, so
                                    # it is recorded here or lost.
                                    from bsimvis.app.services import tag_provenance

                                    rows = tag_provenance.capa_rows(capa_doc)
                                    tag_provenance.put_rules(rows, self.r_data)
                                    if file_md5 and rows:
                                        hits = {
                                            f"{collection}:file:{file_md5}": list(rows)
                                        }
                                        for addr, ids in capa_rules_by_addr.items():
                                            hits[
                                                f"{collection}:func:{file_md5}:{addr}"
                                            ] = sorted(ids)
                                        tag_provenance.record_hits_bulk(
                                            collection, hits, self.r_data
                                        )
                                except Exception as e:
                                    self.job_service.add_log(
                                        job_id, f"capa parse failed: {e}"
                                    )
                                self.job_service.add_log(
                                    job_id,
                                    f"capa tagged {len(capa_tags_by_addr)} functions.",
                                )

                        # stream_bsim_data() reads this straight off the payload it
                        # is handed as `options`, and hangs the tags on the function
                        # whose entry point matches the key.
                        if capa_tags_by_addr:
                            payload["capa_tags"] = {
                                k: sorted(v) for k, v in capa_tags_by_addr.items()
                            }

                        yara_tags_by_addr = {}
                        if skip_yara and skip_rulezet:
                            self.job_service.add_log(job_id, "yara skipped by request.")
                        else:
                            # Unlike capa this runs in-process rather than as a
                            # subprocess kicked off earlier -- compiling and
                            # matching the vendored ruleset is sub-second, so
                            # there is nothing to overlap with Ghidra's analysis
                            # by starting it any sooner.
                            try:
                                from bsimvis.app.services.yara_service import scan_file
                                from bsimvis.app.services.tag_taxonomy import (
                                    yara_file_tags,
                                )
                                from bsimvis.app.services import tag_provenance

                                matches, extra_tags = scan_file(
                                    temp_path,
                                    vendored=not skip_yara,
                                    mirror=not skip_rulezet,
                                )
                                yara_tags_by_addr = self._yara_tags_for_program(
                                    matches, program, extra_tags
                                )
                                # The match is a fact about the file; which
                                # function it belongs to is an attribution that
                                # can fail (unmapped offset, data with no xref).
                                # Record the fact first, unconditionally, so a
                                # rule can never match and leave no trace.
                                file_tags = yara_file_tags(matches, extra_tags)
                                if file_tags:
                                    payload["tags"] = sorted(
                                        set(payload.get("tags") or []) | file_tags
                                    )
                                # The match object is the only place the rule's
                                # file path / rulezet uuid exists; downstream
                                # everything is the flat tag string. Record it
                                # here or it is unrecoverable.
                                rows = tag_provenance.match_rows(matches, extra_tags)
                                tag_provenance.put_rules(rows, self.r_data)
                                collection = payload.get("collection", "main")
                                if file_md5 and rows:
                                    # Keyed by the same ids the UI renders tags
                                    # against, so a tag chip asks for its own
                                    # hits without a second lookup. The file
                                    # entry is every rule that fired; a function
                                    # entry is only the rules whose strings
                                    # resolved to that function, which is the
                                    # whole point -- 4 rules instead of the
                                    # ~5k the tag carries ruleset-wide.
                                    hits = {
                                        f"{collection}:file:{file_md5}": list(rows)
                                    }
                                    for addr, ids in self._funcs_by_offset(
                                        tag_provenance.match_offsets(matches),
                                        program,
                                        # `<coll>:func:<md5>:<addr>`, the entry
                                        # point formatted the way
                                        # `stream_bsim_data` builds function ids
                                        # (`Address.toString()`, not `hex()`).
                                        lambda f: str(f.getEntryPoint()).split(":")[-1],
                                    ).items():
                                        hits[f"{collection}:func:{file_md5}:{addr}"] = (
                                            sorted(ids)
                                        )
                                    tag_provenance.record_hits_bulk(
                                        collection, hits, self.r_data
                                    )
                                self.job_service.add_log(
                                    job_id,
                                    f"yara matched {len(file_tags)} rules, "
                                    f"tagged {len(yara_tags_by_addr)} functions.",
                                )
                            except Exception as e:
                                self.job_service.add_log(
                                    job_id, f"yara scan failed: {e}"
                                )
                        if yara_tags_by_addr:
                            payload["yara_tags"] = {
                                k: sorted(v) for k, v in yara_tags_by_addr.items()
                            }

                        self._stream_program_chunks(program, payload, hosts, job_id)
                    finally:
                        # close() releases every program importProgram() registered
                        # and disposes the project's LocalFileSystem, which is what
                        # stops its "File System Listener" thread. Without it the
                        # thread and the whole Ghidra object graph it pins leak for
                        # the life of the worker. Must run before the enclosing
                        # TemporaryDirectory removes the project directory.
                        #
                        # Do NOT call program.release(project) first -- close()
                        # already does it, and the second release throws
                        # IllegalArgumentException: unknown consumer. The
                        # openProject path above is different: those programs come
                        # from DomainFile.getDomainObject(), are not tracked in
                        # GhidraProject.openPrograms, and must be released by hand.
                        project.close()

            self.job_service.add_log(
                job_id, f"Analysis and streaming complete for {orig_name}."
            )

            return True
        except Exception as e:
            self.job_service.add_log(job_id, f"Analysis failed: {str(e)}")
            raise
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            if temp_dir and os.path.exists(temp_dir):
                import shutil

                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass


def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [ghidra] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--name", default="ghidra-child")
    args = parser.parse_args(argv)

    analyzer = GhidraAnalyzer(name=args.name)
    job = analyzer.r_queue.hgetall(f"job:{args.job_id}")
    if not job:
        logging.error(f"Job {args.job_id} has no metadata.")
        return 2
    payload = json.loads(job.get("payload", "{}"))

    try:
        ok = analyzer.run(payload, args.job_id)
    finally:
        # Report our own peak. The parent measures its VmHWM, which does NOT
        # include this process -- but this process DOES share the parent's
        # systemd scope, so the JVM counts against the same MemoryMax. Without
        # this, ghidra_analyze was weighted at the worker's ~0.8 GiB while the
        # scope was actually hitting the 3 GB cap and being OOM-killed.
        peak = _peak_rss()
        if peak:
            try:
                analyzer.job_service.record_job_peak(JobType.GHIDRA_ANALYZE.value, peak)
            except Exception as e:
                logging.warning(f"Could not record Ghidra peak: {e}")
            logging.info(f"[#] Ghidra child peak RSS {peak / 1024**3:.2f} GiB")

    # Exit code is the whole interface back to the worker.
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
