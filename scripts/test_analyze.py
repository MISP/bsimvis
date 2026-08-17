import sys
import logging

logging.basicConfig(level=logging.INFO)
from pyghidra.launcher import HeadlessPyGhidraLauncher

launcher = HeadlessPyGhidraLauncher()
launcher.start()

from ghidra.base.project import GhidraProject
from java.io import File
from ghidra.app.plugin.core.analysis import AutoAnalysisManager
from ghidra.util.task import ConsoleTaskMonitor
import tempfile

tmp = tempfile.mkdtemp()
project = GhidraProject.createProject(tmp, "TestProject", False)
monitor = ConsoleTaskMonitor()
try:
    prog = project.importProgram(File("/bin/ls"))
    options = prog.getOptions("Analyzers")
    options.setBoolean("Function ID", True)

    # We should run within a transaction
    tx_id = prog.startTransaction("Analyze")
    try:
        mgr = AutoAnalysisManager.getAnalysisManager(prog)
        mgr.reAnalyzeAll(None)
        mgr.startAnalysis(monitor)
    finally:
        prog.endTransaction(tx_id, True)

    mgr.waitForAnalysis(100000, monitor)

    # check if any function has a fid-related property or name
    fm = prog.getFunctionManager()
    funcs = fm.getFunctions(True)
    fid_count = 0
    for f in funcs:
        sym = f.getSymbol()
        if sym.getSource().name() == "ANALYSIS":
            fid_count += 1

    print(f"Analysis complete. Found {fid_count} functions named by ANALYSIS.")
finally:
    project.close()
