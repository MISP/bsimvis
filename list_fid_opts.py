import sys
import logging

logging.basicConfig(level=logging.INFO)
from pyghidra.launcher import HeadlessPyGhidraLauncher

launcher = HeadlessPyGhidraLauncher()
launcher.start()

from ghidra.base.project import GhidraProject
from java.io import File
import tempfile

tmp = tempfile.mkdtemp()
project = GhidraProject.createProject(tmp, "TestProject", False)
try:
    prog = project.importProgram(File("/bin/ls"))
    options = prog.getOptions("Analyzers")
    for name in options.getOptionNames():
        if "Function ID" in name:
            print("OPT:", name)
finally:
    project.close()
