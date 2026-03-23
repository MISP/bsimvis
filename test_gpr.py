import pyghidra
pyghidra.start()
from ghidra.base.project import GhidraProject
from pathlib import Path

target_path = Path("/home/test/test/ghidra/ghidra-projects/MiraiMalwareBatchProject.gpr")
project = GhidraProject.openProject(target_path.parent, target_path.stem)
root = project.getProjectData().getRootFolder()
f = root.getFiles()[0]
print("Methods for DomainFile:")
for m in dir(f):
    if "DomainObject" in m:
        print(m)

print("Methods for program:")
prog = f.getImmutableDomainObject(project, -1, None)
print(prog)
for m in dir(prog):
    if m == 'release':
        print("Program has release!")

