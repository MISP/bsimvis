import re

current_file = None
changes = []
with open("diff_output.txt", "r") as f:
    for line in f:
        if line.startswith("diff --git"):
            current_file = line.split(" b/")[-1].strip()
        elif line.startswith("+") and not line.startswith("+++"):
            clean_line = line[1:].strip()
            if clean_line.startswith("def ") or clean_line.startswith("class "):
                changes.append(f"{current_file}: Added {clean_line}")
        elif line.startswith("-") and not line.startswith("---"):
            clean_line = line[1:].strip()
            if clean_line.startswith("def ") or clean_line.startswith("class "):
                changes.append(f"{current_file}: Removed {clean_line}")

for c in sorted(set(changes)):
    print(c)
