with open("bsimvis/app/services/similarity_service.py", "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "v_id = int(k_str.split(\":\")[2])" in line:
        indent = line.split("v_id")[0]
        lines[i] = indent + "try:\n" + indent + "    v_id = int(k_str.split(\":\")[2])\n" + indent + "except ValueError:\n" + indent + "    continue\n"

with open("bsimvis/app/services/similarity_service.py", "w") as f:
    f.writelines(lines)
