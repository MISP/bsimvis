"""Trims the MBC and ATT&CK STIX bundles to the id -> name catalog BSimVis ships.

    uv run python -I scripts/trim_capability_catalog.py MBC_JSON ATTACK_JSON OUT_JSON

MBC_JSON: mbc/mbc.json of MBCProject/mbc-stix2.1; ATTACK_JSON:
enterprise-attack/enterprise-attack.json of mitre/cti. See
data/capabilities/VENDORED_FROM.md for the pinned commits.
"""

import json
import sys


def _ext(o, source):
    for ref in o.get("external_references") or []:
        if ref.get("source_name") == source and ref.get("external_id"):
            return ref["external_id"]
    return None


def main(mbc_path, attack_path, out_path):
    mbc = json.load(open(mbc_path))["objects"]
    objectives = {o["id"]: o["name"] for o in mbc if o["type"] == "malware-objective"}
    out = {"mbc": {}, "attack": {}}
    for o in mbc:
        if o["type"] != "malware-behavior":
            continue
        bid = o["obj_defn"]["external_id"]
        out["mbc"][bid] = {
            "name": o["name"],
            "objective": next(
                (
                    objectives[r]
                    for r in o.get("objective_refs") or []
                    if r in objectives
                ),
                None,
            ),
            "micro": bid.startswith("C"),
        }
    for o in json.load(open(attack_path))["objects"]:
        if o.get("type") != "attack-pattern" or o.get("revoked"):
            continue
        if o.get("x_mitre_deprecated"):
            continue
        tid = _ext(o, "mitre-attack")
        if tid:
            out["attack"][tid] = {
                "name": o["name"],
                "tactics": sorted(
                    p["phase_name"] for p in o.get("kill_chain_phases") or []
                ),
            }
    out["mbc"] = dict(sorted(out["mbc"].items()))
    out["attack"] = dict(sorted(out["attack"].items()))
    with open(out_path, "w") as f:
        json.dump(out, f, indent=0, sort_keys=False)
        f.write("\n")
    print(f"{len(out['mbc'])} MBC behaviours, {len(out['attack'])} ATT&CK techniques")


if __name__ == "__main__":
    main(*sys.argv[1:4])
