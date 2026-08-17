"""CLI for the rulezet.org YARA mirror: sync, quarantine, index-tags."""

import shutil

from bsimvis.app.services import rulezet_service as rz


def _list_quarantine():
    p = rz.paths()
    log = p["quarantine_log"]
    if not log.exists():
        print("Nothing quarantined.")
        return
    print(log.read_text().rstrip())


def _release(names):
    """Move rules back out of quarantine and remember the decision.

    Recorded in `released.txt` so the next sync's gate skips them -- otherwise
    every sync would re-quarantine the same rule and the override would mean
    nothing.
    """
    p = rz.paths()
    p["rules"].mkdir(parents=True, exist_ok=True)
    released, moved = [], 0
    for name in names:
        src = p["quarantine"] / f"{name}.yara"
        if not src.exists():
            print(f"  not in quarantine: {name}")
            continue
        shutil.move(str(src), str(p["rules"] / f"{name}.yara"))
        released.append(name)
        moved += 1
    if released:
        with open(p["released"], "a") as f:
            for name in released:
                f.write(name + "\n")
    print(f"Released {moved} rule(s). Run `bsimvis rulezet sync` to recompile.")


def run_rulezet(host, port, args):
    action = args.action
    if action == "sync":
        rz.sync(
            full=getattr(args, "full", False),
            limit=getattr(args, "limit", None),
            meta_only=getattr(args, "meta_only", False),
        )
    elif action == "index-tags":
        rz.index_tags(args.galaxy, limit=getattr(args, "limit", None))
    elif action == "quarantine":
        if getattr(args, "release", None):
            _release(args.release)
        else:
            _list_quarantine()
