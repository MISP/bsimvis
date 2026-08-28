#!/usr/bin/env python3
"""git-stash-alike for bin_sim/file/function notes and tags.

Snapshots notes+tags for one function, one file, one bin_sim pair
(similarity), or every noted/tagged function+file in a collection, removes
them from Kvrocks through the real NoteService/TagService (so the owner and
tag index sets stay correct), and stores the snapshot in
scripts/.tag_stash.json. pop/apply replay it back through the same services.

Similarity (bin_sim pair) notes don't exist as a concept -- only tags are
attached to a pair's sid doc -- so a --scope similarity stash only ever
touches tags.

    uv run python scripts/tag_stash.py stash --collection main --scope function --func-id main:func:<md5>:<addr> -m "before clean rerun"
    uv run python scripts/tag_stash.py stash --collection main --scope file --file-id main:file:<md5>
    uv run python scripts/tag_stash.py stash --collection main --scope file-functions --file-id main:file:<md5>  # every function in that file
    uv run python scripts/tag_stash.py stash --collection main --scope similarity --a <id1> --b <id2> [--algo unweighted_cosine]
    uv run python scripts/tag_stash.py stash --collection main --scope collection --owner llm
    uv run python scripts/tag_stash.py list
    uv run python scripts/tag_stash.py show 0
    uv run python scripts/tag_stash.py pop        # restore + drop stash@{0}
    uv run python scripts/tag_stash.py apply 0    # restore, keep it stacked
    uv run python scripts/tag_stash.py drop 0      # discard without restoring
"""
import argparse
import json
import sys
import time
from pathlib import Path

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.note_service import NoteService
from bsimvis.app.services.tag_service import TagService
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.app.services.llm_batch_service import LLM_NOTE_OWNER

STASH_FILE = Path(__file__).parent / ".tag_stash.json"


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def _load():
    return json.loads(STASH_FILE.read_text()) if STASH_FILE.exists() else []


def _save(stack):
    STASH_FILE.write_text(json.dumps(stack, indent=2))


def _owner_ok(owner_filter, owner):
    return owner_filter == "all" or owner_filter == owner


def _tag_owner(vocab, tag):
    return LLM_NOTE_OWNER if vocab.get(tag, {}).get("llm") else "user"


def _entity_tags(ts, collection, entity_type, entity_id, owner_filter, vocab):
    doc = ts._get_doc(ts._resolve_doc_id(collection, entity_type, entity_id))
    all_tags = (doc or {}).get("user_tags", [])
    return [t for t in all_tags if _owner_ok(owner_filter, _tag_owner(vocab, t))]


def snapshot_function(ns, ts, collection, func_id, owner_filter, vocab):
    notes = [n for n in ns.get_notes(collection, func_id) if _owner_ok(owner_filter, n["owner"])]
    tags = _entity_tags(ts, collection, "function", func_id, owner_filter, vocab)
    return {"notes": notes, "tags": tags}


def snapshot_file(ns, ts, collection, file_id, owner_filter, vocab):
    notes = [n for n in ns.get_file_notes(collection, file_id) if _owner_ok(owner_filter, n["owner"])]
    tags = _entity_tags(ts, collection, "file", file_id, owner_filter, vocab)
    return {"notes": notes, "tags": tags}


def snapshot_similarity(ts, sim, collection, a, b, algo, owner_filter, vocab):
    """a/b are the two entities' own ids (function ids for a func-level pair,
    file ids for a bin_sim / whole-binary pair) -- SimilarityService turns
    them into the pair's sid the same way tag_similarity/untag_similarity
    (the real /api/similarity/tag path) does, so this matches what the app
    already wrote regardless of which kind of pair it is.
    """
    sid = sim._canonicalize_sid(collection, a, b, algo)
    doc = ts._get_doc(sid)
    all_tags = (doc or {}).get("user_tags", [])
    tags = [t for t in all_tags if _owner_ok(owner_filter, _tag_owner(vocab, t))]
    return {"tags": tags, "algo": algo, "a": a, "b": b}


def clear_function(ns, ts, collection, func_id, snap):
    for n in snap["notes"]:
        ns.remove_note(collection, func_id, n["id"])
    for t in snap["tags"]:
        ts.remove_user_tag(collection, "function", func_id, t)


def clear_file(ns, ts, collection, file_id, snap):
    for n in snap["notes"]:
        ns.remove_file_note(collection, file_id, n["id"])
    for t in snap["tags"]:
        ts.remove_user_tag(collection, "file", file_id, t)


def clear_similarity(sim, collection, snap):
    for t in snap["tags"]:
        sim.untag_similarity(collection, snap["a"], snap["b"], snap["algo"], t)


def restore_function(ns, ts, collection, func_id, snap):
    for n in snap["notes"]:
        ns.add_note(collection, func_id, n["text"], owner=n["owner"])
    for t in snap["tags"]:
        ts.add_user_tag(collection, "function", func_id, t)


def restore_file(ns, ts, collection, file_id, snap):
    for n in snap["notes"]:
        ns.add_file_note(collection, file_id, n["text"], owner=n["owner"])
    for t in snap["tags"]:
        ts.add_user_tag(collection, "file", file_id, t)


def restore_similarity(sim, collection, snap):
    for t in snap["tags"]:
        sim.tag_similarity(collection, snap["a"], snap["b"], snap["algo"], t)


def file_function_ids(r, ts, collection, file_id):
    """Every function id belonging to one file, via the same
    {collection}:idx:file:functions:{md5} index tag_service._propagate_user_tag
    already reads to fan a file tag out to its functions.
    """
    doc_id = ts._resolve_doc_id(collection, "file", file_id)
    md5 = (doc_id[:-5] if doc_id.endswith(":meta") else doc_id).split(":")[-1]
    return [_s(x) for x in r.smembers(f"{collection}:idx:file:functions:{md5}")]


def discover_collection(r, collection, owner_filter, vocab):
    """Every function/file id currently carrying a matching note or tag,
    found via the existing owner/tag index sets -- no full collection scan.

    ponytail: similarity pairs are skipped here. Their index only stores the
    resolved sid (base_coll:sim:algo:c1::c2), and un-hashing that back into
    the original --a/--b ids tag_similarity/untag_similarity need is more
    parsing than a collection-wide sweep is worth. Stash a pair explicitly
    with --scope similarity --a --b instead.
    """
    owners = [owner_filter] if owner_filter != "all" else ["user", LLM_NOTE_OWNER]
    funcs, files = set(), set()
    for o in owners:
        funcs |= r.smembers(f"{collection}:idx:func:note_owners:{o}")
        files |= r.smembers(f"{collection}:idx:file:note_owners:{o}")
    for tag in vocab:
        if not _owner_ok(owner_filter, _tag_owner(vocab, tag)):
            continue
        tl = tag.lower()
        funcs |= r.smembers(f"{collection}:idx:func:user_tags:{tl}")
        files |= r.smembers(f"{collection}:idx:file:user_tags:{tl}")
    return {_s(x) for x in funcs}, {_s(x) for x in files}


def cmd_stash(args, r, ns, ts, sim):
    vocab = ts.get_collection_tags(args.collection)
    items = {"function": {}, "file": {}, "similarity": {}}

    if args.scope == "function":
        snap = snapshot_function(ns, ts, args.collection, args.func_id, args.owner, vocab)
        if snap["notes"] or snap["tags"]:
            items["function"][args.func_id] = snap
    elif args.scope == "file":
        snap = snapshot_file(ns, ts, args.collection, args.file_id, args.owner, vocab)
        if snap["notes"] or snap["tags"]:
            items["file"][args.file_id] = snap
    elif args.scope == "similarity":
        snap = snapshot_similarity(ts, sim, args.collection, args.a, args.b, args.algo, args.owner, vocab)
        if snap["tags"]:
            sid = sim._canonicalize_sid(args.collection, args.a, args.b, args.algo)
            items["similarity"][sid] = snap
    elif args.scope == "file-functions":
        for fid in file_function_ids(r, ts, args.collection, args.file_id):
            snap = snapshot_function(ns, ts, args.collection, fid, args.owner, vocab)
            if snap["notes"] or snap["tags"]:
                items["function"][fid] = snap
    elif args.scope == "collection":
        func_ids, file_ids = discover_collection(r, args.collection, args.owner, vocab)
        for fid in func_ids:
            snap = snapshot_function(ns, ts, args.collection, fid, args.owner, vocab)
            if snap["notes"] or snap["tags"]:
                items["function"][fid] = snap
        for fid in file_ids:
            snap = snapshot_file(ns, ts, args.collection, fid, args.owner, vocab)
            if snap["notes"] or snap["tags"]:
                items["file"][fid] = snap

    n_notes = sum(len(v["notes"]) for v in items["function"].values()) + sum(
        len(v["notes"]) for v in items["file"].values()
    )
    n_tags = sum(len(v["tags"]) for bucket in items.values() for v in bucket.values())
    if n_notes == 0 and n_tags == 0:
        print("Nothing matched -- nothing stashed.")
        return

    for fid, snap in items["function"].items():
        clear_function(ns, ts, args.collection, fid, snap)
    for fid, snap in items["file"].items():
        clear_file(ns, ts, args.collection, fid, snap)
    for sid, snap in items["similarity"].items():
        clear_similarity(sim, args.collection, snap)

    stack = _load()
    stack.insert(0, {
        "timestamp": int(time.time()),
        "message": args.message or "",
        "scope": args.scope,
        "collection": args.collection,
        "owner": args.owner,
        "items": items,
    })
    _save(stack)
    print(f"Stashed {n_notes} note(s), {n_tags} tag(s) as stash@{{0}}: {args.message or '(no message)'}")


def _restore_entry(ns, ts, sim, entry, drop_after, stack, index):
    collection = entry["collection"]
    for fid, snap in entry["items"]["function"].items():
        restore_function(ns, ts, collection, fid, snap)
    for fid, snap in entry["items"]["file"].items():
        restore_file(ns, ts, collection, fid, snap)
    for sid, snap in entry["items"]["similarity"].items():
        restore_similarity(sim, collection, snap)

    n_notes = sum(len(v["notes"]) for v in entry["items"]["function"].values()) + sum(
        len(v["notes"]) for v in entry["items"]["file"].values()
    )
    n_tags = sum(len(v["tags"]) for bucket in entry["items"].values() for v in bucket.values())
    verb = "Popped" if drop_after else "Applied"
    print(f"{verb} stash@{{{index}}}: restored {n_notes} note(s), {n_tags} tag(s).")

    if drop_after:
        stack.pop(index)
        _save(stack)


def cmd_pop_apply(args, ns, ts, sim, drop_after):
    stack = _load()
    if not stack:
        print("No stash entries.")
        return
    if args.index >= len(stack):
        print(f"No stash@{{{args.index}}}.", file=sys.stderr)
        sys.exit(1)
    _restore_entry(ns, ts, sim, stack[args.index], drop_after, stack, args.index)


def cmd_drop(args):
    stack = _load()
    if args.index >= len(stack):
        print(f"No stash@{{{args.index}}}.", file=sys.stderr)
        sys.exit(1)
    entry = stack.pop(args.index)
    _save(stack)
    print(f"Dropped stash@{{{args.index}}}: {entry['message'] or '(no message)'}")


def cmd_list(args):
    stack = _load()
    if not stack:
        print("No stash entries.")
        return
    for i, e in enumerate(stack):
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(e["timestamp"]))
        print(f"stash@{{{i}}}: [{e['scope']}/{e['owner']}] {e['collection']} -- {e['message'] or '(no message)'} ({when})")


def cmd_show(args):
    stack = _load()
    if args.index >= len(stack):
        print(f"No stash@{{{args.index}}}.", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(stack[args.index], indent=2))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    ps = sub.add_parser("stash", help="snapshot + clear notes/tags")
    ps.add_argument("--collection", required=True)
    ps.add_argument("--scope", required=True, choices=["function", "file", "file-functions", "similarity", "collection"])
    ps.add_argument("--func-id")
    ps.add_argument("--file-id")
    ps.add_argument("--a", help="first entity id (similarity scope) -- a function id for a func-pair, a file id for a bin_sim/whole-binary pair")
    ps.add_argument("--b", help="second entity id (similarity scope), same kind as --a")
    ps.add_argument("--algo", default="unweighted_cosine")
    ps.add_argument("--owner", default="all", choices=["all", "user", LLM_NOTE_OWNER])
    ps.add_argument("-m", "--message")

    for name, help_ in [("pop", "restore stash@{N} and drop it (default N=0)"),
                         ("apply", "restore stash@{N}, keep it stacked (default N=0)"),
                         ("drop", "discard stash@{N} without restoring (default N=0)"),
                         ("show", "print stash@{N} as JSON (default N=0)")]:
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("index", nargs="?", type=int, default=0)

    sub.add_parser("list", help="list stash entries")

    args = p.parse_args()

    if args.cmd == "stash":
        if args.scope == "function" and not args.func_id:
            p.error("--scope function requires --func-id")
        if args.scope in ("file", "file-functions") and not args.file_id:
            p.error(f"--scope {args.scope} requires --file-id")
        if args.scope == "similarity" and not (args.a and args.b):
            p.error("--scope similarity requires --a and --b")
        r = get_redis()
        cmd_stash(args, r, NoteService(r), TagService(r), SimilarityService(r))
    elif args.cmd == "pop":
        r = get_redis()
        cmd_pop_apply(args, NoteService(r), TagService(r), SimilarityService(r), drop_after=True)
    elif args.cmd == "apply":
        r = get_redis()
        cmd_pop_apply(args, NoteService(r), TagService(r), SimilarityService(r), drop_after=False)
    elif args.cmd == "drop":
        cmd_drop(args)
    elif args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "show":
        cmd_show(args)


if __name__ == "__main__":
    main()
