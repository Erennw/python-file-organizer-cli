#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple


# -----------------------------
# Configuration (rules)
# -----------------------------

DEFAULT_RULES: Dict[str, Dict[str, Iterable[str]]] = {
    "Images": {"ext": ["jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "heic", "svg"]},
    "Videos": {"ext": ["mp4", "mkv", "mov", "avi", "wmv", "webm", "m4v"]},
    "Audio": {"ext": ["mp3", "wav", "flac", "aac", "m4a", "ogg", "opus"]},
    "Documents": {"ext": ["pdf", "doc", "docx", "txt", "rtf", "md", "odt"]},
    "Spreadsheets": {"ext": ["xls", "xlsx", "csv", "ods"]},
    "Presentations": {"ext": ["ppt", "pptx", "key", "odp"]},
    "Archives": {"ext": ["zip", "rar", "7z", "tar", "gz", "bz2", "xz"]},
    "Code": {"ext": ["py", "js", "ts", "java", "c", "cpp", "h", "hpp", "cs", "go", "rs", "php", "html", "css", "json", "yaml", "yml", "sql", "sh"]},
    "Executables": {"ext": ["exe", "msi", "dmg", "pkg", "deb", "rpm", "apk"]},
    "Fonts": {"ext": ["ttf", "otf", "woff", "woff2"]},
}

SPECIAL_FILES: Dict[str, Tuple[str, ...]] = {
    "README": ("readme", "readme.txt", "readme.md"),
    "LICENSE": ("license", "license.txt", "license.md"),
}


# -----------------------------
# Data structures
# -----------------------------

@dataclass(frozen=True)
class MoveAction:
    src: str
    dst: str
    time_utc: str
    src_sha256: str


# -----------------------------
# Utility helpers
# -----------------------------

def sha256_of_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def is_probably_temporary(name: str) -> bool:
    lower = name.lower()
    return (
        lower.endswith(".tmp")
        or lower.endswith(".part")
        or lower.endswith(".crdownload")
        or lower.startswith("~$")
    )


def ensure_dir(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    path.mkdir(parents=True, exist_ok=True)


def is_under(path: Path, parent: Path) -> bool:
    """Return True if 'path' is inside 'parent' (or equals), without raising on different drives."""
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


# -----------------------------
# Rule engine
# -----------------------------

def category_for_file(path: Path, rules: Dict[str, Dict[str, Iterable[str]]]) -> str:
    name_lower = path.name.lower()

    # Special files go under Documents (consistent naming)
    if name_lower in SPECIAL_FILES["README"] or name_lower in SPECIAL_FILES["LICENSE"]:
        return "Documents"

    if is_probably_temporary(path.name):
        return "Temp"

    ext = path.suffix.lower().lstrip(".")
    if not ext:
        return "NoExtension"

    for category, spec in rules.items():
        exts = set(e.lower() for e in spec.get("ext", []))
        if ext in exts:
            return category

    return "Other"


# -----------------------------
# File iteration
# -----------------------------

def iter_files(
    root: Path,
    recursive: bool,
    include_hidden: bool,
    exclude_dirs: Iterable[str],
) -> Iterator[Path]:
    exclude_set = {d.lower() for d in exclude_dirs}

    def should_skip_dir(p: Path) -> bool:
        return p.is_dir() and p.name.lower() in exclude_set

    if recursive:
        for dirpath, dirnames, filenames in os.walk(root):
            dirpath_p = Path(dirpath)

            # Prune excluded/hidden directories
            dirnames[:] = [
                d for d in dirnames
                if not should_skip_dir(dirpath_p / d)
                and (include_hidden or not d.startswith("."))
            ]

            for fn in filenames:
                if not include_hidden and fn.startswith("."):
                    continue
                yield dirpath_p / fn
    else:
        for p in root.iterdir():
            if p.is_dir():
                continue
            if not include_hidden and p.name.startswith("."):
                continue
            yield p


# -----------------------------
# Move logic + duplicates
# -----------------------------

def resolve_destination(dst: Path, duplicates: str) -> Optional[Path]:
    """
    Return a destination path to use.
    - overwrite: keep dst
    - skip: return None if dst exists
    - rename: find a free name
    """
    if not dst.exists():
        return dst

    if duplicates == "overwrite":
        return dst

    if duplicates == "skip":
        return None

    # rename
    stem = dst.stem
    suffix = dst.suffix
    parent = dst.parent
    i = 1
    while True:
        candidate = parent / f"{stem} ({i}){suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def move_file(
    src: Path,
    dst: Path,
    dry_run: bool,
    duplicates: str,
    transaction_log: Optional[Path],
) -> Optional[MoveAction]:
    dst_resolved = resolve_destination(dst, duplicates)
    if dst_resolved is None:
        logging.info("SKIP (exists): %s", src)
        return None

    if src.resolve() == dst_resolved.resolve():
        return None

    file_hash = sha256_of_file(src)
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    action = MoveAction(src=str(src), dst=str(dst_resolved), time_utc=ts, src_sha256=file_hash)

    logging.info("MOVE: %s -> %s", src, dst_resolved)

    if not dry_run:
        ensure_dir(dst_resolved.parent, dry_run=False)
        if dst_resolved.exists() and duplicates == "overwrite":
            dst_resolved.unlink()
        shutil.move(str(src), str(dst_resolved))

        if transaction_log:
            with transaction_log.open("a", encoding="utf-8") as f:
                f.write(json.dumps(action.__dict__) + "\n")

    return action


# -----------------------------
# Undo logic
# -----------------------------

def read_actions(transaction_log: Path) -> list[MoveAction]:
    actions: list[MoveAction] = []
    if not transaction_log.exists():
        return actions

    with transaction_log.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            actions.append(MoveAction(**obj))
    return actions


def undo_moves(transaction_log: Path, dry_run: bool) -> None:
    actions = read_actions(transaction_log)
    if not actions:
        print("Nothing to undo. No transaction log found or it is empty.")
        return

    actions = list(reversed(actions))
    undone = 0

    for a in actions:
        src = Path(a.src)
        dst = Path(a.dst)

        if not dst.exists():
            logging.warning("UNDO SKIP (missing): %s", dst)
            continue

        current_hash = sha256_of_file(dst)
        if current_hash != a.src_sha256:
            logging.warning("UNDO SKIP (hash mismatch): %s", dst)
            continue

        logging.info("UNDO: %s -> %s", dst, src)
        if not dry_run:
            ensure_dir(src.parent, dry_run=False)

            # Avoid overwriting if src already exists
            if src.exists():
                src_backup = src.with_name(src.name + ".backup")
                i = 1
                while src_backup.exists():
                    src_backup = src.with_name(src.name + f".backup{i}")
                    i += 1
                shutil.move(str(src), str(src_backup))
                logging.warning("Existing original saved as backup: %s", src_backup)

            shutil.move(str(dst), str(src))
        undone += 1

    if dry_run:
        print("Dry-run undo complete (no files were moved).")
        return

    archived = transaction_log.with_suffix(".undone.jsonl")
    shutil.move(str(transaction_log), str(archived))
    print(f"Undo complete. Archived transaction log: {archived}. Actions undone: {undone}")


# -----------------------------
# Main organize routine
# -----------------------------

def organize(
    root: Path,
    recursive: bool,
    dry_run: bool,
    duplicates: str,
    include_hidden: bool,
    exclude_dirs: Iterable[str],
    keep_structure: bool,
    transaction_log: Optional[Path],
) -> None:
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Root directory not found or not a directory: {root}")

    organized_dir = root / "Organized"
    # Force exclusion of "Organized" to avoid self-scanning/moving
    exclude_dirs_effective = list(dict.fromkeys([*exclude_dirs, "Organized"]))

    moved = 0
    scanned = 0

    for file_path in iter_files(
        root,
        recursive=recursive,
        include_hidden=include_hidden,
        exclude_dirs=exclude_dirs_effective,
    ):
        scanned += 1
        if not file_path.is_file():
            continue

        # Extra safety: ignore anything already under Organized
        if is_under(file_path, organized_dir):
            continue

        cat = category_for_file(file_path, DEFAULT_RULES)

        base = organized_dir / cat

        if keep_structure and recursive:
            rel = file_path.parent.relative_to(root)
            dst = base / rel / file_path.name
        else:
            dst = base / file_path.name

        action = move_file(
            file_path,
            dst,
            dry_run=dry_run,
            duplicates=duplicates,
            transaction_log=transaction_log,
        )
        if action:
            moved += 1

    summary = f"Scanned: {scanned}, Moved: {moved}, Dry-run: {dry_run}"
    logging.info(summary)
    print(summary)


# -----------------------------
# CLI
# -----------------------------

def setup_logging(logfile: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(logfile, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="organizer",
        description="Modern File Organizer (safe, rules-based, undoable).",
    )
    p.add_argument("--log", dest="logfile", type=str, default="organizer.log",
                   help="Log file path. Default: organizer.log")

    sub = p.add_subparsers(dest="cmd", required=True)

    org = sub.add_parser("organize", help="Organize files in a directory.")
    org.add_argument("path", type=str, help="Root directory to organize.")
    org.add_argument("--recursive", action="store_true", help="Scan subfolders recursively.")
    org.add_argument("--dry-run", action="store_true", help="Preview actions without moving files.")
    org.add_argument(
        "--duplicates",
        choices=["rename", "skip", "overwrite"],
        default="rename",
        help="What to do when the destination file already exists.",
    )
    org.add_argument("--include-hidden", action="store_true", help="Include hidden files (dotfiles).")
    org.add_argument(
        "--exclude-dir",
        action="append",
        default=[],
        help="Directory name to exclude (can be used multiple times).",
    )
    org.add_argument(
        "--keep-structure",
        action="store_true",
        help="When used with --recursive, preserve subfolder structure under each category.",
    )
    org.add_argument(
        "--transaction-log",
        type=str,
        default=".organizer_transactions.jsonl",
        help="Transaction log file for undo. Default: .organizer_transactions.jsonl",
    )

    und = sub.add_parser("undo", help="Undo the last organization using the transaction log.")
    und.add_argument("--path", type=str, required=True, help="Root directory that was organized.")
    und.add_argument("--dry-run", action="store_true", help="Preview undo without moving files.")
    und.add_argument(
        "--transaction-log",
        type=str,
        default=".organizer_transactions.jsonl",
        help="Transaction log file used during organize. Default: .organizer_transactions.jsonl",
    )

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logfile = Path(args.logfile).expanduser().resolve()
    setup_logging(logfile)

    if args.cmd == "organize":
        root = Path(args.path).expanduser().resolve()
        transaction_log = Path(args.transaction_log).expanduser().resolve()

        organize(
            root=root,
            recursive=args.recursive,
            dry_run=args.dry_run,
            duplicates=args.duplicates,
            include_hidden=args.include_hidden,
            exclude_dirs=args.exclude_dir,
            keep_structure=args.keep_structure,
            transaction_log=transaction_log,
        )
        return

    if args.cmd == "undo":
        root = Path(args.path).expanduser().resolve()
        transaction_log = Path(args.transaction_log).expanduser().resolve()

        # If transaction log path is relative, interpret it relative to the provided root
        if not transaction_log.is_absolute():
            transaction_log = (root / transaction_log).resolve()

        undo_moves(transaction_log=transaction_log, dry_run=args.dry_run)
        return


if __name__ == "__main__":
    main()
