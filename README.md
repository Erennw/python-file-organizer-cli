# Python File Organizer

A safe and modern command-line tool to organize files in a directory based on file types, with support for dry-run previews and undo operations.

## Features
- Organizes files into categorized folders (Images, Videos, Documents, etc.)
- Supports recursive directory scanning
- Dry-run mode to preview actions without moving files
- Duplicate handling (rename, skip, overwrite)
- Transaction log with full undo support
- Optional preservation of folder structure
- Logging for transparency and safety

## Technologies Used
- Python 3
- argparse, pathlib, dataclasses

## Usage

### Organize files
```bash
python organizer.py organize /path/to/folder --recursive
