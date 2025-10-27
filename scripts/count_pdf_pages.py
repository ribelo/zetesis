#!/usr/bin/env python3
"""
Quick-and-dirty PDF page counter.

Usage:
    python scripts/count_pdf_pages.py [root-dir]

Dependencies:
    pip install pypdf
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - defensive guard for ad-hoc usage
    sys.stderr.write(
        "Missing dependency: install pypdf first, e.g. `pip install pypdf`.\n"
    )
    sys.exit(1)


def count_pages(root: Path) -> tuple[int, int]:
    total_pages = 0
    total_files = 0

    for pdf_path in root.rglob("*.pdf"):
        try:
            reader = PdfReader(str(pdf_path))
            total_pages += len(reader.pages)
            total_files += 1
        except Exception as exc:  # pragma: no cover - best effort reporting
            sys.stderr.write(f"Failed to read {pdf_path}: {exc}\n")

    return total_files, total_pages


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Count PDF pages under a directory."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default="data",
        type=Path,
        help="Root directory to scan (default: %(default)s)",
    )
    args = parser.parse_args()

    root = args.root
    if not root.exists():
        sys.stderr.write(f"Path {root} does not exist.\n")
        sys.exit(1)

    files, pages = count_pages(root)
    print(f"PDF files: {files}")
    print(f"Total pages: {pages}")


if __name__ == "__main__":
    main()
