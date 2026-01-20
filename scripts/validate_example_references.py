#!/usr/bin/env python3
"""Validate Example File References in Documentation

This script validates that all file reference callouts (📁 pattern) in
documentation actually point to existing files or directories.

Usage:
    python scripts/validate_example_references.py

Exit codes:
    0 - All references valid
    1 - Broken references found

Copyright 2026 Joe Harris / BenchBox Project
Licensed under the MIT License.
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple


def find_markdown_files(docs_dir: Path) -> List[Path]:
    """Find all markdown files in documentation directory."""
    return list(docs_dir.rglob("*.md"))


def extract_file_references(content: str, file_path: Path) -> List[Tuple[int, str, str]]:
    """Extract file references from markdown content.

    Returns:
        List of (line_number, link_text, file_path) tuples
    """
    references = []

    # Pattern: **📁 Complete[,tested example:]** ... [`text`](path)
    # Matches the 📁 callout pattern we use
    pattern = r"📁.*?\[`([^`]+)`\]\(([^)]+)\)"

    for line_num, line in enumerate(content.split("\n"), 1):
        if "📁" in line:
            for match in re.finditer(pattern, line):
                link_text = match.group(1)
                link_path = match.group(2)
                references.append((line_num, link_text, link_path))

    return references


def validate_reference(ref_path: str, doc_file: Path, repo_root: Path) -> Tuple[bool, str]:
    """Validate a single reference path.

    Args:
        ref_path: The path from the markdown link
        doc_file: The markdown file containing the reference
        repo_root: Repository root directory

    Returns:
        (is_valid, resolved_path)
    """
    # Handle relative paths
    if ref_path.startswith("../../"):
        # Relative to doc file location
        resolved = (doc_file.parent / ref_path).resolve()
    elif ref_path.startswith("../"):
        resolved = (doc_file.parent / ref_path).resolve()
    elif ref_path.startswith("/"):
        # Absolute from repo root
        resolved = repo_root / ref_path.lstrip("/")
    else:
        # Relative to doc file
        resolved = (doc_file.parent / ref_path).resolve()

    # Check if file or directory exists
    exists = resolved.exists()

    return exists, str(resolved)


def main():
    """Main validation entry point."""
    repo_root = Path(__file__).parent.parent
    docs_dir = repo_root / "docs"

    if not docs_dir.exists():
        print(f"❌ Documentation directory not found: {docs_dir}")
        return 1

    print("=" * 60)
    print("Validating Example File References")
    print("=" * 60)

    md_files = find_markdown_files(docs_dir)
    print(f"\nScanning {len(md_files)} markdown files...")

    total_references = 0
    broken_references = []

    for md_file in md_files:
        try:
            content = md_file.read_text()
            references = extract_file_references(content, md_file)

            if not references:
                continue

            rel_path = md_file.relative_to(repo_root)
            print(f"\n📄 {rel_path}")
            print(f"   Found {len(references)} file reference(s)")

            for line_num, link_text, link_path in references:
                total_references += 1
                is_valid, resolved_path = validate_reference(link_path, md_file, repo_root)

                if is_valid:
                    print(f"   ✅ Line {line_num}: {link_text}")
                else:
                    print(f"   ❌ Line {line_num}: {link_text}")
                    print(f"      Path: {link_path}")
                    print(f"      Resolved: {resolved_path}")
                    broken_references.append(
                        {
                            "file": str(rel_path),
                            "line": line_num,
                            "text": link_text,
                            "path": link_path,
                            "resolved": resolved_path,
                        }
                    )

        except Exception as e:
            print(f"   ⚠️  Error reading {md_file.name}: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("Validation Summary")
    print("=" * 60)
    print(f"Total file references: {total_references}")
    print(f"Valid references: {total_references - len(broken_references)}")
    print(f"Broken references: {len(broken_references)}")

    if broken_references:
        print("\n❌ Broken References Found:")
        for ref in broken_references:
            print(f"\n  File: {ref['file']}:{ref['line']}")
            print(f"  Link text: {ref['text']}")
            print(f"  Path: {ref['path']}")
            print(f"  Resolved to: {ref['resolved']}")
            print("  Status: File/directory does not exist")

        print("\n" + "=" * 60)
        print("❌ VALIDATION FAILED")
        print("=" * 60)
        return 1
    else:
        print("\n✅ All file references are valid!")
        return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Validation interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
