#!/usr/bin/env python3
"""Conservatively identify documentation-only PR diffs for the Rust CI gate."""
import argparse
import subprocess


ROOT_DOCS = {"README.md", "CONTRIBUTING.md", "SECURITY.md"}


def runtime_required(paths):
    # Unknown paths, evidence JSON, workflows and build inputs always run CI.
    return not paths or any(
        path not in ROOT_DOCS
        and not (path.startswith("docs/") and path.endswith(".md"))
        for path in paths
    )


def changed_paths(base, head):
    # Disable rename detection so moving code into docs cannot hide its deletion.
    result = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", "-z", f"{base}...{head}", "--"],
        check=True, capture_output=True,
    )
    return [path.decode("utf-8", errors="surrogateescape")
            for path in result.stdout.split(b"\0") if path]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()
    # Only commit IDs supplied by GitHub, never options or arbitrary ref syntax.
    for sha in (args.base, args.head):
        if len(sha) != 40 or any(c not in "0123456789abcdef" for c in sha):
            parser.error("base and head must be full lowercase commit SHAs")
    print(f"runtime={str(runtime_required(changed_paths(args.base, args.head))).lower()}")


if __name__ == "__main__":
    main()
