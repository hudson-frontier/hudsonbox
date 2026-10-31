#!/usr/bin/env python3
"""Hudsonbox version: one source of truth, consistency checks and bumps.

The root VERSION file holds the product version as strict SemVer `X.Y.Z`
(no `v` prefix); release tags are `vX.Y.Z`. Every manifest that carries the
product version must equal it. See docs/versioning.md.

    version.py show                        print VERSION
    version.py check                       fail if any manifest differs from VERSION
    version.py set X.Y.Z                   rewrite VERSION and every manifest
    version.py next {major,minor,patch,auto}
    version.py bump {major,minor,patch,auto}
    version.py changelog X.Y.Z [--date D] [--write]
    version.py notes X.Y.Z                 print the CHANGELOG.md section for X.Y.Z

`auto` reads Conventional Commit messages since the newest `v*` tag (all history
when there is none). Standard library only; Python 3.11+.
"""

import argparse
import datetime
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_URL = "https://github.com/hudson-frontier/hudsonbox"
SEMVER = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)")
LEVELS = ("major", "minor", "patch")
CONVENTIONAL = re.compile(r"^(?P<type>[A-Za-z]+)(?:\([^)]*\))?(?P<bang>!)?: ?(?P<description>.+)$")
BREAKING_FOOTER = re.compile(r"^BREAKING[ -]CHANGE: ?", re.MULTILINE)
SECTION = re.compile(r"^## \[(?P<version>[^\]]+)\]", re.MULTILINE)


class VersionError(Exception):
    pass


def parse(text):
    """Return (major, minor, patch) for a strict `X.Y.Z` string."""
    match = SEMVER.fullmatch(text)
    if not match:
        raise VersionError(f"not a strict X.Y.Z version: {text!r}")
    return tuple(int(part) for part in match.groups())


def increment(version, level):
    major, minor, patch = parse(version)
    if level == "major":
        return f"{major + 1}.0.0"
    if level == "minor":
        return f"{major}.{minor + 1}.0"
    if level == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise VersionError(f"unknown bump level: {level!r}")


@dataclass(frozen=True)
class Field:
    """One version occurrence: `pattern` has exactly one group, `v`."""

    path: str
    pattern: str
    count: int = 1

    def matches(self, text):
        return list(re.finditer(self.pattern, text, re.MULTILINE))


def workspace_crates(root):
    names = []
    for manifest in sorted((root / "crates").glob("*/Cargo.toml")):
        names.append(tomllib.loads(manifest.read_text(encoding="utf-8"))["package"]["name"])
    return names


def fields(root):
    crates = workspace_crates(root)
    lock = "|".join(re.escape(name) for name in crates)
    return [
        Field("Cargo.toml", r'^\[workspace\.package\]\n(?:[^\[\n].*\n|\n)*?version = "(?P<v>[^"]*)"'),
        # Workspace members are the only lock entries without a `source`.
        Field("Cargo.lock", rf'^name = "(?:{lock})"\nversion = "(?P<v>[^"]*)"\n(?!source)', len(crates)),
        Field("api/openapi.json", r'^  "info": \{\n(?:    .*\n)*?    "version": "(?P<v>[^"]*)"'),
        Field("api/conformance/workflow.json", r'^  "api_version": "(?P<v>[^"]*)"'),
        Field("sdk/python/pyproject.toml", r'^\[project\]\n(?:[^\[\n].*\n|\n)*?version = "(?P<v>[^"]*)"'),
        # Generated from api/openapi.json; `generate_api.py --check` keeps them exact.
        Field("sdk/python/src/hudsonbox/_schema.py", r"'version': '(?P<v>[^']*)'\}\n?\Z"),
        Field("sdk/typescript/src/schema.ts", r'^  "version": "(?P<v>[^"]*)"\n\};\n?\Z'),
        Field("sdk/typescript/package.json", r'\A\{\n  "name": "[^"]*",\n  "version": "(?P<v>[^"]*)"'),
        Field("sdk/typescript/package-lock.json", r'\A\{\n  "name": "[^"]*",\n  "version": "(?P<v>[^"]*)"'),
        Field("sdk/typescript/package-lock.json", r'^    "": \{\n      "name": "[^"]*",\n      "version": "(?P<v>[^"]*)"'),
        Field("sdk/mcp/pyproject.toml", r'^\[project\]\n(?:[^\[\n].*\n|\n)*?version = "(?P<v>[^"]*)"'),
        Field("sdk/mcp/pyproject.toml", r'"hudsonbox-client==(?P<v>[^"]*)"'),
        Field("sdk/mcp/src/hudsonbox_mcp/__init__.py", r'^__version__ = "(?P<v>[^"]*)"$'),
    ]


def read_version(root=ROOT):
    path = root / "VERSION"
    text = path.read_text(encoding="utf-8")
    if not text.endswith("\n") or text.count("\n") != 1:
        raise VersionError("VERSION must be exactly one line ending in a newline")
    version = text[:-1]
    parse(version)
    return version


def check(root=ROOT):
    """Return a list of human-readable mismatches against VERSION."""
    version = read_version(root)
    problems = []
    for field in fields(root):
        path = root / field.path
        if not path.is_file():
            problems.append(f"{field.path}: missing")
            continue
        found = field.matches(path.read_text(encoding="utf-8"))
        if len(found) != field.count:
            problems.append(
                f"{field.path}: expected {field.count} version occurrence(s) for "
                f"/{field.pattern}/, found {len(found)}"
            )
        for match in found:
            if match["v"] != version:
                line = match.string.count("\n", 0, match.start("v")) + 1
                problems.append(f"{field.path}:{line}: {match['v']!r} != VERSION {version!r}")
    changelog = root / "CHANGELOG.md"
    sections = SECTION.findall(changelog.read_text(encoding="utf-8")) if changelog.is_file() else []
    if version not in sections:
        problems.append(f"CHANGELOG.md: no `## [{version}]` section for the release notes")
    return problems


def set_version(new, root=ROOT):
    """Rewrite VERSION and every field, changing only the version text."""
    parse(new)
    planned = {}
    for field in fields(root):
        path = root / field.path
        text = planned.get(path) or path.read_text(encoding="utf-8")
        found = field.matches(text)
        if len(found) != field.count:
            raise VersionError(
                f"{field.path}: expected {field.count} version occurrence(s), found {len(found)}; "
                "refusing a partial rewrite"
            )
        for match in reversed(found):
            text = text[: match.start("v")] + new + text[match.end("v") :]
        planned[path] = text
    planned[root / "VERSION"] = new + "\n"
    for path, text in planned.items():
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")


def git(root, *args):
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


def last_tag(root=ROOT):
    result = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0", "--match", "v[0-9]*"],
        cwd=root, capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def commits_since(tag, root=ROOT):
    """Return full commit messages reachable from HEAD but not from `tag`."""
    revision = f"{tag}..HEAD" if tag else "HEAD"
    output = git(root, "log", "--no-merges", "--format=%B%x00", revision)
    return [message.strip() for message in output.split("\0") if message.strip()]


def classify(message):
    """Return (level or None, change type, description) for one commit message."""
    subject, _, body = message.partition("\n")
    match = CONVENTIONAL.match(subject.strip())
    breaking = bool(BREAKING_FOOTER.search(body))
    if not match:
        return ("major" if breaking else None), None, subject.strip()
    kind = match["type"].lower()
    description = match["description"].strip()
    if breaking or match["bang"]:
        return "major", kind, description
    if kind == "feat":
        return "minor", kind, description
    if kind in ("fix", "perf"):
        return "patch", kind, description
    return None, kind, description


def auto_level(messages):
    levels = {classify(message)[0] for message in messages}
    for level in LEVELS:
        if level in levels:
            return level
    return None


def next_version(current, level, messages=()):
    """Return the next version, or None when `auto` finds nothing releasable.

    Before 1.0.0 a breaking change found by `auto` bumps the minor version: the
    0.x series may break on minor releases, and 1.0.0 is a deliberate `major`.
    """
    if level == "auto":
        level = auto_level(messages)
        if level is None:
            return None
        if level == "major" and parse(current)[0] == 0:
            level = "minor"
    return increment(current, level)


def changelog_section(version, messages, date):
    groups = {"Breaking changes": [], "Added": [], "Fixed": [], "Performance": []}
    for message in messages:
        level, kind, description = classify(message)
        if level == "major":
            groups["Breaking changes"].append(description)
        elif kind == "feat":
            groups["Added"].append(description)
        elif kind == "fix":
            groups["Fixed"].append(description)
        elif kind == "perf":
            groups["Performance"].append(description)
    lines = [f"## [{version}] - {date}", ""]
    for heading, items in groups.items():
        if items:
            lines += [f"### {heading}", "", *(f"- {item}" for item in items), ""]
    if len(lines) == 2:
        lines += ["No user-facing changes.", ""]
    return "\n".join(lines)


def prepend_changelog(text, section, version):
    """Insert `section` before the newest release section and add its link."""
    if re.search(rf"^## \[{re.escape(version)}\]", text, re.MULTILINE):
        raise VersionError(f"CHANGELOG.md already has a section for {version}")
    first = SECTION.search(text)
    position = first.start() if first else len(text)
    head = text[:position].rstrip("\n") + "\n\n"
    text = head + section.rstrip("\n") + "\n\n" + text[position:].lstrip("\n")
    link = f"[{version}]: {REPOSITORY_URL}/releases/tag/v{version}"
    # Newest link first, matching the section order.
    lines = text.rstrip("\n").split("\n")
    for index, line in enumerate(lines):
        if re.match(r"^\[\d+\.\d+\.\d+\]: ", line):
            lines.insert(index, link)
            return "\n".join(lines) + "\n"
    return "\n".join(lines) + "\n\n" + link + "\n"


def release_notes(text, version):
    """Return the body of the CHANGELOG.md section for `version`."""
    sections = list(SECTION.finditer(text))
    for index, match in enumerate(sections):
        if match["version"] == version:
            start = text.index("\n", match.start()) + 1
            end = sections[index + 1].start() if index + 1 < len(sections) else len(text)
            body = text[start:end]
            body = re.sub(r"^\[\d+\.\d+\.\d+\]: .*$", "", body, flags=re.MULTILINE)
            return body.strip() + "\n"
    raise VersionError(f"CHANGELOG.md has no section for {version}")


def main(argv=None, root=ROOT):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("show", help="print VERSION")
    commands.add_parser("check", help="fail if any manifest differs from VERSION")
    setter = commands.add_parser("set", help="rewrite VERSION and every manifest")
    setter.add_argument("version")
    for name in ("next", "bump"):
        command = commands.add_parser(name, help=f"{name} version by level")
        command.add_argument("level", choices=(*LEVELS, "auto"))
    changelog = commands.add_parser("changelog", help="generate a CHANGELOG.md section from commits")
    changelog.add_argument("version")
    changelog.add_argument("--date", default=datetime.date.today().isoformat())
    changelog.add_argument("--write", action="store_true", help="prepend it to CHANGELOG.md")
    notes = commands.add_parser("notes", help="print the CHANGELOG.md section for a version")
    notes.add_argument("version")
    args = parser.parse_args(argv)

    try:
        if args.command == "show":
            print(read_version(root))
        elif args.command == "check":
            problems = check(root)
            if problems:
                print("\n".join(problems), file=sys.stderr)
                print("Run `python3 scripts/version.py set X.Y.Z` to resynchronize.", file=sys.stderr)
                return 1
            print(f"All manifests match VERSION {read_version(root)}.")
        elif args.command == "set":
            set_version(args.version, root)
            print(args.version)
        elif args.command in ("next", "bump"):
            current = read_version(root)
            messages = commits_since(last_tag(root), root) if args.level == "auto" else ()
            new = next_version(current, args.level, messages)
            if new is None:
                print("no release needed", file=sys.stderr)
                return 0
            if args.command == "bump":
                set_version(new, root)
            print(new)
        elif args.command == "changelog":
            parse(args.version)
            section = changelog_section(args.version, commits_since(last_tag(root), root), args.date)
            if args.write:
                path = root / "CHANGELOG.md"
                path.write_text(prepend_changelog(path.read_text(encoding="utf-8"), section, args.version), encoding="utf-8")
            else:
                print(section, end="")
        elif args.command == "notes":
            parse(args.version)
            print(release_notes((root / "CHANGELOG.md").read_text(encoding="utf-8"), args.version), end="")
    except VersionError as error:
        print(f"version.py: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
