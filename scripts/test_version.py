"""Tests for scripts/version.py. Standard library and a local `git` only."""

import contextlib
import io
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import version

ROOT = Path(__file__).resolve().parents[1]
GIT_ENV = {
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
}


def copy_manifests(root):
    """Copy VERSION, CHANGELOG.md and every versioned manifest into `root`."""
    paths = {"VERSION", "CHANGELOG.md"} | {field.path for field in version.fields(ROOT)}
    paths |= {str(p.relative_to(ROOT)) for p in ROOT.glob("crates/*/Cargo.toml")}
    for name in paths:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, root / name)
    return version.read_version(root)


def snapshot(root):
    return {p: p.read_text(encoding="utf-8") for p in root.rglob("*") if p.is_file() and ".git" not in p.parts}


def run(argv, root):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = version.main(argv, root)
    return code, out.getvalue(), err.getvalue()


class ParseTests(unittest.TestCase):
    def test_accepts_only_strict_versions(self):
        self.assertEqual(version.parse("0.0.1"), (0, 0, 1))
        self.assertEqual(version.parse("10.20.30"), (10, 20, 30))
        for bad in ["v0.0.1", "0.1", "0.0.1-rc.1", "01.0.0", "0.0.1\n", " 0.0.1", "1.2.3.4", ""]:
            with self.subTest(bad=bad), self.assertRaises(version.VersionError):
                version.parse(bad)

    def test_increment_resets_lower_parts(self):
        self.assertEqual(version.increment("1.2.3", "major"), "2.0.0")
        self.assertEqual(version.increment("1.2.3", "minor"), "1.3.0")
        self.assertEqual(version.increment("1.2.3", "patch"), "1.2.4")


class ConventionalCommitTests(unittest.TestCase):
    def test_classification(self):
        cases = {
            "feat: add pools (#1)": "minor",
            "feat(api): add route": "minor",
            "fix: close leak": "patch",
            "perf(store): index lookups": "patch",
            "feat!: drop v0 route": "major",
            "refactor(api)!: rename field": "major",
            "fix: rename\n\nBREAKING CHANGE: field renamed": "major",
            "chore: tidy\n\nBREAKING-CHANGE: config moved": "major",
            "docs: explain": None,
            "test: cover": None,
            "ci: cache": None,
            "release: hudsonbox v0.0.2": None,
            "Merge branch main": None,
            "fix: mention BREAKING CHANGE inline only": "patch",
        }
        for message, level in cases.items():
            with self.subTest(message=message):
                self.assertEqual(version.classify(message)[0], level)

    def test_highest_level_wins(self):
        self.assertEqual(version.auto_level(["docs: a", "fix: b", "feat: c"]), "minor")
        self.assertEqual(version.auto_level(["fix: b", "feat!: c"]), "major")
        self.assertIsNone(version.auto_level(["docs: a", "chore: b"]))
        self.assertIsNone(version.auto_level([]))

    def test_next_version(self):
        self.assertEqual(version.next_version("0.0.1", "auto", ["fix: a"]), "0.0.2")
        self.assertEqual(version.next_version("0.0.1", "auto", ["feat: a"]), "0.1.0")
        # Breaking changes before 1.0.0 bump minor; 1.0.0 is a deliberate choice.
        self.assertEqual(version.next_version("0.3.1", "auto", ["feat!: a"]), "0.4.0")
        self.assertEqual(version.next_version("1.3.1", "auto", ["feat!: a"]), "2.0.0")
        self.assertEqual(version.next_version("0.3.1", "major"), "1.0.0")
        self.assertIsNone(version.next_version("0.0.1", "auto", ["docs: a"]))


class ChangelogTests(unittest.TestCase):
    TEXT = (
        "# Changelog\n\nIntro.\n\n## [0.0.1] - 2026-10-31\n\n### Added\n\n- first\n\n"
        "[0.0.1]: https://github.com/hudson-frontier/hudsonbox/releases/tag/v0.0.1\n"
    )

    def test_section_groups_by_type(self):
        section = version.changelog_section(
            "0.1.0",
            ["feat: pools (#2)", "fix: leak", "docs: x", "perf: faster", "feat!: drop route"],
            "2026-11-01",
        )
        self.assertTrue(section.startswith("## [0.1.0] - 2026-11-01\n"))
        self.assertIn("### Breaking changes\n\n- drop route\n", section)
        self.assertIn("### Added\n\n- pools (#2)\n", section)
        self.assertIn("### Fixed\n\n- leak\n", section)
        self.assertIn("### Performance\n\n- faster\n", section)
        self.assertNotIn("- x", section)
        self.assertIn("No user-facing changes.", version.changelog_section("0.0.2", ["docs: a"], "d"))

    def test_prepend_then_notes_round_trip(self):
        section = version.changelog_section("0.0.2", ["fix: leak"], "2026-11-01")
        text = version.prepend_changelog(self.TEXT, section, "0.0.2")
        self.assertLess(text.index("## [0.0.2]"), text.index("## [0.0.1]"))
        self.assertLess(text.index("[0.0.2]: "), text.index("[0.0.1]: "))
        self.assertTrue(text.startswith("# Changelog\n\nIntro.\n\n## [0.0.2]"))
        self.assertEqual(version.release_notes(text, "0.0.2"), "### Fixed\n\n- leak\n")
        self.assertEqual(version.release_notes(text, "0.0.1"), "### Added\n\n- first\n")
        with self.assertRaises(version.VersionError):
            version.prepend_changelog(text, section, "0.0.2")
        with self.assertRaises(version.VersionError):
            version.release_notes(text, "9.9.9")

    def test_repository_changelog_has_current_release_notes(self):
        notes = version.release_notes(
            (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), version.read_version(ROOT)
        )
        self.assertIn("### Known limitations", notes)


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.current = copy_manifests(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def snapshot(self):
        return snapshot(self.root)

    def test_repository_is_consistent(self):
        self.assertEqual(version.check(ROOT), [])

    def test_set_changes_only_version_text_and_is_idempotent(self):
        before = self.snapshot()
        version.set_version("12.34.56", self.root)
        after = self.snapshot()
        self.assertEqual(version.read_version(self.root), "12.34.56")
        problems = version.check(self.root)
        # Only the changelog lacks a 12.34.56 section; every manifest matches.
        self.assertEqual(problems, ["CHANGELOG.md: no `## [12.34.56]` section for the release notes"])
        occurrences = sum(field.count for field in version.fields(self.root)) + 1
        changed = 0
        for path, text in before.items():
            old, new = text.splitlines(), after[path].splitlines()
            self.assertEqual(len(old), len(new), path)
            for a, b in zip(old, new):
                if a != b:
                    changed += 1
                    self.assertEqual(a.replace(self.current, "12.34.56"), b, path)
        self.assertEqual(changed, occurrences)
        version.set_version("12.34.56", self.root)
        self.assertEqual(self.snapshot(), after)

    def test_check_reports_a_drifted_manifest(self):
        path = self.root / "sdk/typescript/package.json"
        path.write_text(path.read_text().replace(f'"version": "{self.current}"', '"version": "9.9.9"'))
        problems = version.check(self.root)
        self.assertEqual(len(problems), 1)
        self.assertIn("sdk/typescript/package.json:3: '9.9.9'", problems[0])
        code, _, err = run(["check"], self.root)
        self.assertEqual(code, 1)
        self.assertIn("9.9.9", err)

    def test_set_refuses_partial_rewrite_when_a_field_is_missing(self):
        path = self.root / "sdk/mcp/src/hudsonbox_mcp/__init__.py"
        path.write_text('"""Docstring only."""\n')
        before = self.snapshot()
        with self.assertRaises(version.VersionError):
            version.set_version("1.0.0", self.root)
        self.assertEqual(self.snapshot(), before)

    def test_version_file_must_be_one_strict_line(self):
        for text in ["v0.0.1\n", "0.0.1", "0.0.1\n0.0.2\n", "0.1\n"]:
            (self.root / "VERSION").write_text(text)
            with self.subTest(text=text), self.assertRaises(version.VersionError):
                version.read_version(self.root)
        code, _, err = run(["show"], self.root)
        self.assertEqual(code, 2)
        self.assertIn("version.py:", err)

    def test_set_rejects_invalid_version(self):
        code, _, err = run(["set", "v1.0.0"], self.root)
        self.assertEqual(code, 2)
        self.assertIn("strict X.Y.Z", err)
        self.assertEqual(version.read_version(self.root), self.current)


@unittest.skipUnless(shutil.which("git"), "git is required")
class AutoBumpTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.env = {**os.environ, **GIT_ENV}
        self.git("init", "-q", "-b", "main")
        self.current = copy_manifests(self.root)
        self.commit("chore: initial import")

    def tearDown(self):
        self.temporary.cleanup()

    def git(self, *args):
        subprocess.run(["git", *args], cwd=self.root, env=self.env, check=True, capture_output=True)

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", message)

    def test_no_release_needed_without_qualifying_commits(self):
        self.commit("docs: explain")
        before = snapshot(self.root)
        code, out, err = run(["bump", "auto"], self.root)
        self.assertEqual((code, out), (0, ""))
        self.assertIn("no release needed", err)
        self.assertEqual(snapshot(self.root), before)

    def test_bump_auto_uses_commits_since_last_tag(self):
        self.commit("feat: before tag")
        self.git("tag", "-a", f"v{self.current}", "-m", "release")
        self.commit("fix: after tag")
        self.commit("test: cover it")
        major, minor, patch = version.parse(self.current)
        expected = f"{major}.{minor}.{patch + 1}"
        self.assertEqual(run(["next", "auto"], self.root)[1], expected + "\n")
        code, out, _ = run(["bump", "auto"], self.root)
        self.assertEqual((code, out), (0, expected + "\n"))
        self.assertEqual(version.read_version(self.root), expected)

    def test_untagged_history_is_read_in_full(self):
        self.commit("feat: something new")
        self.commit("docs: later")
        major, minor, _ = version.parse(self.current)
        self.assertEqual(run(["next", "auto"], self.root)[1], f"{major}.{minor + 1}.0\n")

    def test_changelog_write_and_notes(self):
        self.git("tag", "-a", f"v{self.current}", "-m", "release")
        self.commit("fix: leak (#9)")
        code, _, _ = run(["changelog", "7.7.7", "--date", "2026-11-01", "--write"], self.root)
        self.assertEqual(code, 0)
        code, out, _ = run(["notes", "7.7.7"], self.root)
        self.assertEqual((code, out), (0, "### Fixed\n\n- leak (#9)\n"))


if __name__ == "__main__":
    unittest.main()
