import os
import textwrap
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ci_scope import changed_paths, runtime_required


class ScopeTests(unittest.TestCase):
    def test_documentation_only(self):
        self.assertFalse(runtime_required(["README.md", "docs/data-models.md", "docs/a b.md"]))

    def test_unknown_and_runtime_inputs_require_full_checks(self):
        for path in ["Cargo.lock", "crates/a/src/lib.rs", "migrations/1.sql",
                     ".github/workflows/rust.yml", "scripts/ci_scope.py", "Makefile",
                     "sdk/python/README.md", "docs/evidence/result.json",
                     "dev/object-storage/Dockerfile", "docs/file.md.rs"]:
            with self.subTest(path=path):
                self.assertTrue(runtime_required(["docs/architecture.md", path]))
        self.assertTrue(runtime_required([]))

    def test_missing_history_fails_instead_of_skipping(self):
        with patch("ci_scope.subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                changed_paths("a" * 40, "b" * 40)

    def test_real_diff_detects_code_renamed_into_docs(self):
        with tempfile.TemporaryDirectory() as temp:
            def git(*args):
                return subprocess.check_output(["git", "-C", temp, *args], text=True).strip()
            git("init", "-q")
            git("config", "user.email", "test@example.invalid")
            git("config", "user.name", "CI scope test")
            source = Path(temp) / "source.rs"
            source.write_text("fn main() {}\n")
            git("add", ".")
            git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            (Path(temp) / "docs").mkdir()
            source.rename(Path(temp) / "docs" / "example.md")
            git("add", "-A")
            git("commit", "-qm", "rename")
            head = git("rev-parse", "HEAD")
            real_run = subprocess.run
            with patch("ci_scope.subprocess.run", side_effect=lambda *a, **kw: real_run(*a, cwd=temp, **kw)):
                paths = changed_paths(base, head)
            self.assertIn("source.rs", paths)
            self.assertTrue(runtime_required(paths))


class RequiredGateTests(unittest.TestCase):
    def test_actual_workflow_gate_fails_closed(self):
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/rust.yml"
        script = textwrap.dedent(workflow.read_text().rsplit("        run: |\n", 1)[1])
        cases = [
            ("success", "false", "skipped", True),
            ("success", "true", "success", True),
            ("failure", "", "skipped", False),
            ("cancelled", "", "skipped", False),
            ("success", "true", "failure", False),
            ("success", "true", "cancelled", False),
            ("success", "true", "skipped", False),
            ("success", "", "success", False),
            ("success", "false", "failure", False),
        ]
        jobs = ("CLIENTS_RESULT", "LINT_RESULT", "TESTS_RESULT")
        for scope, required, result, expected in cases:
            # Every runtime job alone carrying the result, the others passing
            # (or skipped for documentation-only scope), plus all three at once.
            ok = "skipped" if required == "false" else "success"
            variants = [{job: result if job == bad else ok for job in jobs} for bad in jobs]
            variants.append(dict.fromkeys(jobs, result))
            for results in variants:
                with self.subTest(scope=scope, required=required, results=results):
                    env = dict(os.environ, SCOPE_RESULT=scope,
                               RUNTIME_REQUIRED=required, **results)
                    run = subprocess.run(["bash", "-e", "-c", script], env=env,
                                         capture_output=True, check=False)
                    self.assertEqual(run.returncode == 0, expected)
