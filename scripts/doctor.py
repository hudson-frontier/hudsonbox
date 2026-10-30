#!/usr/bin/env python3
"""Local prerequisites only: no installation, service startup or isolation claim."""

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path

# Importing the sibling preflight must not create a repository __pycache__ file.
sys.dont_write_bytecode = True
from check_linux_host import inspect_host

ROOT = Path(__file__).resolve().parents[1]
SCOPE = (
    "Read-only prerequisite checks. No services, workloads or isolation tests run. "
    "Tool presence does not verify dependency health, images, delegated cgroups, "
    "network policy or the supported x86_64 release gates."
)
TIMEOUT = 3


def check(name, status, detail, action=""):
    return {"name": name, "status": status, "detail": detail, "action": action}


def probe(command, pattern, action, minimum=None, exact=None, rust_channel=None):
    """Probe fixed version commands; discard raw output and never use a shell."""
    name = " ".join(command[:-1])
    executable = shutil.which(command[0])
    if executable is None:
        return check(name, "missing", "Executable not found on PATH", action)
    environment = os.environ.copy()
    environment.update(RUSTUP_AUTO_INSTALL="0", CARGO_NET_OFFLINE="true")
    if rust_channel is not None:
        environment["RUSTUP_TOOLCHAIN"] = rust_channel
    try:
        result = subprocess.run(
            [executable, *command[1:]],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=TIMEOUT,
            env=environment,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return check(name, "failed", "Version probe timed out", action)
    except OSError:
        return check(name, "failed", "Cannot execute version probe", action)
    # Never print stderr, arbitrary tool output, paths or environment values.
    if result.returncode != 0:
        return check(name, "failed", "Version probe returned a nonzero exit", action)
    match = re.search(pattern, result.stdout)
    if match is None:
        return check(name, "failed", "Unrecognized version response", action)
    version = match.group(1)
    if minimum is not None and tuple(map(int, version.split("."))) < minimum:
        return check(name, "failed", "Version " + version + " is too old", action)
    if exact is not None and version != exact:
        return check(name, "failed", "Version " + version + " differs from repository pin", action)
    return check(name, "passed", "Version " + version)


def inspect(profile):
    system = platform.system()
    checks = []
    checks.append(check(
        "python", "passed" if sys.version_info >= (3, 11) else "failed",
        "Version " + platform.python_version(),
        "Use Python 3.11+; see docs/local-development.md",
    ))
    if profile == "control-plane":
        checks.append(check(
            "platform", "passed" if system in ("Darwin", "Linux") else "failed",
            "Native control-plane development on " + system,
            "Use macOS or Linux; see docs/local-development.md",
        ))
        # Read the repository pin rather than duplicating a toolchain version.
        import tomllib

        channel = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
        setup = "Install the pinned toolchain explicitly; see rust-toolchain.toml"
        checks.extend([
            probe(["rustc", "--version"], r"^rustc (\d+\.\d+\.\d+)(?:\s|$)", setup,
                  exact=channel, rust_channel=channel),
            probe(["cargo", "--version"], r"^cargo (\d+\.\d+\.\d+)(?:\s|$)", setup,
                  exact=channel, rust_channel=channel),
            probe(["protoc", "--version"], r"^libprotoc (\d+(?:\.\d+)+)\b",
                  "Install protobuf; see docs/local-development.md"),
            probe(["docker", "--version"], r"^Docker version (\d+\.\d+\.\d+)\b",
                  "Configure a local container engine; see docs/local-development.md"),
            probe(["docker", "compose", "version"], r"^Docker Compose version v?(\d+\.\d+\.\d+)\b",
                  "Install Compose v2; see docs/local-development.md", minimum=(2, 0, 0)),
            probe(["node", "--version"], r"^v(\d+\.\d+\.\d+)\s*$",
                  "Use Node 24.9.0+ for SDK checks; see docs/language-clients.md", minimum=(24, 9, 0)),
            probe(["npm", "--version"], r"^(\d+\.\d+\.\d+)\s*$",
                  "Install npm for SDK checks; see docs/language-clients.md"),
        ])
    elif system != "Linux":
        checks.append(check(
            "linux-runtime", "failed", "Firecracker needs Linux/KVM; macOS cannot run it directly",
            "Run this profile inside the dedicated Linux VM; see docs/linux-development.md",
        ))
    else:
        host = inspect_host()
        checks.append(check(
            "linux-kvm", "failed" if host["errors"] else "passed",
            "; ".join(host["errors"]) if host["errors"] else "KVM API 12 and root cgroup controllers available",
            "See docs/linux-development.md; do not infer delegation or isolation from this check",
        ))
        if host["architecture"] == "aarch64":
            checks.append(check("evidence-scope", "info", "aarch64 development; not x86_64 release qualification"))
        setup = "Put a matching verified Firecracker/jailer pair on PATH; see docs/linux-development.md"
        firecracker = probe(["firecracker", "--version"], r"^Firecracker v(\d+\.\d+\.\d+)\b", setup)
        jailer = probe(["jailer", "--version"], r"^Jailer v(\d+\.\d+\.\d+)\b", setup)
        checks.extend([firecracker, jailer])
        if firecracker["status"] == jailer["status"] == "passed":
            matches = firecracker["detail"] == jailer["detail"]
            checks.append(check("firecracker-jailer-pair", "passed" if matches else "failed",
                                "Matching versions" if matches else "Version mismatch", setup))
    return {
        "schema_version": 1,
        "profile": profile,
        "system": system,
        "architecture": platform.machine(),
        "scope": SCOPE,
        "ready": all(item["status"] in ("passed", "info") for item in checks),
        "checks": checks,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("control-plane", "linux-runtime"), default="control-plane")
    parser.add_argument("--json", action="store_true", help="Versioned machine-readable report")
    args = parser.parse_args(argv)
    # tomllib is needed for the repository pin; fail clearly on older Python.
    if sys.version_info < (3, 11):
        report = {"schema_version": 1, "profile": args.profile, "system": platform.system(),
                  "architecture": platform.machine(), "scope": SCOPE, "ready": False,
                  "checks": [check("python", "failed", "Python 3.11+ required", "Use Python 3.11+")]}
    else:
        report = inspect(args.profile)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("Hudsonbox prerequisites: " + report["profile"])
        for item in report["checks"]:
            print(item["status"] + "  " + item["name"] + ": " + item["detail"])
            if item["status"] in ("failed", "missing") and item["action"]:
                print("  " + item["action"])
        print(report["scope"])
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
