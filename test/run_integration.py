#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Integration test runner for test/integration_*.py scripts.

Usage:
  python3 test/run_integration.py
  python3 test/run_integration.py --list
  python3 test/run_integration.py --only burst,offline
  python3 test/run_integration.py --continue-on-fail

Notes:
  - Individual tests may require Redis. Some tests auto-start Redis via Docker.
  - This runner executes scripts sequentially and propagates environment variables.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def discover() -> list[Path]:
    scripts: list[Path] = []
    scripts.extend(sorted(ROOT.glob("integration_*.py")))

    # Always include legacy offline delivery scripts (kept explicit).
    for name in [
        "offline_delivery.py",
        "offline_delivery_simple.py",
        "offline_delivery_more.py",
    ]:
        p = ROOT / name
        if p.exists():
            scripts.append(p)

    # De-duplicate while preserving order.
    seen: set[Path] = set()
    out: list[Path] = []
    for p in scripts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def short_name(path: Path) -> str:
    if path.name.startswith("integration_"):
        return path.name.removeprefix("integration_").removesuffix(".py")
    return path.name.removesuffix(".py")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="List discovered integration scripts")
    ap.add_argument(
        "--only",
        default="",
        help="Comma-separated substrings to filter tests by name (e.g. 'offline,burst')",
    )
    ap.add_argument(
        "--continue-on-fail",
        action="store_true",
        help="Run all tests even if some fail",
    )
    args = ap.parse_args()

    scripts = discover()
    if not scripts:
        print("No integration scripts found.", file=sys.stderr)
        return 2

    if args.list:
        for p in scripts:
            print(short_name(p))
        return 0

    filters: list[str] = [f.strip() for f in args.only.split(",") if f.strip()]
    if filters:
        scripts = [p for p in scripts if any(f in short_name(p) for f in filters)]

    if not scripts:
        print("No scripts matched --only filter.", file=sys.stderr)
        return 2

    failures: list[tuple[str, int]] = []
    start_all = time.time()
    for p in scripts:
        name = short_name(p)
        print(f"\n=== RUN {name} ===", flush=True)
        start = time.time()
        proc = subprocess.run(
            [sys.executable, str(p)],
            cwd=str(ROOT.parent),
            env=os.environ.copy(),
        )
        elapsed = time.time() - start
        if proc.returncode == 0:
            print(f"=== OK  {name} ({elapsed:.2f}s) ===", flush=True)
        else:
            print(
                f"=== FAIL {name} rc={proc.returncode} ({elapsed:.2f}s) ===",
                file=sys.stderr,
                flush=True,
            )
            failures.append((name, proc.returncode))
            if not args.continue_on_fail:
                break

    elapsed_all = time.time() - start_all
    if failures:
        print("\nFailed tests:", file=sys.stderr)
        for n, rc in failures:
            print(f"- {n}: rc={rc}", file=sys.stderr)
        print(f"\nTOTAL: FAIL ({elapsed_all:.2f}s)", file=sys.stderr)
        return 1

    print(f"\nTOTAL: OK ({elapsed_all:.2f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

