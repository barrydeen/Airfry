#!/usr/bin/env python3
"""Byte-for-byte conformance check: airfry-negentropy Rust harness vs the strfry/negentropy
reference C++ harness.

Generates identical item sets, drives both harnesses through a full reconciliation, and
compares every emitted line. Exits non-zero on any mismatch.

Usage: python3 scripts/negentropy_conformance.py
"""
import hashlib
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CPP_HARNESS = os.path.join(
    ROOT, "third_party/strfry/external/negentropy/test/cpp/harness"
)
RUST_HARNESS = os.path.join(ROOT, "target/release/negentropy-harness")


def make_id(seed: int) -> str:
    return hashlib.sha256(seed.to_bytes(8, "little")).hexdigest()


def build_script(n, offset, frame_limit=None):
    """Build a harness input script: insert n items then seal."""
    lines = []
    for i in range(n):
        seed = offset + i
        lines.append(f"item,{1700000000 + seed},{make_id(seed)}")
    lines.append("seal")
    return "\n".join(lines) + "\n"


def run_harness(binary, script, frame_limit=None):
    env = dict(os.environ)
    if frame_limit:
        env["FRAMESIZELIMIT"] = str(frame_limit)
    p = subprocess.run(
        [binary], input=script, capture_output=True, text=True, env=env, timeout=120
    )
    if p.returncode != 0:
        raise RuntimeError(f"{binary} failed: {p.stderr}")
    return p.stdout


def drive_pair(client_bin, server_bin, client_script, server_script, frame_limit=None):
    """Drive a full reconciliation between two harness processes, returning the transcript."""
    env = dict(os.environ)
    if frame_limit:
        env["FRAMESIZELIMIT"] = str(frame_limit)

    c = subprocess.Popen(
        [client_bin], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, env=env,
    )
    s = subprocess.Popen(
        [server_bin], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, env=env,
    )

    def send(proc, line):
        proc.stdin.write(line + "\n")
        proc.stdin.flush()

    def recv(proc):
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError("harness closed unexpectedly")
        return line.strip()

    transcript = []
    c.stdin.write(client_script)
    c.stdin.flush()
    s.stdin.write(server_script)
    s.stdin.flush()

    send(c, "initiate")
    msg = recv(c)
    transcript.append(("client", msg))

    for _ in range(10000):
        send(s, msg)
        reply = recv(s)
        transcript.append(("server", reply))
        if reply == "msg,":
            break
        send(c, reply)
        # client may emit have/need lines before its msg/done
        while True:
            line = recv(c)
            transcript.append(("client", line))
            if line.startswith("msg,") or line == "done":
                break
        if line == "done":
            break
        msg = line

    c.stdin.close()
    s.stdin.close()
    c.wait(timeout=30)
    s.wait(timeout=30)
    return transcript


def compare(name, client_bin, server_bin, client_script, server_script, frame_limit=None):
    t = drive_pair(client_bin, server_bin, client_script, server_script, frame_limit)
    return t


def main():
    if not os.path.exists(CPP_HARNESS):
        print(f"SKIP: reference harness not built at {CPP_HARNESS}")
        return 0
    if not os.path.exists(RUST_HARNESS):
        print(f"SKIP: rust harness not built at {RUST_HARNESS}")
        return 0

    scenarios = [
        ("identical-100", build_script(100, 0), build_script(100, 0), None),
        ("disjoint-100", build_script(100, 0), build_script(100, 100), None),
        ("partial-200", build_script(200, 0), build_script(200, 100), None),
        ("identical-1000", build_script(1000, 0), build_script(1000, 0), None),
        ("disjoint-1000", build_script(1000, 0), build_script(1000, 1000), None),
        ("framelimit-500", build_script(500, 0), build_script(500, 250), 60000),
    ]

    failures = 0
    for name, cs, ss, fl in scenarios:
        # Rust client vs C++ server
        rust_vs_cpp = compare(name, RUST_HARNESS, CPP_HARNESS, cs, ss, fl)
        # C++ client vs Rust server
        cpp_vs_rust = compare(name, CPP_HARNESS, RUST_HARNESS, cs, ss, fl)
        # Rust vs Rust (self-consistency)
        rust_vs_rust = compare(name, RUST_HARNESS, RUST_HARNESS, cs, ss, fl)

        # Compare the transcripts: rust_vs_cpp and cpp_vs_rust should be identical
        # (same messages exchanged), and rust_vs_rust should match too.
        ok = rust_vs_cpp == cpp_vs_rust == rust_vs_rust
        status = "OK" if ok else "MISMATCH"
        print(f"[{status}] {name}: {len(rust_vs_cpp)} transcript lines")
        if not ok:
            failures += 1
            for i, (a, b, c) in enumerate(zip(rust_vs_cpp, cpp_vs_rust, rust_vs_rust)):
                if not (a == b == c):
                    print(f"  line {i}: rust/cpp={a} cpp/rust={b} rust/rust={c}")
                    break

    if failures:
        print(f"\nFAILED: {failures} scenario(s) mismatched")
        return 1
    print("\nAll conformance scenarios matched byte-for-byte.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
