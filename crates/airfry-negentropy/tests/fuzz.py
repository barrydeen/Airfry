#!/usr/bin/env python3
"""Port of the upstream negentropy `test/fuzz.pl` interop driver.

The upstream perl driver depends on `Session::Token` (not installed here), so this script
reproduces its semantics exactly: it generates two item sets with the same probability model
(P1/P2/P3, CLUSTERED, NODUPS, RECS, FRAMESIZELIMIT1/2), drives two harness binaries over the
line protocol, and verifies that both sides converge to identical sets.

Usage:
    fuzz.py <harness1> <harness2> [--recs N] [--p1 P] [--p2 P] [--p3 P] [--clustered]
            [--nodups] [--frame1 N] [--frame2 N] [--seed N] [--deltatrace]

Exit code 0 on success (prints "-----------OK-----------"), non-zero on mismatch.
"""

import argparse
import os
import random
import subprocess
import sys


def rnd(n):
    return int(random.random() * n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("h1")
    ap.add_argument("h2")
    ap.add_argument("--recs", type=int, default=None)
    ap.add_argument("--min-recs", type=int, default=1)
    ap.add_argument("--max-recs", type=int, default=10000)
    ap.add_argument("--p1", type=float, default=1)
    ap.add_argument("--p2", type=float, default=1)
    ap.add_argument("--p3", type=float, default=98)
    ap.add_argument("--clustered", action="store_true")
    ap.add_argument("--nodups", action="store_true")
    ap.add_argument("--frame1", type=int, default=None)
    ap.add_argument("--frame2", type=int, default=None)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--deltatrace", action="store_true")
    ap.add_argument("--num-segs", type=int, default=50000)
    ap.add_argument("--recs-per-seg", type=int, default=50)
    args = ap.parse_args()

    random.seed(args.seed)

    total = args.p1 + args.p2 + args.p3
    if total == 0:
        sys.exit("zero prob")
    p1, p2, p3 = args.p1 / total, args.p2 / total, args.p3 / total

    min_recs, max_recs = args.min_recs, args.max_recs
    if args.recs is not None:
        min_recs = max_recs = args.recs

    def spawn(cmd, frame):
        env = dict(os.environ)
        if frame is not None:
            env["FRAMESIZELIMIT"] = str(frame)
        else:
            env.pop("FRAMESIZELIMIT", None)
        return subprocess.Popen(
            cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            env=env, text=True, bufsize=1,
        )

    p_1 = spawn(args.h1, args.frame1)
    p_2 = spawn(args.h2, args.frame2)

    ids1, ids2 = {}, {}

    def send(proc, line):
        proc.stdin.write(line + "\n")
        proc.stdin.flush()

    def recv(proc):
        line = proc.stdout.readline()
        if not line:
            if args.deltatrace:
                for d in trace1:
                    print(f"DELTATRACE1 {d}")
                for d in trace2:
                    print(f"DELTATRACE2 {d}")
            sys.exit("harness closed stdout unexpectedly")
        return line.rstrip("\n")

    def new_id():
        return "".join(random.choice("0123456789abcdef") for _ in range(64))

    if args.clustered:
        segments = rnd(args.num_segs)
        curr = 0
        for _ in range(segments):
            num = rnd(args.recs_per_seg) + 1
            mode_rnd = random.random()
            for _ in range(num):
                created = 1677970534 + curr
                curr += 1
                ident = new_id()
                if mode_rnd < p1:
                    send(p_1, f"item,{created},{ident}")
                    ids1[ident] = 1
                elif mode_rnd < p1 + p2:
                    send(p_2, f"item,{created},{ident}")
                    ids2[ident] = 1
                else:
                    send(p_1, f"item,{created},{ident}")
                    send(p_2, f"item,{created},{ident}")
                    ids1[ident] = 1
                    ids2[ident] = 1
    else:
        num = min_recs + rnd(max_recs - min_recs)
        for _ in range(num):
            created = 1677970534 + rnd(num)
            ident = new_id()
            mode_rnd = random.random()
            if mode_rnd < p1:
                send(p_1, f"item,{created},{ident}")
                ids1[ident] = 1
            elif mode_rnd < p1 + p2:
                send(p_2, f"item,{created},{ident}")
                ids2[ident] = 1
            else:
                send(p_1, f"item,{created},{ident}")
                send(p_2, f"item,{created},{ident}")
                ids1[ident] = 1
                ids2[ident] = 1

    send(p_1, "seal")
    send(p_2, "seal")
    send(p_1, "initiate")

    round_no = 0
    total_up = total_down = 0
    optimal_up = optimal_down = 0
    trace1, trace2 = [], []

    while True:
        msg = recv(p_1)
        if msg.startswith("have,") or msg.startswith("need,"):
            action, ident = msg.split(",", 1)
            if action == "need":
                if args.nodups and ids1.get(ident):
                    sys.exit(f"duplicate insert of need,{ident}")
                if not ids1.get(ident):
                    optimal_down += 32
                ids1[ident] = 1
            else:
                if args.nodups and ids2.get(ident):
                    sys.exit(f"duplicate insert of have,{ident}")
                if not ids2.get(ident):
                    optimal_up += 32
                ids2[ident] = 1
            continue
        elif msg.startswith("msg,"):
            data = msg[4:]
            trace1.append(data)
            send(p_2, f"msg,{data}")
            total_up += len(data) // 2
        elif msg.startswith("done"):
            break
        else:
            sys.exit(f"unexpected line from 1: '{msg}'")

        msg = recv(p_2)
        if msg.startswith("msg,"):
            data = msg[4:]
            trace2.append(data)
            send(p_1, f"msg,{data}")
            total_down += len(data) // 2
        else:
            sys.exit(f"unexpected line from 2: '{msg}'")
        round_no += 1

    p_1.kill()
    p_2.kill()

    for ident in ids1:
        if ident not in ids2:
            sys.exit(f"{ident} not in ids2")
    for ident in ids2:
        if ident not in ids1:
            sys.exit(f"{ident} not in ids1")

    if args.deltatrace:
        for d in trace1:
            print(f"DELTATRACE1 {d}")
        for d in trace2:
            print(f"DELTATRACE2 {d}")

    def overhead(t, o):
        return "inf" if o == 0 else f"{(t / o - 1) * 100:.2f}%"

    print(f"UP: {total_up} bytes ({overhead(total_up, optimal_up)} overhead), "
          f"DOWN: {total_down} bytes ({overhead(total_down, optimal_down)} overhead)")
    print("\n-----------OK-----------")


if __name__ == "__main__":
    main()
