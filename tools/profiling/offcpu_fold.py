#!/usr/bin/env python3
"""
Fold bpftrace off-CPU map output into formats consumed by flamegraph.pl.

Usage:
    offcpu_fold.py <raw> <folded> <reason-summary>

  raw            bpftrace output containing @offcpu[kstack, ustack] map.
  folded         Flamegraph-compatible folded stacks, grouped by blocking
                 reason category (caller-first, μs).
  reason-summary Per-category totals and percentages.
"""
import re, sys
from collections import defaultdict

# Each off-CPU span is classified by a pattern in its kernel stack.  The
# kernel stack at de-schedule time ends in schedule/__schedule; the frame
# above that identifies the blocking primitive.  Patterns are checked in
# order — first match wins, so more specific patterns come first.
REASON_RULES = [
    ("[fsync]",    ("ext4_sync_file", "vfs_fsync_range", "do_fsync", "__do_fsync")),
    ("[disk-io]",  ("io_schedule", "submit_bio_wait", "wait_on_buffer", "folio_wait_bit")),
    ("[epoll]",    ("do_epoll_wait", "ep_poll", "do_sys_poll", "do_select")),
    ("[net-recv]", ("sk_wait_data", "tcp_recvmsg", "unix_stream_read_generic",
                    "skb_wait_for_more_packets", "wait_for_packet")),
    ("[net-send]", ("sk_stream_wait_memory", "wait_for_tcp_memory")),
    ("[futex]",    ("futex_wait", "do_futex")),
    ("[sleep]",    ("do_nanosleep", "hrtimer_nanosleep", "schedule_hrtimeout",
                    "schedule_timeout")),
    ("[pipe]",     ("pipe_read", "pipe_wait")),
    ("[signal]",   ("do_signal_stop", "do_wait")),
]


def classify(kframes):
    blob = " ".join(kframes)
    for label, needles in REASON_RULES:
        if any(n in blob for n in needles):
            return label
    return "[other]"


def parse_frames(raw):
    out = []
    for line in raw.splitlines():
        s = line.strip()
        if s:
            out.append(re.sub(r"\+0x[0-9a-f]+$", "", s))
    return out


def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    raw_path, folded_path, summary_path = sys.argv[1:4]

    with open(raw_path) as f:
        text = f.read()

    # bpftrace prints: @offcpu[<kstack>, <ustack>]: <microseconds>
    pat = re.compile(r"@offcpu\[\s*(.*?)\]:\s*(\d+)", re.DOTALL)

    folded = defaultdict(int)
    totals = defaultdict(int)

    for m in pat.finditer(text):
        body, val = m.group(1), int(m.group(2))
        parts   = body.split(",", 1)
        k_raw   = parts[0]
        u_raw   = parts[1] if len(parts) == 2 else ""
        kframes = parse_frames(k_raw)
        uframes = parse_frames(u_raw)
        if not (kframes or uframes):
            continue

        # Caller-first: user stack at bottom, kernel stack on top.
        # Reason category is prepended as a synthetic root frame so
        # flamegraph.pl groups all stacks of the same type together.
        stack  = list(reversed(uframes)) + list(reversed(kframes))
        reason = classify(kframes)

        folded[f"{reason};" + ";".join(stack)] += val
        totals[reason]                         += val

    with open(folded_path, "w") as f:
        for k, v in sorted(folded.items(), key=lambda kv: -kv[1]):
            f.write(f"{k} {v}\n")

    total_us = sum(totals.values()) or 1
    with open(summary_path, "w") as f:
        for reason, us in sorted(totals.items(), key=lambda kv: -kv[1]):
            pct = 100.0 * us / total_us
            f.write(f"  {reason:<12}  {us:>14d} us  {pct:5.1f}%\n")


if __name__ == "__main__":
    main()
