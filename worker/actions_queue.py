"""Is a manual "지금 확인" run waiting behind this one? Standard library only.

The home runner runs one job at a time. In seconds mode a run holds it for
~170s, so a manual run dispatched from the app sat "실행 중" for up to three
minutes. The burst loop asks this between rounds and steps aside instead.
"""
from __future__ import annotations

import json
import os
import urllib.request

TIMEOUT_SEC = 5
WORKFLOW_FILE = "watch.yml"
# Statuses of a run that has not started yet. "pending" is what a run waiting
# on its concurrency group shows.
WAITING = {"queued", "pending", "waiting", "requested"}


def manual_run_waiting() -> bool:
    """True if a workflow_dispatch run of watch.yml is waiting to start.

    Any failure (no token outside Actions, network, rate limit) answers
    False: a missed yield only delays a manual check, while a false yield
    would cut the burst short for nothing.
    """
    repo = os.environ.get("GITHUB_REPOSITORY") or ""
    token = os.environ.get("GITHUB_TOKEN") or ""
    if not repo or not token:
        return False
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW_FILE}"
        "/runs?event=workflow_dispatch&per_page=5",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "ktx-srt-watcher",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            runs = json.loads(resp.read().decode("utf-8")).get("workflow_runs") or []
    except Exception:
        return False
    return any(r.get("status") in WAITING for r in runs)
