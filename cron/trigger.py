#!/usr/bin/env python3
"""
Render hourly cron: triggers investment-tracker update.yml when GitHub Actions misses its schedule.
Logic mirrors watchdog.yml but runs on Render's infrastructure (more reliable than GitHub self-scheduling).
"""
import os
import sys
from datetime import datetime, timezone

import requests

REPO       = "raychao0420-lang/iinvestment-tracker"
WORKFLOW   = "update.yml"
STALE_SECS = 3 * 3600  # trigger if last success > 3 hours ago


def main():
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        print("ERROR: GH_TOKEN not set.")
        sys.exit(1)

    now     = datetime.now(timezone.utc)
    weekday = now.weekday()   # 0=Mon … 6=Sun
    utc_min = now.hour * 60 + now.minute

    # Skip weekends — except Sunday UTC 22-23 (= Mon TWN 06-07, pre-market prep)
    if weekday >= 5:
        if not (weekday == 6 and now.hour >= 22):
            print(f"Weekend ({now.strftime('%a %H:%M UTC')}), skipping.")
            return

    # Skip Taiwan market hours: UTC 01:00-05:30 = TWN 09:00-13:30
    if 60 <= utc_min <= 330:
        print(f"Taiwan market hours ({now.strftime('%H:%M')} UTC), skipping.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Skip if update.yml already running or queued
    resp = requests.get(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW}/runs",
        headers=headers,
        params={"per_page": 5},
        timeout=15,
    )
    resp.raise_for_status()
    active = [r for r in resp.json()["workflow_runs"]
              if r["status"] in ("in_progress", "queued")]
    if active:
        print(f"update.yml already active ({len(active)} run(s)), skip.")
        return

    # Get last successful run
    resp = requests.get(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW}/runs",
        headers=headers,
        params={"per_page": 1, "status": "success"},
        timeout=15,
    )
    resp.raise_for_status()
    runs = resp.json()["workflow_runs"]

    if not runs:
        print("No prior success found, triggering.")
        dispatch(headers)
        return

    last_dt  = datetime.fromisoformat(runs[0]["updated_at"].replace("Z", "+00:00"))
    diff_sec = (now - last_dt).total_seconds()
    print(f"Last success: {runs[0]['updated_at']}  ({diff_sec / 3600:.1f}h ago)")

    if diff_sec > STALE_SECS:
        print("Stale! Triggering update.yml...")
        dispatch(headers)
    else:
        print("Fresh enough, no action needed.")


def dispatch(headers):
    resp = requests.post(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW}/dispatches",
        headers=headers,
        json={"ref": "main"},
        timeout=15,
    )
    resp.raise_for_status()
    print("Dispatched OK.")


if __name__ == "__main__":
    main()
