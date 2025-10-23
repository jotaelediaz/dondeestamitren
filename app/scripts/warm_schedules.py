# app/scripts/warm_schedules.py
from __future__ import annotations

import json
import os
import urllib.request


def main() -> int:
    url = os.environ.get("WARM_URL", "http://127.0.0.1:8000/admin/warm-schedules")
    token = os.environ.get("INTERNAL_TASK_TOKEN", "")

    req = urllib.request.Request(url, method="POST")
    if token:
        req.add_header("X-Task-Token", token)

    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(body)
        except Exception:
            print(body)
            return 0
        print(json.dumps(data, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
