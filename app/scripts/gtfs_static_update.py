# app/scripts/gtfs_static_update.py
from __future__ import annotations

import json
import sys

from app.services.gtfs_static_manager import check_and_update, on_swap_rebuild_indexes


def rebuild_everything():
    # from app.services.stops_repo import get_repo as get_stops_repo
    # get_stops_repo().reload()
    pass


def main() -> int:
    res = check_and_update()
    print(json.dumps(res, indent=2, ensure_ascii=False))
    if res.get("changed"):
        on_swap_rebuild_indexes(rebuild_everything)
        print("Rebuild done.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
