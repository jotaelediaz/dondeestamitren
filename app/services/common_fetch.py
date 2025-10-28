# app/services/common_fetch.py
from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
FetchFn = Callable[[], tuple[T | None, str | None]]


def fetch_with_retry(
    primary_fetch: FetchFn,
    fallback_fetch: FetchFn | None = None,
    *,
    attempts: int = 1,
    delay: float = 0.0,
    primary_label: str = "primary",
    fallback_label: str | None = None,
) -> tuple[T | None, str | None, str | None]:
    last_error: str | None = None

    for i in range(max(1, attempts)):
        data, err = primary_fetch()
        if data is not None:
            return data, primary_label, None
        last_error = err
        if i < attempts - 1 and delay > 0:
            time.sleep(delay)

    if fallback_fetch is None:
        return None, None, last_error

    label = fallback_label or "fallback"
    for i in range(max(1, attempts)):
        data, err = fallback_fetch()
        if data is not None:
            return data, label, None
        last_error = err
        if i < attempts - 1 and delay > 0:
            time.sleep(delay)

    return None, None, last_error
