# app/utils/train_numbers.py
from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

__all__ = [
    "extract_train_number_str",
    "extract_train_number_int",
    "extract_train_number_from_train",
    "extract_train_number_int_from_train",
    "extract_train_number_from_vehicle",
]


_NUM_SUFFIX_RE = re.compile(r"(\d{4,6})(?!.*\d)")
_NUM_GENERIC_RE = re.compile(r"(?<!\d)(\d{3,6})(?!\d)")
_PLATFORM_TOKEN_RE = re.compile(r"PLATF\.\(\s*[0-9A-Z]+\s*\)", re.IGNORECASE)


def _clean_candidate(value: Any) -> str:
    s = "" if value is None else str(value).strip()
    if not s:
        return ""
    return _PLATFORM_TOKEN_RE.sub("", s)


def _normalized_candidates(candidates: Iterable[Any]) -> list[str]:
    return [c for c in (_clean_candidate(v) for v in candidates) if c]


def extract_train_number_str(*candidates: Any) -> str | None:
    """
    Extract a train number from the provided candidates, returning the textual digits.
    Candidates can be strings, numbers or any object that stringifies to something useful.
    Preference is given to numbers that appear as a suffix (Renfe habit), falling back to
    any 3-6 digit token.
    """
    values = _normalized_candidates(candidates)
    for text in values:
        match = _NUM_SUFFIX_RE.search(text)
        if match:
            return match.group(1)
    for text in values:
        match = _NUM_GENERIC_RE.search(text)
        if match:
            return match.group(1)
    return None


def extract_train_number_int(*candidates: Any) -> int | None:
    """
    Same as extract_train_number_str but returning the parsed integer when possible.
    """
    token = extract_train_number_str(*candidates)
    if token is None:
        return None
    try:
        return int(token)
    except ValueError:
        return None


def extract_train_number_from_train(train: Any) -> str | None:
    """
    Convenience helper for live/scheduled train objects that expose attributes such as
    train_number, train_id or label.
    """
    if train is None:
        return None
    attrs = []
    for attr in ("train_number", "train_id", "vehicle_id", "label"):
        attrs.append(getattr(train, attr, None))
    return extract_train_number_str(*attrs)


def extract_train_number_int_from_train(train: Any) -> int | None:
    token = extract_train_number_from_train(train)
    if token is None:
        return None
    try:
        return int(token)
    except ValueError:
        return None


def extract_train_number_from_vehicle(vehicle: Any) -> int | None:
    """
    Extract the numeric identifier from a GTFS-realtime vehicle struct/dict.
    """
    if vehicle is None:
        return None
    if isinstance(vehicle, dict):
        candidates = [vehicle.get("id"), vehicle.get("label")]
    else:
        candidates = [getattr(vehicle, "id", None), getattr(vehicle, "label", None)]
    return extract_train_number_int(*candidates)
