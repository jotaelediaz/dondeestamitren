# app/viewmodels/train_detail.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TrainScheduleView:
    origin_time: str | None = None
    scheduled_origin_time: str | None = None
    origin_delay_minutes: int | None = None
    origin_time_state: str = "on-time"
    show_origin_schedule_time: bool = False
    destination_time: str | None = None
    scheduled_destination_time: str | None = None
    destination_delay_minutes: int | None = None
    destination_time_state: str = "on-time"
    show_destination_schedule_time: bool = False


@dataclass
class TrainStatusView:
    train_type_label: str
    train_flow_state: str
    status_descriptor: str
    train_status_icon: str
    station_name: str | None
    is_live_train: bool
    is_seen_stale: bool
    seen_age_seconds: int | None
    seen_at_destination: bool
    live_badge_class: str
    show_through_label: bool = False


@dataclass
class StopView:
    stop: dict[str, Any]
    scheduled_time: str | None
    rt_arrival_time: dict[str, Any] | None
    normalized_status: str
    status_class: str
    station_position: str
    is_next_stop: bool


@dataclass
class TrainDetailView:
    schedule: TrainScheduleView
    status: TrainStatusView
    stops: list[StopView] = field(default_factory=list)
    stop_count: int = 0
    next_stop_id: str | None = None
    rt_updated_iso: str | None = None


def build_train_detail_view(
    vm: dict[str, Any],
    rt_arrival_times: dict[str, Any],
    routes_repo: Any,
    *,
    last_seen_stop_id: str | None = None,
) -> TrainDetailView:
    train_service = vm.get("unified") or {}
    trip = vm.get("trip") or {}
    stops = list(trip.get("stops") or [])
    stop_count = len(stops)
    first_stop = stops[0] if stops else None
    last_stop = stops[-1] if stops else None

    origin_stop_id = vm.get("origin_stop_id") or train_service.get("origin_stop_id")
    destination_stop_id = vm.get("destination_stop_id") or train_service.get("destination_stop_id")
    if not origin_stop_id and first_stop:
        origin_stop_id = first_stop.get("stop_id")
    if not destination_stop_id and last_stop:
        destination_stop_id = last_stop.get("stop_id")

    rt_map = {str(k): v for k, v in (rt_arrival_times or {}).items()}

    schedule_view = _build_schedule_view(
        train_service=train_service,
        first_stop=first_stop,
        last_stop=last_stop,
        origin_stop_id=origin_stop_id,
        destination_stop_id=destination_stop_id,
        rt_arrival_times=rt_map,
    )

    stop_scope = _compute_stop_scope(stops)
    rt_prediction = train_service.get("rt_prediction") or {}
    next_stop_id = rt_prediction.get("next_stop_id") or (
        stop_scope["upcoming"].get("stop_id") if stop_scope["upcoming"] else None
    )

    status_view = _build_status_view(
        vm=vm,
        train_service=train_service,
        stop_scope=stop_scope,
        routes_repo=routes_repo,
        origin_stop_id=origin_stop_id,
        destination_stop_id=destination_stop_id,
        last_seen_stop_id=last_seen_stop_id,
        next_stop_id=next_stop_id,
    )

    stops_view = _build_stop_views(
        stops=stops,
        origin_stop_id=origin_stop_id,
        destination_stop_id=destination_stop_id,
        next_stop_id=next_stop_id,
        rt_arrival_times=rt_map,
    )

    return TrainDetailView(
        schedule=schedule_view,
        status=status_view,
        stops=stops_view,
        stop_count=stop_count,
        next_stop_id=next_stop_id,
        rt_updated_iso=trip.get("tu_updated_iso"),
    )


def _resolve_time(stop: dict[str, Any] | None, keys: tuple[str, ...]) -> str | None:
    if not stop:
        return None
    for key in keys:
        val = stop.get(key)
        if val not in (None, ""):
            return val
    return None


def _build_schedule_view(
    *,
    train_service: dict[str, Any],
    first_stop: dict[str, Any] | None,
    last_stop: dict[str, Any] | None,
    origin_stop_id: str | None,
    destination_stop_id: str | None,
    rt_arrival_times: dict[str, Any],
) -> TrainScheduleView:
    schedule = TrainScheduleView()
    schedule.origin_time = _resolve_time(
        first_stop,
        ("eta_dep_hhmm", "eta_arr_hhmm", "sched_dep_hhmm", "sched_arr_hhmm"),
    )
    if schedule.origin_time is None:
        schedule.origin_time = train_service.get("scheduled_departure_hhmm")

    origin_lookup_id = origin_stop_id or (first_stop.get("stop_id") if first_stop else None)
    origin_rt = _lookup_rt(rt_arrival_times, origin_lookup_id)
    if origin_rt:
        schedule.origin_time = origin_rt.get("hhmm") or schedule.origin_time
        origin_delay_s = origin_rt.get("delay_s")
        if origin_delay_s is not None:
            schedule.origin_delay_minutes = int(origin_delay_s // 60)
            schedule.origin_time_state = _delay_state(origin_delay_s)

    origin_sched_time = None
    origin_sched_epoch = None
    origin_eta_epoch = None
    origin_delay_seconds = None
    if first_stop:
        origin_sched_time = first_stop.get("sched_dep_hhmm") or first_stop.get("sched_arr_hhmm")
        origin_sched_epoch = first_stop.get("sched_dep_epoch") or first_stop.get("sched_arr_epoch")
        origin_eta_epoch = first_stop.get("eta_dep_epoch") or first_stop.get("eta_arr_epoch")
        origin_delay_seconds = first_stop.get("tu_delay_s")
    if origin_sched_time is None:
        origin_sched_time = train_service.get("scheduled_departure_hhmm")

    if schedule.origin_delay_minutes is None and origin_delay_seconds is not None:
        schedule.origin_delay_minutes = int(origin_delay_seconds // 60)
        schedule.origin_time_state = _delay_state(origin_delay_seconds)
    elif (
        schedule.origin_delay_minutes is None
        and origin_sched_epoch is not None
        and origin_eta_epoch is not None
    ):
        origin_epoch_delay = origin_eta_epoch - origin_sched_epoch
        schedule.origin_delay_minutes = int(origin_epoch_delay // 60)
        schedule.origin_time_state = _delay_state(origin_epoch_delay)

    schedule.scheduled_origin_time = origin_sched_time
    schedule.show_origin_schedule_time = bool(
        origin_sched_time
        and (
            (schedule.origin_time and schedule.origin_time != origin_sched_time)
            or (schedule.origin_delay_minutes not in (None, 0))
        )
    )

    destination_rt = _lookup_rt(rt_arrival_times, destination_stop_id)
    if destination_rt:
        schedule.destination_time = destination_rt.get("hhmm") or schedule.destination_time
        delay_s = destination_rt.get("delay_s")
        if delay_s is not None:
            schedule.destination_delay_minutes = int(delay_s // 60)
            schedule.destination_time_state = _delay_state(delay_s)
    if not schedule.destination_time:
        schedule.destination_time = _resolve_time(
            last_stop,
            ("eta_arr_hhmm", "eta_dep_hhmm", "sched_arr_hhmm", "sched_dep_hhmm"),
        )

    sched_time = None
    sched_epoch = None
    eta_epoch = None
    delay_seconds = None
    if last_stop:
        sched_time = last_stop.get("sched_arr_hhmm") or last_stop.get("sched_dep_hhmm")
        sched_epoch = last_stop.get("sched_arr_epoch") or last_stop.get("sched_dep_epoch")
        eta_epoch = last_stop.get("eta_arr_epoch") or last_stop.get("eta_dep_epoch")
        delay_seconds = last_stop.get("tu_delay_s")
    if sched_time is None:
        sched_time = train_service.get("scheduled_arrival_hhmm")

    if schedule.destination_delay_minutes is None and delay_seconds is not None:
        schedule.destination_delay_minutes = int(delay_seconds // 60)
        schedule.destination_time_state = _delay_state(delay_seconds)
    elif schedule.destination_delay_minutes is None and sched_epoch and eta_epoch:
        delay_seconds = eta_epoch - sched_epoch
        schedule.destination_delay_minutes = int(delay_seconds // 60)
        schedule.destination_time_state = _delay_state(delay_seconds)

    schedule.scheduled_destination_time = sched_time
    schedule.show_destination_schedule_time = bool(
        sched_time
        and (
            (schedule.destination_time and schedule.destination_time != sched_time)
            or (schedule.destination_delay_minutes not in (None, 0))
        )
    )
    return schedule


def _delay_state(delay_seconds: int) -> str:
    if delay_seconds > 0:
        return "late"
    if delay_seconds < 0:
        return "early"
    return "on-time"


def _compute_stop_scope(stops: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    scope = {"current": None, "upcoming": None}
    for stop in stops:
        normalized = (stop.get("status") or stop.get("flag") or "").upper()
        if scope["current"] is None and normalized == "CURRENT":
            scope["current"] = stop
        elif scope["upcoming"] is None and normalized in {"NEXT", "UPCOMING", "FUTURE"}:
            scope["upcoming"] = stop
        if scope["current"] and scope["upcoming"]:
            break
    return scope


def _build_status_view(
    *,
    vm: dict[str, Any],
    train_service: dict[str, Any],
    stop_scope: dict[str, dict[str, Any] | None],
    routes_repo: Any,
    origin_stop_id: str | None,
    destination_stop_id: str | None,
    last_seen_stop_id: str | None,
    next_stop_id: str | None,
) -> TrainStatusView:
    kind = vm.get("kind")
    is_live_train = kind == "live"
    base_status_text = train_service.get("status_text")
    status_text_lower = (base_status_text or "").lower()

    current_stop = stop_scope.get("current")
    upcoming_stop = stop_scope.get("upcoming")

    station_name = None
    train_status_variant = "scheduled" if not is_live_train else "unknown"
    if is_live_train:
        if current_stop:
            station_name = _stop_display_name(current_stop, routes_repo)
            train_status_variant = "stationary"
        elif upcoming_stop:
            station_name = _stop_display_name(upcoming_stop, routes_repo)
            arriving_keywords = ("llegando", "incoming", "arriving")
            is_arriving = any(token in status_text_lower for token in arriving_keywords)
            train_status_variant = "arriving" if is_arriving else "enroute"

    icon_map = {
        "stationary": "step_into",
        "arriving": "step",
        "enroute": "arrow_right_alt",
        "scheduled": "schedule",
        "unknown": "help",
    }
    train_status_icon = icon_map.get(train_status_variant, "info")
    status_descriptor = train_status_variant

    seen_age_seconds = vm.get("train_seen_age")
    seen_age_seconds = (
        int(seen_age_seconds) if isinstance(seen_age_seconds, (int | float)) else None
    )
    is_seen_stale = bool(seen_age_seconds is not None and seen_age_seconds > 60)

    def _match_stop(target: str | None) -> bool:
        if not (is_live_train and target and last_seen_stop_id):
            return False
        return str(last_seen_stop_id) == str(target)

    seen_at_destination = _match_stop(destination_stop_id)
    seen_at_origin = _match_stop(origin_stop_id)

    train_type_label = "scheduled"
    train_flow_state = "scheduled"
    if is_live_train:
        if is_seen_stale:
            if seen_at_destination:
                train_type_label = "destination"
                train_flow_state = "destination"
            else:
                train_type_label = "seen_age"
                train_flow_state = "in-transit"
        elif seen_at_origin:
            train_type_label = "origin"
            train_flow_state = "origin"
        elif seen_at_destination:
            train_type_label = "destination"
            train_flow_state = "destination"
        else:
            train_type_label = "in_transit"
            train_flow_state = "in-transit"

    if not station_name and next_stop_id:
        station_name = _stop_display_name_by_id(next_stop_id, routes_repo)

    live_badge_class = ""
    if not is_live_train:
        live_badge_class = "is-stale"
    elif is_seen_stale and seen_at_destination:
        live_badge_class = "is-completed"
    elif is_seen_stale:
        live_badge_class = "is-semi-stale"

    return TrainStatusView(
        train_type_label=train_type_label,
        train_flow_state=train_flow_state,
        status_descriptor=status_descriptor,
        train_status_icon=train_status_icon,
        station_name=station_name,
        is_live_train=is_live_train,
        is_seen_stale=is_seen_stale,
        seen_age_seconds=seen_age_seconds,
        seen_at_destination=seen_at_destination,
        live_badge_class=live_badge_class,
        show_through_label=False,
    )


def _stop_display_name(stop: dict[str, Any], routes_repo: Any) -> str:
    stop_id = stop.get("stop_id")
    name = stop.get("stop_name") or stop.get("name")
    if name:
        return str(name).strip()
    if routes_repo and stop_id:
        repo_name = routes_repo.get_stop_name(stop_id)
        if repo_name:
            return repo_name
    return str(stop_id or "?")


def _stop_display_name_by_id(stop_id: str, routes_repo: Any) -> str:
    if not stop_id:
        return ""
    if routes_repo:
        name = routes_repo.get_stop_name(stop_id)
        if name:
            return name
    return str(stop_id)


def _build_stop_views(
    *,
    stops: list[dict[str, Any]],
    origin_stop_id: str | None,
    destination_stop_id: str | None,
    next_stop_id: str | None,
    rt_arrival_times: dict[str, Any],
) -> list[StopView]:
    stop_views: list[StopView] = []

    for stop in stops:
        stop_id = str(stop.get("stop_id", ""))
        scheduled_time = (
            stop.get("scheduled_hhmm") or stop.get("sched_arr_hhmm") or stop.get("sched_dep_hhmm")
        )
        normalized_status = (stop.get("status") or stop.get("flag") or "UPCOMING").upper()
        status_class = _status_class_for(normalized_status)
        station_position = _station_position_for(stop_id, origin_stop_id, destination_stop_id)

        rt_arrival = _lookup_rt(rt_arrival_times, stop_id)
        rt_arrival = _merge_passed_info(stop, rt_arrival)

        stop_views.append(
            StopView(
                stop=stop,
                scheduled_time=scheduled_time,
                rt_arrival_time=rt_arrival,
                normalized_status=normalized_status,
                status_class=status_class,
                station_position=station_position,
                is_next_stop=bool(next_stop_id and stop_id and str(next_stop_id) == stop_id),
            )
        )

    return stop_views


def _status_class_for(normalized: str) -> str:
    if normalized == "CURRENT":
        return "current-station"
    if normalized == "PASSED":
        return "passed-station"
    return "future-station"


def _station_position_for(
    stop_id: str, origin_stop_id: str | None, destination_stop_id: str | None
) -> str:
    if origin_stop_id and stop_id == str(origin_stop_id):
        return "origin-station"
    if destination_stop_id and stop_id == str(destination_stop_id):
        return "destination-station"
    return "mid-station"


def _lookup_rt(rt_arrival_times: dict[str, Any], stop_id: str | None) -> dict[str, Any] | None:
    if not stop_id:
        return None
    return rt_arrival_times.get(str(stop_id))


def _merge_passed_info(
    stop: dict[str, Any], rt_arrival_time: dict[str, Any] | None
) -> dict[str, Any] | None:
    passed_epoch = stop.get("passed_at_epoch")
    passed_hhmm = stop.get("passed_at_hhmm")
    passed_delay_s = stop.get("passed_delay_s")
    passed_delay_min = stop.get("passed_delay_min")
    if passed_epoch is None and passed_hhmm is None:
        return rt_arrival_time

    base = rt_arrival_time or {}
    if passed_delay_min is not None:
        delay_min = passed_delay_min
    elif passed_delay_s is not None:
        delay_min = passed_delay_s // 60
    else:
        delay_min = base.get("delay_min")

    return {
        "hhmm": passed_hhmm or base.get("hhmm"),
        "epoch": passed_epoch if passed_epoch is not None else base.get("epoch"),
        "delay_s": passed_delay_s if passed_delay_s is not None else base.get("delay_s"),
        "delay_min": delay_min,
        "is_passed": True,
        "ts": passed_epoch if passed_epoch is not None else base.get("ts") or base.get("epoch"),
    }
