# app/services/eta_projector.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VPInfo:
    stop_id: str | None
    current_status: str | None  # "STOPPED_AT" | "IN_TRANSIT_TO" | "INCOMING_AT"
    ts_unix: int | None


def _norm_status(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip().upper()
    return s if s in {"STOPPED_AT", "IN_TRANSIT_TO", "INCOMING_AT"} else None


def _leg_runtime_seconds(
    arr_by_stop: dict[str, int],
    dep_by_stop: dict[str, int],
    prev_sid: str,
    next_sid: str,
) -> int | None:
    a_next = arr_by_stop.get(str(next_sid))
    d_prev = dep_by_stop.get(str(prev_sid))
    a_prev = arr_by_stop.get(str(prev_sid))
    if a_next is None or (d_prev is None and a_prev is None):
        return None
    base_prev = d_prev if isinstance(d_prev, int) else a_prev
    if base_prev is None:
        return None
    dt = int(a_next) - int(base_prev)
    return dt if dt >= 0 else 0


def project_future_arrivals_from_vp(
    order_sids: list[str],
    sched_arrival_by_stop: dict[str, int],
    sched_departure_by_stop: dict[str, int],
    *,
    now_ts: int,
    pivot_sid: str,
    vp: VPInfo | None = None,
    tu_pivot_eta_ts: int | None = None,
    dwell_buffer_s: int = 0,
    min_ahead_s: int = 5,
) -> dict[str, int]:
    """
    Devuelve ETAs (epoch) desde 'pivot_sid' inclusive hasta el final.

    Pivote:
      ETA_pivot = max( ETA_TU(if), ETA_física_min, arrival_sched(pivot), now + min_for_pivot )
      donde:
        - Si STOPPED_AT en la parada previa: ETA_física_min = now + dwell_buffer_s + runtime(prev→pivot)
        - Si STOPPED_AT en el propio pivot:  ETA_física_min = now
        - Si IN_TRANSIT/INCOMING:            ETA_física_min = now + min_ahead_s

    Propagación:
      ETA[i] = arrival_sched[i] + delay0, con monotonía y clamp “no pasado”.
    """
    eta_by_stop: dict[str, int] = {}
    pivot_sid = str(pivot_sid)
    if pivot_sid not in sched_arrival_by_stop:
        return eta_by_stop

    # Localiza pivote y previa
    try:
        p_idx = order_sids.index(pivot_sid)
    except ValueError:
        return eta_by_stop
    prev_sid = order_sids[p_idx - 1] if p_idx > 0 else None

    # Runtime programado del tramo previo→pivot
    rt_leg: int | None = None
    if prev_sid:
        rt_leg = _leg_runtime_seconds(
            sched_arrival_by_stop, sched_departure_by_stop, prev_sid, pivot_sid
        )

    # Estado VP → ETA física mínima
    st = _norm_status(getattr(vp, "current_status", None) if vp else None)
    vp_sid = getattr(vp, "stop_id", None) if vp else None

    # Mínimo por defecto
    eta_phys_min = int(now_ts) + min_ahead_s

    if st == "STOPPED_AT":
        if vp_sid and str(vp_sid) == str(pivot_sid):
            # Ya parado en el pivot → llegada es ahora
            eta_phys_min = int(now_ts)
        elif prev_sid and vp_sid and str(vp_sid) == str(prev_sid):
            # Parado en la previa → buffer de andén + runtime del tramo
            if isinstance(rt_leg, int):
                eta_phys_min = int(now_ts) + dwell_buffer_s + max(0, rt_leg)
            else:
                eta_phys_min = int(now_ts) + dwell_buffer_s + 60
    elif st in {"IN_TRANSIT_TO", "INCOMING_AT"}:
        eta_phys_min = int(now_ts) + min_ahead_s

    # Programado y TU del pivote
    sched_pivot = sched_arrival_by_stop.get(pivot_sid)
    if not isinstance(sched_pivot, int):
        return eta_by_stop
    eta_tu = int(tu_pivot_eta_ts) if isinstance(tu_pivot_eta_ts, int) else None

    # min_for_pivot: si estás ya en el pivot (STOPPED_AT@pivot) permitimos "now"; si no, al menos now+ε
    min_for_pivot = (
        0 if (st == "STOPPED_AT" and vp_sid and str(vp_sid) == str(pivot_sid)) else min_ahead_s
    )

    # Candidato base
    candidates = [eta_phys_min, int(now_ts) + min_for_pivot, int(sched_pivot)]
    if eta_tu is not None:
        candidates.append(eta_tu)
    eta_pivot = max(candidates)

    # Regla anti-pasado explícita cuando NO hay TU y el programado quedó atrás
    if eta_tu is None and sched_pivot < int(now_ts):
        eta_pivot = max(eta_phys_min, int(now_ts) + min_for_pivot)

    # Clamp final por seguridad
    if eta_pivot < int(now_ts) + min_for_pivot:
        eta_pivot = int(now_ts) + min_for_pivot

    # Offset y propagación
    delay0 = int(eta_pivot) - int(sched_pivot)
    if delay0 < -60:
        delay0 = -60  # evita offsets demasiado negativos por desfases horarios

    for sid in order_sids[p_idx:]:
        sa = sched_arrival_by_stop.get(str(sid))
        if not isinstance(sa, int):
            continue
        eta = int(sa) + int(delay0)
        # Monotonía
        if eta_by_stop:
            prev_eta = list(eta_by_stop.values())[-1]
            if eta < prev_eta + min_ahead_s:
                eta = prev_eta + min_ahead_s
        # No-pasado (para pivot permitimos now si procede)
        if eta < int(now_ts) + (0 if str(sid) == pivot_sid and min_for_pivot == 0 else min_ahead_s):
            eta = int(now_ts) + (0 if str(sid) == pivot_sid and min_for_pivot == 0 else min_ahead_s)
        eta_by_stop[str(sid)] = eta

    # Si hubiese TU más tardío para el pivot, respétalo
    if eta_tu is not None and eta_tu > eta_by_stop.get(pivot_sid, 0):
        eta_by_stop[pivot_sid] = eta_tu

    return eta_by_stop
