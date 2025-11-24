// app/static/js/train-detail.js
// Train detail auto-refresh, progress interpolation, and scroll handling
(function () {
    let trainDetailScrollHandlersBound = false;
    let trainDetailRefreshHandlersBound = false;

    // ------------------ Utilities ------------------
    function disableBoostForStopLinks(root = document) {
        root.querySelectorAll('.grid-route-map a[href*="/stops/"]').forEach(a => {
            if (a.getAttribute('hx-boost') !== 'false') a.setAttribute('hx-boost', 'false');
        });
    }

    // ------------------ Train detail anchor scroll ------------------
    function scrollTrainDetailToAnchor() {
        const scrollContainer = document.querySelector('.grouped-lists-scroll');
        const gridContainer = scrollContainer?.querySelector('.grid-route-map');

        if (!scrollContainer || !gridContainer) {
            return;
        }

        const hash = window.location.hash;
        let targetElement = null;
        let targetId = '';

        if (hash) {
            targetId = hash.substring(1);
            targetElement = document.getElementById(targetId);
            if (!targetElement) {
                console.warn(`Target element with ID "${targetId}" not found`);
                return;
            }
        } else if (gridContainer.classList.contains('live-train')) {
            targetElement = gridContainer.querySelector('.grid-route-map-station.next-stop');
            if (!targetElement) {
                return;
            }
            targetId = targetElement.id || '';
        } else {
            return;
        }

        const allStations = Array.from(gridContainer.querySelectorAll('.grid-route-map-station'));

        const targetIndex = allStations.findIndex(el => el === targetElement);

        if (targetIndex === -1) {
            console.warn('Could not locate target element within station list');
            return;
        }

        const computedStyle = window.getComputedStyle(gridContainer);
        const stationBlockHeightStr = computedStyle.getPropertyValue('--station-block-height').trim();

        const stationBlockHeightNum = parseFloat(stationBlockHeightStr);
        const remToPixels = parseFloat(computedStyle.fontSize);
        const stationBlockHeightPx = stationBlockHeightNum * remToPixels;

        const scrollPosition = (targetIndex * stationBlockHeightPx) - 100;
        const targetScrollTop = Math.max(0, scrollPosition);

        scrollContainer.scrollTo({
            top: targetScrollTop,
            behavior: 'smooth'
        });

        targetElement.classList.add('scroll-target');

        setTimeout(function() {
            targetElement.classList.remove('scroll-target');
        }, 2000);
    }

    function bindTrainDetailScrollHandlers() {
        if (trainDetailScrollHandlersBound) return;
        trainDetailScrollHandlersBound = true;

        const scheduleScroll = (delay = 400) => {
            setTimeout(scrollTrainDetailToAnchor, delay);
        };

        const handleHtmx = (evt) => {
            const target = evt?.detail?.target;
            if (!target) return;
            if (target.id === 'content' || (typeof target.closest === 'function' && target.closest('#content'))) {
                scheduleScroll(400);
            }
        };

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', function handleDomContentLoaded() {
                document.removeEventListener('DOMContentLoaded', handleDomContentLoaded);
                scheduleScroll(500);
            });
        } else {
            scheduleScroll(500);
        }

        document.addEventListener('htmx:afterSettle', handleHtmx);
        document.addEventListener('htmx:afterSwap', handleHtmx);
        window.addEventListener('popstate', () => scheduleScroll(400));
        window.addEventListener('hashchange', () => scheduleScroll(400));
    }

    bindTrainDetailScrollHandlers();

    // ------------------ Train detail auto refresh ------------------
    const TRAIN_DETAIL_INTERVALS = {
        base: 30_000,
        approaching: 12_000,
        stopped: 15_000,
        scheduled: 30_000,
        stale: 30_000,
        maxBackoff: 120_000,
    };
    const TRAIN_PROGRESS_TICK = 500;
    const PROGRESS_DEADBAND = 2;
    const PROGRESS_CATCHUP_MAX = 5;
    const PROGRESS_MIN_SLOPE_PPS = 0.05;
    const STATUS_META = {
        IN_TRANSIT_TO: { text: 'En tránsito', descriptor: 'En tránsito a:', icon: 'arrow_right_alt', className: 'train-status--enroute' },
        INCOMING_AT: { text: 'Llegando', descriptor: 'Llegando a:', icon: 'arrow_right_alt', className: 'train-status--arriving' },
        STOPPED_AT: { text: 'En estación', descriptor: 'En estación:', icon: 'directions_subway', className: 'train-status--stopped' },
        SCHEDULED: { text: 'Programado', descriptor: 'Programado', icon: 'schedule', className: 'train-status--scheduled' },
        UNKNOWN: { text: 'Estado desconocido', descriptor: 'Estado desconocido', icon: 'help', className: 'train-status--unknown' },
    };

    const numberOrNull = (val) => {
        if (val === null || val === undefined || val === '') return null;
        if (typeof val === 'string') {
            const cleaned = val.trim().replace(/%$/, '').replace(',', '.');
            const n = Number(cleaned);
            return Number.isFinite(n) ? n : null;
        }
        const n = Number(val);
        return Number.isFinite(n) ? n : null;
    };

    const progressFromSegmentTimes = (segment, nowTsSec = null) => {
        if (!segment) return null;
        const dep = numberOrNull(segment.dep_epoch ?? segment.segment_dep_epoch);
        const arr = numberOrNull(segment.arr_epoch ?? segment.segment_arr_epoch);
        if (!Number.isFinite(dep) || !Number.isFinite(arr) || arr <= dep) return null;
        const now = Number.isFinite(nowTsSec) ? nowTsSec : (Date.now() / 1000);
        return Math.max(0, Math.min(100, ((now - dep) / (arr - dep)) * 100));
    };

    function statusMetaFor(status) {
        const key = String(status || '').toUpperCase();
        return STATUS_META[key] || STATUS_META.UNKNOWN;
    }

    function statusClassFor(status) {
        return statusMetaFor(status).className;
    }

    function statusTextFor(status) {
        return statusMetaFor(status).text;
    }

    function setHidden(el, hidden) {
        if (!el) return;
        if (hidden) el.setAttribute('hidden', '');
        else el.removeAttribute('hidden');
    }

    function replacePrefixedClasses(el, prefix, value) {
        if (!el || !el.classList) return;
        el.classList.forEach((cls) => {
            if (cls.startsWith(prefix)) el.classList.remove(cls);
        });
        if (value) el.classList.add(`${prefix}${value}`);
    }

    function normalizeTrainPayload(payload) {
        if (!payload) return null;
        const train = payload.train || {};
        const position = payload.position || {};
        const segment = payload.segment || {};
        const interpolation = payload.interpolation || {};
        const schedule = payload.schedule || payload.train_detail?.schedule || payload.detail?.schedule || {};
        const stopsBlock = payload.stops || {};
        const rawStops = Array.isArray(stopsBlock.items)
            ? stopsBlock.items
            : ((payload.train_detail && payload.train_detail.stops) || payload.detail?.stops || []);
        const statusObj = payload.status || {};
        const statusKey = (statusObj.key || payload.status || payload.train_detail?.train_status_key || '').toString().toUpperCase() || 'UNKNOWN';
        const nextStopId = segment?.to_stop?.id ?? payload.next_stop_id ?? null;
        const currentStopId = segment?.from_stop?.id ?? payload.current_stop_id ?? null;
        const nextStopName = segment?.to_stop?.name ?? payload.next_stop_name ?? null;
        const currentStopName = segment?.from_stop?.name ?? payload.current_stop_name ?? null;
        const stops = rawStops.map((stop) => {
            const sid = stop?.stop_id ?? stop?.stopId;
            const isNext = stop?.is_next_stop ?? stop?.is_next ?? (nextStopId && sid && String(sid) === String(nextStopId));
            const isCurrent = stop?.is_current_stop ?? stop?.is_current ?? (currentStopId && sid && String(sid) === String(currentStopId));
            return { ...stop, is_next_stop: !!isNext, is_current_stop: !!isCurrent };
        });
        const segmentDep = segment?.dep_epoch
            ?? segment?.segment_dep_epoch
            ?? payload.segment_dep_epoch
            ?? payload.dep_epoch;
        const segmentArr = segment?.arr_epoch
            ?? segment?.segment_arr_epoch
            ?? payload.segment_arr_epoch
            ?? payload.arr_epoch;
        const progressPct = numberOrNull(
            segment?.progress_pct
            ?? payload.next_stop_progress_pct
            ?? payload.progress_pct
            ?? payload.train_detail?.next_stop_progress_pct
            ?? payload.train_detail?.progress_pct
            ?? payload.detail?.next_stop_progress_pct
            ?? payload.detail?.progress_pct
            ?? payload.train?.next_stop_progress_pct
            ?? payload.train?.progress_pct
            ?? payload.status?.progress_pct
        )
            ?? progressFromSegmentTimes(
                { ...segment, dep_epoch: segmentDep, arr_epoch: segmentArr },
                numberOrNull(position?.ts_unix),
            );
        const seenAge = numberOrNull(train?.seen?.age_s ?? payload.train_seen_age ?? payload.train_seen_age_seconds);

        const derivedSchedule = { ...schedule };
        if (!derivedSchedule.origin && rawStops.length > 0) {
            const first = rawStops[0]?.times || {};
            derivedSchedule.origin = {
                display: first?.rt?.hhmm || first?.scheduled || '--:--',
                scheduled: first?.scheduled,
                rt: first?.rt?.hhmm,
                state: (first?.rt && first?.rt?.hhmm && first?.scheduled && first?.rt?.hhmm !== first?.scheduled) ? 'late' : 'on-time',
                show_scheduled: Boolean(first?.scheduled && first?.rt?.hhmm && first?.rt?.hhmm !== first?.scheduled),
            };
        }
        if (!derivedSchedule.destination && rawStops.length > 0) {
            const last = rawStops[rawStops.length - 1]?.times || {};
            derivedSchedule.destination = {
                display: last?.rt?.hhmm || last?.scheduled || '--:--',
                scheduled: last?.scheduled,
                rt: last?.rt?.hhmm,
                state: (last?.rt && last?.rt?.hhmm && last?.scheduled && last?.rt?.hhmm !== last?.scheduled) ? 'late' : 'on-time',
                show_scheduled: Boolean(last?.scheduled && last?.rt?.hhmm && last?.rt?.hhmm !== last?.scheduled),
            };
        }

        return {
            train_kind: train.kind || payload.kind || '',
            status_key: statusKey || 'UNKNOWN',
            status_class: statusClassFor(statusKey || 'UNKNOWN'),
            flow_state: statusObj.flow_state || payload.train_detail?.train_flow_state || payload.detail?.train_flow_state || '',
            train_type: statusObj.train_type || payload.train_detail?.train_type || payload.detail?.train_type || {},
            schedule: derivedSchedule,
            stops,
            stop_count: stopsBlock.count ?? payload.train_detail?.stop_count ?? stops.length,
            current_stop_id: currentStopId,
            next_stop_id: nextStopId,
            current_stop_name: currentStopName,
            next_stop_name: nextStopName,
            progress_pct: progressPct,
            server_progress: progressPct,
            segment_dep_epoch: segmentDep,
            segment_arr_epoch: segmentArr,
            seen_age: seenAge,
            seen_iso: train?.seen?.iso ?? payload.train_seen_iso ?? null,
            rt_updated_iso: schedule.rt_updated_iso,
            position,
            interpolation: {
                anchor_progress: numberOrNull(interpolation.anchor_progress) ?? progressPct ?? 0,
                anchor_ts: numberOrNull(interpolation.anchor_ts) ?? numberOrNull(position?.ts_unix),
                target_ts: numberOrNull(interpolation.target_ts) ?? segmentArr,
                is_stopped: Boolean(interpolation.is_stopped),
            },
        };
    }

    function findStopNode(panel, stopId) {
        if (!panel || !stopId) return null;
        const sid = String(stopId);
        return Array.from(panel.querySelectorAll('[data-stop-id]')).find(
            (el) => String(el.dataset.stopId || el.getAttribute('data-stop-id') || '') === sid,
        ) || null;
    }

    function updateStopRow(panel, stop) {
        if (!panel || !stop) return;
        const stopId = stop.stop_id || stop.stopId;
        const el = findStopNode(panel, stopId);
        if (!el) return;

        const classes = ['grid-route-map-station'];
        if (stop.status_class) classes.push(stop.status_class);
        if (stop.station_position) classes.push(stop.station_position);
        const isNext = stop.is_next_stop ?? stop.is_next ?? false;
        const isCurrent = stop.is_current_stop ?? stop.is_current ?? false;
        if (isNext) classes.push('next-stop');
        if (isCurrent) classes.push('current-stop');
        el.className = classes.join(' ').trim();
        if (stop.station_id) el.dataset.stationId = stop.station_id;
        if (stop.stop_id) el.dataset.stopId = stop.stop_id;

        const stationName = el.querySelector('.station-name');
        if (stationName && stop.name) stationName.textContent = stop.name;

        const times = stop.times || {};
        const rtBlock = el.querySelector('[data-stop-rt]');
        if (rtBlock) {
            setHidden(rtBlock, !times.show_rt);
            const tEl = rtBlock.querySelector('[data-stop-rt-time]');
            if (tEl) {
                const val = times?.rt?.hhmm || '';
                tEl.textContent = val || '';
                if (val) tEl.setAttribute('datetime', val);
                else tEl.removeAttribute('datetime');
                const epoch = times?.rt?.epoch;
                if (epoch !== undefined && epoch !== null && epoch !== '') tEl.dataset.epoch = String(epoch);
                else delete tEl.dataset.epoch;
            }
        }

        const schedBlock = el.querySelector('[data-stop-scheduled]');
        if (schedBlock) {
            const showScheduled = !!times.show_scheduled;
            setHidden(schedBlock, !showScheduled);
            const schedTime = schedBlock.querySelector('[data-stop-sched-time]');
            if (schedTime) {
                const val = times.scheduled || '';
                schedTime.textContent = val || '';
                if (val) schedTime.setAttribute('datetime', val);
                else schedTime.removeAttribute('datetime');
            }
            const rtVal = times?.rt?.hhmm || '';
            const schedVal = times?.scheduled || '';
            const offCurrent = Boolean(times.show_rt && rtVal && schedVal && rtVal !== schedVal);
            schedBlock.classList.toggle('scheduled-time-is-not-current', offCurrent);
        }

        const delayWrap = el.querySelector('[data-stop-delay-wrapper]');
        if (delayWrap) {
            const delayVal = Number(times.delay_value || 0);
            const showDelay = Boolean(times.show_rt && delayVal !== 0);
            setHidden(delayWrap, !showDelay);
            const delay = delayWrap.querySelector('[data-stop-delay]');
            if (delay) {
                delay.className = 'stop-time-label delay-label';
                if (delayVal > 0) delay.classList.add('delayed-train');
                else if (delayVal < 0) delay.classList.add('advanced-train');
                delay.value = delayVal;
                const abs = Math.abs(delayVal);
                delay.textContent = delayVal > 0 ? `(+${delayVal} min)` : `(-${abs} min)`;
                delay.setAttribute(
                    'aria-label',
                    delayVal > 0 ? `+${delayVal} minutos de retraso` : `${abs} minutos de adelanto`,
                );
            }
        }

        const platform = stop.platform || {};
        const badge = el.querySelector('[data-platform-badge]');
        if (badge) {
            badge.dataset.platform = platform.label ?? '';
            badge.dataset.src = platform.src ?? '';
            if (platform.habitual) badge.dataset.habitual = platform.habitual;
            else badge.removeAttribute('data-habitual');
            const baseCls = ['platform-badge'];
            if (platform.base_class) baseCls.push(platform.base_class);
            if (platform.exceptional) baseCls.push('exceptional-platform');
            badge.className = baseCls.join(' ').trim();
            const unit = badge.querySelector('.platform-unit');
            if (unit) unit.textContent = platform.label ?? '?';
        }
    }

    function updateTrainDetailDebug(panel, model) {
        if (!panel) return;
        const statusCode = model?.status_key || 'UNKNOWN';
        let status = statusTextFor(statusCode);

        const destDelay = model?.schedule?.destination?.delay_minutes
            ?? model?.schedule?.destination?.delay_value;
        if (typeof destDelay === 'number' && destDelay !== 0) {
            const sign = destDelay > 0 ? '+' : '';
            status += ` (${sign}${destDelay} min)`;
        }

        const current = model?.current_stop_name || model?.current_stop_id || '—';
        const next = model?.next_stop_name || model?.next_stop_id || '—';

        // Update debug list elements (separate from main UI elements)
        // The debug list is outside the panel, in the parent content area
        const scope = panel.closest('.train-details-content-area') || panel;

        // Support both new and old attribute names for backwards compatibility
        const debugStatus = scope.querySelector('[data-debug-status]')
            || scope.querySelector('li[data-train-status-text]');
        const debugCurrent = scope.querySelector('[data-debug-current]')
            || scope.querySelector('li[data-train-current-stop]');
        const debugNext = scope.querySelector('[data-debug-next]')
            || scope.querySelector('li[data-train-next-stop]');

        if (debugStatus) debugStatus.textContent = `Estado en directo: ${status}`;
        if (debugCurrent) debugCurrent.textContent = `Parada actual: ${current}`;
        if (debugNext) debugNext.textContent = `Siguiente parada: ${next}`;
    }

    function updateTrainDetailUI(panel, model) {
        if (!panel || !model) return;
        const trainStatusKey = model.status_key || 'UNKNOWN';
        const flowState = model.flow_state || model.train_type?.flow_state || '';
        const statusMeta = statusMetaFor(trainStatusKey);

        panel.dataset.trainStatus = String(trainStatusKey).toLowerCase();

        panel.querySelectorAll('[data-train-flow-state]').forEach((el) => {
            replacePrefixedClasses(el, 'train-flow-state--', flowState);
        });

        const typeBadge = panel.querySelector('[data-train-type-badge]');
        if (typeBadge) {
            typeBadge.classList.remove('is-live', 'is-scheduled');
            if (model.train_type?.is_live) typeBadge.classList.add('is-live');
            else typeBadge.classList.add('is-scheduled');
            replacePrefixedClasses(typeBadge, 'train-flow-state--', flowState);
            const labelText = model.train_type?.text || statusTextFor(trainStatusKey);
            const lbl = typeBadge.querySelector('.train-type-badge__label');
            if (lbl) lbl.textContent = labelText;
            typeBadge.title = labelText || '';
            typeBadge.setAttribute('aria-label', `Servicio ${labelText || ''}`);
            const dot = typeBadge.querySelector('.train-type-live-dot');
            if (dot) {
                dot.className = `live-pill train-type-live-dot${model.train_type?.live_badge_class ? ` ${model.train_type.live_badge_class}` : ''}`;
            }
        }

        const origin = model.schedule?.origin || {};
        const destination = model.schedule?.destination || {};
        const originDisplayText = origin.display || origin.rt || origin.scheduled || '--:--';
        const destinationDisplayText = destination.display || destination.rt || destination.scheduled || '--:--';
        const originDisplay = panel.querySelector('[data-origin-display]');
        if (originDisplay) originDisplay.textContent = originDisplayText;
        const destDisplay = panel.querySelector('[data-destination-display]');
        if (destDisplay) destDisplay.textContent = destinationDisplayText;

        const originLabelWrap = panel.querySelector('[data-origin-label]');
        const shouldShowOriginLabel = Boolean(
            origin.show_scheduled && origin.scheduled && origin.scheduled !== originDisplayText,
        );
        if (originLabelWrap) setHidden(originLabelWrap, !shouldShowOriginLabel);
        const originSched = panel.querySelector('[data-origin-scheduled]');
        if (originSched) {
            originSched.textContent = origin.scheduled || originDisplayText || '';
            replacePrefixedClasses(originSched, 'train-arrival-time--', origin.state || 'on-time');
        }

        const destLabelWrap = panel.querySelector('[data-destination-label]');
        const shouldShowDestLabel = Boolean(
            destination.show_scheduled && destination.scheduled && destination.scheduled !== destinationDisplayText,
        );
        if (destLabelWrap) setHidden(destLabelWrap, !shouldShowDestLabel);
        const destSched = panel.querySelector('[data-destination-scheduled]');
        if (destSched) {
            destSched.textContent = destination.scheduled || destinationDisplayText || '';
            replacePrefixedClasses(destSched, 'train-arrival-time--', destination.state || 'on-time');
        }

        const labelsWrap = panel.querySelector('[data-route-labels]');
        const showStatusLabel = Boolean(trainStatusKey && trainStatusKey !== 'UNKNOWN');
        if (labelsWrap) {
            const showLabels = shouldShowOriginLabel || shouldShowDestLabel || showStatusLabel;
            setHidden(labelsWrap, !showLabels);
        }

        const statusLabel = panel.querySelector('[data-train-status-label]');
        if (statusLabel) {
            setHidden(statusLabel, !showStatusLabel);
            statusLabel.title = statusMeta.descriptor || '';
            statusLabel.setAttribute('aria-label', statusMeta.descriptor || '');
            const icon = statusLabel.querySelector('[data-train-status-icon]');
            if (icon) icon.textContent = statusMeta.icon || '';
            const txt = statusLabel.querySelector('[data-train-status-text]');
            if (txt) txt.textContent = model.next_stop_name || model.current_stop_name || statusMeta.descriptor || '';
        }

        const map = panel.querySelector('[data-train-progress-map]');
        if (map) {
            const base = Array.from(map.classList).filter((cls) => !cls.startsWith('train-status--'));
            if (model.status_class) base.push(model.status_class);
            map.className = base.join(' ').trim();
            if (trainStatusKey) map.dataset.trainStatus = String(trainStatusKey).toLowerCase();
        }

        const progress = model.progress_pct;
        if (Number.isFinite(progress)) {
            updateTrainProgressUI(panel, progress);
        }

        (model.stops || []).forEach((stop) => updateStopRow(panel, stop));
        updateTrainDetailDebug(panel, model);
    }

    function stopTrainDetailAuto(panel) {
        const st = panel?.__trainAuto;
        if (!st) return;
        if (st.timerId) { clearTimeout(st.timerId); st.timerId = null; }
        if (st.abort) { try { st.abort.abort(); } catch (_) {} st.abort = null; }
        st.running = false;
        st.inFlight = false;
        if (st.progressTimer) { clearInterval(st.progressTimer); st.progressTimer = null; }
    }

    function scheduleTrainDetailTick(panel, ms) {
        const st = panel?.__trainAuto;
        if (!st || !st.running) return;
        if (st.timerId) clearTimeout(st.timerId);
        st.timerId = setTimeout(() => refreshTrainDetail(panel), ms);
    }

    function applyTrainDetailPayload(panel, payload) {
        if (!panel || !payload) return null;
        const model = normalizeTrainPayload(payload);
        if (!model) return null;
        const st = panel.__trainAuto || {};
        const nextStopId = model.next_stop_id || null;
        const inferred = updateProgressInference(panel, model);
        model.progress_pct = Number.isFinite(inferred) ? inferred : null;
        model.porcentaje_inferido = model.progress_pct;
        const serverProgressForLabel = Number.isFinite(model.server_progress)
            ? model.server_progress
            : (st.serverProgress ?? null);
        model.server_progress = serverProgressForLabel;

        const html = payload.html;
        if (typeof html === 'string' && html.trim()) {
            panel.innerHTML = html;
            disableBoostForStopLinks(panel);
            const animVal = inferProgressAt(st, Date.now());
            if (animVal !== null) updateTrainProgressUI(panel, animVal);
        }

        if (model.train_kind) panel.dataset.trainKind = model.train_kind;
        if (model.status_key) panel.dataset.trainStatus = model.status_key;

        updateTrainDetailUI(panel, model);
        updateServerProgressLabel(panel, serverProgressForLabel);
        ensureProgressTimer(panel);
        return model;
    }

    function nextTrainDetailInterval(model, panel) {
        const kind = (model && model.train_kind) || (panel?.dataset?.trainKind) || 'live';
        if (kind !== 'live') return TRAIN_DETAIL_INTERVALS.scheduled;

        const status = String(model?.status_key || '').toUpperCase();
        if (status === 'INCOMING_AT') return TRAIN_DETAIL_INTERVALS.approaching;
        if (status === 'STOPPED_AT') return TRAIN_DETAIL_INTERVALS.stopped;

        const progress = numberOrNull(model?.progress_pct);
        if (Number.isFinite(progress) && progress >= 70) return TRAIN_DETAIL_INTERVALS.approaching;

        const seenAge = numberOrNull(model?.seen_age);
        if (typeof seenAge === 'number' && seenAge > 180) return TRAIN_DETAIL_INTERVALS.stale;

        return TRAIN_DETAIL_INTERVALS.base;
    }

    async function refreshTrainDetail(panel) {
        const st = panel?.__trainAuto;
        if (!st || !st.running) return;

        if (!panel || !panel.isConnected) {
            stopTrainDetailAuto(panel);
            return;
        }

        if (document.hidden) {
            scheduleTrainDetailTick(panel, st.baseInterval || TRAIN_DETAIL_INTERVALS.base);
            return;
        }

        const apiUrl = panel.dataset.trainApi;
        if (!apiUrl) {
            scheduleTrainDetailTick(panel, TRAIN_DETAIL_INTERVALS.scheduled);
            return;
        }

        if (st.abort) { try { st.abort.abort(); } catch (_) {} }
        st.abort = new AbortController();
        st.inFlight = true;

        let url = apiUrl;
        try {
            const u = new URL(apiUrl, location.origin);
            u.searchParams.set('_ts', Date.now().toString());
            url = u.toString();
        } catch (_) {
            const sep = apiUrl.includes('?') ? '&' : '?';
            url = `${apiUrl}${sep}_ts=${Date.now()}`;
        }

        try {
            const resp = await fetch(url, {
                signal: st.abort.signal,
                headers: { 'Accept': 'application/json' },
                cache: 'no-store',
            });
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const payload = await resp.json();
            const model = applyTrainDetailPayload(panel, payload);
            st.errors = 0;
            const base = nextTrainDetailInterval(model, panel);
            const jitter = Math.floor(Math.random() * 500);
            scheduleTrainDetailTick(panel, base + jitter);
        } catch (err) {
            st.errors = Math.min(st.errors + 1, 6);
            const backoffBase = (panel?.dataset?.trainKind === 'scheduled')
                ? TRAIN_DETAIL_INTERVALS.scheduled
                : TRAIN_DETAIL_INTERVALS.base;
            const penalty = Math.min(
                backoffBase * (2 ** (st.errors - 1)),
                TRAIN_DETAIL_INTERVALS.maxBackoff,
            );
            scheduleTrainDetailTick(panel, penalty);
        } finally {
            st.inFlight = false;
            st.abort = null;
        }
    }

    function startTrainDetailAuto(panel) {
        if (!panel) return;
        if (!panel.__trainAuto) {
            panel.__trainAuto = {
                timerId: null,
                abort: null,
                running: false,
                inFlight: false,
                errors: 0,
                baseInterval: TRAIN_DETAIL_INTERVALS.base,
                anchorProgress: null,
                anchorTs: null,
                targetTs: null,
                progressSlopePerMs: null,
                progressCeil: null,
                inferredProgress: null,
                serverProgress: null,
                lastStopId: null,
                progressTimer: null,
            };
        }

        const st = panel.__trainAuto;
        st.baseInterval = (panel.dataset.trainKind === 'scheduled')
            ? TRAIN_DETAIL_INTERVALS.scheduled
            : TRAIN_DETAIL_INTERVALS.base;

        if (st.running) return;
        st.running = true;
        st.errors = 0;

        if (document.hidden) scheduleTrainDetailTick(panel, st.baseInterval);
        else refreshTrainDetail(panel);
        ensureProgressTimer(panel);
    }

    function progressValueMode(panel) {
        const st = panel?.__trainAuto;
        if (!st) return 'Inferido';
        const anchorTs = Number(st.anchorTs);
        if (!Number.isFinite(anchorTs)) return 'Inferido';
        const delta = Date.now() - anchorTs;
        return delta <= TRAIN_PROGRESS_TICK ? 'Real' : 'Inferido';
    }

    function updateServerProgressLabel(panel, serverProgress) {
        const scope = panel?.closest('.train-details-content-area') || panel;
        const label = scope?.querySelector('[data-train-progress-real]');
        if (!label) return;
        const pct = Number.isFinite(serverProgress) ? Math.round(serverProgress) : null;
        const text = pct !== null
            ? `Último porcentaje de avance en servidor: ${pct}%`
            : 'Último porcentaje de avance en servidor: —';
        label.textContent = text;
    }

    function updateTrainProgressUI(panel, progress) {
        if (!panel || progress === null || !Number.isFinite(progress)) return;
        const pct = Math.max(0, Math.min(100, Math.round(progress)));
        const scope = panel.closest('.train-details-content-area') || panel;
        const maps = scope.querySelectorAll('[data-train-progress-map]');
        const labels = scope.querySelectorAll('[data-train-progress]');
        maps.forEach((map) => {
            if (map?.style) map.style.setProperty('--next_stop_progress', `${pct}%`);
        });
        const mode = progressValueMode(panel);
        labels.forEach((li) => {
            li.textContent = `Porcentaje de avance: ${pct}% (${mode})`;
        });
    }

    const MOVING_STATUS_KEYS = new Set(['IN_TRANSIT_TO', 'INCOMING_AT']);
    const clampProgress = (p) => Math.max(0, Math.min(100, p));

    function dataTimestampMs(model) {
        const posTsSec = numberOrNull(model?.position?.ts_unix ?? model?.position?.timestamp);
        if (Number.isFinite(posTsSec)) return posTsSec * 1000;
        const seenAge = numberOrNull(model?.seen_age);
        if (Number.isFinite(seenAge)) return Date.now() - (seenAge * 1000);
        return Date.now();
    }

    function inferProgressAt(st, tsMs) {
        if (!st) return null;
        const anchor = numberOrNull(st.anchorProgress);
        const anchorTs = numberOrNull(st.anchorTs);
        const slope = Number(st.progressSlopePerMs);
        if (anchor === null || anchorTs === null || !Number.isFinite(slope)) {
            return Number.isFinite(st.inferredProgress) ? st.inferredProgress : null;
        }
        const delta = Math.max(0, tsMs - anchorTs);
        let val = anchor + delta * slope;
        if (Number.isFinite(st.progressCeil)) {
            val = Math.min(val, st.progressCeil);
        }
        val = clampProgress(val);
        st.inferredProgress = val;
        return val;
    }

    function updateProgressInference(panel, model) {
        const st = panel?.__trainAuto;
        if (!st) return null;

        const nowMs = Date.now();
        const serverProgress = numberOrNull(model?.server_progress ?? model?.progress_pct);
        const nextStopId = model?.next_stop_id || null;

        const statusKey = String(model?.status_key || '').toUpperCase();
        const isStopped = statusKey === 'STOPPED_AT';

        const interp = model?.interpolation || {};
        let anchorProgress = numberOrNull(interp.anchor_progress) ?? serverProgress ?? 0;
        const anchorTsSec = numberOrNull(interp.anchor_ts);
        const targetTsSec = numberOrNull(interp.target_ts);
        const isTrainStopped = Boolean(interp.is_stopped);

        // Detect segment change (different next stop)
        const segmentChanged = st.lastStopId && nextStopId && String(st.lastStopId) !== String(nextStopId);

        const currentInferred = inferProgressAt(st, nowMs);
        if (Number.isFinite(currentInferred) && Number.isFinite(anchorProgress)) {
            if (segmentChanged) {
                // Segment changed: use server value directly without smoothing
                // The train moved to a new segment, so progress resets
                anchorProgress = anchorProgress;
            } else {
                const diff = anchorProgress - currentInferred;
                if (diff > 0) {
                    // Server ahead: catch up gradually
                    anchorProgress = currentInferred + diff * 0.5;
                } else if (diff < -5) {
                    // Server significantly behind without segment change: possible GPS correction
                    anchorProgress = currentInferred + diff * 0.3;
                } else {
                    // Small difference: keep current to avoid jitter
                    anchorProgress = currentInferred;
                }
            }
        }

        if (isStopped) {
            st.anchorProgress = 0;
            st.anchorTs = anchorTsSec ? anchorTsSec * 1000 : nowMs;
            st.targetTs = st.anchorTs;
            st.progressSlopePerMs = 0;
            st.progressCeil = 0;
            st.serverProgress = Number.isFinite(serverProgress) ? serverProgress : st.serverProgress;
            st.inferredProgress = 0;
            if (nextStopId) st.lastStopId = String(nextStopId);
            return 0;
        }

        const anchorTsMs = anchorTsSec ? anchorTsSec * 1000 : nowMs;
        const targetTsMs = targetTsSec ? targetTsSec * 1000 : anchorTsMs + (st.baseInterval || TRAIN_DETAIL_INTERVALS.base);

        st.anchorProgress = clampProgress(anchorProgress);
        st.anchorTs = anchorTsMs;
        st.targetTs = targetTsMs;
        st.serverProgress = Number.isFinite(serverProgress) ? serverProgress : st.serverProgress;
        st.progressCeil = 95;

        let slope = 0;
        if (MOVING_STATUS_KEYS.has(statusKey) && !isTrainStopped) {
            const denomMs = Math.max(500, targetTsMs - anchorTsMs);
            const desiredTarget = 100;
            slope = (desiredTarget - st.anchorProgress) / denomMs;
            if (slope < 0) slope = 0;
            const minSlope = PROGRESS_MIN_SLOPE_PPS / 1000;
            if (slope > 0 && slope < minSlope) slope = minSlope;
        }

        st.progressSlopePerMs = slope;

        const inferredNow = inferProgressAt(st, nowMs);
        st.inferredProgress = inferredNow;
        if (nextStopId) st.lastStopId = String(nextStopId);
        return inferredNow;
    }

    function ensureProgressTimer(panel) {
        const st = panel?.__trainAuto;
        if (!st) return;
        if (st.progressTimer) return;
        st.progressTimer = setInterval(() => {
            const val = inferProgressAt(st, Date.now());
            if (val !== null) updateTrainProgressUI(panel, val);
        }, TRAIN_PROGRESS_TICK);
    }

    function bindTrainDetailVisibilityHandler() {
        if (trainDetailRefreshHandlersBound) return;
        trainDetailRefreshHandlersBound = true;
        document.addEventListener('visibilitychange', () => {
            const panels = document.querySelectorAll('[data-train-detail]');
            panels.forEach((panel) => {
                if (!panel.__trainAuto) return;
                if (document.hidden) stopTrainDetailAuto(panel);
                else startTrainDetailAuto(panel);
            });
        }, { passive: true });
    }

    function bindTrainDetailAutoRefresh(root = document) {
        if (!root) return;
        bindTrainDetailVisibilityHandler();
        const panels = (root.matches && root.matches('[data-train-detail]'))
            ? [root]
            : Array.from(root.querySelectorAll ? root.querySelectorAll('[data-train-detail]') : []);
        panels.forEach((panel) => {
            if (!panel.dataset.trainApi) return;
            startTrainDetailAuto(panel);
        });
    }

    // Expose functions globally for use by other scripts and htmx
    window.TrainDetail = {
        bindAutoRefresh: bindTrainDetailAutoRefresh,
        startAuto: startTrainDetailAuto,
        stopAuto: stopTrainDetailAuto,
        scrollToAnchor: scrollTrainDetailToAnchor,
    };

    // Auto-bind on DOMContentLoaded and htmx events
    document.addEventListener('DOMContentLoaded', () => bindTrainDetailAutoRefresh());
    document.body.addEventListener('htmx:afterSettle', (evt) => {
        const target = evt.detail?.target;
        if (target) bindTrainDetailAutoRefresh(target);
    });
})();
