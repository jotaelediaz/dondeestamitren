// static/js/scripts.js
(function () {
    const upgradedForms = new WeakSet();
    const boundButtons  = new WeakSet();
    const boundDialogs  = new WeakSet();
    const boundPanels   = new WeakSet();
    let   htmxHooked    = false;
    let   reverseHandlerBound = false;
    let   trainsGlobalHandlersBound = false;
    let   stopGlobalHandlersBound   = false;
    let   drawerCloseDelegatedBound = false;
    let   trainDetailScrollHandlersBound = false;
    let   trainDetailRefreshHandlersBound = false;

// ------------------ Utilities ------------------
    function stripHxAttrs(html) {
        return String(html).replace(/\s(hx-(target|swap|indicator|trigger))(=(".*?"|'.*?'|[^\s>]+))?/gi, "");
    }

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
    const TRAIN_PROGRESS_MAX_DROP = 15;
    const TRAIN_PROGRESS_TICK = 1_000;

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
        if (!panel || !payload) return;
        const st = panel.__trainAuto || {};
        const service = payload.train_service || payload.trainService || {};
        const nextStopId = service.next_stop_id || service.nextStopId || null;
        const rawProgress = Number(service.next_stop_progress_pct ?? service.nextStopProgressPct);
        const now = Date.now();
        const currentAnimated = progressFromState(now, st);
        let progress = Number.isFinite(rawProgress) ? rawProgress : null;

        const prevStopId = st.lastStopId || null;
        const prevProgress = Number.isFinite(st.lastProgress) ? st.lastProgress : null;
        const sameStop = prevStopId && nextStopId && String(prevStopId) === String(nextStopId);

        if (sameStop && prevProgress !== null && progress !== null) {
            const drop = prevProgress - progress;
            if (drop > 0 && drop < TRAIN_PROGRESS_MAX_DROP) {
                progress = prevProgress;
            }
        }

        if (progress !== null) {
            st.progressStart = Number.isFinite(currentAnimated) ? currentAnimated : progress;
            st.progressTarget = progress;
            st.progressStartTs = now;
            st.progressDuration = st.baseInterval || TRAIN_DETAIL_INTERVALS.base;
            st.lastProgress = progress;
        } else if (prevProgress !== null && sameStop) {
            st.progressStart = Number.isFinite(currentAnimated) ? currentAnimated : prevProgress;
            st.progressTarget = prevProgress;
            st.progressStartTs = now;
            st.progressDuration = st.baseInterval || TRAIN_DETAIL_INTERVALS.base;
            st.lastProgress = prevProgress;
        }
        if (nextStopId) st.lastStopId = String(nextStopId);

        const html = payload.html;
        if (typeof html === 'string' && html.trim()) {
            panel.innerHTML = html;
            disableBoostForStopLinks(panel);
            const animVal = progressFromState(Date.now(), st);
            if (animVal !== null) updateTrainProgressUI(panel, animVal);
        }

        if (payload.kind) panel.dataset.trainKind = payload.kind;
        if (service.current_status) panel.dataset.trainStatus = service.current_status;

        ensureProgressTimer(panel);
    }

    function nextTrainDetailInterval(payload, panel) {
        const kind = (payload && payload.kind) || (panel?.dataset?.trainKind) || 'live';
        if (kind !== 'live') return TRAIN_DETAIL_INTERVALS.scheduled;

        const service = payload?.train_service || payload?.trainService || {};
        const status = String(service.current_status || '').toUpperCase();
        if (status === 'INCOMING_AT') return TRAIN_DETAIL_INTERVALS.approaching;
        if (status === 'STOPPED_AT') return TRAIN_DETAIL_INTERVALS.stopped;

        const progress = Number(service.next_stop_progress_pct);
        if (Number.isFinite(progress) && progress >= 70) return TRAIN_DETAIL_INTERVALS.approaching;

        const seenAge = (typeof payload?.train_seen_age === 'number')
            ? payload.train_seen_age
            : (typeof payload?.train_seen_age_seconds === 'number' ? payload.train_seen_age_seconds : null);
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
            applyTrainDetailPayload(panel, payload);
            st.errors = 0;
            const base = nextTrainDetailInterval(payload, panel);
            const jitter = Math.floor(Math.random() * 500);
            scheduleTrainDetailTick(panel, base + jitter);
        } catch (err) {
            console.debug('[train-detail] refresh error', err);
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
                progressStart: null,
                progressTarget: null,
                progressStartTs: null,
                progressDuration: TRAIN_DETAIL_INTERVALS.base,
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

    function updateTrainProgressUI(panel, progress) {
        if (!panel || progress === null || !Number.isFinite(progress)) return;
        const pct = Math.max(0, Math.min(100, Math.round(progress)));
        const map = panel.querySelector('[data-train-progress-map]');
        if (map && map.style) {
            map.style.setProperty('--next_stop_progress', `${pct}%`);
        }
        const li = panel.querySelector('[data-train-progress]');
        if (li) {
            li.textContent = `Porcentaje de avance: ${pct}%`;
        }
    }

    function progressFromState(now, st) {
        if (!st) return null;
        const start = Number.isFinite(st.progressStart) ? st.progressStart : null;
        const target = Number.isFinite(st.progressTarget) ? st.progressTarget : null;
        const startTs = Number.isFinite(st.progressStartTs) ? st.progressStartTs : null;
        const duration = Number.isFinite(st.progressDuration) ? st.progressDuration : null;
        if (start === null || target === null || startTs === null || duration === null || duration <= 0) {
            return target ?? start ?? null;
        }
        const t = Math.min(1, Math.max(0, (now - startTs) / duration));
        return start + (target - start) * t;
    }

    function ensureProgressTimer(panel) {
        const st = panel?.__trainAuto;
        if (!st) return;
        if (st.progressTimer) return;
        st.progressTimer = setInterval(() => {
            const val = progressFromState(Date.now(), st);
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

    function getStaticDrawer() {
        const panel = document.getElementById('drawer');
        const body  = document.getElementById('drawer-content');
        return { panel, body };
    }

    function setDrawerMode(mode) {
        const { panel } = getStaticDrawer();
        if (!panel) return;
        panel.classList.remove('drawer-trains','drawer-stop');

        if (mode === 'trains') {
            panel.classList.add('drawer-trains');

            const st = panel.__stopAuto;
            if (st) {
                if (st.timerId) { clearTimeout(st.timerId); st.timerId = null; }
                if (st.abort)   { try { st.abort.abort(); } catch(_) {} st.abort = null; }
                st.running = false;
            }
        } else if (mode === 'stop') {
            panel.classList.add('drawer-stop');
        }
        if (mode) panel.dataset.mode = mode; else delete panel.dataset.mode;
    }

    function openStaticDrawer() {
        console.debug('[drawer] openStaticDrawer()');
        const { panel } = getStaticDrawer();
        if (!panel) return;
        panel.hidden = false;
        panel.removeAttribute('inert');
        panel.setAttribute('aria-hidden', 'false');
        panel.getBoundingClientRect();
        panel.classList.add('open');
    }

    function closeStaticDrawer() {
        console.debug('[drawer] closeStaticDrawer()');
        const { panel } = getStaticDrawer();
        if (!panel) return;
        panel.classList.remove('open');
        panel.setAttribute('aria-hidden', 'true');
        panel.setAttribute('inert', '');
        panel.classList.remove('drawer-trains','drawer-stop');
        setTimeout(() => { panel.hidden = true; }, 260);
    }

    function swapDrawerHTML(html) {
        const { body } = getStaticDrawer();
        if (!body) return;
        body.innerHTML = html;
        if (window.htmx) htmx.process(body);
        body.querySelectorAll('.drawer-close,[data-close-sheet]').forEach(btn => {
            if (boundButtons.has(btn)) return;
            boundButtons.add(btn);
            btn.addEventListener('click', (ev) => { ev.preventDefault(); closeStaticDrawer(); });
        });
    }

    function loadIntoDrawer(url, opts = {}) {
        const { panel, body } = getStaticDrawer();
        if (!panel || !body) return;
        const inner = panel.querySelector('.drawer-inner') || body;
        const indicator = body.querySelector('.htmx-indicator');
        if (indicator) indicator.style.opacity = '1';
        fetch(url, { headers: { 'HX-Request': 'true' } })
            .then(r => r.ok ? r.text() : Promise.reject(r))
            .then(html => {
                inner.classList.add('is-fading');
                const animEl = panel.querySelector('.drawer-body') || inner;
                let swapped = false;
                const doSwap = () => {
                    if (swapped) return;
                    swapped = true;
                    swapDrawerHTML(html);
                    requestAnimationFrame(() => inner.classList.remove('is-fading'));
                    if (opts.afterLoad) { try { opts.afterLoad(); } catch(_) {} }
                };
                const onEnd = (ev) => {
                    if (ev.propertyName !== 'opacity') return;
                    animEl.removeEventListener('transitionend', onEnd);
                    doSwap();
                };
                animEl.addEventListener('transitionend', onEnd, { once: true });
                setTimeout(() => {
                    try { animEl.removeEventListener('transitionend', onEnd); } catch(_) {}
                    doSwap();
                }, 320);
            })
            .catch(() => {
                swapDrawerHTML('<p>Error al cargar.</p>');
                panel.querySelector('.drawer-inner')?.classList.remove('is-fading');
            })
            .finally(() => {
                if (indicator) indicator.style.opacity = '';
            });
    }

// ---------- Context helpers ----------
    const RouteCtx = {
        getMain() {
            const n = document.getElementById('route-context');
            if (!n) return null;
            return {
                nucleus:  n.getAttribute('data-nucleus') || '',
                lineId:   n.getAttribute('data-line-id') || '',
                routeId:  n.getAttribute('data-route-id') || '',
                dir:      n.getAttribute('data-dir') || ''
            };
        },
        getDrawerFromHTML(html) {
            try {
                const tpl = document.createElement('template');
                tpl.innerHTML = String(html);
                const n = tpl.content.querySelector('#drawer-context');
                if (!n) return null;
                return {
                    nucleus:  n.getAttribute('data-nucleus') || '',
                    lineId:   n.getAttribute('data-line-id') || '',
                    routeId:  n.getAttribute('data-route-id') || '',
                    dir:      n.getAttribute('data-dir') || ''
                };
            } catch (_) { return null; }
        },
        equal(a, b) {
            if (!a || !b) return false;
            return a.nucleus === b.nucleus
                && a.lineId  === b.lineId
                && a.dir     === b.dir
                && (!!b.routeId ? a.routeId === b.routeId : true);
        },
        appendToURL(url, ctx) {
            if (!url || !ctx) return url;
            try {
                const u = new URL(url, location.origin);
                if (!u.searchParams.has('dir'))        u.searchParams.set('dir', ctx.dir || '');
                if (!u.searchParams.has('source_rid')) u.searchParams.set('source_rid', ctx.routeId || '');
                if (!u.searchParams.has('nucleus'))    u.searchParams.set('nucleus', ctx.nucleus || '');
                return u.toString();
            } catch(_) {
                return url;
            }
        }
    };

// ---------- DrawerCtx for Stop ----------
    const DrawerStopCtx = {
        getFromHTML(html) {
            try {
                const tpl = document.createElement('template');
                tpl.innerHTML = String(html);
                const n = tpl.content.querySelector('#drawer-context');
                if (!n) return null;
                return {
                    nucleus: n.getAttribute('data-nucleus') || '',
                    lineId:  n.getAttribute('data-line-id') || '',
                    routeId: n.getAttribute('data-route-id') || '',
                    dir:     n.getAttribute('data-dir') || '',
                    stopId:  n.getAttribute('data-stop-id') || '',
                    servicesUrl: n.getAttribute('data-services-url') || '',
                    servicesLimit: n.getAttribute('data-services-limit') || '',
                    servicesTz: n.getAttribute('data-services-tz') || ''
                };
            } catch (_) { return null; }
        },
        equal(a, b) {
            if (!a || !b) return false;
            const routeOk = (!!b.routeId ? a.routeId === b.routeId : true);
            const dirOk   = (!!b.dir     ? a.dir     === b.dir     : true);
            return a.nucleus === b.nucleus && a.lineId === b.lineId && routeOk && dirOk && a.stopId === b.stopId;
        }
    };

    const RollingNumber = (() => {
        const DIGIT_OFFSET = 2;
        const IDX_LT = 1;
        const IDX_ONE = DIGIT_OFFSET + 1;

        function _withVisible(el, fn) {
            if (!el || typeof fn !== 'function') return typeof fn === 'function' ? fn() : undefined;
            const wasHidden = el.hasAttribute('hidden');
            const wasInert = el.hasAttribute('inert');
            const prevAria = el.getAttribute('aria-hidden');
            const prevVisibility = el.style.visibility;
            const prevPointer = el.style.pointerEvents;
            if (wasHidden) el.removeAttribute('hidden');
            if (wasInert) el.removeAttribute('inert');
            if (prevAria !== null) el.setAttribute('aria-hidden', prevAria);
            el.style.visibility = 'hidden';
            el.style.pointerEvents = 'none';
            try {
                return fn();
            } finally {
                el.style.visibility = prevVisibility || '';
                el.style.pointerEvents = prevPointer || '';
                if (wasHidden) el.setAttribute('hidden', '');
                if (wasInert) el.setAttribute('inert', '');
                if (prevAria !== null) el.setAttribute('aria-hidden', prevAria);
                else el.removeAttribute('aria-hidden');
            }
        }

        function _parseNumberish(a, t) {
            const c = [];
            if (a != null && a !== '') c.push(String(a));
            if (t != null && t !== '') c.push(String(t));
            for (const v of c) {
                const s = v.replace(/[^\d,.\-]/g, '').replace(',', '.');
                const n = Number(s);
                if (Number.isFinite(n)) return Math.round(n);
                const m = s.match(/-?\d+/);
                if (m) return Math.round(Number(m[0]));
            }
            return 0;
        }

        function _pad2(n) {
            return String(Math.max(0, Math.min(99, Number(n) || 0))).padStart(2, '0');
        }

        function _measureStepPx(el) {
            return _withVisible(el, () => {
                const digits = el.__rnDigitColumns?.[0];
                let span = digits && digits.children && digits.children[2];
                if (!span && digits) span = digits.querySelector('span:nth-child(3)');
                let px = span ? span.getBoundingClientRect().height : 0;
                if (!px || !Number.isFinite(px) || px <= 0) {
                    const fs = parseFloat(getComputedStyle(el).fontSize);
                    px = Number.isFinite(fs) && fs > 0 ? fs : 16;
                }
                return px;
            });
        }

        function _measureColWidthPx(el) {
            return _withVisible(el, () => {
                const cols = el.__rnDigitColumns || [];
                let w = 0;
                cols.forEach((digits) => {
                    if (!digits) return;
                    const r = digits.getBoundingClientRect();
                    if (r && Number.isFinite(r.width) && r.width > w) w = r.width;
                });
                return w || 0;
            });
        }

        function _applyTransformsPx(el, idx0, idx1) {
            const step = el.__rnStepPx || (el.__rnStepPx = _measureStepPx(el));
            const cols = el.__rnDigitColumns || [];
            const targets = [idx0, idx1];
            for (let i = 0; i < 2; i++) {
                const digits = cols[i];
                const target = targets[i];
                if (!digits || !Number.isFinite(target)) continue;
                void digits.offsetWidth;
                digits.style.transform = `translateY(${target * -step}px)`;
            }
        }

        function _frameIdxPair(val, ltMinute) {
            const safe = Number.isFinite(val) ? val : _parseNumberish(val);
            const n = Math.max(0, Math.min(99, Number(safe) || 0));
            if (ltMinute) return [IDX_LT, IDX_ONE];
            const s = _pad2(n);
            return [DIGIT_OFFSET + Number(s[0]), DIGIT_OFFSET + Number(s[1])];
        }

        function _unitText(el) {
            const u = el.querySelector && el.querySelector('.unit');
            return u ? u.textContent : 'min';
        }

        function _stationLabel(el) {
            return el.getAttribute('data-station-label')
                || el.dataset.stationLabel
                || DEFAULT_STOP_PANEL_STRINGS.stationStatusLabel;
        }

        function _build(el, val, ltMinute) {
            const initialStationActive = el.dataset.stationActive === 'true';
            const showLtMinute = !!ltMinute;
            const [i0, i1] = _frameIdxPair(val, showLtMinute);
            const unit = _unitText(el);
            const stationLabel = _stationLabel(el);

            el.classList.add('rolling-number');
            el.innerHTML = '';

            const facesWrapper = document.createElement('span');
            facesWrapper.className = 'rolling-number__faces';

            const facesInner = document.createElement('span');
            facesInner.className = 'rolling-number__faces-inner';

            const numericFace = document.createElement('span');
            numericFace.className = 'rolling-number__face rolling-number__face--numeric';

            const digitColumns = [];
            for (let k = 0; k < 2; k++) {
                const col = document.createElement('span');
                col.className = 'rolling-number__column';
                const digits = document.createElement('span');
                digits.className = 'rolling-number__digits';

                const blank = document.createElement('span');
                blank.textContent = '';
                digits.appendChild(blank);

                const lt = document.createElement('span');
                lt.textContent = '<';
                digits.appendChild(lt);

                for (let d = 0; d <= 9; d += 1) {
                    const span = document.createElement('span');
                    span.textContent = String(d);
                    digits.appendChild(span);
                }

                col.appendChild(digits);
                numericFace.appendChild(col);
                digitColumns.push(digits);
            }

            const unitSpan = document.createElement('span');
            unitSpan.className = 'unit';
            unitSpan.textContent = unit;
            numericFace.appendChild(unitSpan);

            const stationFace = document.createElement('span');
            stationFace.className = 'rolling-number__face rolling-number__face--station';
            const stationLabelNode = document.createElement('span');
            stationLabelNode.className = 'rolling-number__station-label';
            stationLabelNode.textContent = stationLabel;
            stationFace.appendChild(stationLabelNode);

            facesInner.appendChild(numericFace);
            facesInner.appendChild(stationFace);
            facesWrapper.appendChild(facesInner);
            el.appendChild(facesWrapper);

            el.__rnDigitColumns = digitColumns;
            el.__rnFaces = {
                wrapper: facesWrapper,
                inner: facesInner,
                numericFace,
                stationFace,
                stationLabel: stationLabelNode,
                unit: unitSpan,
            };

            requestAnimationFrame(() => {
                _withVisible(el, () => {
                    el.__rnStepPx = _measureStepPx(el);
                    const cw = _measureColWidthPx(el);
                    if (cw) el.style.setProperty('--rn-col-w', `${cw}px`);
                    const sampleDigit = digitColumns[0] && digitColumns[0].children && digitColumns[0].children[2]
                        ? digitColumns[0].children[2]
                        : digitColumns[0]?.querySelector('span:nth-child(3)');
                    const digitRect = sampleDigit ? sampleDigit.getBoundingClientRect() : null;
                    if (digitRect && Number.isFinite(digitRect.height) && digitRect.height > 0) {
                        el.style.setProperty('--rn-digit-height', `${digitRect.height}px`);
                    }
                    const numericRect = numericFace.getBoundingClientRect();
                    const stationRect = stationFace.getBoundingClientRect();
                    const faceHeight = Math.max(
                        0,
                        Number.isFinite(numericRect?.height) ? numericRect.height : 0,
                        Number.isFinite(stationRect?.height) ? stationRect.height : 0,
                    );
                    if (faceHeight > 0) facesWrapper.style.setProperty('--rn-face-height', `${faceHeight}px`);
                    _applyTransformsPx(el, i0, i1);
                    setStationState(el, initialStationActive, stationLabel);
                });
            });

            el.dataset.value0 = String(i0);
            el.dataset.value1 = String(i1);
            el.dataset.ltMinute = showLtMinute ? 'true' : 'false';
            setStationState(el, initialStationActive, stationLabel);
        }

        function build(el, value, ltMinuteOverride) {
            const showLtMinute = typeof ltMinuteOverride === 'boolean'
                ? ltMinuteOverride
                : el.dataset.ltMinute === 'true';
            _build(el, _parseNumberish(value, el.textContent), showLtMinute);
        }

        function update(el, next, ltMinuteOverride) {
            const val = _parseNumberish(next, el.textContent);
            const showLtMinute = typeof ltMinuteOverride === 'boolean'
                ? ltMinuteOverride
                : el.dataset.ltMinute === 'true';
            if (!el.classList.contains('rolling-number')) {
                build(el, val, showLtMinute);
                return;
            }
            const [i0, i1] = _frameIdxPair(val, showLtMinute);
            el.dataset.ltMinute = showLtMinute ? 'true' : 'false';
            if (el.dataset.value0 === String(i0) && el.dataset.value1 === String(i1)) return;
            el.dataset.value0 = String(i0);
            el.dataset.value1 = String(i1);
            _applyTransformsPx(el, i0, i1);
        }

        function ensure(el) {
            const attr = el.getAttribute && el.getAttribute('data-eta-min');
            const val = _parseNumberish(attr, el.textContent);
            const showLtMinute = el.dataset.ltMinute === 'true';
            if (!el.classList.contains('rolling-number')) build(el, val, showLtMinute);
            else update(el, val, showLtMinute);
        }

        function setStationState(el, active, label) {
            if (!el) return;
            const isActive = !!active;
            if (!el.classList.contains('rolling-number')) {
                if (label) el.setAttribute('data-station-label', label);
                el.dataset.stationActive = isActive ? 'true' : 'false';
                return;
            }
            const faces = el.__rnFaces;
            if (!faces) return;
            if (label && faces.stationLabel) faces.stationLabel.textContent = label;

            el.dataset.stationActive = isActive ? 'true' : 'false';
            el.classList.toggle('is-station', isActive);
            if (faces.inner) {
                faces.inner.classList.toggle('is-station', isActive);
                faces.inner.style.removeProperty('transform');
            }
            if (faces.numericFace) {
                faces.numericFace.setAttribute('aria-hidden', isActive ? 'true' : 'false');
            }
            if (faces.stationFace) {
                faces.stationFace.setAttribute('aria-hidden', isActive ? 'false' : 'true');
            }
            if (faces.unit) faces.unit.setAttribute('aria-hidden', isActive ? 'true' : 'false');
        }

        return { build, update, ensure, setStationState };
    })();

    const RollingClock = (() => {
        const DIGIT_OFFSET = 2;

        const _pad2 = (value) =>
            String(Math.max(0, Math.min(99, Number(value) || 0))).padStart(2, '0');

        const _unitText = (el) => {
            if (!el) return 'h';
            const attr = el.getAttribute('data-clock-unit');
            if (attr) return attr;
            const node = el.querySelector && el.querySelector('.unit');
            const text = (node && node.textContent) || 'h';
            el.setAttribute('data-clock-unit', text);
            return text;
        };

        const _withVisible = (el, fn) => {
            if (!el || typeof fn !== 'function') return fn ? fn() : undefined;
            const wasHidden = el.hasAttribute('hidden');
            const wasInert = el.hasAttribute('inert');
            const prevAria = el.getAttribute('aria-hidden');
            const prevVisibility = el.style.visibility;
            const prevPointer = el.style.pointerEvents;
            if (wasHidden) el.removeAttribute('hidden');
            if (wasInert) el.removeAttribute('inert');
            if (prevAria !== null) el.setAttribute('aria-hidden', prevAria);
            el.style.visibility = 'hidden';
            el.style.pointerEvents = 'none';
            try {
                return fn();
            } finally {
                el.style.visibility = prevVisibility || '';
                el.style.pointerEvents = prevPointer || '';
                if (wasHidden) el.setAttribute('hidden', '');
                if (wasInert) el.setAttribute('inert', '');
                if (prevAria !== null) el.setAttribute('aria-hidden', prevAria);
                else el.removeAttribute('aria-hidden');
            }
        };

        const _measureStepPx = (el) =>
            _withVisible(el, () => {
                const digits = el?.__rcDigits?.[0];
                let sample = digits && digits.children && digits.children[2];
                if (!sample && digits) sample = digits.querySelector('span:nth-child(3)');
                let px = sample ? sample.getBoundingClientRect().height : 0;
                if (!px || !Number.isFinite(px) || px <= 0) {
                    const fs = parseFloat(getComputedStyle(el).fontSize);
                    px = Number.isFinite(fs) && fs > 0 ? fs : 16;
                }
                return px;
            });

        const _measureColWidthPx = (el) =>
            _withVisible(el, () => {
                const cols = el?.__rcColumns || [];
                let w = 0;
                cols.forEach((column) => {
                    if (!column) return;
                    const rect = column.getBoundingClientRect();
                    if (rect && Number.isFinite(rect.width) && rect.width > w) w = rect.width;
                });
                return w || 0;
            });

        const _applyTransforms = (el, idxs) => {
            const step = el.__rcStepPx || (el.__rcStepPx = _measureStepPx(el));
            const digits = el.__rcDigits || [];
            for (let i = 0; i < digits.length; i += 1) {
                const dg = digits[i];
                const target = idxs[i];
                if (!dg || !Number.isFinite(target)) continue;
                void dg.offsetWidth;
                dg.style.transform = `translateY(${target * -step}px)`;
            }
        };

        const _persistState = (el, time, idxs) => {
            const hh = _pad2(time.hh);
            const mm = _pad2(time.mm);
            const hhmm = `${hh}:${mm}`;
            el.dataset.clockIdxs = JSON.stringify(idxs);
            el.dataset.clockHhmm = hhmm;
            el.setAttribute('data-eta-hhmm', hhmm);
        };

        const _createDigitColumn = () => {
            const column = document.createElement('span');
            column.className = 'rolling-number__column';
            const digits = document.createElement('span');
            digits.className = 'rolling-number__digits';

            const blank = document.createElement('span');
            blank.textContent = '';
            digits.appendChild(blank);

            const lt = document.createElement('span');
            lt.textContent = '<';
            digits.appendChild(lt);

            for (let d = 0; d <= 9; d += 1) {
                const span = document.createElement('span');
                span.textContent = String(d);
                digits.appendChild(span);
            }

            column.appendChild(digits);
            return { column, digits };
        };

        const _separator = () => {
            const sep = document.createElement('span');
            sep.className = 'rolling-number__sep';
            sep.textContent = ':';
            sep.setAttribute('aria-hidden', 'true');
            return sep;
        };

        const _idxsFor = (time) => {
            const hh = _pad2(Math.max(0, Math.min(23, Number(time.hh) || 0)));
            const mm = _pad2(Math.max(0, Math.min(59, Number(time.mm) || 0)));
            return [
                DIGIT_OFFSET + Number(hh[0]),
                DIGIT_OFFSET + Number(hh[1]),
                DIGIT_OFFSET + Number(mm[0]),
                DIGIT_OFFSET + Number(mm[1]),
            ];
        };

        const _timeFromMinutes = (minutes) => {
            const mins = Math.max(0, Math.round(Number(minutes) || 0));
            const dt = new Date(Date.now() + mins * 60000);
            return { hh: dt.getHours(), mm: dt.getMinutes() };
        };

        const _parseMinutesAttr = (el) => {
            const attr = el?.getAttribute && el.getAttribute('data-eta-min');
            if (attr == null) return null;
            const n = Number(attr);
            if (Number.isFinite(n)) return n;
            const cleaned = String(attr).replace(/[^\d,.\-]/g, '').replace(',', '.');
            const parsed = Number(cleaned);
            return Number.isFinite(parsed) ? parsed : null;
        };

        const _parseHhmmAttr = (el) => {
            const attr =
                (el?.getAttribute && el.getAttribute('data-eta-hhmm')) ||
                el?.dataset?.clockHhmm;
            if (!attr) return null;
            const text = String(attr).trim();
            const match = text.match(/^(\d{1,2})[:hH](\d{1,2})$/);
            if (!match) return null;
            const hh = Math.max(0, Math.min(23, Number.parseInt(match[1], 10)));
            const mm = Math.max(0, Math.min(59, Number.parseInt(match[2], 10)));
            if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
            return { hh, mm };
        };

        const _resolveTime = (el) => {
            const hhmm = _parseHhmmAttr(el);
            if (hhmm) return hhmm;
            const rawMinutes = _parseMinutesAttr(el);
            if (rawMinutes != null) return _timeFromMinutes(rawMinutes);
            return _timeFromMinutes(0);
        };

        const _build = (el, time) => {
            const idxs = _idxsFor(time);
            const unitText = _unitText(el);

            el.classList.add('rolling-number', 'rolling-clock');
            el.innerHTML = '';

            const facesWrapper = document.createElement('span');
            facesWrapper.className = 'rolling-number__faces';

            const facesInner = document.createElement('span');
            facesInner.className = 'rolling-number__faces-inner';

            const numericFace = document.createElement('span');
            numericFace.className = 'rolling-number__face rolling-number__face--numeric';

            const digits = [];
            const columns = [];
            for (let i = 0; i < 4; i += 1) {
                const { column, digits: colDigits } = _createDigitColumn();
                numericFace.appendChild(column);
                digits.push(colDigits);
                columns.push(column);
                if (i === 1) numericFace.appendChild(_separator());
            }

            const unit = document.createElement('span');
            unit.className = 'unit';
            unit.textContent = unitText;
            numericFace.appendChild(unit);

            facesInner.appendChild(numericFace);
            facesWrapper.appendChild(facesInner);
            el.appendChild(facesWrapper);

            el.__rcDigits = digits;
            el.__rcColumns = columns;
            el.__rcFaces = { wrapper: facesWrapper, inner: facesInner, numericFace, unit };

            requestAnimationFrame(() => {
                _withVisible(el, () => {
                    el.__rcStepPx = _measureStepPx(el);
                    const cw = _measureColWidthPx(el);
                    if (cw) el.style.setProperty('--rn-col-w', `${cw}px`);
                    const sampleDigit = digits[0]?.children?.[2];
                    const digitRect = sampleDigit ? sampleDigit.getBoundingClientRect() : null;
                    if (digitRect && Number.isFinite(digitRect.height) && digitRect.height > 0) {
                        el.style.setProperty('--rn-digit-height', `${digitRect.height}px`);
                    }
                    const faceRect = numericFace.getBoundingClientRect();
                    if (faceRect && Number.isFinite(faceRect.height) && faceRect.height > 0) {
                        facesWrapper.style.setProperty('--rn-face-height', `${faceRect.height}px`);
                    }
                    _applyTransforms(el, idxs);
                });
            });

            _persistState(el, time, idxs);
        };

        const _update = (el, time) => {
            if (!el.classList.contains('rolling-clock') || !el.__rcDigits) {
                _build(el, time);
                return;
            }
            const nextIdxs = _idxsFor(time);
            let unchanged = false;
            try {
                const prev = JSON.parse(el.dataset.clockIdxs || '[]');
                if (prev.length === 4 && prev.every((v, i) => v === nextIdxs[i])) {
                    unchanged = true;
                }
            } catch (_) {
                unchanged = false;
            }
            _persistState(el, time, nextIdxs);
            if (unchanged) return;
            _applyTransforms(el, nextIdxs);
        };

        const _ensure = (el) => {
            if (!el) return;
            const time = _resolveTime(el);
            if (!el.classList.contains('rolling-clock')) _build(el, time);
            else _update(el, time);
        };

        return { ensure: _ensure, update: _update };
    })();

    const ETA_VIEW_PREF_PREFIX = 'stopPanelEtaView:';

    function normalizeStopPanelStopId(reference) {
        if (!reference) return '';
        const attr =
            (reference.dataset && reference.dataset.stopId)
            || (reference.getAttribute && reference.getAttribute('data-stop-id'))
            || '';
        if (attr) return String(attr).trim();
        const card = reference.closest ? reference.closest('[data-stop-primary]') : null;
        if (card) {
            const cardId =
                (card.dataset && card.dataset.stopId)
                || (card.getAttribute && card.getAttribute('data-stop-id'))
                || '';
            if (cardId) return String(cardId).trim();
        }
        const panel = reference.closest ? reference.closest('[data-stop-panel]') : null;
        if (panel) {
            const panelId =
                (panel.dataset && panel.dataset.stopId)
                || (panel.getAttribute && panel.getAttribute('data-stop-id'))
                || '';
            if (panelId) return String(panelId).trim();
        }
        return '';
    }

    function loadEtaViewPreference(stopId) {
        if (!stopId || typeof window === 'undefined' || !window.localStorage) return null;
        try {
            const value = window.localStorage.getItem(`${ETA_VIEW_PREF_PREFIX}${stopId}`);
            return value === 'clock' || value === 'min' ? value : null;
        } catch (_) {
            return null;
        }
    }

    function saveEtaViewPreference(stopId, mode) {
        if (!stopId || typeof window === 'undefined' || !window.localStorage) return;
        const normalized = mode === 'clock' ? 'clock' : mode === 'min' ? 'min' : null;
        if (!normalized) return;
        try {
            window.localStorage.setItem(`${ETA_VIEW_PREF_PREFIX}${stopId}`, normalized);
        } catch (_) {
            /* ignore */
        }
    }

    function inferEtaViewFromMinutes(minutes) {
        if (Number.isFinite(minutes) && minutes >= 60) return 'clock';
        return 'min';
    }

    function applyEtaViewMode(chip, mode) {
        if (!chip) return 'min';
        const minNode = chip.querySelector('#eta-primary');
        const clockNode = chip.querySelector('#eta-clock');
        const hasClock = !!clockNode;
        const wantClock = mode === 'clock' && hasClock;
        const view = wantClock ? 'clock' : 'min';
        const wantMin = !wantClock;

        chip.setAttribute('data-view', view);

        if (minNode) {
            minNode.hidden = !wantMin;
            minNode.setAttribute('aria-hidden', wantMin ? 'false' : 'true');
            minNode.toggleAttribute('inert', !wantMin);
        }
        if (clockNode) {
            clockNode.hidden = !wantClock;
            clockNode.setAttribute('aria-hidden', wantClock ? 'false' : 'true');
            clockNode.toggleAttribute('inert', !wantClock);
        }

        return view;
    }

    function resolvePreferredEtaView(card, minutes) {
        const stopId = normalizeStopPanelStopId(card);
        const stored = loadEtaViewPreference(stopId);
        if (Number.isFinite(minutes) && minutes < 60) return 'min';
        if (stored === 'clock' || stored === 'min') return stored;
        return inferEtaViewFromMinutes(minutes);
    }

    const DEFAULT_STOP_PANEL_STRINGS = Object.freeze({
        primaryLabelRealtime: 'Próximo tren en:',
        primaryLabelScheduled: 'Próxima salida:',
        statusRealtime: 'En tiempo real',
        statusScheduled: 'Programado',
        statusSuffixScheduled: ' · Horario',
        statusSuffixTu: ' · TU',
        trainPlaceholderRealtime: '—',
        trainPlaceholderScheduled: 'Programado',
        trainAriaTemplate: 'Ver detalle del tren %trainId%',
        platformNoteHabitual: 'Habitual: %value%',
        platformNoteConfidence: 'Confianza %value%%',
        platformNoteUnpublishable: 'Sin vía habitual publicable',
        stationStatusLabel: 'En estación',
        secondaryLabel: 'Siguiente tren',
        secondaryPlaceholder: '—',
        secondaryClockPlaceholder: '--:--',
    });

    const stopPanelStringsCache = new WeakMap();

    function getStopPanelStrings(card) {
        if (!card) return DEFAULT_STOP_PANEL_STRINGS;
        if (stopPanelStringsCache.has(card)) return stopPanelStringsCache.get(card);
        const ds = card.dataset || {};
        const strings = {
            primaryLabelRealtime: ds.stringPrimaryLabelRt || DEFAULT_STOP_PANEL_STRINGS.primaryLabelRealtime,
            primaryLabelScheduled: ds.stringPrimaryLabelSched || DEFAULT_STOP_PANEL_STRINGS.primaryLabelScheduled,
            statusRealtime: ds.stringStatusLive || DEFAULT_STOP_PANEL_STRINGS.statusRealtime,
            statusScheduled: ds.stringStatusSched || DEFAULT_STOP_PANEL_STRINGS.statusScheduled,
            statusSuffixScheduled: ds.stringStatusSuffixScheduled || DEFAULT_STOP_PANEL_STRINGS.statusSuffixScheduled,
            statusSuffixTu: ds.stringStatusSuffixTu || DEFAULT_STOP_PANEL_STRINGS.statusSuffixTu,
            trainPlaceholderRealtime: ds.stringTrainPlaceholderLive || DEFAULT_STOP_PANEL_STRINGS.trainPlaceholderRealtime,
            trainPlaceholderScheduled: ds.stringTrainPlaceholderSched || DEFAULT_STOP_PANEL_STRINGS.trainPlaceholderScheduled,
            trainAriaTemplate: ds.stringTrainAriaTemplate || DEFAULT_STOP_PANEL_STRINGS.trainAriaTemplate,
            platformNoteHabitual: ds.stringPlatformNoteHabitual || DEFAULT_STOP_PANEL_STRINGS.platformNoteHabitual,
            platformNoteConfidence: ds.stringPlatformNoteConfidence || DEFAULT_STOP_PANEL_STRINGS.platformNoteConfidence,
            platformNoteUnpublishable: ds.stringPlatformNoteUnpublishable || DEFAULT_STOP_PANEL_STRINGS.platformNoteUnpublishable,
            stationStatusLabel: ds.stringStationLabel || DEFAULT_STOP_PANEL_STRINGS.stationStatusLabel,
            secondaryLabel: ds.stringSecondaryLabel || DEFAULT_STOP_PANEL_STRINGS.secondaryLabel,
            secondaryPlaceholder: ds.stringSecondaryPlaceholder || DEFAULT_STOP_PANEL_STRINGS.secondaryPlaceholder,
            secondaryClockPlaceholder: ds.stringSecondaryClockPlaceholder || DEFAULT_STOP_PANEL_STRINGS.secondaryClockPlaceholder,
        };
        stopPanelStringsCache.set(card, strings);
        return strings;
    }

    function formatStopPanelString(template, params, fallback = '') {
        if (typeof template !== 'string' || template.length === 0) return fallback;
        const hasTokens = /%(\w+)%/.test(template);
        const result = template.replace(/%(\w+)%/g, (match, key) => {
            if (Object.prototype.hasOwnProperty.call(params, key)) {
                return params[key];
            }
            return '';
        });
        if (!hasTokens && fallback) return fallback;
        return result;
    }

    function normalizeStopId(value) {
        if (value === null || value === undefined) return null;
        let text = String(value).trim();
        if (!text) return null;
        text = text.toUpperCase();
        if (/^\d+$/.test(text)) {
            try {
                const n = Number.parseInt(text, 10);
                if (Number.isFinite(n)) return String(n);
            } catch (_) {
                /* ignore */
            }
            return text.replace(/^0+/, '') || '0';
        }
        return text;
    }

    function stopIdsMatch(reference, candidates) {
        const target = normalizeStopId(reference);
        if (!target || !Array.isArray(candidates)) return false;
        for (const candidate of candidates) {
            const normalized = normalizeStopId(candidate);
            if (normalized && normalized === target) return true;
        }
        return false;
    }

        function wireETA(root=document) {
            const chip = root.querySelector ? root.querySelector('.train-eta-chip') : null;
            if (!chip) return;

            const card = chip.closest ? chip.closest('[data-stop-primary]') : null;
            const minEl = chip.querySelector ? chip.querySelector('#eta-primary') : null;
            let clockEl = chip.querySelector ? chip.querySelector('#eta-clock') : null;

            const isLoading = !!(
                (card && card.dataset && card.dataset.loaded === 'false') ||
                (minEl && minEl.getAttribute && minEl.getAttribute('data-loading') === 'true')
            );
            if (isLoading) return;

            if (minEl) RollingNumber.ensure(minEl);

            if (!clockEl) {
                clockEl = document.createElement('output');
                clockEl.id = 'eta-clock';
                clockEl.className = 'train-eta-time is-clock';
                clockEl.setAttribute('role', 'status');
                clockEl.setAttribute('aria-live', 'polite');
                clockEl.setAttribute('aria-atomic', 'true');
                clockEl.setAttribute('data-field', 'primary-clock');
                const attr = minEl && minEl.getAttribute ? minEl.getAttribute('data-eta-min') : null;
                if (attr != null) clockEl.setAttribute('data-eta-min', attr);
                clockEl.hidden = true;
                clockEl.setAttribute('aria-hidden','true');
                clockEl.setAttribute('inert','');
                const time = document.createElement('time');
                time.textContent = '--:--';
                clockEl.appendChild(time);
                const unit = document.createElement('span');
                unit.className = 'unit';
                unit.textContent = 'h';
                clockEl.appendChild(unit);
                chip.appendChild(clockEl);
            }

            if (clockEl) RollingClock.ensure(clockEl);

            const stopId = normalizeStopPanelStopId(chip);
            if (stopId) chip.dataset.stopId = stopId;
            let desiredView = chip.getAttribute('data-view');
            if (desiredView !== 'clock' && desiredView !== 'min') {
                desiredView = loadEtaViewPreference(stopId) || 'min';
            }
            applyEtaViewMode(chip, desiredView);
        }

    let etaDelegatedBound = false;
    function bindGlobalEtaToggleDelegation() {
        if (etaDelegatedBound) return;
        etaDelegatedBound = true;

        document.addEventListener('click', (ev) => {
            const chip = ev.target.closest('.train-eta-chip');
            if (!chip) return;

            const minEl = chip.querySelector('#eta-primary');
            const clockEl = chip.querySelector('#eta-clock');

            if (minEl) RollingNumber.ensure(minEl);
            if (clockEl) {
                const mm = (minEl && (minEl.getAttribute('data-eta-min') || minEl.textContent)) || clockEl.getAttribute('data-eta-min') || '0';
                clockEl.setAttribute('data-eta-min', String(mm).replace(/[^\d,.\-]/g, '').replace(',', '.'));
                if (clockEl.dataset && clockEl.dataset.clockHhmm) {
                    clockEl.setAttribute('data-eta-hhmm', clockEl.dataset.clockHhmm);
                }
                RollingClock.ensure(clockEl);
            }

            const currentView = chip.getAttribute('data-view') || (clockEl && !clockEl.hidden ? 'clock' : 'min');
            const nextIsClock = currentView !== 'clock';
            const wantClock = !!nextIsClock && !!clockEl;
            const nextMode = wantClock ? 'clock' : 'min';
            applyEtaViewMode(chip, nextMode);
            const stopId = normalizeStopPanelStopId(chip);
            saveEtaViewPreference(stopId, nextMode);
        }, { passive: true });
    }

// ------------------ Search box ------------------
    function enhanceSearchBox(form) {
        if (!form || upgradedForms.has(form)) return;
        upgradedForms.add(form);

        const qInput    = form.querySelector('input[name="q"]');
        const nearbyBtn = form.querySelector('.nearby-btn');

        const removeLatLon = () => {
            form.querySelectorAll('input[name="lat"], input[name="lon"]').forEach(el => el.remove());
        };

        const addHidden = (name, value) => {
            const el = document.createElement('input');
            el.type  = 'hidden';
            el.name  = name;
            el.value = value;
            form.appendChild(el);
            return el;
        };

        qInput?.addEventListener('input', removeLatLon);

        form.addEventListener('submit', () => {
            form.querySelectorAll('input[name="lat"], input[name="lon"]').forEach(el => {
                if (!el.value || Number.isNaN(Number(el.value))) el.remove();
            });
            if (qInput && qInput.value.trim() !== '') removeLatLon();
        });

        if (nearbyBtn && !boundButtons.has(nearbyBtn)) {
            boundButtons.add(nearbyBtn);

            nearbyBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                if (!('geolocation' in navigator)) {
                    alert('Tu navegador no soporta geolocalización.');
                    return;
                }

                const wasRequired = qInput?.required ?? false;
                if (qInput) { qInput.required = false; qInput.value = ''; }
                form.setAttribute('novalidate', 'novalidate');

                const prevDisabled = nearbyBtn.disabled;
                nearbyBtn.disabled = true;

                navigator.geolocation.getCurrentPosition(
                    (pos) => {
                        const { latitude, longitude } = pos.coords || {};
                        if (latitude == null || longitude == null) throw new Error('No se pudo obtener la ubicación.');
                        removeLatLon();
                        addHidden('lat', String(Number(latitude.toFixed(6))));
                        addHidden('lon', String(Number(longitude.toFixed(6))));
                        if (typeof form.requestSubmit === 'function') form.requestSubmit();
                        else form.submit();
                    },
                    (err) => {
                        console.error(err);
                        alert('No se pudo obtener la ubicación (permiso denegado o error).');
                        if (qInput) qInput.required = wasRequired;
                        form.removeAttribute('novalidate');
                        nearbyBtn.disabled = prevDisabled;
                    },
                    { enableHighAccuracy: true, timeout: 8000, maximumAge: 0 }
                );
            });
        }
    }

// ------------------ Side sheet ------------------
    function bindSideSheet(root = document) {
        const btn = root.querySelector('.topbar-btn-menu') || document.querySelector('.topbar-btn-menu');
        const dlg = (root.getElementById && root.getElementById('app-sheet')) || document.getElementById('app-sheet');
        if (!dlg) return;

        if (!boundDialogs.has(dlg)) {
            boundDialogs.add(dlg);

            function closeSheet() {
                if (dlg.open && typeof dlg.close === 'function') dlg.close();
                else dlg.removeAttribute('open');
                if (btn) btn.setAttribute('aria-expanded', 'false');
                const last = dlg.__lastFocus;
                if (last && document.contains(last)) { try { last.focus(); } catch(_) {} }
            }
            dlg.__closeSheet = closeSheet;

            dlg.addEventListener('click', (ev) => {
                if (ev.target.closest('[data-close-sheet]')) {
                    ev.preventDefault();
                    closeSheet();
                    return;
                }
                const panel = dlg.querySelector('.sheet-panel');
                if (!panel) return;
                const r = panel.getBoundingClientRect();
                const inPanel = ev.clientX >= r.left && ev.clientX <= r.right && ev.clientY >= r.top && ev.clientY <= r.bottom;
                if (!inPanel) closeSheet();
            });

            if (!htmxHooked) {
                htmxHooked = true;
                document.addEventListener('htmx:beforeSwap', (ev) => {
                    if (dlg.__closeSheet) dlg.__closeSheet();
                });
            }
        }

        if (btn && !boundButtons.has(btn)) {
            boundButtons.add(btn);
            btn.addEventListener('click', () => {
                if (!dlg) return;
                try { dlg.__lastFocus = document.activeElement; } catch(_) {}
                btn.setAttribute('aria-expanded', 'true');
                if (typeof dlg.showModal === 'function') dlg.showModal();
                else dlg.setAttribute('open', '');
            });
        }
    }

// ---------- Toggle line direction cards ----------
    function bindReverseToggleDelegated() {
        if (reverseHandlerBound) return;
        reverseHandlerBound = true;

        document.addEventListener('click', function(ev){
            const btn = ev.target.closest('.line-toggle-reverse');
            if (!btn) return;

            const card = btn.closest('li.line-card');
            if (!card) return;

            const next = card.getAttribute('data-state') === 'a' ? 'b' : 'a';
            card.setAttribute('data-state', next);
            btn.setAttribute('aria-pressed', next === 'b' ? 'true' : 'false');

            const aA = card.querySelector('a.line-link--a');
            const aB = card.querySelector('a.line-link--b');
            if (aA && aB) {
                const showB = next === 'b';
                setTimeout(() => {
                    aA.hidden = showB;
                    aB.hidden = !showB;
                    aA.toggleAttribute('inert', showB);
                    aB.toggleAttribute('inert', !showB);
                    aA.setAttribute('aria-hidden', showB ? 'true' : 'false');
                    aB.setAttribute('aria-hidden', showB ? 'false' : 'true');
                }, 340);
            }
        }, { passive: true });
    }

// ---------- Toggle de correspondencias ----------
    function bindConnectionsToggleDelegated() {
        document.addEventListener('click', function (ev) {
            const btn = ev.target.closest('.toggle-connections-shown');
            if (!btn) return;

            const wrappers = document.querySelectorAll('.grid-route-map .connections-group');
            if (!wrappers.length) return;

            const anyVisible = Array.from(wrappers).some(el => !el.hidden);
            const willShow = !anyVisible;

            wrappers.forEach(el => {
                el.hidden = !willShow;
                el.toggleAttribute('inert', !willShow);
                el.setAttribute('aria-hidden', willShow ? 'false' : 'true');
            });

            btn.setAttribute('aria-pressed', willShow ? 'true' : 'false');

        }, { passive: true });
    }

// ------------------ Drawer Train routes ------------------
    function bindRouteTrainsPanel(root = document) {
        console.debug('[trains] bindRouteTrainsPanel() called. root=', root);
        const btn   = root.querySelector('#btn-toggle-trains') || document.querySelector('#btn-toggle-trains');
        const panel = document.getElementById('drawer');
        const body  = document.getElementById('drawer-content');
        const close = null;
        console.debug('[trains] btn?', !!btn, 'panel?', !!panel, 'body?', !!body, 'btn=', btn);
        if (!panel || !body) return;

        panel.setAttribute('role', 'dialog');
        panel.setAttribute('aria-modal', 'true');
        if (!panel.hasAttribute('tabindex')) panel.setAttribute('tabindex', '-1');
        if (panel.getAttribute('aria-hidden') !== 'false') {
            panel.setAttribute('aria-hidden', 'true');
            panel.setAttribute('inert', '');
            panel.hidden = true;
        }

        function resolveUrl(sourceBtn) {
            try {
                const b = sourceBtn || document.querySelector('#btn-toggle-trains');
                const fromBtn   = b && (b.getAttribute('data-url') || b.dataset?.url);
                const fromPanel = panel.getAttribute('data-url') || panel.dataset?.url;
                if (fromBtn) return fromBtn;
                if (fromPanel) return fromPanel;
                return location.pathname.replace(/\/$/, '') + '/trains';
            } catch(_) {
                return '/trains';
            }
        }

        function urlWithCtx(baseUrl) {
            try {
                const url = new URL(baseUrl, location.origin);
                const ctxForm = body.querySelector('#rtp-ctx');
                if (ctxForm) {
                    const fd = new FormData(ctxForm);
                    for (const [k, v] of fd.entries()) {
                        if (v != null && String(v) !== '') url.searchParams.set(k, String(v));
                    }
                }
                return url.toString();
            } catch(_) {
                return baseUrl;
            }
        }

        console.debug('[trains] boundPanels.has(panel)=', boundPanels.has(panel));
        if (!boundPanels.has(panel)) {
            boundPanels.add(panel);
            let lastFocusEl = null;

            function loadOnce(sourceBtn) {
                const u0  = resolveUrl(sourceBtn);
                const ctx = RouteCtx.getMain();
                const url = RouteCtx.appendToURL(u0, ctx);

                fetch(url, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
                        const incoming = RouteCtx.getDrawerFromHTML(html);
                        const now = RouteCtx.getMain();
                        if (incoming && now && !RouteCtx.equal(now, incoming)) {
                            console.warn('[trains] DESCARTADO por contexto: incoming=', incoming, 'now=', now);
                            return;
                        }

                        const template = document.createElement('template');
                        template.innerHTML = html;
                        if (template.content.firstChild) {
                            body.innerHTML = html;
                            if (window.htmx) htmx.process(body);
                            enrichBodyBindings();
                            panel.dataset.trainsLoaded = '1';
                            panel.dataset.trainsCtx = url;
                        }
                    })
                    .catch(() => { console.debug('[trains] loadOnce() FAILED'); body.innerHTML = '<p>Error al cargar los trenes.</p>'; });
            }

            function bindBodyLinks() {
                body.querySelectorAll('a[href]').forEach(a => {
                    if (boundButtons.has(a)) return;
                    boundButtons.add(a);
                    a.addEventListener('click', () => {
                        closePanel();
                    });
                });
            }

            function enrichBodyBindings() {
                bindBodyLinks();
                const refreshBtn = body.querySelector('#update-route-train-list');
                if (refreshBtn && !boundButtons.has(refreshBtn)) {
                    boundButtons.add(refreshBtn);
                    refreshBtn.addEventListener('click', (ev) => {
                        ev.preventDefault();
                        ev.stopPropagation();
                        if (typeof ev.stopImmediatePropagation === 'function') ev.stopImmediatePropagation();
                        refreshNow();
                    });
                }
            }

            function openPanel(sourceBtn) {
                console.debug('[trains] openPanel()');

                try { ActiveStop.clear(); } catch(_) {}

                lastFocusEl = (document.activeElement && document.contains(document.activeElement))
                    ? document.activeElement : (sourceBtn || document.body);

                setDrawerMode('trains');
                openStaticDrawer();
                if (sourceBtn) sourceBtn.setAttribute('aria-expanded', 'true');
                else {
                    const b = document.querySelector('#btn-toggle-trains');
                    if (b) b.setAttribute('aria-expanded', 'true');
                }

                const focusTarget = panel.querySelector('.drawer-close') || panel;
                requestAnimationFrame(() => { try { focusTarget.focus(); } catch(_) {} });

                loadOnce(sourceBtn);
                try { document.dispatchEvent(new CustomEvent('open:trains-drawer')); } catch(_) {}
            }

            function closePanel() {
                console.debug('[trains] closePanel()');
                const fallback = (document.querySelector('#btn-toggle-trains')) || lastFocusEl || document.body;
                if (panel.contains(document.activeElement)) { try { fallback.focus(); } catch(_) {} }

                closeStaticDrawer();
                try {
                    const b = document.querySelector('#btn-toggle-trains');
                    if (b) b.setAttribute('aria-expanded', 'false');
                } catch(_) {}
            }

            let refreshing = false;
            function refreshNow() {
                if (refreshing) return;
                refreshing = true;

                const inner     = panel.querySelector('.drawer-inner');
                const bodyEl    = body;
                const indicator = bodyEl.querySelector('.htmx-indicator');

                if (indicator) indicator.style.opacity = '1';
                bodyEl.querySelectorAll('#update-route-train-list').forEach(b => b.disabled = true);

                const base = resolveUrl();
                const ctx  = RouteCtx.getMain();
                const url  = RouteCtx.appendToURL(base, ctx);

                fetch(url, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
                        const incoming = RouteCtx.getDrawerFromHTML(html);
                        const now = RouteCtx.getMain();
                        if (incoming && now && !RouteCtx.equal(now, incoming)) {
                            console.warn('[trains] REFRESH descartado por contexto');
                            return;
                        }

                        inner.classList.add('is-fading');
                        const animEl = panel.querySelector('.drawer-body') || inner;

                        let swapped = false;
                        const doSwap = () => {
                            if (swapped) return;
                            swapped = true;

                            const tpl = document.createElement('template');
                            tpl.innerHTML = html;

                            if (tpl.content.firstChild) {
                                bodyEl.innerHTML = html;
                                if (window.htmx) htmx.process(bodyEl);
                                enrichBodyBindings();
                                panel.dataset.trainsLoaded = '1';
                                panel.dataset.trainsCtx = url;
                            } else {
                                bodyEl.innerHTML = '<p>Error al cargar los trenes.</p>';
                            }

                            requestAnimationFrame(() => inner.classList.remove('is-fading'));
                        };

                        const onEnd = (ev) => {
                            if (ev.propertyName !== 'opacity') return;
                            animEl.removeEventListener('transitionend', onEnd);
                            doSwap();
                        };
                        animEl.addEventListener('transitionend', onEnd, { once: true });

                        setTimeout(() => {
                            try { animEl.removeEventListener('transitionend', onEnd); } catch(_) {}
                            doSwap();
                        }, 320);
                    })
                    .catch(() => {
                        body.innerHTML = '<p>Error al cargar los trenes.</p>';
                        panel.querySelector('.drawer-inner')?.classList.remove('is-fading');
                    })
                    .finally(() => {
                        if (indicator) indicator.style.opacity = '';
                        bodyEl.querySelectorAll('#update-route-train-list').forEach(b => b.disabled = false);
                        refreshing = false;
                    });
            }

            panel.__closeTrainsPanel   = closePanel;
            panel.__openTrainsPanel    = openPanel;
            panel.__refreshTrainsPanel = refreshNow;
        }

        if (btn && !boundButtons.has(btn)) {
            console.debug('[trains] binding btn listener', btn);
            boundButtons.add(btn);
            btn.addEventListener('click', (e) => {
                console.debug('[trains] btn click; panel open?', document.getElementById('drawer')?.classList.contains('open'), 'mode=', document.getElementById('drawer')?.dataset.mode, 'hasOpenFn?', !!document.getElementById('drawer')?.__openTrainsPanel);
                e.preventDefault();
                const p = document.getElementById('drawer');
                if (!p) return;
                const isOpen = p.classList.contains('open') && p.dataset.mode === 'trains';
                if (isOpen && p.__closeTrainsPanel) {
                    p.__closeTrainsPanel();
                } else if (p.__openTrainsPanel) {
                    p.__openTrainsPanel(btn);
                } else {
                    bindRouteTrainsPanel(document);
                    const p2 = document.getElementById('drawer');
                    if (p2 && p2.__openTrainsPanel) p2.__openTrainsPanel(btn);
                }
            });
            if (!btn.hasAttribute('aria-controls')) btn.setAttribute('aria-controls', 'drawer');
            if (!btn.hasAttribute('aria-expanded')) btn.setAttribute('aria-expanded', 'false');
        }
    }

// ---------- Active Stop ----------
    const ActiveStop = (() => {
        let currentEl = null;
        let currentStationId = null;

        function _extractStationIdFromHref(href) {
            try {
                const m = String(href).match(/\/stops\/([^\/?#]+)/);
                return m ? decodeURIComponent(m[1]) : null;
            } catch (_) { return null; }
        }

        function _findLiByStationId(sid) {
            if (!sid) return null;
            const anchors = document.querySelectorAll('.grid-route-map a[href*="/stops/"]');
            for (const a of anchors) {
                const raw = a.getAttribute('href') || a.href || '';
                const id  = _extractStationIdFromHref(raw);
                if (id && id === sid) {
                    return a.closest('li.grid-route-map-station');
                }
            }
            return null;
        }

        function setByHref(href) {
            const sid = _extractStationIdFromHref(href);
            if (!sid) return;
            setByStationId(sid);
        }

        function setByStationId(sid) {
            const li = _findLiByStationId(sid);
            if (!li) return;
            if (currentEl === li) return;
            clear();
            currentEl = li;
            currentStationId = sid;
            li.classList.add('is-active');
            li.setAttribute('aria-current', 'true');
        }

        function clear() {
            if (currentEl) {
                currentEl.classList.remove('is-active');
                currentEl.removeAttribute('aria-current');
            }
            currentEl = null;
            currentStationId = null;
        }

        function get() { return { el: currentEl, stationId: currentStationId }; }

        return { setByHref, setByStationId, clear, get };
    })();

// ------------------ Drawer route stops ------------------
    function bindStopDrawer(root = document) {
        const panel = document.getElementById('drawer');
        if (!panel) return;

        const body  = document.getElementById('drawer-content');
        const close = panel.querySelector('.drawer-close');
        if (!body) return;

        const STOP_REFRESH_INTERVALS = {
            subMinute: 12_000,
            halfMinute: 7_000,
            imminent: 4_000,
            dwell: 2_500,
            min: 2_000,
        };

        if (!panel.__stopAuto) {
            panel.__stopAuto = {
                timerId: null,
                abort: null,
                baseInterval: 30_000,
                maxInterval: 180_000,
                errors: 0,
                running: false,
                apiUrl: '',
                inFlight: false,
                minInterval: STOP_REFRESH_INTERVALS.min,
            };
        }

        if (!panel.__a11yInit) {
            panel.setAttribute('role', 'dialog');
            panel.setAttribute('aria-modal', 'true');
            if (!panel.hasAttribute('tabindex')) panel.setAttribute('tabindex', '-1');
            if (panel.getAttribute('aria-hidden') !== 'false') {
                panel.setAttribute('aria-hidden', 'true');
                panel.setAttribute('inert', '');
                panel.hidden = true;
            }
            panel.__a11yInit = true;
        }

        disableBoostForStopLinks(root);

        let lastFocusEl = null;

        function teardownStopAutoRefresh() {
            const st = panel.__stopAuto;
            if (!st) return;
            if (st.timerId) { clearTimeout(st.timerId); st.timerId = null; }
            if (st.abort) { try { st.abort.abort(); } catch(_) {} st.abort = null; }
            st.errors = 0;
            st.running = false;
            st.inFlight = false;
        }

        function scheduleNextStopTick(ms) {
            const st = panel.__stopAuto;
            if (!st || !st.running) return;
            if (st.timerId) clearTimeout(st.timerId);
            st.timerId = setTimeout(() => refreshStopApproachingNow(false), ms);
        }

        function startStopAutoRefresh() {
            const st = panel.__stopAuto;
            if (!st || st.running) return;
            st.running = true;
            st.errors = 0;
            st.inFlight = false;
            if (st.timerId) { clearTimeout(st.timerId); st.timerId = null; }

            if (!panel.dataset.stopApi) {
                const ctxEl = body.querySelector('#drawer-context');
                if (ctxEl) {
                    const rid = ctxEl.getAttribute('data-route-id') || '';
                    const sid = ctxEl.getAttribute('data-stop-id') || '';
                    if (rid && sid) {
                        const limitAttr = ctxEl.getAttribute('data-services-limit') || '10';
                        const tzAttr = ctxEl.getAttribute('data-services-tz') || 'Europe/Madrid';
                        panel.dataset.stopApi = `/api/stops/${rid}/${sid}/services?limit=${limitAttr}&tz=${tzAttr}`;
                    }
                }
            }

            refreshStopApproachingNow(true);
        }

        if (!stopGlobalHandlersBound) {
            stopGlobalHandlersBound = true;
            document.addEventListener('visibilitychange', () => {
                const st = panel.__stopAuto;
                if (!st) return;
                if (document.hidden) {
                    teardownStopAutoRefresh();
            panel.dataset.stopApi = '';
            panel.dataset.stopRouteId = '';
            panel.dataset.stopStopId = '';
            panel.dataset.stopDir = '';
                } else if (panel.classList.contains('open') && panel.dataset.mode === 'stop') {
                    startStopAutoRefresh();
                }
            }, { passive: true });
        }


        function applyStopServicesPayload(payload) {
        const root = body.querySelector('[data-stop-panel]');
        if (!root) return;
        const stopIdRaw =
            (root.dataset && root.dataset.stopId)
            || (root.getAttribute && root.getAttribute('data-stop-id'))
            || '';
        const normalizedStopId = normalizeStopId(stopIdRaw);
        let services = Array.isArray((payload && payload.services)) ? payload.services : [];
        // Hide services very close (<5 min) with no evidence of real approaching
        services = services.filter((svc) => {
            const minutes = Number(svc?.eta_seconds) / 60;
            const etaIsSoon = Number.isFinite(minutes) && minutes < 5;
            const isRealtime = (svc?.status || '').toLowerCase() === 'realtime';
            const hasTu = Boolean(svc?.source && String(svc.source).toLowerCase().includes('tu'));
            const train = svc?.train || {};
            const hasLivePos = train.lat != null || train.lon != null;
            const status = (train.current_status || '').toUpperCase();
            const stopMatches = normalizedStopId
                ? stopIdsMatch(normalizedStopId, [svc?.train?.current_stop_id, svc?.train?.stop_id])
                : false;
            const likelyApproaching = status === 'IN_TRANSIT_TO' || status === 'INCOMING_AT' || stopMatches;
            if (etaIsSoon && !isRealtime && !hasLivePos && !hasTu) return false;
            if (etaIsSoon && isRealtime && !likelyApproaching && !hasTu && !hasLivePos) return false;
            return true;
        });
        const primary = services[0] || null;
        const secondary = services[1] || null;

        const ctxEl = body.querySelector('#drawer-context');
        const nucleusSlug = ctxEl ? (ctxEl.getAttribute('data-nucleus') || '') : '';

        const card = root.querySelector('[data-stop-primary]');
        const footer = root.querySelector('[data-stop-footer]');
        const empty = root.querySelector('[data-field="empty-message"]');
        const lastSeen = root.querySelector('[data-field="last-seen"]');
        const stopAuto = panel.__stopAuto || null;

        if (root && root.dataset) {
            root.dataset.stopState = primary ? 'ready' : 'loading';
        }

        if (stopAuto && stopAuto.emptyTimer) {
            clearTimeout(stopAuto.emptyTimer);
            stopAuto.emptyTimer = null;
        }

        if (empty) {
            empty.hidden = true;
            empty.setAttribute('aria-hidden', 'true');
            empty.style.display = 'none';
        }

        if (!primary) {
            if (card) {
                card.dataset.loaded = 'false';
                card.classList.add('is-loading');
                card.removeAttribute('hidden');
            }
            if (footer) footer.setAttribute('hidden', '');
            if (lastSeen) lastSeen.setAttribute('hidden', '');

            if (stopAuto) {
                const timer = setTimeout(() => {
                    if (stopAuto.emptyTimer !== timer) return;
                    stopAuto.emptyTimer = null;
                    if (card) card.setAttribute('hidden', '');
                    if (empty) {
                        if (root && root.dataset) root.dataset.stopState = 'empty';
                        empty.hidden = false;
                        empty.removeAttribute('hidden');
                        empty.setAttribute('aria-hidden', 'false');
                        empty.style.display = '';
                    }
                }, 1200);
                stopAuto.emptyTimer = timer;
            } else if (empty) {
                if (root && root.dataset) root.dataset.stopState = 'empty';
                empty.hidden = false;
                empty.removeAttribute('hidden');
                empty.setAttribute('aria-hidden', 'false');
                empty.style.display = '';
            }
            return;
        }

            if (card) card.removeAttribute('hidden');
            if (footer) footer.removeAttribute('hidden');
            if (empty) {
                empty.hidden = true;
                empty.setAttribute('aria-hidden', 'true');
                empty.setAttribute('hidden', '');
                empty.style.display = 'none';
            }

            updatePrimaryCard(card, primary, nucleusSlug);
            updateSecondary(card, secondary, nucleusSlug);
            updateFooter(footer, primary, nucleusSlug);
            updateLastSeen(lastSeen, primary);
            wireETA(card);
        }

        function minutesFromService(service) {
            if (!service) return null;
            const sec = typeof service.eta_seconds === 'number' ? service.eta_seconds : null;
            if (sec === null) return null;
            return Math.max(0, Math.round(sec / 60));
        }

        function hhmmFromService(service) {
            if (!service) return null;
            if (service.hhmm) return service.hhmm;
            if (typeof service.epoch === 'number') {
                try {
                    const dt = new Date(service.epoch * 1000);
                    return dt.toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' });
                } catch (_) {
                    return null;
                }
            }
            return null;
        }

        function formatDelayText(service) {
            if (!service) return '';
            if (typeof service.delay_seconds !== 'number') return '';
            const mins = Math.round(service.delay_seconds / 60);
            if (mins === 0) return '';
            return ` (${mins > 0 ? '+' : ''}${mins} min)`;
        }

        function extractPlatformFromLabel(label) {
            if (!label) return null;
            const match = /PLATF\.?\(?([^)]+)\)?/i.exec(String(label));
            if (!match || !match[1]) return null;
            const value = match[1].trim();
            return value && value !== '?' ? value : null;
        }

        function updatePrimaryCard(card, service, nucleusSlug) {
            if (!card) return;
            const strings = getStopPanelStrings(card);
            const minutes = minutesFromService(service);
            const hhmm = hhmmFromService(service);
            const etaSeconds = (typeof service.eta_seconds === 'number' && Number.isFinite(service.eta_seconds))
                ? Math.max(0, service.eta_seconds)
                : null;
            const isLessThanOneMinute = etaSeconds !== null && etaSeconds < 60;

            card.dataset.loaded = 'true';
            card.classList.remove('is-loading');

            const loader = card.querySelector('[data-field="primary-loader"]');
            if (loader) {
                loader.setAttribute('aria-busy', 'false');
                loader.setAttribute('aria-hidden', 'true');
            }

            const chip = card.querySelector('.train-eta-chip');

            const label = card.querySelector('[data-field="primary-label"]');
            if (label) {
                label.textContent = service.status === 'realtime'
                    ? strings.primaryLabelRealtime
                    : strings.primaryLabelScheduled;
            }

            const statusText = card.querySelector('[data-field="primary-status-text"]');
            if (statusText) {
                let text = service.status === 'realtime' ? strings.statusRealtime : strings.statusScheduled;
                const source = (service.source || '').toUpperCase();
                if (source === 'SCHEDULED' && strings.statusSuffixScheduled) text += strings.statusSuffixScheduled;
                else if (source.startsWith('TU') && strings.statusSuffixTu) text += strings.statusSuffixTu;
                const delay = formatDelayText(service);
                statusText.textContent = delay ? text + delay : text;
            }

            card.dataset.serviceStatus = service.status || '';
            card.dataset.serviceSource = service.source || '';

            const stopPanel = card.closest('[data-stop-panel]');
            const stopIdAttr = card.getAttribute('data-stop-id') || card.dataset.stopId || (stopPanel ? stopPanel.dataset.stopId : '') || '';
            const stopId = stopIdAttr != null ? String(stopIdAttr) : '';
            if (chip && stopId) chip.dataset.stopId = stopId;
            const train = service.train || {};
            const trainState = (train.current_status || '').toUpperCase();
            const actualStopCandidates = [];
            const pushActualStop = (value) => {
                if (value === null || value === undefined) return;
                const text = String(value).trim();
                if (!text) return;
                actualStopCandidates.push(text);
            };
            pushActualStop(train.stop_id);
            pushActualStop(train.stopId);
            const serviceActualKeys = [
                'actual_stop_id', 'actualStopId',
                'current_stop_id', 'currentStopId',
                'realtime_stop_id', 'realtimeStopId',
                'stopped_stop_id', 'stoppedStopId',
            ];
            for (const key of serviceActualKeys) {
                pushActualStop(service[key]);
            }
            const row = (service.row && typeof service.row === 'object') ? service.row : {};
            const rowActualKeys = [
                'actual_stop_id', 'actualStopId',
                'current_stop_id', 'currentStopId',
                'realtime_stop_id', 'realtimeStopId',
                'stopped_stop_id', 'stoppedStopId',
                'last_stop_id', 'lastStopId',
            ];
            for (const key of rowActualKeys) {
                pushActualStop(row[key]);
            }
            if (row.current_stop && typeof row.current_stop === 'object') {
                const cur = row.current_stop;
                pushActualStop(cur.stop_id);
                pushActualStop(cur.stopId);
                pushActualStop(cur.id);
            }
            if (row.actual_stop && typeof row.actual_stop === 'object') {
                const cur = row.actual_stop;
                pushActualStop(cur.stop_id);
                pushActualStop(cur.stopId);
                pushActualStop(cur.id);
            }
            const isRealtime = (service.status || '').toLowerCase() === 'realtime';
            const stopMatches = stopIdsMatch(stopId, actualStopCandidates);
            const nextStopId = service.next_stop_id
                || (service.train && service.train.next_stop_id)
                || (row && row.next_stop_id)
                || null;
            const stopMatchesNext = stopIdsMatch(stopId, [nextStopId]);
            const progressPct = Number(service.next_stop_progress_pct
                ?? (service.train && service.train.next_stop_progress_pct));
            const isStoppedAtStation = isRealtime
                && trainState === 'STOPPED_AT'
                && stopMatches;
            const inferredStationByProgress = isRealtime
                && stopMatchesNext
                && Number.isFinite(progressPct)
                && progressPct >= 99;

            const pill = card.querySelector('[data-field="primary-pill"]');
            if (pill) {
                pill.classList.remove('is-tu', 'is-sched', 'is-est');
                pill.classList.add(service.status === 'realtime' ? 'is-tu' : 'is-sched');
            }

            const minEl = card.querySelector('[data-field="primary-minutes"]');
            if (minEl) {
                const value = Number.isFinite(minutes) ? minutes : 0;
                minEl.setAttribute('data-eta-min', value);
                minEl.setAttribute('data-loading', 'false');
                minEl.dataset.ltMinute = isLessThanOneMinute ? 'true' : 'false';
                // Mark station state before building to avoid flicker when stopped at platform
                const stationState = isStoppedAtStation || inferredStationByProgress;
                minEl.dataset.stationActive = stationState ? 'true' : 'false';
                const timeNode = minEl.querySelector('time');
                if (timeNode) {
                    if (Number.isFinite(minutes)) {
                        timeNode.textContent = isLessThanOneMinute ? '<1' : String(value);
                    } else {
                        timeNode.textContent = '--';
                    }
                }
                RollingNumber.ensure(minEl);
                RollingNumber.setStationState(minEl, isStoppedAtStation, strings.stationStatusLabel);
            }

            const clockEl = card.querySelector('[data-field="primary-clock"]');
            if (clockEl) {
                clockEl.setAttribute('data-eta-min', Number.isFinite(minutes) ? minutes : 0);
                const hhmmAttr = typeof hhmm === 'string' ? hhmm.trim() : '';
                const validHhmm = /^(\d{1,2})[:hH](\d{1,2})$/.test(hhmmAttr) ? hhmmAttr : '';
                clockEl.setAttribute('data-eta-hhmm', validHhmm);
                clockEl.setAttribute('data-loading', 'false');
                const timeNode = clockEl.querySelector('time');
                if (timeNode) timeNode.textContent = hhmm || '--:--';
                RollingClock.ensure(clockEl);
            }

            if (chip) {
                const desiredView = resolvePreferredEtaView(card, minutes);
                applyEtaViewMode(chip, desiredView);
            }

            const rowStatus = (service?.row?.status || '').toUpperCase();
            const isRowCurrent = rowStatus === 'CURRENT';
            const nextStopMatches = rowStatus === 'NEXT';
            const approachingStop = stopMatches || nextStopMatches;

            const platformBadge = card.querySelector('[data-field="platform"]');
            const platformLabel = card.querySelector('[data-field="platform-label"]');
            const currentBadgeValue = platformBadge?.dataset?.platform || platformLabel?.textContent || '—';
            const currentBadgeSource = platformBadge?.dataset?.src || 'unknown';
            let platformText = currentBadgeValue;
            let platformSource = currentBadgeSource;
            const info = service.platform_info || null;
            const normalizePlatformValue = (value) => {
                if (value === null || value === undefined) return null;
                const text = String(value).trim();
                return text && text !== '?' ? text : null;
            };
            const publishable = info?.publishable === undefined ? true : !!info?.publishable;
            const predicted = normalizePlatformValue(info?.predicted);
            const predictedAlt = normalizePlatformValue(info?.predicted_alt);
            const habitualCandidate = predicted || predictedAlt || null;
            const initialHabitual = normalizePlatformValue(platformBadge?.dataset?.habitual);
            if (habitualCandidate && platformBadge) {
                platformBadge.dataset.habitual = habitualCandidate;
            }
            const habitualReference = normalizePlatformValue(platformBadge?.dataset?.habitual)
                || habitualCandidate
                || initialHabitual
                || null;
            const rowPlatform = normalizePlatformValue(service?.row?.platform);
            const trainObj = (service?.train && typeof service.train === 'object') ? service.train : null;
            const rowDelta = Number(service?.row?.delta_seq_to_stop);
            const isRowApproaching = rowStatus === 'NEXT' || rowStatus === 'CURRENT';
            const isRowClose = Number.isFinite(rowDelta) && rowDelta <= 1;
            const extendedApproach = approachingStop || isRowApproaching || isRowClose;
            const trainStatus = (trainObj?.current_status || trainState || '').toUpperCase();
            const trainApproaching = ['IN_TRANSIT_TO','INCOMING_AT','STOPPED_AT'].includes(trainStatus);
            const canUseTrainLabelForStop = isRealtime
                && (stopMatches || isRowCurrent)
                && trainStatus !== 'IN_TRANSIT_TO';

            let liveCandidate = normalizePlatformValue(info?.observed) || null;
            if (!liveCandidate && trainObj && extendedApproach && trainApproaching) {
                const mapping = trainObj.platform_by_stop || trainObj.platformByStop;
                if (mapping && stopId && mapping[stopId]) {
                    liveCandidate = normalizePlatformValue(mapping[stopId]);
                }
                if (!liveCandidate && canUseTrainLabelForStop) {
                    liveCandidate = normalizePlatformValue(trainObj.platform);
                }
                if (!liveCandidate && canUseTrainLabelForStop && trainObj.label) {
                    liveCandidate = normalizePlatformValue(extractPlatformFromLabel(trainObj.label));
                }
                if (!liveCandidate && rowPlatform && (stopMatches || isRowApproaching)) {
                    liveCandidate = rowPlatform;
                }
            }

            let updated = false;

            if (liveCandidate) {
                platformText = liveCandidate;
                platformSource = 'live';
                updated = true;
                if (habitualCandidate && habitualCandidate !== liveCandidate) {
                    platformNoteText = formatStopPanelString(
                        strings.platformNoteHabitual,
                        { value: habitualCandidate },
                        `Habitual: ${habitualCandidate}`,
                    );
                }
            } else if (habitualCandidate) {
                platformText = habitualCandidate;
                platformSource = 'habitual';
                updated = true;
            } else if (rowPlatform) {
                platformText = rowPlatform;
                platformSource = 'habitual';
                updated = true;
            }

            if (!updated) {
                return;
            }

            if (platformBadge) {
                platformBadge.dataset.platform = platformText;
                platformBadge.dataset.src = platformSource;
                platformBadge.classList.remove('is-live','is-habitual','is-unknown','exceptional-platform');
                if (platformSource === 'live') platformBadge.classList.add('is-live');
                else if (platformSource === 'habitual') platformBadge.classList.add('is-habitual');
                else platformBadge.classList.add('is-unknown');
                if (platformSource === 'live' && habitualReference && habitualReference !== platformText) {
                    platformBadge.classList.add('exceptional-platform');
                }
            }

            if (platformLabel) platformLabel.textContent = platformText;

        }

        function normalizeTrainToken(value) {
            if (value === null || value === undefined) return '';
            const str = String(value).trim();
            return str;
        }

        function extractNumericTrainIdentifier(value) {
            const token = normalizeTrainToken(value);
            if (!token) return '';
            if (/^\d{3,6}$/.test(token)) return token;
            const matches = token.match(/\d{3,6}/g);
            if (!matches || matches.length === 0) return '';
            for (let i = matches.length - 1; i >= 0; i -= 1) {
                const candidate = matches[i];
                if (/^\d{3,6}$/.test(candidate)) return candidate;
            }
            return '';
        }

        function resolveTrainInfo(service) {
            if (!service) return { label: '', identifier: '' };
            const candidates = [
                service?.train?.train_id,
                service?.train?.train_number,
                service?.train?.label,
                service?.train_id,
                service?.vehicle_id,
                service?.row?.train_id,
                service?.row?.train_number,
                service?.row?.label,
                service?.trip_id,
                service?.service_instance_id,
            ];
            for (const candidate of candidates) {
                const ident = extractNumericTrainIdentifier(candidate);
                if (ident) {
                    return { label: ident, identifier: ident };
                }
            }
            return { label: '', identifier: '' };
        }

        function updateSecondary(card, service, nucleusSlug) {
            if (!card) return;
            const strings = getStopPanelStrings(card);
            const wrapper = card.querySelector('[data-field="secondary-wrapper"]') || card.querySelector('.second-train-approaching');
            const link = card.querySelector('[data-field="secondary-link"]');
            const minutesNode = card.querySelector('[data-field="secondary-minutes"]');
            const minutesValueNode = card.querySelector('[data-field="secondary-minutes-value"]') || minutesNode;
            const clockEl = card.querySelector('[data-field="secondary-clock"]');
            const clockValueNode = card.querySelector('[data-field="secondary-clock-value"]') || clockEl;

            if (!wrapper || !link || !minutesNode) return;

            const placeholderMin = strings.secondaryPlaceholder || '—';
            const placeholderClock = strings.secondaryClockPlaceholder || '--:--';
            const labelText = strings.secondaryLabel || 'Siguiente tren';

            link.textContent = labelText;

            const disableLink = () => {
                link.removeAttribute('href');
                link.setAttribute('aria-hidden', 'true');
                link.setAttribute('tabindex', '-1');
                link.removeAttribute('aria-label');
            };

            const applyPlaceholders = () => {
                wrapper.style.opacity = '0';
                wrapper.setAttribute('aria-hidden', 'true');
                wrapper.style.pointerEvents = 'none';
                minutesNode.removeAttribute('data-eta-min');
                minutesNode.removeAttribute('value');
                if (minutesValueNode) minutesValueNode.textContent = placeholderMin;
                if (clockEl) {
                    clockEl.hidden = true;
                    clockEl.setAttribute('aria-hidden', 'true');
                    clockEl.removeAttribute('data-eta-hhmm');
                    clockEl.dataset.clockHhmm = '';
                }
                if (clockValueNode) clockValueNode.textContent = placeholderClock;
            };

            if (!service) {
                applyPlaceholders();
                disableLink();
                return;
            }

            wrapper.style.opacity = '1';
            wrapper.removeAttribute('aria-hidden');
            wrapper.style.pointerEvents = '';

            const minutes = minutesFromService(service);
            const hhmm = hhmmFromService(service);
            const hasMinutes = Number.isFinite(minutes);

            if (hasMinutes) {
                minutesNode.setAttribute('data-eta-min', String(minutes));
                minutesNode.setAttribute('value', String(minutes));
            } else {
                minutesNode.removeAttribute('data-eta-min');
                minutesNode.removeAttribute('value');
            }
            if (minutesValueNode) {
                minutesValueNode.textContent = hasMinutes ? String(minutes) : placeholderMin;
            }

            if (clockEl) {
                const showClock = Number.isFinite(minutes) && minutes >= 60 && !!hhmm;
                if (showClock) {
                    clockEl.hidden = false;
                    clockEl.setAttribute('aria-hidden', 'false');
                    clockEl.setAttribute('data-eta-hhmm', hhmm);
                    clockEl.dataset.clockHhmm = hhmm;
                    if (clockValueNode) clockValueNode.textContent = hhmm;
                } else {
                    clockEl.hidden = true;
                    clockEl.setAttribute('aria-hidden', 'true');
                    clockEl.removeAttribute('data-eta-hhmm');
                    clockEl.dataset.clockHhmm = '';
                    if (clockValueNode) clockValueNode.textContent = placeholderClock;
                }
            }

            const slug = (nucleusSlug || '').trim().replace(/^\/+/, '');
            const trainInfo = resolveTrainInfo(service);
            const trainId = trainInfo.identifier;
            if (trainId && slug) {
                const href = `/trains/${slug}/${trainId}`;
                link.href = href;
                link.setAttribute('aria-hidden', 'false');
                link.setAttribute('tabindex', '0');
                const ariaLabel = formatStopPanelString(
                    strings.trainAriaTemplate,
                    { trainId },
                    `Ver detalle del tren ${trainId}`,
                );
                link.setAttribute('aria-label', ariaLabel);
            } else {
                disableLink();
            }
        }

        function updateFooter(footer, service, nucleusSlug) {
            if (!footer) return;
            const card = footer.closest('.nearest-train-card');
            const strings = getStopPanelStrings(card);
            const slug = (nucleusSlug || '').trim().replace(/^\/+/, '');
            const link = footer.querySelector('[data-field="train-link"]');
            const placeholder = footer.querySelector('[data-field="train-id-placeholder"]');
            const viewBlock = footer.querySelector('[data-field="train-view-link"]');
            const viewAnchor = footer.querySelector('[data-field="train-view-anchor"]');
            const statusTag = footer.querySelector('[data-field="train-id-tag"]');
            const trainInfo = resolveTrainInfo(service);
            const isScheduled = (service.status || '').toLowerCase() === 'scheduled';
            const fallbackLabel = service.status === 'realtime'
                ? strings.trainPlaceholderRealtime
                : strings.trainPlaceholderScheduled;
            const labelText = trainInfo.label ? `Tren ${trainInfo.label}` : fallbackLabel;
            const linkTarget = (trainInfo.identifier && slug) ? `/trains/${slug}/${trainInfo.identifier}` : '';
            const ariaTrainId = trainInfo.label || labelText.replace(/^Tren\s+/i, '').trim() || labelText;

            const setAriaLabel = (node) => {
                if (!node) return;
                const ariaLabel = formatStopPanelString(
                    strings.trainAriaTemplate,
                    { trainId: ariaTrainId },
                    `Ver detalle del tren ${ariaTrainId}`,
                );
                node.setAttribute('aria-label', ariaLabel);
            };

            if (link && linkTarget) {
                link.hidden = false;
                link.href = linkTarget;
                link.setAttribute('aria-hidden', 'false');
                link.setAttribute('tabindex', '0');
                setAriaLabel(link);
                const span = link.querySelector('[data-field="train-id"]');
                if (span) span.textContent = labelText;
            } else if (link) {
                link.hidden = true;
                link.removeAttribute('href');
                link.setAttribute('aria-hidden', 'true');
                link.setAttribute('tabindex', '-1');
                const span = link.querySelector('[data-field="train-id"]');
                if (span) span.textContent = fallbackLabel;
            }

            if (placeholder) {
                if (linkTarget) {
                    placeholder.hidden = true;
                } else {
                    placeholder.hidden = false;
                    placeholder.textContent = labelText;
                }
            }

            if (viewBlock && viewAnchor) {
                if (linkTarget) {
                    viewBlock.hidden = false;
                    viewAnchor.href = linkTarget;
                    setAriaLabel(viewAnchor);
                } else {
                    viewBlock.hidden = true;
                }
            }

            if (statusTag) {
                if (isScheduled && trainInfo.label) {
                    statusTag.hidden = false;
                    statusTag.removeAttribute('hidden');
                    statusTag.setAttribute('aria-hidden', 'false');
                } else {
                    statusTag.hidden = true;
                    statusTag.setAttribute('hidden', '');
                    statusTag.setAttribute('aria-hidden', 'true');
                }
            }

            const trainIdWrapper = card?.querySelector('[data-field="train-id-wrapper"]');
            if (trainIdWrapper) {
                const hasTrainLabel = !!(trainInfo.label && trainInfo.label.trim());
                trainIdWrapper.style.opacity = hasTrainLabel ? '1' : '0';
            }
        }

        function updateLastSeen(node, service) {
            if (!node) return;
            const age = service?.train_seen?.age_s;
            const panel = node.closest('[data-stop-panel]');
            const pill = panel ? panel.querySelector('[data-field="primary-pill"]') : null;
            const dot = pill ? pill.querySelector('.dot') : null;
            const thresholdAttr = pill?.dataset?.staleThreshold;
            const parsedThreshold = Number(thresholdAttr);
            const threshold = Number.isFinite(parsedThreshold) ? parsedThreshold : 40;
            const span = node.querySelector('[data-field="last-seen-seconds"]');
            const MIN_VISIBLE_SECONDS = 60;

            if (typeof age === 'number') {
                const rounded = Math.max(0, Math.round(age));
                if (span) span.textContent = String(rounded);
                if (dot) dot.setAttribute('title', `Visto hace ${rounded} s`);
                if (rounded > MIN_VISIBLE_SECONDS) {
                    node.hidden = false;
                    node.removeAttribute('hidden');
                } else {
                    node.hidden = true;
                    node.setAttribute('hidden', '');
                }
                if (pill) {
                    if (age > threshold) pill.classList.add('is-semi-stale');
                    else pill.classList.remove('is-semi-stale');
                }
            } else {
                node.hidden = true;
                node.setAttribute('hidden', '');
                if (pill) pill.classList.remove('is-semi-stale');
                if (dot) dot.removeAttribute('title');
            }
        }

        function nextStopRefreshInterval(primary, baseInterval) {
            let interval = baseInterval;
            if (!primary) return interval;

            const etaSeconds = Number(primary?.eta_seconds);
            if (Number.isFinite(etaSeconds)) {
                if (etaSeconds <= 5) {
                    interval = Math.min(interval, STOP_REFRESH_INTERVALS.dwell);
                } else if (etaSeconds <= 15) {
                    interval = Math.min(interval, STOP_REFRESH_INTERVALS.imminent);
                } else if (etaSeconds <= 30) {
                    interval = Math.min(interval, STOP_REFRESH_INTERVALS.halfMinute);
                } else if (etaSeconds <= 60) {
                    interval = Math.min(interval, STOP_REFRESH_INTERVALS.subMinute);
                }
            }

            const state = (primary?.train?.current_status || primary?.row?.status || '').toUpperCase();
            if (state === 'STOPPED_AT') {
                interval = Math.min(interval, STOP_REFRESH_INTERVALS.dwell);
            }

            return Math.max(interval, STOP_REFRESH_INTERVALS.min);
        }

        async function refreshStopApproachingNow(forceImmediate = false) {
            const st = panel.__stopAuto;
            if (!st || !st.running) return;

            const isOpen = panel.classList.contains('open') && panel.dataset.mode === 'stop';
            if (!isOpen || document.hidden) {
                scheduleNextStopTick(st.baseInterval);
                return;
            }

            const apiUrl = panel.dataset.stopApi;
            if (!apiUrl) {
                scheduleNextStopTick(st.baseInterval);
                return;
            }

            if (st.abort) { try { st.abort.abort(); } catch (_) {} }
            st.abort = new AbortController();

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
                applyStopServicesPayload(payload);
                st.errors = 0;
                let interval = st.baseInterval;
                try {
                    const primary = Array.isArray(payload?.services) ? payload.services[0] : null;
                    interval = nextStopRefreshInterval(primary, st.baseInterval);
                } catch (_) {
                    interval = st.baseInterval;
                }
                const jitter = Math.floor(Math.random() * 500);
                scheduleNextStopTick(interval + jitter);
            } catch (err) {
                console.debug('[stop-refresh] Error', err);
                st.errors = Math.min(st.errors + 1, 6);
                const penalty = Math.min(st.baseInterval * (2 ** (st.errors - 1)), st.maxInterval);
                scheduleNextStopTick(penalty);
            } finally {
                if (st) st.abort = null;
            }
        }

        function openWithUrl(url) {
            lastFocusEl = (document.activeElement && document.contains(document.activeElement))
                ? document.activeElement : document.body;

            setDrawerMode('stop');
            try { ActiveStop.setByHref(url); } catch(_) {}

            openStaticDrawer();

            const ctx = RouteCtx.getMain();
            const effectiveUrl = RouteCtx.appendToURL(url, ctx);

            fetch(effectiveUrl, { headers: { 'HX-Request': 'true' } })
                .then(r => r.ok ? r.text() : Promise.reject(r))
                .then(html => {
                    const template = document.createElement('template');
                    template.innerHTML = html;
                    if (template.content.firstChild) {
                        const incomingCtx = DrawerStopCtx.getFromHTML(html);
                        body.innerHTML = html;
                        if (window.htmx) htmx.process(body);
                        wireETA(body);
                        body.querySelectorAll('a[href]').forEach(a => {
                            a.addEventListener('click', (e) => {
                                closePanel();
                            });
                        });

                        if (incomingCtx) {
                            let apiUrl = incomingCtx.servicesUrl || '';
                            if (!apiUrl && incomingCtx.routeId && incomingCtx.stopId) {
                                const lim = incomingCtx.servicesLimit || '10';
                                const tz = incomingCtx.servicesTz || 'Europe/Madrid';
                                apiUrl = `/api/stops/${incomingCtx.routeId}/${incomingCtx.stopId}/services?limit=${lim}&tz=${tz}`;
                            }
                            if (apiUrl) panel.dataset.stopApi = apiUrl; else panel.dataset.stopApi = panel.dataset.stopApi || '';
                            panel.dataset.stopRouteId = incomingCtx.routeId || '';
                            panel.dataset.stopStopId = incomingCtx.stopId || '';
                            panel.dataset.stopDir = incomingCtx.dir || '';
                        }

                        try { history.replaceState({}, '', effectiveUrl); } catch(_) {}
                        panel.dataset.stopUrl = effectiveUrl;
                        teardownStopAutoRefresh();
                        startStopAutoRefresh();
                    }
                })
                .catch(() => {
                    const { body } = getStaticDrawer();
                    if (body) body.innerHTML = '<p>Error al cargar los datos.</p>';
                });
        }

        function closePanel() {
            const fallback = lastFocusEl || document.body;
            if (panel.contains(document.activeElement)) { try { fallback.focus(); } catch(_) {} }

            teardownStopAutoRefresh();
            panel.dataset.stopApi = '';
            panel.dataset.stopRouteId = '';
            panel.dataset.stopStopId = '';
            panel.dataset.stopDir = '';
            try { document.dispatchEvent(new CustomEvent('close:stop-drawer')); } catch(_) {}

            closeStaticDrawer();

            try { ActiveStop.clear(); } catch(_) {}

            try {
                const base = location.pathname.split('/stops')[0] || location.pathname;
                history.replaceState({}, '', base);
            } catch(_) {}
        }

        panel.__openStopWithUrl = openWithUrl;
        panel.__closeStopDrawer = closePanel;

        if (close && !boundButtons.has(close)) {
            boundButtons.add(close);
            close.addEventListener('click', closePanel);
        }

        window.AppDrawers = window.AppDrawers || {};
        window.AppDrawers.openStop  = (url) => { const p = document.getElementById('drawer'); p && p.__openStopWithUrl && p.__openStopWithUrl(url); };
        window.AppDrawers.closeStop = () => { const p = document.getElementById('drawer'); p && p.__closeStopDrawer && p.__closeStopDrawer(); };
    }

// ------------------ Init & observers ------------------

    let trainsRefreshDelegatedBound = false;
    function bindGlobalTrainsRefreshDelegation() {
        if (trainsRefreshDelegatedBound) return;
        trainsRefreshDelegatedBound = true;

        document.addEventListener('click', (ev) => {
            const btn = ev.target.closest('#update-route-train-list');
            if (!btn) return;
            const drawer = document.getElementById('drawer');
            if (!drawer || !drawer.classList.contains('open') || drawer.dataset.mode !== 'trains') return;

            ev.preventDefault();
            ev.stopPropagation();
            if (typeof ev.stopImmediatePropagation === 'function') ev.stopImmediatePropagation();

            if (typeof drawer.__refreshTrainsPanel === 'function') {
                drawer.__refreshTrainsPanel();
            } else {
                bindRouteTrainsPanel(document);
                const drawer2 = document.getElementById('drawer');
                drawer2?.__refreshTrainsPanel?.();
            }
        }, { capture: true, passive: false });
    }

    let stopClicksDelegatedBound = false;
    function bindGlobalStopLinkDelegation() {
        if (stopClicksDelegatedBound) return;
        stopClicksDelegatedBound = true;
        document.addEventListener('click', (ev) => {
            const a = ev.target.closest('.grid-route-map a[href*="/stops/"]');
            if (!a) return;
            if (a.getAttribute('hx-boost') !== 'false') a.setAttribute('hx-boost', 'false');
            ev.preventDefault();
            ev.stopPropagation();
            if (typeof ev.stopImmediatePropagation === 'function') ev.stopImmediatePropagation();
            const href = a.href;

            try { ActiveStop.setByHref(href); } catch(_) {}

            const panel = document.getElementById('drawer');
            if (!panel) { location.href = href; return; }
            if (!panel.__openStopWithUrl) { bindStopDrawer(document); }
            if (panel.__openStopWithUrl) panel.__openStopWithUrl(href);
            else {
                fetch(href, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
                        panel.hidden = false;
                        panel.removeAttribute('inert');
                        panel.setAttribute('aria-hidden', 'false');
                        setDrawerMode('stop');
                        panel.classList.add('open');
                        const body = document.getElementById('drawer-content');
                        if (body) { body.innerHTML = html; if (window.htmx) htmx.process(body); }
                        try { history.replaceState({}, '', href); } catch(_) {}
                    });
            }
        }, { capture: true, passive: false });
    }

    function bindGlobalDrawerCloseDelegation() {
        if (drawerCloseDelegatedBound) return;
        drawerCloseDelegatedBound = true;

        document.addEventListener('click', (ev) => {
            const btn = ev.target.closest('#drawer .drawer-close, #drawer [data-close-sheet]');
            if (!btn) return;

            ev.preventDefault();
            ev.stopPropagation();
            if (typeof ev.stopImmediatePropagation === 'function') ev.stopImmediatePropagation();

            const p = document.getElementById('drawer');
            if (!p) return;

            const mode = p.dataset.mode;
            if (mode === 'trains' && p.__closeTrainsPanel) p.__closeTrainsPanel();
            else if (mode === 'stop' && p.__closeStopDrawer) p.__closeStopDrawer();
            else closeStaticDrawer();
        }, { capture: true, passive: false });
    }


    function init(root = document) {
        console.debug('[init] init(root=)', root);
        root.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
        bindSideSheet(root);
        bindReverseToggleDelegated();
        bindConnectionsToggleDelegated();

        bindRouteTrainsPanel(root);
        bindStopDrawer(root);
        disableBoostForStopLinks(root);
        bindGlobalStopLinkDelegation();
        bindGlobalTrainsRefreshDelegation();
        bindGlobalDrawerCloseDelegation();
        bindGlobalEtaToggleDelegation();
        bindTrainDetailAutoRefresh(root);

        wireETA(root);
    }

    document.addEventListener('DOMContentLoaded', () => init());

    const mo = new MutationObserver(muts => {
        for (const m of muts) {
            m.addedNodes.forEach(node => {
                if (!(node instanceof Element)) return;

                if (node.matches?.('form.search-box, form.search-station-box')) enhanceSearchBox(node);
                node.querySelectorAll?.('form.search-box, form.search-station-box').forEach(enhanceSearchBox);

                if (node.id === 'app-sheet' || node.querySelector?.('#app-sheet') ||
                    node.matches?.('.topbar-btn-menu') || node.querySelector?.('.topbar-btn-menu')) {
                    bindSideSheet(document);
                }

                if (node.id === 'route-trains-panel' || node.querySelector?.('#route-trains-panel') ||
                    node.id === 'bottom-nav' || node.id === 'bottom-actions-nav' ||
                    node.querySelector?.('#btn-toggle-trains') || node.matches?.('#btn-toggle-trains') ||
                    node.hasAttribute?.('data-toggle') || node.querySelector?.('[data-toggle="trains"]')) {
                    console.debug('[trains][MO] rebind triggered by node:', node);
                    bindRouteTrainsPanel(document);
                }

                if (node.matches?.('.grid-route-map') || node.querySelector?.('.grid-route-map') ||
                    node.querySelector?.('a[href*="/stops/"]')) {
                    bindStopDrawer(document);
                    disableBoostForStopLinks(node);
                }

                if (node.querySelector?.('.train-eta-chip') || node.matches?.('.train-eta-chip')) {
                    wireETA(node);
                }
            });
        }
    });
    mo.observe(document.documentElement, { childList: true, subtree: true });

    if (window.htmx) {
        document.body.addEventListener('htmx:afterSwap', (e) => init(e.target));
        document.body.addEventListener('htmx:beforeSwap', (e) => {
            const { panel, body } = getStaticDrawer();
            if (!panel || !body) return;
            const tgt = (e && e.detail && e.detail.target) ? e.detail.target : null;
            if (tgt && (tgt === body || (tgt.closest && tgt.closest('#drawer')))) return;

            const mode = panel.dataset.mode;
            if (panel.classList.contains('open')) {
                if (mode === 'trains' && panel.__closeTrainsPanel) panel.__closeTrainsPanel();
                else if (mode === 'stop' && panel.__closeStopDrawer) panel.__closeStopDrawer();
                else closeStaticDrawer();
                try { ActiveStop.clear(); } catch(_) {}
            }

            try {
                delete panel.dataset.trainsLoaded;
                delete panel.dataset.trainsCtx;
                delete panel.dataset.mode;
            } catch(_) {}
            panel.classList.remove('drawer-trains','drawer-stop');
        });

        document.body.addEventListener('htmx:beforeHistorySave', () => {
            const { panel } = getStaticDrawer();
            if (!panel) return;

            const mode = panel.dataset.mode;
            if (panel.classList.contains('open')) {
                if (mode === 'trains' && panel.__closeTrainsPanel) panel.__closeTrainsPanel();
                else if (mode === 'stop' && panel.__closeStopDrawer) panel.__closeStopDrawer();
                else closeStaticDrawer();
                try { ActiveStop.clear(); } catch(_) {}
            }
            try {
                delete panel.dataset.trainsLoaded;
                delete panel.dataset.trainsCtx;
                delete panel.dataset.mode;
            } catch(_) {}
            panel.classList.remove('drawer-trains','drawer-stop');
        });
    }
})();
