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

    // ------------------ Utilities ------------------
    function stripHxAttrs(html) {
        return String(html).replace(/\s(hx-(target|swap|indicator|trigger))(=(".*?"|'.*?'|[^\s>]+))?/gi, "");
    }

    function disableBoostForStopLinks(root = document) {
        root.querySelectorAll('.grid-route-map a[href*="/stops/"]').forEach(a => {
            if (a.getAttribute('hx-boost') !== 'false') a.setAttribute('hx-boost', 'false');
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
                    stopId:  n.getAttribute('data-stop-id') || ''
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
        const IDX_ARROW = 0;
        const IDX_GT    = 1;
        const DIGIT_OFF = 2;

        function _parseNumberish(a, t) {
            const c = [];
            if (a != null && a !== '') c.push(String(a));
            if (t != null && t !== '') c.push(String(t));
            for (const v of c) {
                const s = v.replace(/[^\d,.\-]/g,'').replace(',','.');
                const n = Number(s);
                if (Number.isFinite(n)) return Math.round(n);
                const m = s.match(/-?\d+/);
                if (m) return Math.round(Number(m[0]));
            }
            return 0;
        }
        function _pad2(n){ return String(Math.max(0, Math.min(99, Number(n)||0))).padStart(2,'0'); }
        function _state(el){ return ((el.getAttribute('data-train-state')||el.getAttribute('data-state')||'')+'').toLowerCase().trim(); }
        function _isStopped(st){ return st.includes('parado') || st.includes('estación') || st==='stopped' || st==='station_stop'; }

        function _measureStepPx(el){
            const s = el.querySelector('.rolling-number__digits > span');
            let px = s ? s.getBoundingClientRect().height : 0;
            if (!px || !Number.isFinite(px) || px<=0){
                const fs = parseFloat(getComputedStyle(el).fontSize);
                px = Number.isFinite(fs)&&fs>0?fs:16;
            }
            return px;
        }
        function _measureColWidthPx(el){
            let w = 0;
            el.querySelectorAll('.rolling-number__digits > span').forEach(s=>{
                const r = s.getBoundingClientRect().width;
                if (r > w) w = r;
            });
            return w || 0;
        }
        function _applyTransformsPx(el, idx0, idx1){
            const step = el.__rnStepPx || (el.__rnStepPx=_measureStepPx(el));
            const cols = el.querySelectorAll('.rolling-number__column .rolling-number__digits');
            const a = [idx0, idx1];
            for (let i=0;i<2;i++){
                const dg=cols[i]; const t=a[i];
                if(!dg || !Number.isFinite(t)) continue;
                void dg.offsetWidth;
                dg.style.transform='translateY('+(t*-step)+'px)';
            }
        }
        function _frameIdxPair(val, st){
            if (val===0 && !_isStopped(st)) return [IDX_GT, DIGIT_OFF + 1];
            if (val===0 &&  _isStopped(st)) return [IDX_ARROW, IDX_ARROW];
            const s=_pad2(val);
            return [DIGIT_OFF + Number(s[0]), DIGIT_OFF + Number(s[1])];
        }
        function _unitText(el){ const u = el.querySelector && el.querySelector('.unit'); return u ? u.textContent : 'min'; }

        function _build(el, val){
            const st=_state(el);
            const [i0,i1]=_frameIdxPair(val, st);
            const unit=_unitText(el);

            el.classList.add('rolling-number');
            el.innerHTML='';

            for (let k=0;k<2;k++){
                const col=document.createElement('span');
                col.className='rolling-number__column';
                const digits=document.createElement('span');
                digits.className='rolling-number__digits';

                { const s=document.createElement('span'); const i=document.createElement('i'); i.className='material-symbols-rounded'; i.textContent='arrow_downward'; s.appendChild(i); digits.appendChild(s); }
                { const s=document.createElement('span'); s.textContent='<'; digits.appendChild(s); }
                for(let d=0; d<=9; d++){ const s=document.createElement('span'); s.textContent=String(d); digits.appendChild(s); }

                col.appendChild(digits);
                el.appendChild(col);
            }

            const u=document.createElement('span');
            u.className='unit';
            u.textContent=unit;
            el.appendChild(u);

            requestAnimationFrame(()=>{
                el.__rnStepPx=_measureStepPx(el);
                const cw=_measureColWidthPx(el);
                if (cw) el.style.setProperty('--rn-col-w', cw+'px');
                _applyTransformsPx(el, i0, i1);
            });

            el.dataset.value0 = String(i0);
            el.dataset.value1 = String(i1);
        }

        function build(el, value){ _build(el, _parseNumberish(value, el.textContent)); }

        function update(el, next){
            const val=_parseNumberish(next, el.textContent);
            const st=_state(el);
            if (!el.classList.contains('rolling-number')){ build(el, val); return; }
            const [i0,i1]=_frameIdxPair(val, st);
            if (el.dataset.value0===String(i0) && el.dataset.value1===String(i1)) return;
            el.dataset.value0=String(i0); el.dataset.value1=String(i1);
            _applyTransformsPx(el, i0, i1);
        }

        function ensure(el){
            const attr=el.getAttribute&&el.getAttribute('data-eta-min');
            const val=_parseNumberish(attr, el.textContent);
            if (!el.classList.contains('rolling-number')) build(el, val);
            else update(el, val);
        }
        return { build, update, ensure };
    })();

    function wireETA(root=document) {
        const el = root.querySelector ? root.querySelector('#eta-primary') : null;
        if (el) RollingNumber.ensure(el);
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
                            return; // no swap
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

        // --- Auto-refresh state for stop drawer ---
        if (!panel.__stopAuto) {
            panel.__stopAuto = {
                timerId: null,
                abort: null,
                baseInterval: 60_000, // 1 min
                maxInterval: 300_000, // 5 min
                errors: 0,
                running: false,
                lastUrl: ''
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
        }

        function scheduleNextStopTick(ms) {
            const st = panel.__stopAuto;
            if (!st) return;
            if (st.timerId) clearTimeout(st.timerId);
            st.timerId = setTimeout(refreshStopApproachingNow, ms);
        }

        function startStopAutoRefresh() {
            const st = panel.__stopAuto;
            if (!st || st.running) return;
            st.running = true;

            const now = Date.now();
            const toNextMinute = 60_000 - (now % 60_000);
            const jitter = Math.floor(Math.random() * 700);
            scheduleNextStopTick(toNextMinute + jitter);
        }

        if (!stopGlobalHandlersBound) {
            stopGlobalHandlersBound = true;
            document.addEventListener('visibilitychange', () => {
                const st = panel.__stopAuto;
                if (!st) return;
                if (document.hidden) {
                    teardownStopAutoRefresh();
                } else if (panel.classList.contains('open') && panel.dataset.mode === 'stop') {
                    startStopAutoRefresh();
                }
            }, { passive: true });
        }

        async function refreshStopApproachingNow() {
            const st = panel.__stopAuto;
            if (!st) return;

            const isOpen = panel.classList.contains('open') && panel.dataset.mode === 'stop';
            if (!isOpen || document.hidden) {
                scheduleNextStopTick(st.baseInterval);
                return;
            }

            const bodyEl = document.getElementById('drawer-content');

            const baseUrl = panel.dataset.stopUrl || location.href;
            const ctx     = RouteCtx.getMain();
            const url     = RouteCtx.appendToURL(baseUrl, ctx);
            st.lastUrl    = url;

            if (st.abort) { try { st.abort.abort(); } catch(_) {} }
            st.abort = new AbortController();

            let html = '';
            try {
                const r = await fetch(url, { headers: { 'HX-Request': 'true' }, signal: st.abort.signal });
                if (!r.ok) throw new Error('HTTP ' + r.status);
                html = await r.text();

                const incomingCtx = DrawerStopCtx.getFromHTML(html);
                const main = RouteCtx.getMain() || {};
                const current = {
                    nucleus: main.nucleus || '',
                    lineId:  main.lineId || '',
                    routeId: main.routeId || '',
                    dir:     main.dir || '',
                    stopId:  (function(){
                        try {
                            const m = String(url).match(/\/stops\/([^\/?#]+)/);
                            return m ? decodeURIComponent(m[1]) : '';
                        } catch(_) { return ''; }
                    })()
                };

                if (incomingCtx && !DrawerStopCtx.equal(current, incomingCtx)) {
                    console.warn('[stop-refresh] Descartado por cambio de contexto', { current, incomingCtx });
                    scheduleNextStopTick(st.baseInterval);
                    return;
                }

                const tpl = document.createElement('template');
                tpl.innerHTML = html;

                let processedNode = null;

                const newWrapper = tpl.content.querySelector('#approaching-trains');
                const curWrapper = bodyEl.querySelector('#approaching-trains');

                if (newWrapper && curWrapper) {
                    const incomingEtaEl = newWrapper.querySelector('#eta-primary');
                    const currentEtaEl  = curWrapper.querySelector('#eta-primary');
                    const incomingState = incomingEtaEl && (incomingEtaEl.getAttribute('data-train-state') || incomingEtaEl.getAttribute('data-state') || '');
                    if (currentEtaEl && incomingState != null) currentEtaEl.setAttribute('data-train-state', incomingState);
                    if (incomingEtaEl && currentEtaEl) {
                        const vAttr = incomingEtaEl.getAttribute('data-eta-min');
                        const nextVal = (function(a, t){
                            const s = (a != null && a !== '') ? String(a) : String(t || '');
                            const norm = s.replace(/[^\d,.\-]/g, '').replace(',', '.');
                            const n = Number(norm);
                            if (Number.isFinite(n)) return Math.round(n);
                            const m = norm.match(/-?\d+/);
                            return m ? Math.round(Number(m[0])) : 0;
                        })(vAttr, incomingEtaEl.textContent);

                        RollingNumber.ensure(currentEtaEl);
                        RollingNumber.update(currentEtaEl, nextVal);

                    } else if (incomingEtaEl && !currentEtaEl) {
                        const where = curWrapper.querySelector('.nearest-train-card .train-eta-chip') || curWrapper.querySelector('.nearest-train-card') || curWrapper;
                        where.appendChild(incomingEtaEl);
                        RollingNumber.ensure(incomingEtaEl);
                    } else if (!incomingEtaEl && currentEtaEl) {
                        currentEtaEl.remove();
                    }
                    const incomingList = newWrapper.querySelector('#approaching-list');
                    const currentList  = curWrapper.querySelector('#approaching-list');
                    if (incomingList && currentList) {
                        incomingList.style.opacity = '0';
                        currentList.replaceWith(incomingList);
                        requestAnimationFrame(() => { void incomingList.offsetWidth; incomingList.style.opacity = '1'; });
                    } else {
                        const newItems = Array.from(newWrapper.querySelectorAll('.train-approaching:not(.nearest-train-card)'));
                        const curItems = Array.from(curWrapper.querySelectorAll('.train-approaching:not(.nearest-train-card)'));
                        curItems.forEach(n => n.remove());
                        if (newItems.length) {
                            const frag = document.createDocumentFragment();
                            newItems.forEach(n => frag.appendChild(n));
                            const h2 = curWrapper.querySelector('h2');
                            if (h2 && h2.after) h2.after(frag); else curWrapper.appendChild(frag);
                        }
                    }
                    if (window.htmx) htmx.process(curWrapper);
                    processedNode = curWrapper;
                    st.errors = 0;
                    scheduleNextStopTick(st.baseInterval);
                    return;
                } else {
                    const newItems = Array.from(tpl.content.querySelectorAll('.train-approaching'));
                    const curItems = Array.from(bodyEl.querySelectorAll('.train-approaching'));
                    if (!curItems.length && !newItems.length) {
                        scheduleNextStopTick(st.baseInterval);
                        st.errors = 0;
                        return;
                    }
                    let parent = null;
                    if (curItems.length) parent = curItems[0].parentElement;
                    if (!parent) parent = bodyEl.querySelector('.stop-modal-body') || bodyEl;

                    curItems.forEach(n => n.remove());
                    if (newItems.length) {
                        const frag = document.createDocumentFragment();
                        newItems.forEach(n => frag.appendChild(n));
                        parent.appendChild(frag);
                    }
                    processedNode = parent;
                }

                if (window.htmx && processedNode) {
                    htmx.process(processedNode);
                }

                st.errors = 0;
                scheduleNextStopTick(st.baseInterval);
            } catch (err) {
                console.debug('[stop-refresh] Error', err);
                st.errors = Math.min(st.errors + 1, 6);
                const penalty = Math.min(st.baseInterval * (2 ** (st.errors - 1)), st.maxInterval);
                scheduleNextStopTick(penalty);
            } finally {
                if (st && st.abort) { st.abort = null; }
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
                        body.innerHTML = html;
                        if (window.htmx) htmx.process(body);
                        wireETA(body);
                        body.querySelectorAll('a[href]').forEach(a => {
                            a.addEventListener('click', (e) => {
                                closePanel();
                            });
                        });
                        try { history.replaceState({}, '', effectiveUrl); } catch(_) {}
                        panel.dataset.stopUrl = effectiveUrl;
                        teardownStopAutoRefresh();
                        startStopAutoRefresh();
                    }
                })
                .catch(() => { body.innerHTML = '<p>Error al cargar la parada.</p>'; });

            try { document.dispatchEvent(new CustomEvent('open:stop-drawer')); } catch(_) {}
        }

        function closePanel() {
            const fallback = lastFocusEl || document.body;
            if (panel.contains(document.activeElement)) { try { fallback.focus(); } catch(_) {} }

            teardownStopAutoRefresh();
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
