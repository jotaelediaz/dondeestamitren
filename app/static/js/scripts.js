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

    // ------------------ Utilities ------------------
    function stripHxAttrs(html) {
        return String(html).replace(/\s(hx-(target|swap|indicator|trigger))(=(".*?"|'.*?'|[^\s>]+))?/gi, "");
    }

    function disableBoostForStopLinks(root = document) {
        root.querySelectorAll('.grid-route-map a[href*="/stops/"]').forEach(a => {
            if (a.getAttribute('hx-boost') !== 'false') a.setAttribute('hx-boost', 'false');
        });
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
        const btn   = root.querySelector('#btn-toggle-trains') || document.querySelector('#btn-toggle-trains');
        const panel = root.querySelector('#route-trains-panel') || document.getElementById('route-trains-panel');
        if (!panel) return;  // no hay DOM → no hacemos nada

        const body  = panel.querySelector('#route-trains-body');
        const close = panel.querySelector('.drawer-close');
        if (!body) return;

        panel.setAttribute('role', 'dialog');
        panel.setAttribute('aria-modal', 'true');
        if (!panel.hasAttribute('tabindex')) panel.setAttribute('tabindex', '-1');
        if (panel.getAttribute('aria-hidden') !== 'false') {
            panel.setAttribute('aria-hidden', 'true');
            panel.setAttribute('inert', '');
            panel.hidden = true;
        }

        if (!boundPanels.has(panel)) {
            boundPanels.add(panel);
            let lastFocusEl = null;

            function resolveUrl() {
                const fromBtn   = btn && (btn.getAttribute('data-url') || btn.dataset?.url);
                const fromPanel = panel.getAttribute('data-url') || panel.dataset?.url;
                if (fromBtn) return fromBtn;
                if (fromPanel) return fromPanel;
                try { return location.pathname.replace(/\/$/, '') + '/trains'; }
                catch(_) { return '/trains'; }
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

            function loadOnce() {
                if (panel.dataset.loaded) { enrichBodyBindings(); return; }
                const url = resolveUrl();
                fetch(url, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
                        const template = document.createElement('template');
                        template.innerHTML = html;
                        if (template.content.firstChild) {
                            body.innerHTML = html;
                            if (window.htmx) htmx.process(body);
                            enrichBodyBindings();
                            panel.dataset.loaded = '1';
                        }
                    })
                    .catch(() => { body.innerHTML = '<p>Error al cargar los trenes.</p>'; });
            }

            function openPanel() {
                lastFocusEl = (document.activeElement && document.contains(document.activeElement))
                    ? document.activeElement : (btn || document.body);

                const stopDrawer = document.getElementById('stop-drawer');
                if (stopDrawer && stopDrawer.__closeStopDrawer && stopDrawer.classList.contains('open')) {
                    stopDrawer.__closeStopDrawer();
                }

                panel.hidden = false;
                panel.removeAttribute('inert');
                panel.setAttribute('aria-hidden', 'false');
                if (btn) btn.setAttribute('aria-expanded', 'true');

                panel.getBoundingClientRect();
                panel.classList.add('open');

                const focusTarget = panel.querySelector('.drawer-close') || panel;
                requestAnimationFrame(() => { try { focusTarget.focus(); } catch(_) {} });

                loadOnce();
                try { document.dispatchEvent(new CustomEvent('open:trains-drawer')); } catch(_) {}
            }

            function closePanel() {
                const fallback = btn || lastFocusEl || document.body;
                if (panel.contains(document.activeElement)) { try { fallback.focus(); } catch(_) {} }

                panel.classList.remove('open');
                panel.setAttribute('aria-hidden', 'true');
                panel.setAttribute('inert', '');
                if (btn) btn.setAttribute('aria-expanded', 'false');
                setTimeout(() => { panel.hidden = true; }, 260);
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

                const url = urlWithCtx(resolveUrl());

                fetch(url, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
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

            panel.__closeTrainsPanel = closePanel;

            if (close && !boundButtons.has(close)) {
                boundButtons.add(close);
                close.addEventListener('click', closePanel);
            }

            if (btn && !boundButtons.has(btn)) {
                boundButtons.add(btn);
                btn.addEventListener('click', (e) => {
                    e.preventDefault();
                    if (panel.classList.contains('open')) closePanel();
                    else openPanel();
                });
                if (!btn.hasAttribute('aria-controls')) btn.setAttribute('aria-controls', 'route-trains-panel');
                if (!btn.hasAttribute('aria-expanded')) btn.setAttribute('aria-expanded', 'false');
            }

            if (!trainsGlobalHandlersBound) {
                trainsGlobalHandlersBound = true;
                document.addEventListener('keydown', (e) => {
                    if (e.key === 'Escape' && panel.classList.contains('open')) closePanel();
                });
                document.addEventListener('open:stop-drawer', () => {
                    if (panel.classList.contains('open')) closePanel();
                });
                document.addEventListener('htmx:beforeSwap', (e) => {
                    let tgt = (e && e.detail && e.detail.target) ? e.detail.target : null;
                    if (tgt && (tgt === body || (tgt.closest && tgt.closest('#route-trains-panel')))) return;
                    if (panel.classList.contains('open')) closePanel();
                });
            }
        }
    }

    // ------------------ Drawer route stops ------------------
    function bindStopDrawer(root = document) {
        const panel = root.querySelector('#stop-drawer') || document.getElementById('stop-drawer');
        if (!panel) return;

        const body  = panel.querySelector('#stop-drawer-body');
        const close = panel.querySelector('.drawer-close');
        if (!body) return;

        // A11y base
        panel.setAttribute('role', 'dialog');
        panel.setAttribute('aria-modal', 'true');
        if (!panel.hasAttribute('tabindex')) panel.setAttribute('tabindex', '-1');
        if (panel.getAttribute('aria-hidden') !== 'false') {
            panel.setAttribute('aria-hidden', 'true');
            panel.setAttribute('inert', '');
            panel.hidden = true;
        }

        disableBoostForStopLinks(root);

        if (!boundPanels.has(panel)) {
            boundPanels.add(panel);
            let lastFocusEl = null;

            function openWithUrl(url) {
                lastFocusEl = (document.activeElement && document.contains(document.activeElement))
                    ? document.activeElement : document.body;

                const trainsPanel = document.getElementById('route-trains-panel');
                if (trainsPanel && trainsPanel.__closeTrainsPanel && trainsPanel.classList.contains('open')) {
                    trainsPanel.__closeTrainsPanel();
                }

                panel.hidden = false;
                panel.removeAttribute('inert');
                panel.setAttribute('aria-hidden', 'false');

                panel.getBoundingClientRect();
                panel.classList.add('open');

                const focusTarget = panel.querySelector('.drawer-close') || panel;
                requestAnimationFrame(() => { try { focusTarget.focus(); } catch(_) {} });

                fetch(url, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
                        const template = document.createElement('template');
                        template.innerHTML = html;
                        if (template.content.firstChild) {
                            body.innerHTML = html;
                            if (window.htmx) htmx.process(body);
                            body.querySelectorAll('a[href]').forEach(a => {
                                a.addEventListener('click', (e) => {
                                    closePanel();
                                });
                            });
                            try { history.replaceState({}, '', url); } catch(_) {}
                        }
                    })
                    .catch(() => { body.innerHTML = '<p>Error al cargar la parada.</p>'; });

                try { document.dispatchEvent(new CustomEvent('open:stop-drawer')); } catch(_) {}
            }

            function closePanel() {
                const fallback = lastFocusEl || document.body;
                if (panel.contains(document.activeElement)) { try { fallback.focus(); } catch(_) {} }

                panel.classList.remove('open');
                panel.setAttribute('aria-hidden', 'true');
                panel.setAttribute('inert', '');
                setTimeout(() => { panel.hidden = true; }, 260);

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
        }

        window.AppDrawers = window.AppDrawers || {};
        window.AppDrawers.openStop  = (url) => { const p = document.getElementById('stop-drawer'); p && p.__openStopWithUrl && p.__openStopWithUrl(url); };
        window.AppDrawers.closeStop = () => { const p = document.getElementById('stop-drawer'); p && p.__closeStopDrawer && p.__closeStopDrawer(); };
    }

    // ------------------ Init & observers ------------------
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
            const panel = document.getElementById('stop-drawer');
            if (!panel) { location.href = href; return; }
            if (!boundPanels.has(panel)) { bindStopDrawer(document); }
            if (panel.__openStopWithUrl) panel.__openStopWithUrl(href);
            else {
                fetch(href, { headers: { 'HX-Request': 'true' } })
                    .then(r => r.ok ? r.text() : Promise.reject(r))
                    .then(html => {
                        panel.hidden = false;
                        panel.removeAttribute('inert');
                        panel.setAttribute('aria-hidden', 'false');
                        panel.classList.add('open');
                        const body = panel.querySelector('#stop-drawer-body');
                        if (body) { body.innerHTML = html; if (window.htmx) htmx.process(body); }
                        try { history.replaceState({}, '', href); } catch(_) {}
                    });
            }
        }, { capture: true, passive: false });
    }

    function init(root = document) {
        root.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
        bindSideSheet(root);
        bindReverseToggleDelegated();
        bindConnectionsToggleDelegated();

        bindRouteTrainsPanel(root);
        bindStopDrawer(root);
        disableBoostForStopLinks(root);
        bindGlobalStopLinkDelegation();
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
                    node.id === 'bottom-nav' || node.querySelector?.('#btn-toggle-trains') || node.matches?.('#btn-toggle-trains') ||
                    node.hasAttribute?.('data-toggle') || node.querySelector?.('[data-toggle="trains"]')) {
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
    }
})();
