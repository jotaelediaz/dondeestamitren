// static/js/scripts.js
(function () {
    const upgradedForms = new WeakSet();
    const boundButtons  = new WeakSet();
    const boundDialogs  = new WeakSet();
    let   htmxHooked    = false;
    let   reverseHandlerBound = false;

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
                if (qInput) {
                    qInput.required = false;
                    qInput.value = '';
                }
                form.setAttribute('novalidate', 'novalidate');

                const prevDisabled = nearbyBtn.disabled;
                nearbyBtn.disabled = true;

                navigator.geolocation.getCurrentPosition(
                    (pos) => {
                        const { latitude, longitude } = pos.coords || {};
                        if (latitude == null || longitude == null) {
                            throw new Error('No se pudo obtener la ubicación.');
                        }

                        removeLatLon();
                        const lat = Number(latitude.toFixed(6));
                        const lon = Number(longitude.toFixed(6));
                        addHidden('lat', String(lat));
                        addHidden('lon', String(lon));

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

    function bindSideSheet(root = document) {
        const btn = root.querySelector('.topbar-btn-menu') || document.querySelector('.topbar-btn-menu');
        const dlg = (root.getElementById && root.getElementById('app-sheet')) || document.getElementById('app-sheet');
        if (!dlg) return;

        if (!boundDialogs.has(dlg)) {
            boundDialogs.add(dlg);

            let lastFocus = null;

            function openSheet() {
                lastFocus = document.activeElement;
                if (typeof dlg.showModal === 'function') dlg.showModal();
                else dlg.setAttribute('open', '');
                if (btn) btn.setAttribute('aria-expanded', 'true');
            }

            function closeSheet() {
                if (dlg.open && typeof dlg.close === 'function') dlg.close();
                else dlg.removeAttribute('open');
                if (btn) btn.setAttribute('aria-expanded', 'false');
                if (lastFocus && document.contains(lastFocus)) {
                    try { lastFocus.focus(); } catch (_) {}
                }
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
                document.addEventListener('htmx:beforeSwap', () => {
                    if (dlg.__closeSheet) dlg.__closeSheet();
                });
            }
        }

        if (btn && !boundButtons.has(btn)) {
            boundButtons.add(btn);
            btn.addEventListener('click', () => {
                btn.setAttribute('aria-expanded', 'true');
                if (dlg && typeof dlg.showModal === 'function') dlg.showModal();
                else if (dlg) dlg.setAttribute('open', '');
            });
        }
    }

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


    function init(root = document) {
        root.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
        bindSideSheet(root);
        bindReverseToggleDelegated();
        bindConnectionsToggleDelegated();
    }

    document.addEventListener('DOMContentLoaded', () => init());

    const mo = new MutationObserver(muts => {
        for (const m of muts) {
            m.addedNodes.forEach(node => {
                if (!(node instanceof Element)) return;
                if (node.matches && node.matches('form.search-box, form.search-station-box')) {
                    enhanceSearchBox(node);
                }
                node.querySelectorAll && node.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
                if (node.id === 'app-sheet' || node.querySelector?.('#app-sheet') || node.matches?.('.topbar-btn-menu') || node.querySelector?.('.topbar-btn-menu')) {
                    bindSideSheet(document);
                }
            });
        }
    });
    mo.observe(document.documentElement, { childList: true, subtree: true });

    if (window.htmx) {
        document.body.addEventListener('htmx:afterSwap', (e) => init(e.target));
    }
})();
