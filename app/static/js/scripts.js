// static/js/scripts.js
(function () {
    const upgradedForms = new WeakSet();
    const boundButtons  = new WeakSet();
    const boundDialogs  = new WeakSet();
    let   htmxHooked    = false;


    function enhanceSearchBox(form) {
        if (!form || upgradedForms.has(form)) return;
        upgradedForms.add(form);

        // Hidden lat/lon
        let latEl = form.querySelector('input[name="lat"]');
        let lonEl = form.querySelector('input[name="lon"]');
        if (!latEl) { latEl = document.createElement('input'); latEl.type = 'hidden'; latEl.name = 'lat'; form.appendChild(latEl); }
        if (!lonEl) { lonEl = document.createElement('input'); lonEl.type = 'hidden'; lonEl.name = 'lon'; form.appendChild(lonEl); }

        const nearbyBtn = form.querySelector('.nearby-btn');
        if (nearbyBtn && !boundButtons.has(nearbyBtn)) {
            boundButtons.add(nearbyBtn);
            nearbyBtn.addEventListener('click', function () {
                const qInput = form.querySelector('input[name="q"]');

                let prevRequired = null;
                if (qInput) {
                    prevRequired = qInput.required;
                    qInput.required = false;
                    qInput.value = '';
                }
                form.setAttribute('novalidate', 'novalidate');

                if (!('geolocation' in navigator)) {
                    alert('Tu navegador no soporta geolocalización.');
                    // restaura required si hicimos cambios
                    if (qInput && prevRequired !== null) qInput.required = prevRequired;
                    form.removeAttribute('novalidate');
                    return;
                }

                const prev = nearbyBtn.textContent;
                nearbyBtn.disabled = true;

                navigator.geolocation.getCurrentPosition(
                    function (pos) {
                        const { latitude, longitude } = pos.coords || {};
                        if (latitude == null || longitude == null) {
                            alert('No se pudo obtener la ubicación.');
                            nearbyBtn.disabled = false;
                            nearbyBtn.textContent = prev;
                            if (qInput && prevRequired !== null) qInput.required = prevRequired;
                            form.removeAttribute('novalidate');
                            return;
                        }
                        latEl.value = latitude;
                        lonEl.value = longitude;
                        form.submit();
                    },
                    function (err) {
                        console.error(err);
                        alert('No se pudo obtener la ubicación (permiso denegado o error).');
                        nearbyBtn.disabled = false;
                        nearbyBtn.textContent = prev;
                        if (qInput && prevRequired !== null) qInput.required = prevRequired;
                        form.removeAttribute('novalidate');
                    },
                    { enableHighAccuracy: true, timeout: 8000, maximumAge: 0 }
                );
            });
        }

        form.addEventListener('submit', function () {
            const qInput = form.querySelector('input[name="q"]');
            if (qInput && qInput.value.trim() !== '') {
                latEl.value = '';
                lonEl.value = '';
            }
        });
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

    function init(root = document) {
        root.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
        bindSideSheet(root);
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
