// static/js/scripts.js
(function () {
    const upgraded = new WeakSet();

    function enhanceSearchBox(form) {
        if (!form || upgraded.has(form)) return;
        upgraded.add(form);

        // Hidden lat/lon
        let latEl = form.querySelector('input[name="lat"]');
        let lonEl = form.querySelector('input[name="lon"]');
        if (!latEl) { latEl = document.createElement('input'); latEl.type = 'hidden'; latEl.name = 'lat'; form.appendChild(latEl); }
        if (!lonEl) { lonEl = document.createElement('input'); lonEl.type = 'hidden'; lonEl.name = 'lon'; form.appendChild(lonEl); }

        const nearbyBtn = form.querySelector('.nearby-btn');
        if (nearbyBtn) {
            nearbyBtn.addEventListener('click', function () {
                const qInput = form.querySelector('input[name="q"]');

                // Para evitar bloqueo por required en búsqueda por ubicación
                let prevRequired = null;
                if (qInput) {
                    prevRequired = qInput.required;
                    qInput.required = false;           // desactiva required
                    qInput.value = '';                 // limpia texto
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
                            nearbyBtn.disabled = false; nearbyBtn.textContent = prev;
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
                        nearbyBtn.disabled = false; nearbyBtn.textContent = prev;
                        if (qInput && prevRequired !== null) qInput.required = prevRequired;
                        form.removeAttribute('novalidate');
                    },
                    { enableHighAccuracy: true, timeout: 8000, maximumAge: 0 }
                );
            });
        }

        // If there's a query, clear lat/lon'
        form.addEventListener('submit', function () {
            const qInput = form.querySelector('input[name="q"]');
            if (qInput && qInput.value.trim() !== '') {
                latEl.value = '';
                lonEl.value = '';
            }
        });
    }

    function init(root = document) {
        root.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
    }

    document.addEventListener('DOMContentLoaded', () => init());

    const mo = new MutationObserver(muts => {
        for (const m of muts) {
            m.addedNodes.forEach(node => {
                if (!(node instanceof Element)) return;
                if (node.matches && node.matches('form.search-box, form.search-station-box')) enhanceSearchBox(node);
                node.querySelectorAll && node.querySelectorAll('form.search-box, form.search-station-box').forEach(enhanceSearchBox);
            });
        }
    });
    mo.observe(document.documentElement, { childList: true, subtree: true });

    if (window.htmx) {
        document.body.addEventListener('htmx:afterSwap', (e) => init(e.target));
    }
})();
