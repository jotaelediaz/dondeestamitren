(function initTrainMap(){
    if (window.__trainMapInitialized) return;
    window.__trainMapInitialized = true;
    const MAP_SELECTOR = '[data-train-map]';
    const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';

    function latLonToTile(lat, lon, zoom) {
        const latRad = lat * Math.PI / 180;
        const n = Math.pow(2, zoom);
        const xtile = Math.floor((lon + 180) / 360 * n);
        const ytile = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n);
        return { x: xtile, y: ytile, z: zoom };
    }

    function renderFallback(root, center, routeLabel) {
        if (root.dataset.mapMounted === 'fallback') {
            return;
        }
        root.dataset.mapMounted = 'fallback';
        const zoom = 13;
        const tile = latLonToTile(center[1], center[0], zoom);
        root.classList.add('train-map-static');
        root.innerHTML = '';
        const img = document.createElement('img');
        img.src = `https://tile.openstreetmap.org/${zoom}/${tile.x}/${tile.y}.png`;
        img.alt = 'Mapa estático de OpenStreetMap';
        root.appendChild(img);

        const marker = document.createElement('div');
        marker.className = 'train-map-marker';
        marker.setAttribute('role', 'img');
        marker.setAttribute('aria-label', 'Posición aproximada del tren ' + (routeLabel || ''));
        marker.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">train</span>';
        root.appendChild(marker);
    }

    function renderInteractive(root, center, heading, routeLabel) {
        if (!window.maplibregl) {
            return false;
        }
        try {
            const map = new maplibregl.Map({
                container: root,
                style: MAP_STYLE,
                center,
                zoom: 11,
                pitch: 45,
                attributionControl: true
            });
            map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-right');
            const markerElement = document.createElement('div');
            markerElement.className = 'train-map-marker';
            markerElement.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">train</span>';
            markerElement.setAttribute('role', 'img');
            markerElement.setAttribute('aria-label', 'Tren en ruta ' + (routeLabel || ''));
            new maplibregl.Marker({ element: markerElement, rotation: heading, rotationAlignment: 'map' })
                .setLngLat(center)
                .addTo(map);
            root.dataset.mapMounted = 'interactive';
            return true;
        } catch (err) {
            console.error('No se pudo inicializar MapLibre:', err);
            return false;
        }
    }

    function initElement(root) {
        if (!root || root.dataset.mapMounted === 'interactive') {
            return;
        }
        const lat = parseFloat(root.dataset.mapLat || 'NaN');
        const lon = parseFloat(root.dataset.mapLon || 'NaN');
        if (Number.isNaN(lat) || Number.isNaN(lon)) {
            root.textContent = 'No se pudo determinar la posición del tren.';
            root.dataset.mapMounted = 'invalid';
            return;
        }
        const heading = parseFloat(root.dataset.mapHeading || '0');
        const routeLabel = root.dataset.mapRoute || root.dataset.mapTrain || '';
        const center = [lon, lat];
        if (!renderInteractive(root, center, heading, routeLabel)) {
            renderFallback(root, center, routeLabel);
        }
    }

    function scan(root) {
        if (!root) {
            return;
        }
        const candidates = root.matches && root.matches(MAP_SELECTOR)
            ? [root]
            : Array.from(root.querySelectorAll ? root.querySelectorAll(MAP_SELECTOR) : []);
        candidates.forEach(initElement);
    }

    document.addEventListener('DOMContentLoaded', function () {
        scan(document);
    });

    document.addEventListener('MapLibreReady', function () {
        const fallbackNodes = document.querySelectorAll(`${MAP_SELECTOR}[data-map-mounted="fallback"]`);
        fallbackNodes.forEach(function (node) {
            node.dataset.mapMounted = '';
            initElement(node);
        });
    });

    if (window.htmx && window.htmx.version) {
        document.body.addEventListener('htmx:afterSwap', function (evt) {
            scan(evt.target);
        });
    }
})();
