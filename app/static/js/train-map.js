(function initTrainMap() {
    if (window.__trainMapInitialized) return;
    window.__trainMapInitialized = true;

    const MAP_SELECTOR = '[data-train-map]';
    const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';
    const ANIM_TICK_MS = 500;
    const POLL_MS = 30_000;
    const SMOOTH_WINDOW_MS = 30_000;
    const DEFAULT_LINE_COLOR = '#0064b4';

    function clamp01(x) {
        return Math.max(0, Math.min(1, x));
    }

    function toNumber(val) {
        const n = Number(val);
        return Number.isFinite(n) ? n : null;
    }

    function haversineM(lat1, lon1, lat2, lon2) {
        const toRad = (deg) => deg * Math.PI / 180;
        const R = 6_371_000;
        const dLat = toRad(lat2 - lat1);
        const dLon = toRad(lon2 - lon1);
        const a = Math.sin(dLat / 2) ** 2
            + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
        return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    function projectFraction(a, b, p) {
        const dx = b[0] - a[0];
        const dy = b[1] - a[1];
        const denom = dx * dx + dy * dy;
        if (!Number.isFinite(denom) || denom <= 0) return 0;
        const t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / denom;
        return clamp01(t);
    }

    function etaFraction(nowMs, depEpoch, arrEpoch) {
        if (!Number.isFinite(depEpoch) || !Number.isFinite(arrEpoch) || arrEpoch <= depEpoch) return null;
        return clamp01((nowMs / 1000 - depEpoch) / (arrEpoch - depEpoch));
    }

    function parseJSON(input) {
        if (!input) return null;
        if (typeof input === 'object') return input;
        try {
            return JSON.parse(input);
        } catch (err) {
            console.warn('No se pudo parsear JSON de mapa', err);
            return null;
        }
    }

    function readGeoJSON(root, attrName, scriptId) {
        const attrVal = root.getAttribute(attrName);
        if (attrVal) return parseJSON(attrVal);
        if (scriptId) {
            const script = document.getElementById(scriptId);
            if (script && script.textContent) return parseJSON(script.textContent);
        }
        return null;
    }

    function extractRouteCoords(routeData) {
        if (!routeData) return null;
        const geom = routeData.geometry || routeData;
        if (!geom) return null;
        if (geom.type === 'LineString') return geom.coordinates;
        if (geom.type === 'MultiLineString' && Array.isArray(geom.coordinates)) {
            return geom.coordinates[0];
        }
        if (geom.type === 'FeatureCollection' && Array.isArray(geom.features)) {
            const feat = geom.features.find((f) => f?.geometry?.type === 'LineString') || geom.features[0];
            return feat?.geometry?.coordinates || null;
        }
        if (geom.type === 'Feature' && geom.geometry) {
            return extractRouteCoords(geom.geometry);
        }
        return null;
    }

    function deriveCenterFromRoute(routeData, stopsData) {
        const coords = extractRouteCoords(routeData) || [];
        let pool = [];
        if (Array.isArray(coords) && coords.length > 0) {
            pool = coords;
        } else if (stopsData && Array.isArray(stopsData.features)) {
            pool = stopsData.features
                .map((f) => f?.geometry?.coordinates)
                .filter((c) => Array.isArray(c) && c.length >= 2);
        }
        if (!pool || pool.length === 0) return null;
        const sum = pool.reduce(
            (acc, c) => {
                acc.lon += Number(c[0]) || 0;
                acc.lat += Number(c[1]) || 0;
                return acc;
            },
            { lon: 0, lat: 0 }
        );
        const n = pool.length || 1;
        return [sum.lon / n, sum.lat / n];
    }

    async function fetchPositionSnapshot(api) {
        if (!api) return null;
        const resp = await fetch(api);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        const position = data.position || data;
        const lat = toNumber(position.lat);
        const lon = toNumber(position.lon);
        const heading = toNumber(position.heading);
        const hasPosition = Number.isFinite(lat) && Number.isFinite(lon);
        return { data, lat: hasPosition ? lat : null, lon: hasPosition ? lon : null, heading, hasPosition };
    }

    function buildRouteCtx(routeData, stopsData) {
        const coords = extractRouteCoords(routeData);
        if (!coords || coords.length < 2) return null;

        const cumLengths = [0];
        for (let i = 1; i < coords.length; i += 1) {
            const [lonA, latA] = coords[i - 1] || [];
            const [lonB, latB] = coords[i] || [];
            const seg = haversineM(latA, lonA, latB, lonB);
            cumLengths[i] = cumLengths[i - 1] + (Number.isFinite(seg) ? seg : 0);
        }

        const ctx = {
            coords,
            cumLengths,
            totalLength: cumLengths[cumLengths.length - 1] || 0,
            stopIndex: {},
        };

        if (stopsData && Array.isArray(stopsData.features)) {
            stopsData.features.forEach((feat) => {
                const sid = feat?.properties?.stop_id || feat?.properties?.station_id;
                const geom = feat?.geometry;
                if (!sid || !geom || geom.type !== 'Point') return;
                const [lon, lat] = geom.coordinates || [];
                if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
                const proj = projectOnRoute(ctx, { lon, lat });
                if (!proj) return;
                ctx.stopIndex[String(sid)] = {
                    idx: proj.idx,
                    dist: proj.distance,
                    coord: [lon, lat],
                };
            });
        }

        return ctx;
    }

    function projectOnRoute(ctx, point) {
        if (!ctx || !ctx.coords || ctx.coords.length < 2) return null;
        let best = null;
        for (let i = 0; i < ctx.coords.length - 1; i += 1) {
            const a = ctx.coords[i];
            const b = ctx.coords[i + 1];
            if (!a || !b) continue;
            const t = projectFraction(a, b, [point.lon, point.lat]);
            const projLon = a[0] + (b[0] - a[0]) * t;
            const projLat = a[1] + (b[1] - a[1]) * t;
            const distToSeg = haversineM(point.lat, point.lon, projLat, projLon);
            const segLen = (ctx.cumLengths[i + 1] || 0) - (ctx.cumLengths[i] || 0);
            const along = (ctx.cumLengths[i] || 0) + segLen * t;
            if (!best || distToSeg < best.err) {
                best = {
                    idx: i,
                    t,
                    distance: along,
                    coord: [projLon, projLat],
                    err: distToSeg,
                };
            }
        }
        return best;
    }

    function coordAtDistance(ctx, distance) {
        if (!ctx || !ctx.coords || ctx.coords.length < 2) return null;
        const target = Math.max(0, Math.min(distance, ctx.totalLength || 0));
        const len = ctx.cumLengths || [];
        let i = 0;
        while (i < len.length - 1 && len[i + 1] < target) {
            i += 1;
        }
        if (i >= ctx.coords.length - 1) i = ctx.coords.length - 2;
        const a = ctx.coords[i];
        const b = ctx.coords[i + 1];
        const segLen = (len[i + 1] || 0) - (len[i] || 0);
        const segT = segLen > 0 ? (target - (len[i] || 0)) / segLen : 0;
        return {
            lon: a[0] + (b[0] - a[0]) * segT,
            lat: a[1] + (b[1] - a[1]) * segT,
        };
    }

    function renderFallback(root, center, routeLabel) {
        if (root.dataset.mapMounted === 'fallback') return;
        root.dataset.mapMounted = 'fallback';
        const zoom = 13;
        const lat = center[1];
        const lon = center[0];
        const latRad = lat * Math.PI / 180;
        const n = 2 ** zoom;
        const xTile = Math.floor((lon + 180) / 360 * n);
        const yTile = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n);
        root.classList.add('train-map-static');
        root.innerHTML = '';
        const img = document.createElement('img');
        img.src = `https://tile.openstreetmap.org/${zoom}/${xTile}/${yTile}.png`;
        img.alt = 'Mapa estático de OpenStreetMap';
        root.appendChild(img);

        const marker = document.createElement('div');
        marker.className = 'train-map-marker';
        marker.setAttribute('role', 'img');
        marker.setAttribute('aria-label', 'Posición aproximada del tren ' + (routeLabel || ''));
        marker.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">train</span>';
        root.appendChild(marker);
    }

    function teardownElement(root) {
        if (!root || !root.__trainMap) return;
        const state = root.__trainMap;
        if (state.animTimer) clearInterval(state.animTimer);
        if (state.pollTimer) clearTimeout(state.pollTimer);
        if (state.abortCtrl) {
            try { state.abortCtrl.abort(); } catch (_) {}
        }
        if (state.map && typeof state.map.remove === 'function') {
            try { state.map.remove(); } catch (_) {}
        }
        root.__trainMap = null;
        root.dataset.mapMounted = '';
    }

    function cleanupInSubtree(root) {
        if (!root) return;
        const nodes = root.matches && root.matches(MAP_SELECTOR)
            ? [root]
            : Array.from(root.querySelectorAll ? root.querySelectorAll(MAP_SELECTOR) : []);
        nodes.forEach(teardownElement);
    }

    function updateMarkerPosition(root) {
        const state = root.__trainMap;
        if (!state || !state.marker) return;
        const now = Date.now();
        let target = null;

        const hasRoute = state.routeCtx && Number.isFinite(state.routeStartDist) && Number.isFinite(state.routeEndDist);
        if (hasRoute) {
            const etaFrac = etaFraction(now, state.segmentDepTs, state.segmentArrTs);
            const anchor = Number.isFinite(state.routeProgressAtRefresh)
                ? clamp01(state.routeProgressAtRefresh)
                : null;
            const anchorTs = state.routeProgressTs || state.lastTs || now;
            const etaArrivalMs = Number.isFinite(state.segmentArrTs) ? state.segmentArrTs * 1000 : null;
            const predictedArrMs = state.predictedArrMs || etaArrivalMs;

            let tRoute = null;
            if (
                etaFrac !== null &&
                anchor !== null &&
                Number.isFinite(state.routeEtaAtRefresh)
            ) {
                const denom = Math.max(1e-3, 1 - state.routeEtaAtRefresh);
                const scaled = anchor + (etaFrac - state.routeEtaAtRefresh) * ((1 - anchor) / denom);
                tRoute = clamp01(scaled);
            } else if (etaFrac !== null) {
                tRoute = etaFrac;
            }
            if (tRoute === null && anchor !== null) {
                const spanMs = Math.max(
                    500,
                    (predictedArrMs && predictedArrMs > anchorTs)
                        ? (predictedArrMs - anchorTs)
                        : SMOOTH_WINDOW_MS
                );
                const projected = anchor + ((now - anchorTs) / spanMs) * (1 - anchor);
                tRoute = clamp01(projected);
            } else if (tRoute !== null && anchor !== null) {
                tRoute = Math.max(tRoute, anchor);
            } else if (
                tRoute === null &&
                Number.isFinite(state.lastTs) &&
                Number.isFinite(state.nextTs) &&
                state.nextTs > state.lastTs
            ) {
                tRoute = clamp01((now - state.lastTs) / Math.max(1, state.nextTs - state.lastTs));
            }
            if (tRoute !== null) {
                const dist = state.routeStartDist + (state.routeEndDist - state.routeStartDist) * tRoute;
                target = coordAtDistance(state.routeCtx, dist);
            }
        }

        if (!target && state.lastPos && state.nextPos) {
            const start = state.lastTs || now;
            const end = state.nextTs || start;
            const tLin = start === end ? 1 : clamp01((now - start) / Math.max(1, end - start));
            target = {
                lon: state.lastPos.lon + (state.nextPos.lon - state.lastPos.lon) * tLin,
                lat: state.lastPos.lat + (state.nextPos.lat - state.lastPos.lat) * tLin,
            };
        }

        if (!target) return;
        state.marker.setLngLat([target.lon, target.lat]);
        if (state.map && !state.userPanned) {
            state.map.setCenter([target.lon, target.lat]);
        }
    }

    function ensureAnimation(root) {
        const state = root.__trainMap;
        if (!state || state.animTimer) return;
        state.animTimer = setInterval(() => updateMarkerPosition(root), ANIM_TICK_MS);
    }

    function schedulePoll(root, delayMs) {
        const state = root.__trainMap;
        if (!state) return;
        if (state.pollTimer) clearTimeout(state.pollTimer);
        state.pollTimer = setTimeout(() => refreshPosition(root), delayMs);
    }

    function setRouteSegment(state, payload) {
        if (!state.routeCtx) return;
        const nowMs = Date.now();
        const segment = payload && payload.segment ? payload.segment : {};
        const progressPct = toNumber(
            segment.progress_pct
            ?? payload.next_stop_progress_pct
            ?? payload.progress_pct
        );
        const progressFrac = Number.isFinite(progressPct) ? clamp01(progressPct / 100) : null;
        const fromId = (
            segment?.from_stop?.id
            ?? payload.segment_from_stop_id
            ?? payload.current_stop_id
            ?? payload.currentStopId
            ?? ''
        ).toString().trim();
        const toId = (
            segment?.to_stop?.id
            ?? payload.segment_to_stop_id
            ?? payload.next_stop_id
            ?? payload.nextStopId
            ?? ''
        ).toString().trim();
        const from = state.routeCtx.stopIndex[fromId] || null;
        const to = state.routeCtx.stopIndex[toId] || null;
        const lastProj = projectOnRoute(state.routeCtx, state.lastPos || state.nextPos || {});
        const nextProj = projectOnRoute(state.routeCtx, state.nextPos || {});

        let startDist = (from && from.dist) ?? null;
        let endDist = (to && to.dist) ?? null;
        if (startDist === null && lastProj) startDist = lastProj.distance;
        if (endDist === null && Number.isFinite(to?.dist)) endDist = to.dist;
        if (endDist === null && nextProj) endDist = nextProj.distance;
        if (endDist === null && Number.isFinite(state.routeCtx?.totalLength) && Number.isFinite(startDist)) {
            // mínimo avance para que haya easing aunque no tengamos destino fiable
            endDist = Math.min(state.routeCtx.totalLength, startDist + Math.max(200, state.routeCtx.totalLength * 0.01));
        }

        const movedMeters = (state.lastPos && state.nextPos)
            ? haversineM(state.lastPos.lat, state.lastPos.lon, state.nextPos.lat, state.nextPos.lon)
            : 0;

        if (Number.isFinite(startDist) && Number.isFinite(endDist) && endDist < startDist) {
            // If projection of the next point is further along the shape, prefer that
            if (nextProj && Number.isFinite(nextProj.distance) && nextProj.distance > startDist) {
                endDist = nextProj.distance;
            } else {
                endDist = startDist;
            }
        }

        if (
            Number.isFinite(startDist) &&
            Number.isFinite(endDist) &&
            Math.abs(endDist - startDist) < 1 &&
            (movedMeters > 5 || (progressFrac !== null && progressFrac > 0.05))
        ) {
            endDist = Math.min(
                state.routeCtx.totalLength || Infinity,
                startDist + Math.max(movedMeters * 0.9, 20)
            );
        }

        state.routeStartDist = Number.isFinite(startDist) ? startDist : null;
        state.routeEndDist = Number.isFinite(endDist) ? endDist : null;
        const segDep = toNumber(
            segment.dep_epoch
            ?? payload.segment_dep_epoch
        );
        const segArr = toNumber(
            segment.arr_epoch
            ?? payload.segment_arr_epoch
        );
        const depForEta = Number.isFinite(segDep) ? segDep : state.segmentDepTs;
        const arrForEta = Number.isFinite(segArr) ? segArr : state.segmentArrTs;
        const etaAtNow = etaFraction(nowMs, depForEta, arrForEta);
        if (Number.isFinite(segDep)) state.segmentDepTs = segDep;
        if (Number.isFinite(segArr)) state.segmentArrTs = segArr;
        const anchor = progressFrac ?? etaAtNow ?? null;
        state.routeProgressAtRefresh = anchor;
        state.routeProgressTs = nowMs;
        state.routeEtaAtRefresh = etaAtNow;
        const etaArrivalMs = Number.isFinite(arrForEta) ? arrForEta * 1000 : null;
        state.predictedArrMs = etaArrivalMs || (state.predictedArrMs && state.predictedArrMs > nowMs ? state.predictedArrMs : null);
        if (!state.predictedArrMs && anchor !== null) {
            state.predictedArrMs = nowMs + Math.max(POLL_MS, SMOOTH_WINDOW_MS);
        }
        state.segmentFromId = fromId || null;
        state.segmentToId = toId || null;
    }

    async function refreshPosition(root) {
        const state = root.__trainMap;
        if (!state || !state.positionApi) return;
        if (state.abortCtrl) {
            try { state.abortCtrl.abort(); } catch (_) {}
        }
        state.abortCtrl = new AbortController();
        const started = Date.now();
        try {
            const snap = await fetchPositionSnapshot(state.positionApi);
            if (!snap) throw new Error('Sin datos de posición');
            const data = snap.data || {};
            const hasPosition = snap.hasPosition;
            const now = Date.now();
            const smoothMs = Math.max(POLL_MS, SMOOTH_WINDOW_MS);
            let prev = state.nextPos;
            if (!prev && state.marker && typeof state.marker.getLngLat === 'function') {
                const ll = state.marker.getLngLat();
                if (ll) prev = { lon: ll.lng, lat: ll.lat };
            }
            if (hasPosition) {
                state.lastPos = prev || { lon: snap.lon, lat: snap.lat };
                state.nextPos = { lon: snap.lon, lat: snap.lat };
                state.lastTs = now;
                const smoothMs = Math.max(POLL_MS, SMOOTH_WINDOW_MS);
                state.nextTs = now + smoothMs;
            }
            const segment = data.segment || {};
            const segDep = toNumber(segment.dep_epoch ?? data.segment_dep_epoch);
            const segArr = toNumber(segment.arr_epoch ?? data.segment_arr_epoch);
            if (Number.isFinite(segDep) && Number.isFinite(segArr) && segArr > segDep) {
                state.segmentDepTs = segDep;
                state.segmentArrTs = segArr;
            } else {
                state.segmentDepTs = now / 1000;
                state.segmentArrTs = state.segmentDepTs + smoothMs / 1000;
            }
            if (Number.isFinite(toNumber(snap.heading))) {
                state.heading = toNumber(snap.heading);
            }
            if (data.route_geojson) {
                root.setAttribute('data-map-route-geojson', JSON.stringify(data.route_geojson));
            }
            if (data.route_stops_geojson) {
                root.setAttribute('data-map-stops-geojson', JSON.stringify(data.route_stops_geojson));
            }
            if (data.route_geojson || data.route_stops_geojson) {
                state.routeCtx = buildRouteCtx(
                    data.route_geojson || readGeoJSON(root, 'data-map-route-geojson', 'train-route-geojson'),
                    data.route_stops_geojson || readGeoJSON(root, 'data-map-stops-geojson', 'train-route-stops-geojson')
                );
            }
            setRouteSegment(state, data);
            if (hasPosition) {
                if (!state.marker && state.map) {
                    const markerEl = document.createElement('div');
                    markerEl.className = 'train-map-marker';
                    markerEl.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">train</span>';
                    markerEl.setAttribute('role', 'img');
                    markerEl.setAttribute('aria-label', 'Tren en ruta ' + (state.routeLabel || root.dataset.mapTrain || ''));
                    state.marker = new maplibregl.Marker({
                        element: markerEl,
                        rotation: state.heading || 0,
                        rotationAlignment: 'map',
                        pitchAlignment: 'map',
                    }).setLngLat([snap.lon, snap.lat]).addTo(state.map);
                }
                updateMarkerPosition(root);
            }
            ensureAnimation(root);
            const elapsed = Date.now() - started;
            schedulePoll(root, Math.max(5_000, POLL_MS - elapsed));
        } catch (err) {
            // Ignore AbortError - it's expected when cleaning up
            if (err.name === 'AbortError') return;
            console.warn('No se pudo obtener la posición del tren', err);
            schedulePoll(root, POLL_MS * 2);
        }
    }

    function renderInteractive(root, center, heading, routeLabel, routeData, stopsData, lineColor, hasPosition = true) {
        if (!window.maplibregl) return false;
        try {
            const map = new maplibregl.Map({
                container: root,
                style: MAP_STYLE,
                center,
                zoom: 12,
                pitch: 0,
                bearing: 0,
                attributionControl: true,
            });
            map.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), 'top-right');
            map.on('dragstart', () => { const st = root.__trainMap; if (st) st.userPanned = true; });

            let marker = null;
            if (hasPosition && Number.isFinite(center[0]) && Number.isFinite(center[1])) {
                const markerEl = document.createElement('div');
                markerEl.className = 'train-map-marker';
                markerEl.innerHTML = '<span class="material-symbols-rounded" aria-hidden="true">train</span>';
                markerEl.setAttribute('role', 'img');
                markerEl.setAttribute('aria-label', 'Tren en ruta ' + (routeLabel || ''));
                marker = new maplibregl.Marker({
                    element: markerEl,
                    rotation: heading || 0,
                    rotationAlignment: 'map',
                    pitchAlignment: 'map',
                }).setLngLat(center).addTo(map);
            }

            root.__trainMap = {
                ...(root.__trainMap || {}),
                map,
                marker,
                nextPos: hasPosition ? { lon: center[0], lat: center[1] } : null,
                lastPos: hasPosition ? { lon: center[0], lat: center[1] } : null,
                routeCtx: buildRouteCtx(routeData, stopsData),
                positionApi: root.dataset.mapApi || '',
                lineColor: lineColor || '',
                routeLabel: routeLabel || '',
            };

            map.on('load', () => {
                const lineColorFinal = lineColor || routeData?.properties?.color || DEFAULT_LINE_COLOR;
                const uid = `train-route-${routeLabel || root.dataset.mapTrain || Date.now()}`;
                if (routeData) {
                    map.addSource(uid, { type: 'geojson', data: routeData });
                    map.addLayer({
                        id: `${uid}-line`,
                        type: 'line',
                        source: uid,
                        paint: {
                            'line-color': lineColorFinal,
                            'line-width': 4,
                            'line-opacity': 0.9,
                        },
                    });
                }
                if (stopsData) {
                    const stopsId = `${uid}-stops`;
                    map.addSource(stopsId, { type: 'geojson', data: stopsData });
                    map.addLayer({
                        id: `${stopsId}-layer`,
                        type: 'circle',
                        source: stopsId,
                        paint: {
                            'circle-radius': 4,
                            'circle-color': '#ffffff',
                            'circle-stroke-color': lineColorFinal,
                            'circle-stroke-width': 2,
                        },
                    });
                }
            });

            root.dataset.mapMounted = 'interactive';
            refreshPosition(root);
            return true;
        } catch (err) {
            console.error('No se pudo inicializar MapLibre:', err);
            return false;
        }
    }

    async function bootstrapMapFromApi(root) {
        if (!root || !root.dataset.mapApi || root.dataset.mapMounted === 'loading') return;
        root.dataset.mapMounted = 'loading';
        try {
            const snap = await fetchPositionSnapshot(root.dataset.mapApi);
            if (!snap) throw new Error('Sin datos de posición');
            if (snap.hasPosition) {
                root.dataset.mapLat = snap.lat;
                root.dataset.mapLon = snap.lon;
                if (Number.isFinite(snap.heading)) {
                    root.dataset.mapHeading = snap.heading;
                }
                root.dataset.mapHasPosition = 'true';
            } else {
                delete root.dataset.mapLat;
                delete root.dataset.mapLon;
                delete root.dataset.mapHasPosition;
            }
            if (snap.data?.route_geojson) {
                root.setAttribute('data-map-route-geojson', JSON.stringify(snap.data.route_geojson));
            }
            if (snap.data?.route_stops_geojson) {
                root.setAttribute('data-map-stops-geojson', JSON.stringify(snap.data.route_stops_geojson));
            }
            root.dataset.mapMounted = '';
            initElement(root);
            if (root.__trainMap) {
                const state = root.__trainMap;
                const now = Date.now();
                if (snap.hasPosition) {
                    state.lastPos = { lon: snap.lon, lat: snap.lat };
                    state.nextPos = { lon: snap.lon, lat: snap.lat };
                    state.lastTs = now;
                    state.nextTs = now + Math.max(POLL_MS, SMOOTH_WINDOW_MS);
                }
                setRouteSegment(state, snap.data || {});
                ensureAnimation(root);
                refreshPosition(root);
            }
        } catch (err) {
            console.warn('No se pudo cargar la posición inicial del tren', err);
            root.textContent = 'No se pudo determinar la posición del tren.';
            root.dataset.mapMounted = 'invalid';
        }
    }

    function initElement(root) {
        if (!root || root.dataset.mapMounted === 'interactive' || root.dataset.mapMounted === 'loading') return;
        const lat = parseFloat(root.dataset.mapLat || 'NaN');
        const lon = parseFloat(root.dataset.mapLon || 'NaN');
        const routeData = readGeoJSON(root, 'data-map-route-geojson', 'train-route-geojson');
        const stopsData = readGeoJSON(root, 'data-map-stops-geojson', 'train-route-stops-geojson');
        const hasLatLon = Number.isFinite(lat) && Number.isFinite(lon);
        if (!hasLatLon && root.dataset.mapApi && !routeData) {
            bootstrapMapFromApi(root);
            return;
        }
        let center = null;
        if (hasLatLon) {
            center = [lon, lat];
        } else {
            center = deriveCenterFromRoute(routeData, stopsData);
        }
        if (!center || !Number.isFinite(center[0]) || !Number.isFinite(center[1])) {
            root.textContent = 'No se pudo determinar la posición del tren.';
            root.dataset.mapMounted = 'invalid';
            return;
        }
        const heading = parseFloat(root.dataset.mapHeading || '0') || 0;
        const routeLabel = root.dataset.mapRoute || root.dataset.mapTrain || '';
        const lineColor = root.dataset.mapLineColor || root.dataset.mapRouteColor || DEFAULT_LINE_COLOR;
        const hasPosition = hasLatLon || root.dataset.mapHasPosition === 'true';

        root.__trainMap = {
            map: null,
            marker: null,
            routeCtx: buildRouteCtx(routeData, stopsData),
            positionApi: root.dataset.mapApi || '',
            userPanned: false,
            lastPos: hasPosition ? { lon: center[0], lat: center[1] } : null,
            nextPos: hasPosition ? { lon: center[0], lat: center[1] } : null,
            lastTs: Date.now(),
            nextTs: Date.now(),
            routeStartDist: null,
            routeEndDist: null,
            segmentDepTs: null,
            segmentArrTs: null,
            segmentFromId: null,
            segmentToId: null,
            routeProgressAtRefresh: null,
            routeProgressTs: null,
            routeEtaAtRefresh: null,
            predictedArrMs: null,
            heading,
            lineColor,
            routeLabel,
        };

        if (!renderInteractive(root, center, heading, routeLabel, routeData, stopsData, lineColor, hasPosition)) {
            renderFallback(root, center, routeLabel);
        }
    }

    function scan(root) {
        if (!root) return;
        const nodes = root.matches && root.matches(MAP_SELECTOR)
            ? [root]
            : Array.from(root.querySelectorAll ? root.querySelectorAll(MAP_SELECTOR) : []);
        nodes.forEach(initElement);
    }

    document.addEventListener('DOMContentLoaded', () => scan(document));

    document.addEventListener('MapLibreReady', () => {
        const fallbackNodes = document.querySelectorAll(`${MAP_SELECTOR}[data-map-mounted="fallback"]`);
        fallbackNodes.forEach((node) => {
            node.dataset.mapMounted = '';
            initElement(node);
        });
    });

    if (window.htmx && window.htmx.version) {
        document.body.addEventListener('htmx:beforeSwap', (evt) => {
            cleanupInSubtree(evt.target || evt.detail?.target || document);
        });
        document.body.addEventListener('htmx:afterSwap', (evt) => {
            scan(evt.target);
        });
    }

    window.addEventListener('pagehide', () => cleanupInSubtree(document));
})();
