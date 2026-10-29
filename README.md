# Orbital Freight

An interactive Earth-view of ocean freight traffic, rendered as an installable PWA.

## What is real

Every number on the page is sourced. Anything that cannot be sourced is displayed
as `--` or "unavailable" rather than filled with a plausible value.

| Data | Source | Key needed |
| --- | --- | --- |
| ISS position, altitude, velocity | [wheretheiss.at](https://wheretheiss.at) · TLE/SGP4 propagation (NORAD 25544) | no |
| Port weather (Lagos, Rotterdam) | Open-Meteo model analysis (nowcast) | no |
| Sea state / wave height | Open-Meteo marine model · offshore North Sea gridpoint | no |
| Day/night terminator | computed from UTC (solar longitude, exact declination, equation of time) | no |
| Earth textures | NASA Blue Marble & "Earth at Night" composites via third-party mirrors | no |
| Vessel positions | AIS provider, via `api/vessels.js` | **yes** |

Route distances are true great-circle calculations. Route ETAs divide that
distance by a stated 17-knot service speed (17 × 1.852 × 24 ≈ 756 km/day) — a
planning figure, not a booking. There is deliberately no distance-based "risk"
rating: real maritime risk is chokepoint- and weather-specific, and no source
here serves it.

## Known simplifications

- Lighting directions are computed exactly; on-screen distances are compressed
  for display (the Moon is drawn at 1/60 of its true 60-Earth-radii distance).
- The background starfield is synthetic, not a star catalog.
- City-lights texture is the pre-VIIRS NASA composite, not Black Marble.
- ISS position is propagated client-side (satellite.js SGP4, element set
  refreshed every 6 h from a third-party TLE service); without the TLE feed the
  app falls back to wheretheiss.at SGP4 anchors with orbital-mechanics
  extrapolation between 15-second fixes — not live radar tracking.

## Enabling live vessel positions

Live AIS comes from [aisstream.io](https://aisstream.io), which is WebSocket-only and explicitly does not allow browser connections from its docs — that is exactly why the key cannot live in the page. Get a free key from their dashboard, then set it in the Vercel environment for `geolocateshippingdashboard`:

```
AISSTREAM_API_KEY=your-key-here
```

Optional tuning:

```
AIS_BBOX=103.4,1.0,104.4,1.9   # "minLon,minLat,maxLon,maxLat" — one region to cover
AIS_LIMIT=120                  # cap vessels returned
```

By default `api/vessels.js` keeps one long-lived WebSocket to the stream service and covers the six busy corridors the globe draws. The key stays server-side; the browser only ever sees the normalized vessel list. Without a key it returns `{ configured: false }` and the UI states that the live feed is unavailable instead of plotting invented markers.

To go live locally:

```
vercel dev
```

Tests for the proxy's response mapping, junk-row filtering, and error paths:

```
node test/vessels.test.mjs
```

## Running without Vercel

The globe and all free telemetry work from any static server:

```
python -m http.server 8000
```

Only the AIS panel will report the proxy as unreachable.

## Structure

- `index.html` — the whole application: styles, markup, and the three.js module
- `sw.js` — service worker. Caches the shell, never caches `/api/` or live data
- `manifest.webmanifest` — PWA manifest with maskable icons and shortcuts
- `api/vessels.js` — server-side AIS proxy
- `test/vessels.test.mjs` — proxy tests (no key required)
- `icons/` — 192, 512, and maskable icons
