/**
 * Live vessel positions, sourced from aisstream.io.
 *
 * Why this shape is unusual
 * ------------------------
 * aisstream.io is WebSocket-only, and its documentation is explicit that
 * browser connections are not permitted: the API key must stay server-side and
 * clients must be proxied. So this is not a simple request/response proxy.
 *
 * Instead the module keeps ONE long-lived WebSocket, lets it stream into an
 * in-memory position table, and answers HTTP requests from that table.
 *
 *   - On a warm instance the socket stays open between requests, so responses
 *     are instant and cost nothing extra.
 *   - On a cold start the first request waits for a short collection window
 *     rather than for a full vessel cycle, so it always answers with something.
 *
 * Bounding boxes are required by the provider, so coverage is bounded to the
 * busy trade corridors rather than the whole ocean. Set AIS_BBOX to override.
 */

// Node runtime, deliberately not Edge: the Edge runtime forbids touching I/O
// created in one request's context from another request's handler, which
// kills the persistent aisstream WebSocket (observed live: "Cannot perform
// I/O on behalf of a different request"). Node serverless keeps module state
// across warm invocations, so one socket really does serve every warm poll.
export const config = { runtime: 'nodejs', maxDuration: 30 };

const STREAM_URL = 'wss://stream.aisstream.io/v0/stream';

// Corridors the app actually draws. Chosen for traffic density; the provider
// requires boxes, and an unbounded global subscription would be enormous.
const DEFAULT_BOXES = [
  [[1.0, 103.4], [1.9, 104.4]],      // Singapore / Malacca approaches
  [[29.5, 31.5], [32.0, 34.5]],      // Suez + Port Said
  [[8.0, -80.0], [10.0, -78.0]],     // Panama Canal, Atlantic side
  [[3.0, 1.0], [6.0, 8.0]],          // Nigeria coastal, Lagos / Port Harcourt
  [[50.5, 2.0], [53.0, 6.0]],        // Rotterdam / North Sea
  [[29.5, 120.0], [33.0, 124.0]],    // Shanghai approaches
];

const COLLECT_MS = 9000;     // cold-start collection window
const STALE_MS = 15 * 60 * 1000;
const DEFAULT_LIMIT = 140;

function corsHeaders() {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'cache-control': 'no-store',
  };
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders(), 'content-type': 'application/json' },
  });
}

/* ------------------------------------------------------------------ state */
const state = {
  socket: null,
  connecting: null,
  confirmed: false,
  positions: new Map(),      // mmsi -> normalized vessel (must have a position to plot)
  staticByMmsi: new Map(),   // mmsi -> { name, type } from static-data frames
  lastMessageAt: 0,
  lastError: null,
};

function bboxEnv() {
  const raw = (process.env.AIS_BBOX || '').trim();
  if (!raw) return DEFAULT_BOXES;
  // "minLon,minLat,maxLon,maxLat" -> one box of [[lat,lon],[lat,lon]]
  const p = raw.split(',').map(Number);
  if (p.length !== 4 || !p.every(Number.isFinite)) return DEFAULT_BOXES;
  return [[[p[1], p[0]], [p[3], p[2]]]];
}

/**
 * AIS TrueHeading uses 511 for "not available", some encoders emit 360, and a
 * bearing of 359.999° reports as 0-359. Anything outside 0-359 is untrustable,
 * so a heading we cannot trust is better reported as absent.
 */
function normalizeHeading(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0 || n > 359) return null;
  return Math.round(n);
}

const NAV_STATUS = {
  0: 'Under way', 1: 'Under way', 2: 'Under way', 3: 'Under way',
  4: 'Engaged', 5: 'Moored', 6: 'Moored', 7: 'Drifting', 8: 'Aground',
  9: 'Reserved', 10: 'Reserved', 11: 'Towing', 12: 'Pushing',
  13: 'Reserved', 14: 'AIS-SART', 15: 'Unknown',
};

function navLabel(code) {
  const n = Number(code);
  return NAV_STATUS[n] || (Number.isFinite(n) ? `Nav status ${n}` : 'Unknown');
}

/**
 * Position freshness has two machine-readable sources in aisstream:
 *   - MetaData.time_utc: a full RFC-like timestamp. Prefer it when present.
 *   - Message.PositionReport.Timestamp: the seconds field of the position's
 *     UTC clock time (the "32" in "...:32"). It is not an epoch and not even
 *     minute-precision on its own, so it is only a coarse fallback.
 */
function freshness(meta, pr, nowMs) {
  const iso = meta?.time_utc;
  if (typeof iso === 'string' && iso) {
    const normalized = iso.replace(' ', 'T').replace(' UTC', 'Z').replace(' +0000 UTC', 'Z');
    const t = Date.parse(normalized);
    if (Number.isFinite(t)) return Math.floor(t / 1000);
  }
  const ais = Number(pr?.Timestamp);
  if (Number.isFinite(ais) && ais >= 0 && ais < 60) {
    // Fall back to the current minute plus the reported seconds field.
    const minuteStart = Math.floor(nowMs / 60000) * 60;
    return Math.floor(minuteStart + ais);
  }
  return Math.floor(nowMs / 1000);
}

function validMmsi(m) {
  const s = String(m ?? '').trim();
  return /^\d{6,10}$/.test(s) ? s : null;
}

/* ------------------------------------------------------------ static data */
// AIS vessel type + name arrive on static-data frames, not position reports.
// aisstream delivers them under two historical shapes: MessageType
// 'StaticDataReport' (Message.StaticDataReport: ISO/IEC 6-crate extension,
// fields directly on the object) and 'ShipStaticData' (ITU 5: Message.
// ShipStaticData.Name / .Type). Both are accepted so either frame shape
// enriches the same table; static-only vessels never get plotted positions.
function staticFieldsFrom(msg) {
  const candidates = [
    msg.Message?.StaticDataReport,
    msg.Message?.ShipStaticData,
  ].filter(Boolean);
  for (const s of candidates) {
    const type = Number(s.Type);
    const name = typeof s.Name === 'string' ? s.Name.trim() : '';
    if (name || Number.isFinite(type)) {
      return {
        name,
        type: Number.isFinite(type) && type >= 0 && type <= 99 ? type : null,
      };
    }
  }
  return null;
}

function handleEvent(msg, nowMs, onError) {
  if (msg.MessageType === 'SubscriptionConfirmation') {
    state.confirmed = true;
    return;
  }
  if (msg.MessageType === 'Error') {
    state.lastError = msg.Message?.error || 'subscription rejected';
    onError && onError();
    return;
  }

  const staticFields = staticFieldsFrom(msg);
  if (staticFields) {
    const meta = msg.MetaData || {};
    const mmsi = validMmsi(meta.MMSI_String ?? meta.MMSI);
    if (!mmsi) return;
    const prevStatic = state.staticByMmsi.get(mmsi) || {};
    state.staticByMmsi.set(mmsi, {
      name: staticFields.name || prevStatic.name || '',
      type: staticFields.type ?? prevStatic.type ?? null,
    });
    const pos = state.positions.get(mmsi);
    if (pos) {
      if (staticFields.name) pos.name = staticFields.name;
      if (staticFields.type != null) pos.type = staticFields.type;
    }
    return;
  }

  if (msg.MessageType !== 'PositionReport') return;

  const meta = msg.MetaData || {};
  const pr = msg.Message?.PositionReport || {};

  // aisstream embeds lat/lon in both places, with differing key casing:
  // MetaData uses lowercase latitude/longitude, PositionReport uses
  // capitalized Latitude/Longitude. Prefer the richer PositionReport, fall
  // back to MetaData so older frames still map.
  const mmsi = validMmsi(meta.MMSI_String ?? meta.MMSI ?? pr.UserID);
  if (!mmsi) return;

  const lat = Number(pr.Latitude ?? meta.Latitude ?? meta.latitude);
  const lon = Number(pr.Longitude ?? meta.Longitude ?? meta.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return;

  if (typeof process !== 'undefined' && process.env.AIS_TEST_VERBOSE) {
    console.log('  api -> handleEvent', mmsi, lat, lon, 'positions before:', state.positions.size);
  }

  const prev = state.positions.get(mmsi);
  const staticInfo = state.staticByMmsi.get(mmsi) || {};
  state.positions.set(mmsi, {
    mmsi,
    name: (meta.ShipName || staticInfo.name || prev?.name || '').trim(),
    lat,
    lon,
    speedKn: Number.isFinite(Number(pr.Sog)) ? Number(pr.Sog) : null,
    course: Number.isFinite(Number(pr.Cog)) ? Number(pr.Cog) : null,
    heading: normalizeHeading(pr.TrueHeading),
    navStatus: navLabel(pr.NavigationalStatus),
    // Type arrives on static frames; positions alone carry none
    type: staticInfo.type ?? prev?.type ?? null,
    timestamp: freshness(meta, pr, nowMs),
  });
  state.lastMessageAt = Date.now();
}

function dropStale(nowMs) {
  const cutoff = nowMs - STALE_MS;
  for (const [mmsi, v] of state.positions) {
    if (v.timestamp * 1000 < cutoff) state.positions.delete(mmsi);
  }
  // Static table is keyed by MMSI with no clock of its own; cap it so a very
  // long-lived instance cannot grow without bound.
  if (state.staticByMmsi.size > 20000) {
    for (const mmsi of state.staticByMmsi.keys()) {
      state.staticByMmsi.delete(mmsi);
      if (state.staticByMmsi.size <= 15000) break;
    }
  }
}

function connect(key) {
  if (state.socket && state.socket.readyState <= 1) return Promise.resolve();
  if (state.connecting) return state.connecting;

  state.connecting = new Promise(resolve => {
    let settled = false;
    const done = () => { if (!settled) { settled = true; state.connecting = null; resolve(); } };

    let socket;
    try {
      socket = new WebSocket(STREAM_URL);
    } catch (err) {
      state.lastError = err.message || 'socket construction failed';
      done();
      return;
    }
    state.socket = socket;

    // The provider closes the connection if no subscription arrives in 3s.
    const timer = setTimeout(() => {
      if (!state.confirmed) state.lastError = 'no subscription confirmation within 3s';
      done();
    }, 3500);

    socket.addEventListener('open', () => {
      socket.send(JSON.stringify({
        APIKey: key,
        BoundingBoxes: bboxEnv(),
        // Static frames carry ship name/type so vessel classes can be shown;
        // positions alone cannot classify a mark.
        FilterMessageTypes: ['PositionReport', 'StaticDataReport'],
      }));
    });

    socket.addEventListener('message', async ev => {
      let text;
      if (typeof ev.data === 'string') text = ev.data;
      else if (ev.data instanceof Blob) text = await ev.data.text();
      else if (ArrayBuffer.isView(ev.data)) text = new TextDecoder().decode(ev.data);
      else text = String(ev.data);

      try {
        // The provider's explicit rejection must surface immediately, not
        // after the 3s confirmation timeout overwrites it.
        handleEvent(JSON.parse(text), Date.now(), () => {
          clearTimeout(timer);
          done();
        });
      } catch {
        /* a malformed frame should not kill the stream */
      }
      if (state.confirmed) { clearTimeout(timer); done(); }
    });

    socket.addEventListener('error', () => {
      state.lastError = state.lastError || 'websocket error';
      clearTimeout(timer);
      done();
    });

    socket.addEventListener('close', ev => {
      state.socket = null;
      state.confirmed = false;
      state.lastError = `stream closed (code ${ev.code})`;
      clearTimeout(timer);
      done();
    });
  });

  return state.connecting;
}

function snapshot() {
  const nowMs = Date.now();
  dropStale(nowMs);
  const rows = [...state.positions.values()]
    .sort((a, b) => b.timestamp - a.timestamp);
  return {
    confirmed: state.confirmed,
    lastMessageAt: state.lastMessageAt,
    error: state.lastError,
    vessels: rows,
  };
}

/* ----------------------------------------------------------------- handler */
// Node runtime wants fetch-style named exports; a default export returning a
// Response is silently ignored there (verified in production logs), which
// dead-ends every request until the runtime timeout.
async function handle(request) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }

  const key = process.env.AISSTREAM_API_KEY;
  if (!key) {
    return json({
      configured: false,
      provider: 'aisstream.io',
      message: 'Live AIS is not configured. Set AISSTREAM_API_KEY in the deployment environment.',
      count: 0,
      vessels: [],
    });
  }

  const limit = Math.min(Number(process.env.AIS_LIMIT) || DEFAULT_LIMIT, 400);

  try {
    await connect(key);

    // Cold instance: give the stream a moment to deliver before answering.
    if (!state.lastMessageAt) {
      await new Promise(r => setTimeout(r, COLLECT_MS));
    }
  } catch (err) {
    return json({
      configured: true,
      provider: 'aisstream.io',
      error: err.message || String(err),
      count: 0,
      vessels: [],
    }, 502);
  }

  const snap = snapshot();
  if (typeof process !== 'undefined' && process.env.AIS_TEST_VERBOSE) {
    console.log('  api -> snapshot size:', state.positions.size, 'confirmed:', state.confirmed);
  }
  return json({
    configured: true,
    provider: 'aisstream.io',
    connected: snap.confirmed,
    count: snap.vessels.length,
    fetchedAt: Date.now(),
    ...(snap.error && !snap.vessels.length ? { error: snap.error } : {}),
    vessels: snap.vessels.slice(0, limit),
  });
}

export { handle as GET, handle as OPTIONS };