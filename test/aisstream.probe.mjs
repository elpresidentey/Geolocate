/**
 * One-off check that the aisstream key authenticates and that positions arrive.
 * Run: node test/aisstream.probe.mjs
 * Reads AISSTREAM_API_KEY from the environment; never hardcodes it.
 */
const key = process.env.AISSTREAM_API_KEY;
if (!key) {
  console.error('Set AISSTREAM_API_KEY first.');
  process.exit(1);
}

// A box over the Strait of Malacca / Singapore approaches: dense, fast traffic.
const BOX = [[[1.0, 103.5], [1.8, 104.3]]];

const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');

const seen = new Map();
let confirmed = false;
let done = false;

function finish(code) {
  if (done) return;
  done = true;
  const rows = [...seen.values()].slice(0, 5);
  console.log(`\nconfirmed: ${confirmed}`);
  console.log(`distinct vessels: ${seen.size}`);
  for (const v of rows) console.log('  ' + JSON.stringify(v));
  try { ws.close(); } catch {}
  process.exit(code);
}

ws.addEventListener('open', () => {
  console.log('socket open, subscribing…');
  ws.send(JSON.stringify({
    APIKey: key,
    BoundingBoxes: BOX,
    FilterMessageTypes: ['PositionReport'],
  }));
});

ws.addEventListener('message', async ev => {
  // The server sends binary frames; Node's WebSocket surfaces them as Blobs.
  let text;
  if (typeof ev.data === 'string') text = ev.data;
  else if (ev.data instanceof Blob) text = await ev.data.text();
  else if (ArrayBuffer.isView(ev.data)) text = new TextDecoder().decode(ev.data);
  else text = String(ev.data);

  let msg;
  try { msg = JSON.parse(text); }
  catch {
    console.log('NON-JSON frame:', text.slice(0, 300));
    return;
  }
  if (!confirmed) console.log('first frame:', JSON.stringify(msg).slice(0, 400));

  if (msg.MessageType === 'SubscriptionConfirmation') {
    confirmed = true;
    console.log('subscription confirmed, compression:', msg.Message?.CompressionEnabled);
    return;
  }
  if (msg.MessageType === 'Error') {
    console.log('service error:', JSON.stringify(msg));
    finish(1);
    return;
  }
  if (msg.MessageType !== 'PositionReport') return;

  const m = msg.MetaData || {};
  const p = msg.Message?.PositionReport || {};
  const mmsi = String(m.MMSI ?? p.UserID ?? '');
  if (!mmsi) return;
  if (!seen.has(mmsi)) {
    seen.set(mmsi, {
      mmsi,
      name: (m.ShipName || '').trim(),
      lat: m.Latitude ?? p.Latitude,
      lon: m.Longitude ?? p.Longitude,
      sog: p.Sog,
      cog: p.Cog,
      heading: p.TrueHeading,
      navStatus: p.NavigationalStatus,
      aisTs: p.Timestamp,
    });
    console.log('  vessel ' + seen.size + ': ' + (m.ShipName || '(unnamed)') +
      ` @ ${Number(m.Latitude).toFixed(3)},${Number(m.Longitude).toFixed(3)} sog=${p.Sog}`);
  }
});

ws.addEventListener('error', e => {
  console.log('socket error:', e.message || e.type);
  finish(1);
});

ws.addEventListener('close', e => {
  console.log('socket closed, code', e.code, e.reason || '');
  finish(confirmed && seen.size ? 0 : 1);
});

setTimeout(() => {
  console.log('30s window elapsed');
  finish(confirmed && seen.size ? 0 : 1);
}, 30000);
