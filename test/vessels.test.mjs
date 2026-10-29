/**
 * Tests for the aisstream-backed vessel proxy.
 *
 * A fake WebSocket is injected so the suite never touches the network and never
 * needs a real API key. Run: node test/vessels.test.mjs
 */

const KEY = 'test-key-not-a-real-credential';

/* ------------------------------------------------------------------ fake ws */
const handlers = new Map();
let lastSubscription = null;
let autoConfirm = true;

class FakeSocket {
  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.sent = [];
    FakeSocket.instances.push(this);
    // open on next tick, like a real socket
    queueMicrotask(() => {
      this.readyState = 1;
      this.emit('open', {});
      if (autoConfirm) {
        queueMicrotask(() => this.serverSend({
          MessageType: 'SubscriptionConfirmation',
          Message: { CompressionEnabled: true },
        }));
      }
    });
  }
  emit(type, ev) {
    const listeners = handlers.get(type) || [];
    if (process.env.AIS_TEST_VERBOSE) console.log('  fake -> emit', type, 'to', listeners.length, 'listeners');
    listeners.forEach(fn => fn(ev));
  }
  on(type, fn) {
    if (!handlers.has(type)) handlers.set(type, []);
    handlers.get(type).push(fn);
  }
  addEventListener(type, fn) { this.on(type, fn); }
  send(data) { this.sent.push(data); lastSubscription = JSON.parse(data); }
  close() { this.readyState = 3; this.emit('close', { code: 1000, reason: '' }); }

  /** Test helper: deliver a frame the way the real service does (binary). */
  serverSend(obj) {
    if (process.env.AIS_TEST_VERBOSE) console.log('  fake -> emit message frame,', JSON.stringify(obj).length, 'bytes');
    this.emit('message', { data: new TextEncoder().encode(JSON.stringify(obj)) });
  }
  serverSendError(kind) {
    this.emit('message', { data: { then: null, text: async () => kind } });
  }
}
FakeSocket.instances = [];

function reset({ confirm = true } = {}) {
  handlers.clear();
  FakeSocket.instances.length = 0;
  lastSubscription = null;
  autoConfirm = confirm;
  globalThis.WebSocket = FakeSocket;
  delete process.env.AISSTREAM_API_KEY;
  delete process.env.AIS_BBOX;
  delete process.env.AIS_LIMIT;
}

/* ------------------------------------------------------------------- setup */
const { GET: handler, __reset } = await import('../api/vessels.js?fresh=' + Math.random());
function freshHandler() {
  // module keeps its socket between calls; re-import for a clean table
  return import('../api/vessels.js?fresh=' + Math.random()).then(m => m.GET);
}

const req = (method = 'GET') =>
  new Request('https://example.vercel.app/api/vessels', { method });

const results = [];
const check = (label, cond) => results.push(`${cond ? 'PASS' : 'FAIL'}  ${label}`);

/* --------------------------------------------------- 1. unconfigured state */
{
  reset();
  let h = await freshHandler();
  const res = await h(req());
  const body = await res.json();
  check('unconfigured: status 200', res.status === 200);
  check('unconfigured: configured is false', body.configured === false);
  check('unconfigured: no vessels', body.vessels.length === 0);
  check('unconfigured: names the env var', /AISSTREAM_API_KEY/.test(body.message));
  check('unconfigured: does not open a socket', FakeSocket.instances.length === 0);
}

/* ---------------------------------------------- 2. subscription is correct */
{
  reset();
  process.env.AISSTREAM_API_KEY = KEY;
  let h = await freshHandler();
  const res = await h(req());
  await res.json();

  check('subscribes to the documented stream url',
    FakeSocket.instances[0].url === 'wss://stream.aisstream.io/v0/stream');
  check('sends the API key', lastSubscription.APIKey === KEY);
  check('requests position + static reports',
    JSON.stringify(lastSubscription.FilterMessageTypes) ===
      JSON.stringify(['PositionReport', 'StaticDataReport']));
  check('sends bounding boxes (provider requires them)',
    Array.isArray(lastSubscription.BoundingBoxes) && lastSubscription.BoundingBoxes.length > 0);
  check('boxes are [lat,lon] corner pairs',
    lastSubscription.BoundingBoxes.every(b => b.length === 2 &&
      b[0].length === 2 && b[1].length === 2 &&
      b[0][0] === b[0][1] * 0 + b[0][0]));   // sanity: numeric
  check('boxes are numeric', lastSubscription.BoundingBoxes
    .flat(2).every(v => typeof v === 'number' && Number.isFinite(v)));
  check('Nigeria coastal corridor is covered',
    JSON.stringify(lastSubscription.BoundingBoxes).includes('[3,1]'));
}

/* ------------------------------------------- 3. AIS_BBOX override honoured */
{
  reset();
  process.env.AISSTREAM_API_KEY = KEY;
  process.env.AIS_BBOX = '10,20,30,40';   // minLon,minLat,maxLon,maxLat
  let h = await freshHandler();
  await (await h(req())).json();
  const box = lastSubscription.BoundingBoxes[0];
  check('AIS_BBOX: single box requested', lastSubscription.BoundingBoxes.length === 1);
  check('AIS_BBOX: lon/lat swapped into provider order',
    JSON.stringify(box) === JSON.stringify([[20, 10], [40, 30]]));
}

/* -------------------------------------------------- 4. frame normalization */
{
  reset();
  process.env.AISSTREAM_API_KEY = KEY;
  process.env.AIS_LIMIT = '5';
  let h = await freshHandler();
  const res = await h(req());
  const body = await res.json();

  const sock = FakeSocket.instances[0];
  // position lives in Message.PositionReport, not MetaData
  sock.serverSend({
    MessageType: 'PositionReport',
    MetaData: { MMSI: 368207620, ShipName: '  EVER GIVEN  ' },
    Message: { PositionReport: {
      UserID: 368207620, Latitude: 1.2643, Longitude: 103.8210,
      Sog: 12.4, Cog: 86.7, TrueHeading: 87, NavigationalStatus: 0,
    } },
  });
  // true heading 511 means "not available" and must not be passed through
  sock.serverSend({
    MessageType: 'PositionReport',
    MetaData: { MMSI: 563002840, ShipName: 'WAVEMASTER 5' },
    Message: { PositionReport: {
      UserID: 563002840, Latitude: 1.2622, Longitude: 103.8188,
      Sog: 0.7, Cog: 107.8, TrueHeading: 511, NavigationalStatus: 5,
    } },
  });

  const again = await (await h(req())).json();
  check('parses position reports', again.count === 2);
  check('reports connected', again.connected === true);

  const v = again.vessels.find(x => x.mmsi === '368207620');
  if (!v) {
    console.log('  DEBUG instances:', FakeSocket.instances.length,
      'listeners:', (handlers.get('message') || []).length,
      'body:', JSON.stringify(again));
    check('normalize: vessel present', false);
  }
  check('trims ship name', v.name === 'EVER GIVEN');
  check('keeps lat/lon', v.lat === 1.2643 && v.lon === 103.8210);
  check('speed over ground in knots', v.speedKn === 12.4);
  check('course over ground kept', v.course === 86.7);
  check('valid heading kept', v.heading === 87);
  check('511 heading reported as absent', v && v.heading !== 511);
  check('nav status labelled', v.navStatus === 'Under way');
  check('timestamp rebased to a real clock time',
    Math.abs(v.timestamp - Math.floor(Date.now() / 1000)) < 3700);
  check('AIS_LIMIT respected', again.vessels.length <= 5);

  const moored = again.vessels.find(x => x.mmsi === '563002840');
  check('moored nav status labelled', moored.navStatus === 'Moored');
  check('511 becomes null heading', moored.heading === null);
}

/* ------------------------------------------------------ 5. junk is rejected */
{
  reset();
  process.env.AISSTREAM_API_KEY = KEY;
  let h = await freshHandler();
  await (await h(req())).json();
  const sock = FakeSocket.instances[0];

  sock.serverSend({ MessageType: 'PositionReport',
    MetaData: { MMSI: 0 }, Message: { PositionReport: { Latitude: 5, Longitude: 5 } } });
  sock.serverSend({ MessageType: 'PositionReport',
    MetaData: { MMSI: 12345 }, Message: { PositionReport: { Latitude: 5, Longitude: 5 } } });
  sock.serverSend({ MessageType: 'PositionReport',
    MetaData: { MMSI: 111111111 }, Message: { PositionReport: { Latitude: 999, Longitude: 5 } } });
  sock.serverSend({ MessageType: 'PositionReport',
    MetaData: { MMSI: 111111112 }, Message: { PositionReport: { Longitude: 5 } } }); // no lat
  sock.serverSend({ MessageType: 'ShipStaticData',
    MetaData: { MMSI: 111111113 }, Message: {} });

  const out = await (await h(req())).json();
  check('rejects MMSI 0', !out.vessels.some(v => v.mmsi === '0'));
  check('rejects short MMSI', out.count === 0);
  check('rejects out-of-range latitude', !out.vessels.some(v => v.mmsi === '111111111'));
  check('rejects report with no latitude', !out.vessels.some(v => v.mmsi === '111111112'));
  check('ignores non-position message types',
    !out.vessels.some(v => v.mmsi === '111111113'));
  check('junk frames leave the table empty', out.count === 0);
}

/* ------------------------------------------------------- 6. error surfacing */
{
  reset({ confirm: false });
  process.env.AISSTREAM_API_KEY = KEY;
  let h = await freshHandler();
  const sockPromise = h(req());
  await new Promise(r => setTimeout(r, 50));
  FakeSocket.instances[0].serverSend({
    MessageType: 'Error', Message: { error: 'invalid api key' },
  });
  const body = await (await sockPromise).json();
  check('provider error is reported, not swallowed',
    body.vessels.length === 0 && /invalid api key/i.test(JSON.stringify(body)));
  if (!/invalid api key/i.test(JSON.stringify(body))) {
    console.log('  DEBUG error-path body:', JSON.stringify(body));
  }
}

/* -------------------------------------------------------- 7. closed socket */
{
  reset();
  process.env.AISSTREAM_API_KEY = KEY;
  let h = await freshHandler();
  await (await h(req())).json();
  const sock = FakeSocket.instances[0];
  sock.serverSend({
    MessageType: 'PositionReport',
    MetaData: { MMSI: 636019999, ShipName: 'MSC GULSUN' },
    Message: { PositionReport: { Latitude: 12.5, Longitude: 44.9, Sog: 13.2, Cog: 210 } },
  });
  check('vessel retained before close',
    (await (await h(req())).json()).count === 1);
  sock.emit('close', { code: 1006, reason: 'gone' });
  const after = await (await h(req())).json();
  check('reconnects after an unexpected close', after.connected === true);
  check('previous positions survive a reconnect', after.count >= 1);
}

/* -------------------------------------------- 8. static data enriches rows */
{
  reset();
  process.env.AISSTREAM_API_KEY = KEY;
  let h = await freshHandler();
  await (await h(req())).json();
  const sock = FakeSocket.instances[0];

  // ShipStaticData (ITU message 5 shape): name + AIS ship type arriving
  // BEFORE the vessel is ever seen in a position report
  sock.serverSend({
    MessageType: 'ShipStaticData',
    MetaData: { MMSI: 273449120 },
    Message: { ShipStaticData: { Name: 'MSC OCEANIA', Type: 79 } },
  });
  sock.serverSend({
    MessageType: 'PositionReport',
    MetaData: { MMSI: 273449120 },
    Message: { PositionReport: { Latitude: 30.6, Longitude: 121.2, Sog: 8.2, Cog: 275 } },
  });

  // StaticDataReport shape arriving for a vessel already holding a position
  sock.serverSend({
    MessageType: 'PositionReport',
    MetaData: { MMSI: 219026663, ShipName: 'MAERSK SELETAR' },
    Message: { PositionReport: { Latitude: 50.9, Longitude: 2.9, Sog: 14.5, Cog: 288 } },
  });
  sock.serverSend({
    MessageType: 'StaticDataReport',
    MetaData: { MMSI: 219026663 },
    Message: { StaticDataReport: { Type: 70 } },
  });

  // out-of-range AIS type (0-99 valid) must be refused, not stored
  sock.serverSend({
    MessageType: 'ShipStaticData',
    MetaData: { MMSI: 412666666 },
    Message: { ShipStaticData: { Name: 'BAD TYPE', Type: 123 } },
  });
  sock.serverSend({
    MessageType: 'PositionReport',
    MetaData: { MMSI: 412666666 },
    Message: { PositionReport: { Latitude: 1.3, Longitude: 104.3, Sog: 10, Cog: 89 } },
  });

  // Static-only MMSI: must NOT create a plottable row (no position exists)
  sock.serverSend({
    MessageType: 'StaticDataReport',
    MetaData: { MMSI: 999888777 },
    Message: { StaticDataReport: { Name: 'GHOST SHIP', Type: 71 } },
  });

  const out = await (await h(req())).json();
  const typed = {};
  for (const v of out.vessels) typed[v.mmsi] = v;
  check('static type merged into position row', typed['219026663']?.type === 70);
  check('static name from ShipStaticData lands once position arrives',
    typed['273449120']?.name === 'MSC OCEANIA');
  check('position name preserved when static carries none',
    typed['219026663']?.name === 'MAERSK SELETAR');
  check('out-of-range type reported absent', typed['412666666']?.type === null);
  check('static-only vessel is not plotted', typed['999888777'] === undefined);
}

/* ---------------------------------------------------------------- 9. CORS */
{
  reset();
  let h = await freshHandler();
  const res = await h(req('OPTIONS'));
  check('preflight returns 204', res.status === 204);
  check('preflight allows the origin', res.headers.get('access-control-allow-origin') === '*');
  check('preflight allows GET', (res.headers.get('access-control-allow-methods') || '').includes('GET'));
}

console.log(results.join('\n'));
const failed = results.filter(r => r.startsWith('FAIL')).length;
console.log(`\n${results.length - failed}/${results.length} passed`);
process.exit(failed ? 1 : 0);