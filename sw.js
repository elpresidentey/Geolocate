const CACHE = 'orbital-freight-v7';
const CORE = [
  './',
  './index.html',
  './manifest.webmanifest',
  './icons/icon-192.png',
  './icons/icon-512.png',
  './icons/icon-maskable.png'
];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(CORE)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  // Live data must never be served from cache.
  if (new URL(e.request.url).pathname.startsWith('/api/')) return;
  const host = new URL(e.request.url).hostname;
  if (e.request.mode === 'navigate') {
    e.respondWith(
      fetch(e.request).then(r => { caches.open(CACHE).then(c => c.put(e.request, r.clone())); return r; })
        .catch(() => caches.match('./index.html'))
    );
    return;
  }
  // Live feeds (ISS telemetry, TLE, weather): network first, cached copy
  // only as an offline fallback. A cache-first policy here once served a
  // frozen hours-old fix as if it were current — never again.
  if (/api\.wheretheiss\.at|api\.open-meteo\.com|tle\.ivanstanojevic\.me/.test(host)) {
    e.respondWith(
      fetch(e.request).then(r => {
        const copy = r.clone();
        if (r.status === 200 || r.type === 'opaque') {
          caches.open(CACHE).then(c => c.put(e.request, copy));
        }
        return r;
      }).catch(() => caches.match(e.request))
    );
    return;
  }
  // cache-first for icons + tiles, network-first passthrough for live data
  e.respondWith(
    caches.match(e.request).then(hit => hit || fetch(e.request).then(r => {
      const clone = r.clone();
      if (r.status === 200 || r.type === 'opaque') {
        caches.open(CACHE).then(c => c.put(e.request, clone));
      }
      return r;
    }))
  );
});
