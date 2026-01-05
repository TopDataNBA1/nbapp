const CACHE_NAME = 'nba-app-v1';
const urlsToCache = [
  '/nbapp/',
  '/nbapp/index.html',
  '/nbapp/manifest.json',
  '/nbapp/icon-192.png',
  '/nbapp/icon-512.png'
];

// Instalar service worker
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(urlsToCache))
  );
});

// Interceptar requests
self.addEventListener('fetch', event => {
  event.respondWith(
    caches.match(event.request)
      .then(response => response || fetch(event.request))
  );
});

// Manejar notificaciones push (si usas servidor push)
self.addEventListener('push', event => {
  if (event.data) {
    const data = event.data.json();
    
    const options = {
      body: data.body,
      icon: data.icon || '/nbapp/icon-192.png',
      badge: '/nbapp/icon-192.png',
      tag: data.tag || 'nba-notification'
    };

    event.waitUntil(
      self.registration.showNotification(data.title || 'NBA Update', options)
    );
  }
});
