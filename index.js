'use strict';

require('dotenv').config();

const path = require('path');
const fs = require('fs');
const http = require('http');
const express = require('express');
const rateLimit = require('express-rate-limit');
const multer = require('multer');
const { Server } = require('socket.io');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';
const TRUST_PROXY = process.env.TRUST_PROXY === '1' || process.env.NODE_ENV === 'production';
const ADMIN_KEY = process.env.ADMIN_KEY || 'change-me-secret-key';

const app = express();
app.disable('x-powered-by');
if (TRUST_PROXY) app.set('trust proxy', 1);

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
    methods: ['GET', 'POST']
  },
  maxHttpBufferSize: 50 * 1024 * 1024,
  pingTimeout: 25000,
  pingInterval: 20000
});

app.use(express.json({ limit: '16kb' }));
app.use(express.static(path.join(__dirname, 'public'), { maxAge: NODE_ENV === 'production' ? '1h' : 0 }));

const UPLOAD_DIR = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
app.use('/uploads', express.static(UPLOAD_DIR, { maxAge: '1h' }));

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, UPLOAD_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase() || '.mp4';
      cb(null, `admin_video_${Date.now()}${ext}`);
    }
  }),
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = ['video/mp4', 'video/webm', 'video/quicktime'].includes(file.mimetype);
    cb(ok ? null : new Error('Only mp4/webm/mov allowed'), ok);
  }
});

const apiLimiter = rateLimit({
  windowMs: 60 * 1000, max: 120,
  standardHeaders: true, legacyHeaders: false
});
const reportLimiter = rateLimit({
  windowMs: 10 * 60 * 1000, max: 20,
  standardHeaders: true, legacyHeaders: false,
  message: { error: 'Too many reports. Please try again later.' }
});
const adminLimiter = rateLimit({
  windowMs: 60 * 1000, max: 20,
  standardHeaders: true, legacyHeaders: false
});
app.use('/api/', apiLimiter);

// ---------- Dual-pool state ----------
// Video mode: existing behavior (WebRTC)
// Text mode: text-only, users provide a name
const pools = {
  video: { queue: new Set(), matches: new Map() },
  text:  { queue: new Set(), matches: new Map() }
};

const sessions = new Map();
const recentReports = [];
const MAX_REPORTS = 1000;

// Admin
let adminSocketId = null;
let adminVideoUrl = null;
let adminDurationSeconds = 0;
let adminReplies = [];
let adminAutoNextTimer = null;

function sanitizeString(s, max = 500) {
  if (typeof s !== 'string') return '';
  return s.replace(/[\u0000-\u001F\u007F]/g, '').slice(0, max).trim();
}

function getMode(socket) {
  const q = socket.handshake.query.mode;
  return q === 'text' ? 'text' : 'video';
}

function getPool(socket) {
  return pools[getMode(socket)];
}

function clearMatch(socketId, notifyPeer = true) {
  const s = sessions.get(socketId);
  if (!s) return null;
  const pool = pools[s.mode];
  const peerId = pool.matches.get(socketId);
  if (!peerId) return null;
  pool.matches.delete(socketId);
  pool.matches.delete(peerId);
  if (notifyPeer) {
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket) peerSocket.emit('peer_left');
  }
  return peerId;
}

function broadcastStats() {
  io.emit('stats_update', {
    online: io.engine.clientsCount,
    waiting: pools.video.queue.size + pools.text.queue.size,
    inChat: (pools.video.matches.size + pools.text.matches.size) / 2,
    videoWaiting: pools.video.queue.size,
    textWaiting: pools.text.queue.size
  });
}

function tryMatchInPool(mode) {
  const pool = pools[mode];
  if (pool.queue.size < 2) return;
  const ids = Array.from(pool.queue);

  while (ids.length >= 2) {
    const a = ids.shift();
    const b = ids.shift();
    if (!a || !b || a === b) continue;
    const socketA = io.sockets.sockets.get(a);
    const socketB = io.sockets.sockets.get(b);
    if (!socketA || !socketB) {
      pool.queue.delete(a); pool.queue.delete(b);
      continue;
    }
    if (pool.matches.has(a) || pool.matches.has(b)) continue;

    pool.queue.delete(a);
    pool.queue.delete(b);
    pool.matches.set(a, b);
    pool.matches.set(b, a);

    const initiator = a < b ? a : b;
    const other = initiator === a ? b : a;

    const socketA2 = io.sockets.sockets.get(initiator);
    const socketB2 = io.sockets.sockets.get(other);

    // Include names for text mode
    const nameOf = (sid) => sessions.get(sid)?.name || 'Stranger';
    const payloadA = {
      peerId: other,
      initiator: true,
      peerName: nameOf(other)
    };
    const payloadB = {
      peerId: initiator,
      initiator: false,
      peerName: nameOf(initiator)
    };

    socketA2.emit('match_found', payloadA);
    socketB2.emit('match_found', payloadB);

    // Admin auto-next for video mode
    if (mode === 'video' && adminVideoUrl && (initiator === adminSocketId || other === adminSocketId)) {
      scheduleAutoNext(initiator === adminSocketId ? other : initiator);
    }

    broadcastStats();
  }
}

function scheduleAutoNext(realUserSocketId) {
  if (adminAutoNextTimer) clearTimeout(adminAutoNextTimer);
  adminAutoNextTimer = setTimeout(() => {
    if (adminSocketId && pools.video.matches.get(adminSocketId)) {
      const peer = pools.video.matches.get(adminSocketId);
      clearMatch(adminSocketId, false);
      if (!pools.video.queue.has(adminSocketId)) pools.video.queue.add(adminSocketId);
      const peerSocket = io.sockets.sockets.get(peer);
      if (peerSocket) peerSocket.emit('peer_left');
      broadcastStats();
      tryMatchInPool('video');
    }
    adminAutoNextTimer = null;
  }, adminDurationSeconds * 1000);
}

function cancelAutoNext() {
  if (adminAutoNextTimer) {
    clearTimeout(adminAutoNextTimer);
    adminAutoNextTimer = null;
  }
}

const SIGNAL_WINDOW_MS = 1000;
const SIGNAL_MAX_PER_WINDOW = 60;
const CHAT_WINDOW_MS = 3000;
const CHAT_MAX_PER_WINDOW = 12;
const CHAT_MAX_LENGTH = 500;

function rateLimitSignal(socket) {
  const t = Date.now();
  const arr = socket.data.signalTimestamps || (socket.data.signalTimestamps = []);
  while (arr.length && t - arr[0] > SIGNAL_WINDOW_MS) arr.shift();
  if (arr.length >= SIGNAL_MAX_PER_WINDOW) return false;
  arr.push(t);
  return true;
}
function rateLimitChat(socket) {
  const t = Date.now();
  const arr = socket.data.chatTimestamps || (socket.data.chatTimestamps = []);
  while (arr.length && t - arr[0] > CHAT_WINDOW_MS) arr.shift();
  if (arr.length >= CHAT_MAX_PER_WINDOW) return false;
  arr.push(t);
  return true;
}

// ---------- Socket.IO ----------
io.use((socket, next) => {
  socket.data.signalTimestamps = [];
  socket.data.chatTimestamps = [];
  socket.data.isAdmin = false;
  socket.data.mode = getMode(socket);
  sessions.set(socket.id, {
    joinedAt: Date.now(),
    mode: socket.data.mode,
    name: null,
    ua: sanitizeString(socket.handshake.headers['user-agent'] || '', 300),
    ip: socket.handshake.address
  });
  next();
});

io.on('connection', (socket) => {
  broadcastStats();
  const mode = socket.data.mode;
  const pool = pools[mode];

  // Admin registration (video only)
  socket.on('admin_register', ({ key }) => {
    if (sanitizeString(key, 200) !== ADMIN_KEY) {
      socket.emit('admin_error', { message: 'Invalid admin key.' });
      return;
    }
    socket.data.isAdmin = true;
    adminSocketId = socket.id;
    socket.emit('admin_registered');
  });

  socket.on('admin_arm', ({ videoUrl, durationSeconds, replies }) => {
    if (!socket.data.isAdmin) return;
    if (typeof videoUrl !== 'string' || !videoUrl.startsWith('/uploads/')) return;
    const dur = parseInt(durationSeconds, 10);
    if (!dur || dur < 5 || dur > 3600) {
      socket.emit('admin_error', { message: 'Duration must be 5-3600 seconds.' });
      return;
    }
    adminVideoUrl = videoUrl;
    adminDurationSeconds = dur;
    adminReplies = Array.isArray(replies)
      ? replies.map(r => sanitizeString(r, CHAT_MAX_LENGTH)).filter(Boolean).slice(0, 50)
      : [];

    if (!pools.video.matches.has(socket.id) && !pools.video.queue.has(socket.id)) {
      pools.video.queue.add(socket.id);
      tryMatchInPool('video');
    }
  });

  socket.on('admin_disarm', () => {
    if (!socket.data.isAdmin) return;
    cancelAutoNext();
    pools.video.queue.delete(socket.id);
    clearMatch(socket.id, true);
    adminVideoUrl = null;
    adminDurationSeconds = 0;
    adminReplies = [];
    broadcastStats();
  });

  // ---------- Join queue ----------
  socket.on('join_queue', (payload) => {
    // For text mode, capture name
    if (mode === 'text') {
      const name = sanitizeString(payload && payload.name, 24);
      if (name.length < 2) {
        socket.emit('error_message', { message: 'Invalid name.' });
        return;
      }
      const s = sessions.get(socket.id);
      if (s) s.name = name;
    }

    if (pool.matches.has(socket.id)) {
      socket.emit('error_message', { message: 'You are already in a chat.' });
      return;
    }
    if (pool.queue.has(socket.id)) {
      socket.emit('queue_joined', { position: pool.queue.size });
      return;
    }
    pool.queue.add(socket.id);
    socket.emit('queue_joined', { position: pool.queue.size });
    broadcastStats();
    tryMatchInPool(mode);
  });

  socket.on('next_stranger', () => {
    cancelAutoNext();
    const peerId = clearMatch(socket.id, true);
    if (peerId) {
      const peerSocket = io.sockets.sockets.get(peerId);
      if (peerSocket) peerSocket.emit('chat_ended');
    }
    pool.queue.delete(socket.id);
    if (!pool.queue.has(socket.id)) pool.queue.add(socket.id);
    socket.emit('queue_joined', { position: pool.queue.size });
    broadcastStats();
    tryMatchInPool(mode);
  });

  socket.on('leave_chat', () => {
    cancelAutoNext();
    const peerId = clearMatch(socket.id, true);
    pool.queue.delete(socket.id);
    if (peerId) {
      const peerSocket = io.sockets.sockets.get(peerId);
      if (peerSocket) peerSocket.emit('chat_ended');
    }
    socket.emit('queue_left');
    broadcastStats();
  });

  // ---------- WebRTC ----------
  socket.on('webrtc_offer', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    const sdp = payload && payload.sdp;
    if (typeof to !== 'string' || !sdp || typeof sdp !== 'object') return;
    if (pool.matches.get(socket.id) !== to) {
      socket.emit('error_message', { message: 'Not matched with that peer.' });
      return;
    }
    const target = io.sockets.sockets.get(to);
    if (target) target.emit('webrtc_offer', { from: socket.id, sdp });
  });

  socket.on('webrtc_answer', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    const sdp = payload && payload.sdp;
    if (typeof to !== 'string' || !sdp || typeof sdp !== 'object') return;
    if (pool.matches.get(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (target) target.emit('webrtc_answer', { from: socket.id, sdp });
  });

  socket.on('webrtc_ice_candidate', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    const candidate = payload && payload.candidate;
    if (typeof to !== 'string' || !candidate) return;
    if (pool.matches.get(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (target) target.emit('webrtc_ice_candidate', { from: socket.id, candidate });
  });

  // ---------- Chat ----------
  socket.on('chat_message', (payload) => {
    if (!rateLimitChat(socket)) {
      socket.emit('error_message', { message: 'Sending too fast.' });
      return;
    }
    const to = payload && payload.to;
    const text = sanitizeString(payload && payload.text, CHAT_MAX_LENGTH);
    if (!text) return;

    let targetId = to;
    if (socket.data.isAdmin && !targetId) {
      targetId = pool.matches.get(socket.id);
    }
    if (!targetId || pool.matches.get(socket.id) !== targetId) {
      socket.emit('error_message', { message: 'Not matched with that peer.' });
      return;
    }
    const target = io.sockets.sockets.get(targetId);
    if (target) target.emit('chat_message', { from: socket.id, text, ts: Date.now() });
  });

  socket.on('typing', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    if (typeof to !== 'string' || pool.matches.get(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (target) target.emit('typing', { from: socket.id });
  });

  socket.on('stop_typing', (payload) => {
    const to = payload && payload.to;
    if (typeof to !== 'string' || pool.matches.get(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (target) target.emit('stop_typing', { from: socket.id });
  });

  socket.on('block_peer', () => {
    const peerId = pool.matches.get(socket.id);
    if (!peerId) return;
    clearMatch(socket.id, false);
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket) peerSocket.emit('chat_ended');
    socket.emit('blocked');
    broadcastStats();
  });

  socket.on('disconnect', () => {
    cancelAutoNext();
    pool.queue.delete(socket.id);
    const peerId = clearMatch(socket.id, true);
    if (peerId) {
      const peerSocket = io.sockets.sockets.get(peerId);
      if (peerSocket) peerSocket.emit('peer_left');
    }
    sessions.delete(socket.id);
    if (socket.id === adminSocketId) {
      adminSocketId = null;
      adminVideoUrl = null;
      adminDurationSeconds = 0;
      adminReplies = [];
    }
    broadcastStats();
  });
});

// ---------- REST API ----------
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), node: process.version, env: NODE_ENV });
});

app.get('/api/stats', (req, res) => {
  res.json({
    online: io.engine.clientsCount,
    waiting: pools.video.queue.size + pools.text.queue.size,
    inChat: (pools.video.matches.size + pools.text.matches.size) / 2,
    videoWaiting: pools.video.queue.size,
    textWaiting: pools.text.queue.size,
    uptimeSeconds: Math.floor(process.uptime())
  });
});

app.post('/api/admin/verify', adminLimiter, (req, res) => {
  const key = sanitizeString(req.body && req.body.key, 200);
  if (key !== ADMIN_KEY) return res.status(401).json({ error: 'Invalid key.' });
  res.json({ ok: true });
});

app.post('/api/admin/upload-video', adminLimiter, (req, res, next) => {
  const key = req.headers['x-admin-key'];
  if (sanitizeString(key, 200) !== ADMIN_KEY) {
    return res.status(401).json({ error: 'Unauthorized.' });
  }
  upload.single('video')(req, res, (err) => {
    if (err) return res.status(400).json({ error: err.message });
    if (!req.file) return res.status(400).json({ error: 'No file uploaded.' });
    const url = `/uploads/${req.file.filename}`;
    res.json({ ok: true, url });
  });
});

app.post('/api/report', reportLimiter, (req, res) => {
  const body = req.body || {};
  const reason = sanitizeString(body.reason, 40);
  const details = sanitizeString(body.details, 500);
  const peerSocketId = sanitizeString(body.peerSocketId, 100);
  const validReasons = new Set(['nudity', 'harassment', 'hate', 'violence', 'spam', 'minor', 'other']);
  if (!validReasons.has(reason)) return res.status(400).json({ error: 'Invalid reason.' });

  const report = {
    id: `r_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    reason, details,
    reporterIp: req.ip,
    peerSocketId: peerSocketId || null,
    createdAt: new Date().toISOString()
  };
  recentReports.push(report);
  if (recentReports.length > MAX_REPORTS) recentReports.shift();
  if (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY) {
    persistReportToSupabase(report).catch(e => console.error('[report]', e.message));
  }
  res.json({ ok: true, id: report.id });
});

async function persistReportToSupabase(report) {
  const url = `${process.env.SUPABASE_URL.replace(/\/$/, '')}/rest/v1/reports`;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'apikey': key,
      'Authorization': `Bearer ${key}`,
      'Prefer': 'return=minimal'
    },
    body: JSON.stringify({
      reason: report.reason, details: report.details,
      reporter_ip: report.reporterIp, peer_socket_id: report.peerSocketId,
      created_at: report.createdAt
    })
  });
  if (!resp.ok) throw new Error('Supabase ' + resp.status);
}

app.get('/api/ice', (req, res) => {
  const ice = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ];
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    ice.push({
      urls: process.env.TURN_URL.split(',').map(s => s.trim()).filter(Boolean),
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }
  res.json({ iceServers: ice });
});

app.get('/adminsecretjay.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'adminsecretjay.html'));
});
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api/') || req.path.startsWith('/socket.io/') || req.path.startsWith('/uploads/')) return next();
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use('/api', (req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, _next) => {
  console.error('[error]', err.message);
  res.status(500).json({ error: 'Internal server error' });
});

setInterval(() => {
  for (const mode of ['video', 'text']) {
    const pool = pools[mode];
    for (const id of pool.queue) {
      if (!io.sockets.sockets.get(id)) pool.queue.delete(id);
    }
    for (const [a, b] of pool.matches) {
      if (!io.sockets.sockets.get(a) || !io.sockets.sockets.get(b)) {
        pool.matches.delete(a); pool.matches.delete(b);
      }
    }
  }
}, 30 * 1000).unref();
setInterval(broadcastStats, 15 * 1000).unref();

server.listen(PORT, () => {
  console.log(`\n🎥 Stranger Cam running at http://localhost:${PORT}`);
  console.log(`   💬 Text chat:   http://localhost:${PORT}/messaging.html`);
  console.log(`   🔐 Admin panel: http://localhost:${PORT}/adminsecretjay.html`);
  console.log(`   Admin key: ${ADMIN_KEY === 'change-me-secret-key' ? '⚠️  USING DEFAULT — set ADMIN_KEY in .env!' : '(configured)'}\n`);
});

function shutdown(signal) {
  console.log(`\n${signal}, shutting down…`);
  io.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 8000).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
