'use strict';

require('dotenv').config();

const path = require('path');
const http = require('http');
const express = require('express');
const rateLimit = require('express-rate-limit');
const { Server } = require('socket.io');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';
const TRUST_PROXY = process.env.TRUST_PROXY === '1';

const app = express();
app.disable('x-powered-by');
if (TRUST_PROXY) app.set('trust proxy', 1);

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
    methods: ['GET', 'POST']
  },
  maxHttpBufferSize: 1e6,
  pingTimeout: 25000,
  pingInterval: 20000
});

app.use(express.json({ limit: '16kb' }));
app.use(express.static(path.join(__dirname, 'public'), { maxAge: NODE_ENV === 'production' ? '1h' : 0 }));

const apiLimiter = rateLimit({
  windowMs: 60 * 1000, max: 120,
  standardHeaders: true, legacyHeaders: false
});
const reportLimiter = rateLimit({
  windowMs: 10 * 60 * 1000, max: 20,
  standardHeaders: true, legacyHeaders: false,
  message: { error: 'Too many reports. Please try again later.' }
});
app.use('/api/', apiLimiter);

// ---------- In-memory state ----------
const waitingQueue = new Set();
const activeMatches = new Map();
const sessions = new Map();
const recentReports = [];
const MAX_REPORTS = 1000;

const SIGNAL_WINDOW_MS = 1000;
const SIGNAL_MAX_PER_WINDOW = 60;

// ▼ NEW: chat rate limit (separate from signaling)
const CHAT_WINDOW_MS = 3000;
const CHAT_MAX_PER_WINDOW = 12;
const CHAT_MAX_LENGTH = 500;

function now() { return Date.now(); }

function sanitizeString(s, max = 500) {
  if (typeof s !== 'string') return '';
  return s.replace(/[\u0000-\u001F\u007F]/g, '').slice(0, max).trim();
}

function isMatched(socketId) { return activeMatches.has(socketId); }
function getPeer(socketId) { return activeMatches.get(socketId) || null; }
function removeFromQueue(socketId) { return waitingQueue.delete(socketId); }

function clearMatch(socketId, notifyPeer = true) {
  const peerId = activeMatches.get(socketId);
  if (!peerId) return null;
  activeMatches.delete(socketId);
  activeMatches.delete(peerId);
  if (notifyPeer) {
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket) peerSocket.emit('peer_left');
  }
  return peerId;
}

function broadcastStats() {
  io.emit('stats_update', {
    online: io.engine.clientsCount,
    waiting: waitingQueue.size,
    inChat: activeMatches.size / 2
  });
}

function tryMatch() {
  if (waitingQueue.size < 2) return;
  const ids = Array.from(waitingQueue);
  while (ids.length >= 2) {
    const a = ids.shift();
    const b = ids.shift();
    if (!a || !b || a === b) continue;
    const socketA = io.sockets.sockets.get(a);
    const socketB = io.sockets.sockets.get(b);
    if (!socketA || !socketB) {
      waitingQueue.delete(a); waitingQueue.delete(b);
      continue;
    }
    if (isMatched(a) || isMatched(b)) continue;

    waitingQueue.delete(a);
    waitingQueue.delete(b);
    activeMatches.set(a, b);
    activeMatches.set(b, a);

    const initiator = a < b ? a : b;
    const other = initiator === a ? b : a;

    io.sockets.sockets.get(initiator).emit('match_found', { peerId: other, initiator: true });
    io.sockets.sockets.get(other).emit('match_found', { peerId: initiator, initiator: false });

    broadcastStats();
  }
}

io.use((socket, next) => {
  socket.data.signalTimestamps = [];
  socket.data.chatTimestamps = [];        // ▼ NEW
  socket.data.reportCount = 0;
  sessions.set(socket.id, {
    joinedAt: now(),
    ua: sanitizeString(socket.handshake.headers['user-agent'] || '', 300),
    ip: socket.handshake.address
  });
  next();
});

function rateLimitSignal(socket) {
  const t = now();
  const arr = socket.data.signalTimestamps;
  while (arr.length && t - arr[0] > SIGNAL_WINDOW_MS) arr.shift();
  if (arr.length >= SIGNAL_MAX_PER_WINDOW) return false;
  arr.push(t);
  return true;
}

// ▼ NEW: chat rate limiter
function rateLimitChat(socket) {
  const t = now();
  const arr = socket.data.chatTimestamps;
  while (arr.length && t - arr[0] > CHAT_WINDOW_MS) arr.shift();
  if (arr.length >= CHAT_MAX_PER_WINDOW) return false;
  arr.push(t);
  return true;
}

io.on('connection', (socket) => {
  broadcastStats();

  socket.on('join_queue', () => {
    if (isMatched(socket.id)) {
      socket.emit('error_message', { message: 'You are already in a chat.' });
      return;
    }
    if (waitingQueue.has(socket.id)) {
      socket.emit('queue_joined', { position: waitingQueue.size });
      return;
    }
    waitingQueue.add(socket.id);
    socket.emit('queue_joined', { position: waitingQueue.size });
    broadcastStats();
    tryMatch();
  });

  socket.on('next_stranger', () => {
    const peerId = clearMatch(socket.id, true);
    if (peerId) {
      const peerSocket = io.sockets.sockets.get(peerId);
      if (peerSocket) peerSocket.emit('chat_ended');
    }
    removeFromQueue(socket.id);
    if (!waitingQueue.has(socket.id)) waitingQueue.add(socket.id);
    socket.emit('queue_joined', { position: waitingQueue.size });
    broadcastStats();
    tryMatch();
  });

  socket.on('leave_chat', () => {
    const peerId = clearMatch(socket.id, true);
    removeFromQueue(socket.id);
    if (peerId) {
      const peerSocket = io.sockets.sockets.get(peerId);
      if (peerSocket) peerSocket.emit('chat_ended');
    }
    socket.emit('queue_left');
    broadcastStats();
  });

  // ---- WebRTC signaling ----
  socket.on('webrtc_offer', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    const sdp = payload && payload.sdp;
    if (typeof to !== 'string' || !sdp || typeof sdp !== 'object') return;
    if (getPeer(socket.id) !== to) {
      socket.emit('error_message', { message: 'Not matched with that peer.' });
      return;
    }
    const target = io.sockets.sockets.get(to);
    if (!target) return;
    target.emit('webrtc_offer', { from: socket.id, sdp });
  });

  socket.on('webrtc_answer', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    const sdp = payload && payload.sdp;
    if (typeof to !== 'string' || !sdp || typeof sdp !== 'object') return;
    if (getPeer(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (!target) return;
    target.emit('webrtc_answer', { from: socket.id, sdp });
  });

  socket.on('webrtc_ice_candidate', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    const candidate = payload && payload.candidate;
    if (typeof to !== 'string' || !candidate) return;
    if (getPeer(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (!target) return;
    target.emit('webrtc_ice_candidate', { from: socket.id, candidate });
  });

  // ▼ NEW: chat message relay
  socket.on('chat_message', (payload) => {
    if (!rateLimitChat(socket)) {
      socket.emit('error_message', { message: 'You are sending messages too fast.' });
      return;
    }
    const to = payload && payload.to;
    if (typeof to !== 'string' || getPeer(socket.id) !== to) {
      socket.emit('error_message', { message: 'Not matched with that peer.' });
      return;
    }
    const rawText = payload.text;
    const text = sanitizeString(rawText, CHAT_MAX_LENGTH);
    if (!text) return;

    const target = io.sockets.sockets.get(to);
    if (!target) return;

    target.emit('chat_message', {
      from: socket.id,
      text,
      ts: Date.now()
    });
  });

  // ▼ NEW: typing indicators (also rate limited)
  socket.on('typing', (payload) => {
    if (!rateLimitSignal(socket)) return;
    const to = payload && payload.to;
    if (typeof to !== 'string' || getPeer(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (!target) return;
    target.emit('typing', { from: socket.id });
  });

  socket.on('stop_typing', (payload) => {
    const to = payload && payload.to;
    if (typeof to !== 'string' || getPeer(socket.id) !== to) return;
    const target = io.sockets.sockets.get(to);
    if (!target) return;
    target.emit('stop_typing', { from: socket.id });
  });

  socket.on('block_peer', () => {
    const peerId = getPeer(socket.id);
    if (!peerId) return;
    clearMatch(socket.id, false);
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket) peerSocket.emit('chat_ended');
    socket.emit('blocked');
    broadcastStats();
  });

  socket.on('disconnect', () => {
    removeFromQueue(socket.id);
    const peerId = clearMatch(socket.id, true);
    if (peerId) {
      const peerSocket = io.sockets.sockets.get(peerId);
      if (peerSocket) peerSocket.emit('peer_left');
    }
    sessions.delete(socket.id);
    broadcastStats();
  });
});

// ---------- REST API ----------
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok', uptime: process.uptime(), node: process.version,
    env: NODE_ENV, timestamp: new Date().toISOString()
  });
});

app.get('/api/stats', (req, res) => {
  res.json({
    online: io.engine.clientsCount,
    waiting: waitingQueue.size,
    inChat: activeMatches.size / 2,
    uptimeSeconds: Math.floor(process.uptime())
  });
});

app.post('/api/report', reportLimiter, (req, res) => {
  const body = req.body || {};
  const reason = sanitizeString(body.reason, 40);
  const details = sanitizeString(body.details, 500);
  const peerSocketId = sanitizeString(body.peerSocketId, 100);
  const validReasons = new Set(['nudity','harassment','hate','violence','spam','minor','other']);
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
    persistReportToSupabase(report).catch((e) => console.error('[report] supabase:', e.message));
  }
  return res.json({ ok: true, id: report.id });
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
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`Supabase ${resp.status}: ${text.slice(0, 200)}`);
  }
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

app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api/') || req.path.startsWith('/socket.io/')) return next();
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});
app.use('/api', (req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, _next) => {
  console.error('[error]', err.message);
  res.status(500).json({ error: 'Internal server error' });
});

setInterval(() => {
  for (const id of waitingQueue) {
    if (!io.sockets.sockets.get(id)) waitingQueue.delete(id);
  }
  for (const [a, b] of activeMatches) {
    if (!io.sockets.sockets.get(a) || !io.sockets.sockets.get(b)) {
      activeMatches.delete(a); activeMatches.delete(b);
    }
  }
}, 30 * 1000).unref();
setInterval(broadcastStats, 15 * 1000).unref();

server.listen(PORT, () => {
  console.log(`\n🎥 Stranger Cam running at http://localhost:${PORT}`);
  console.log(`   env=${NODE_ENV}  pid=${process.pid}`);
  console.log('   Queue is in-memory: resets on restart, single-instance only.\n');
});

function shutdown(signal) {
  console.log(`\n${signal} received, shutting down…`);
  io.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 8000).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
