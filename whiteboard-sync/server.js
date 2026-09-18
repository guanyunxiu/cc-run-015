'use strict';

/* ===========================================================================
 * 协作白板 v2 - 服务端（CRDT 协作内核）
 *
 * 与 v1 的本质区别：
 *  - 服务端 seq 只用于日志排序 / 观测，不再承担冲突仲裁；
 *  - 冲突由 LWW-Register CRDT（lamport + clientId）在各端确定性折叠；
 *  - 每条信封携带 clientId / lamport / clock（依赖向量）；
 *  - CausalBuffer 做 happens-before 因果投递，整事务（txnId）原子广播；
 *  - 支持选择性撤销（逆操作信封）、操作压缩（squashKey + 快照水位）。
 *
 * 消息：
 *  C→S  join {roomId,userId,lastSeq,since:knownVC?}
 *       ops  {envelopes:[...]}            // 单条或事务原子组
 *       ping
 *  S→C  joined / snapshot / delta / ack / error / pong
 *       ops  {envelopes:[...], fromClientId}
 * =========================================================================== */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const WB = require('./public/kernel.js');

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');

const HEARTBEAT_INTERVAL_MS = 15000;
const CLIENT_TIMEOUT_MS = 45000;
const MAX_ENVELOPES = 20000;          // 每房间信封硬上限（超出触发压缩 + 快照水位）
const COMPACT_AT = 300;               // 超过该条数触发一次压缩（移动/缩放类日志占大头）
const MAX_POINTS_PER_OP = 20000;
const MAX_MSG_BYTES = 2 * 1024 * 1024;

const VALID_KINDS = new Set([
  'create', 'set', 'delete', 'restore', 'group', 'ungroup', 'layer', 'erase'
]);

/**
 * rooms: Map<roomId, {
 *   clients: Set<client>,
 *   log: env[],                 // 全量信封（按到达 seq 升序，seq 仅排序用）
 *   seq: number,
 *   doc: WB.Doc,                // 服务端权威物化（供快照/观测）
 *   applied: Set<envId>,        // 幂等
 *   watermark: number|null,     // 已压缩进快照的信封数（log 中该下标之前已被折叠）
 *   knownVC: Object             // 跨所有已应用信封合并的版本向量（快照基线）
 * }>
 */
const rooms = new Map();

function getOrCreateRoom(roomId) {
  let room = rooms.get(roomId);
  if (!room) {
    room = {
      clients: new Set(),
      log: [],
      seq: 0,
      doc: new WB.Doc(),
      buf: new WB.CausalBuffer(),
      applied: new Set(),
      watermark: null,
      knownVC: Object.create(null)
    };
    rooms.set(roomId, room);
    console.log(`[room] created: ${roomId}`);
  }
  return room;
}

function sendJSON(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (_) { /* noop */ }
  }
}
const nowTs = () => Date.now();

/* --------------------------- 信封校验 --------------------------- */

function isFiniteNum(v) { return typeof v === 'number' && Number.isFinite(v); }
function isVC(v) {
  if (!v || typeof v !== 'object') return false;
  for (const k of Object.keys(v)) {
    if (typeof k !== 'string' || k.length > 64) return false;
    if (!Number.isInteger(v[k]) || v[k] < 0 || v[k] > 1e9) return false;
  }
  return true;
}

function validateEnvelope(env) {
  if (!env || typeof env !== 'object') return 'bad envelope';
  if (typeof env.id !== 'string' || !/^[\w.:-]{1,100}$/.test(env.id)) return 'bad id';
  if (typeof env.clientId !== 'string' || !env.clientId || env.clientId.length > 64) return 'bad clientId';
  if (!Number.isInteger(env.lamport) || env.lamport < 0) return 'bad lamport';
  if (!isVC(env.clock)) return 'bad clock';
  // 信封自洽：clock 中必须声明自己的本地计数
  if (!Number.isInteger(env.clock[env.clientId]) || env.clock[env.clientId] <= 0) return 'clock missing self';
  const op = env.op;
  if (!op || typeof op !== 'object' || !VALID_KINDS.has(op.kind)) return 'bad op kind';
  if (op.kind === 'create') {
    if (!Array.isArray(op.objects) || op.objects.length === 0 || op.objects.length > 500) return 'bad objects';
    for (const o of op.objects) {
      if (!o || typeof o.oid !== 'string' || !o.oid || typeof o.type !== 'string') return 'bad object';
      if (o.fields && typeof o.fields !== 'object') return 'bad fields';
    }
  }
  if (op.kind === 'set') {
    if (typeof op.oid !== 'string' || !op.oid) return 'bad oid';
    if (!op.fields || typeof op.fields !== 'object') return 'bad set fields';
  }
  if (op.kind === 'delete' || op.kind === 'restore') {
    if (!Array.isArray(op.oids) || op.oids.length === 0 || op.oids.length > 1000) return 'bad oids';
  }
  if (op.kind === 'group' || op.kind === 'ungroup') {
    if (typeof op.gid !== 'string' || !Array.isArray(op.oids) || op.oids.length === 0) return 'bad group';
  }
  if (op.kind === 'layer') {
    if (typeof op.oid !== 'string' || typeof op.z !== 'string') return 'bad layer';
  }
  if (op.kind === 'erase') {
    if (!Array.isArray(op.chunks) || op.chunks.length === 0 || op.chunks.length > 256) return 'bad chunks';
    for (const ch of op.chunks) {
      if (!ch || typeof ch.oid !== 'string' || !Number.isInteger(ch.tx) || !Number.isInteger(ch.ty)) return 'bad chunk';
      if (!Array.isArray(ch.cells) || ch.cells.some((c) => !Array.isArray(c) || c.length !== 2)) return 'bad cells';
    }
  }
  if (env.txnId != null && (typeof env.txnId !== 'string' || env.txnId.length > 80)) return 'bad txnId';
  if (env.squashKey != null && (typeof env.squashKey !== 'string' || env.squashKey.length > 120)) return 'bad squashKey';
  return null;
}

/* --------------------------- 房间逻辑 --------------------------- */

function mergeVCInto(room, clock) {
  for (const k of Object.keys(clock || {})) {
    const v = clock[k] | 0;
    if (v > (room.knownVC[k] | 0)) room.knownVC[k] = v;
  }
}

/**
 * 应用一批信封（同一发送者、可能是事务原子组）。
 * 服务端同样是一个 CRDT 副本：信封先过 CausalBuffer（保证物化时 create 先于 set，
 * 快照永远建立在因果一致的状态上），就绪信封按因果顺序 apply、分配仅用于日志排序的
 * seq，再在同一帧广播给其他成员；事务成员在就绪序列中相邻，随同一条 ops 消息原子下发。
 * 冲突仲裁仍完全由 LWW CRDT 完成，seq 不参与。
 */
function ingestBatch(room, client, envelopes) {
  const ids = [];
  let buffered = 0;
  for (const env of envelopes) {
    ids.push(env.id);
    if (room.applied.has(env.id)) continue;
    room.applied.add(env.id);
    room.buf.enqueue(env); // 整批先入队（事务成员收齐），再一次性冲刷，保证原子广播
    buffered += 1;
  }
  void buffered;

  // 冲刷所有满足因果（可能跨多个发送者）的就绪信封
  const fresh = drainReady(room);

  if (fresh.length) {
    maybeCompact(room);
    const payload = { type: 'ops', envelopes: fresh };
    for (const other of room.clients) {
      if (other !== client) sendJSON(other.ws, payload);
    }
    const txnIds = new Set(fresh.map((e) => e.txnId).filter(Boolean));
    console.log(`[ops] seq~${room.seq} room=${client.roomId} user=${client.userId} ` +
      `n=${fresh.length}${txnIds.size ? ` txns=${txnIds.size}` : ''} log=${room.log.length} ` +
      `pending=${room.buf.pendingCount} broadcast=${room.clients.size - 1}`);
  }
  return ids;
}

/** 冲刷因果缓冲：就绪信封物化到权威 Doc、分配仅用于日志排序的 seq */
function drainReady(room) {
  return room.buf.drain((env) => {
    room.seq += 1;
    env.seq = room.seq;
    env.serverTs = nowTs();
    room.log.push(env);
    room.doc.apply(env);
    mergeVCInto(room, env.clock);
  });
}

/**
 * 日志压缩：squashKey 相同的信封只留最终一条；
 * 被压缩掉的信封折叠进“快照水位”，新加入/水位之后的重连者走 snapshot，不再需要它们。
 * 水位之前的信封从 log 移除，但 applied 集合保留 id，防止重发重复入库。
 */
function maybeCompact(room) {
  if (room.log.length < COMPACT_AT && room.log.length < MAX_ENVELOPES) return;
  const before = room.log.length;
  const compact = WB.squash(room.log);
  // squash 只去掉被更高 lamport 同 squashKey 覆盖的信封；
  // 被去掉的信封不再需要下发给任何人（它们的最终效果已在保留信封里），
  // 但为了让“晚加入者”仍能重建，把压缩时刻的物化结果作为水位快照基线。
  if (compact.length < before) {
    room.log = compact;
  }
  // 无论 squash 是否减少，超过硬上限都做快照水位裁剪：折叠最旧信封
  if (room.log.length >= MAX_ENVELOPES) {
    const cut = Math.floor(room.log.length / 2);
    room.watermark = (room.watermark || 0) + cut;
    room.log.splice(0, cut);
  }
}

function handleJoin(client, msg) {
  const roomId = String(msg.roomId || '').trim();
  if (!roomId || roomId.length > 64) {
    sendJSON(client.ws, { type: 'error', message: 'invalid roomId' });
    return;
  }
  leaveRoom(client);

  const userId = String(msg.userId || '').slice(0, 64) || 'anon-' + client.id.slice(0, 4);
  client.userId = userId;
  client.roomId = roomId;

  const room = getOrCreateRoom(roomId);
  room.clients.add(client);

  // 发送当前快照 + 水位之后的增量信封。
  // since（客户端已知的版本向量）若覆盖快照基线，可只发 delta；否则全量 snapshot。
  const snapshot = room.doc.snapshot(room.knownVC);
  const lastSeq = room.seq;

  sendJSON(client.ws, { type: 'joined', roomId, userId, lastSeq });
  sendJSON(client.ws, {
    type: 'snapshot',
    watermark: room.watermark || 0,
    lastSeq,
    snapshot,
    envelopes: room.log.slice() // 快照之后仍在日志中的信封（客户端按 id 幂等折叠）
  });

  console.log(`[join] room=${roomId} user=${userId} members=${room.clients.size} ` +
    `objs=${snapshot.objects.length} log=${room.log.length}${room.watermark != null ? ` wm=${room.watermark}` : ''}`);
}

function leaveRoom(client) {
  if (!client.roomId) return;
  const room = rooms.get(client.roomId);
  if (room) room.clients.delete(client);
  client.roomId = null;
}

function handleOps(client, msg, rawSize) {
  if (!client.roomId) { sendJSON(client.ws, { type: 'error', message: 'join a room first' }); return; }
  if (rawSize > MAX_MSG_BYTES) { sendJSON(client.ws, { type: 'error', message: 'message too large' }); return; }
  const list = Array.isArray(msg.envelopes) ? msg.envelopes : null;
  if (!list || list.length === 0 || list.length > 1000) {
    sendJSON(client.ws, { type: 'error', message: 'envelopes required' });
    return;
  }
  // 事务原子性：一批信封若声明同一 txnId，必须整组一起被接受/拒绝/广播
  const txnIds = new Set(list.map((e) => e && e.txnId).filter(Boolean));
  if (txnIds.size > 1) { sendJSON(client.ws, { type: 'error', message: 'mixed txnId in one batch' }); return; }

  // 全部信封必须属于同一发送者（不能代发他人操作）
  for (const env of list) {
    const err = validateEnvelope(env);
    if (err) { sendJSON(client.ws, { type: 'error', message: err, envId: env && env.id }); return; }
    if (env.clientId !== client.userId) {
      sendJSON(client.ws, { type: 'error', message: 'clientId mismatch', envId: env.id });
      return;
    }
    // 粗粒度体积护栏：单笔点数上限
    const pts = env.op && env.op.objects && env.op.objects[0] &&
      env.op.objects[0].fields && env.op.objects[0].fields.stroke &&
      env.op.objects[0].fields.stroke.points;
    if (pts && pts.length > MAX_POINTS_PER_OP) {
      sendJSON(client.ws, { type: 'error', message: 'too many points', envId: env.id });
      return;
    }
  }

  const ids = ingestBatch(rooms.get(client.roomId), client, list);
  sendJSON(client.ws, { type: 'ack', ids, lastSeq: room_lastSeq(client.roomId) });
}
function room_lastSeq(roomId) { const r = rooms.get(roomId); return r ? r.seq : 0; }

/* ----------------------------- HTTP ----------------------------- */

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml'
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (url.pathname === '/api/room') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    const snap = room.doc.snapshot(room.knownVC);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      roomId: url.searchParams.get('roomId'),
      members: room.clients.size,
      seq: room.seq,
      logLen: room.log.length,
      watermark: room.watermark,
      objectCount: snap.objects.length,
      liveCount: room.doc.liveObjects().length,
      knownVC: room.knownVC,
      log: room.log.map((e) => ({ seq: e.seq, id: e.id, clientId: e.clientId,
        lamport: e.lamport, kind: e.op && e.op.kind, txnId: e.txnId || null,
        squashKey: e.squashKey || null }))
    }));
    return;
  }

  // 手动触发一次日志压缩（测试/运维用）：squashKey 相同的连续操作折叠为最终状态
  if (url.pathname === '/api/compact') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    const before = room.log.length;
    room.log = WB.squash(room.log);
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({
      before, after: room.log.length
    }));
    return;
  }

  if (url.pathname === '/api/rooms') {
    const summary = [];
    for (const [roomId, room] of rooms) {
      summary.push({
        roomId, members: room.clients.size, seq: room.seq,
        logLen: room.log.length, watermark: room.watermark,
        liveCount: room.doc.liveObjects().length
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(summary));
    return;
  }

  let pathname = decodeURIComponent(url.pathname);
  if (pathname === '/') pathname = '/index.html';
  const filePath = path.normalize(path.join(PUBLIC_DIR, pathname));
  if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403).end('Forbidden'); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404).end('Not Found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* --------------------------- WebSocket --------------------------- */

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const client = {
    id: crypto.randomBytes(6).toString('hex'),
    ws, userId: null, roomId: null, lastSeen: nowTs(), alive: true
  };

  ws.on('pong', () => { client.lastSeen = nowTs(); client.alive = true; });

  ws.on('message', (raw) => {
    client.lastSeen = nowTs(); client.alive = true;
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (_) { sendJSON(ws, { type: 'error', message: 'invalid json' }); return; }

    switch (msg.type) {
      case 'join': handleJoin(client, msg); break;
      case 'ops': handleOps(client, msg, raw.length); break;
      case 'ping': sendJSON(ws, { type: 'pong', ts: nowTs() }); break;
      default: sendJSON(ws, { type: 'error', message: `unknown type: ${msg.type}` });
    }
  });

  ws.on('close', () => { leaveRoom(client); });
  ws.on('error', (err) => {
    console.error('[ws error]', err.message);
    try { ws.terminate(); } catch (_) { /* noop */ }
    leaveRoom(client);
  });
});

const heartbeatTimer = setInterval(() => {
  const now = nowTs();
  for (const room of rooms.values()) {
    for (const client of room.clients) {
      if (client.ws.readyState !== client.ws.OPEN || now - client.lastSeen > CLIENT_TIMEOUT_MS) {
        try { client.ws.terminate(); } catch (_) { /* noop */ }
      } else {
        try { client.ws.ping(); } catch (_) { /* noop */ }
      }
    }
  }
}, HEARTBEAT_INTERVAL_MS);
wss.on('close', () => clearInterval(heartbeatTimer));

server.listen(PORT, HOST, () => {
  console.log('==============================================');
  console.log('  Collaborative Whiteboard v2 (CRDT kernel)');
  console.log(`  HTTP : http://localhost:${PORT}/`);
  console.log(`  WS   : ws://<host>:${PORT}/ws`);
  console.log(`  API  : http://localhost:${PORT}/api/rooms`);
  console.log('==============================================');
});

process.on('uncaughtException', (err) => console.error('[uncaughtException]', err));
