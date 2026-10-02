'use strict';

// End-to-end convergence and durability test suite.
//
// These tests assert STRUCTURAL convergence (identical Yjs state-vector
// binary hash across all clients and a fresh PostgreSQL-only rebuild) and
// DURABLE persistence boundaries (commit-before-ack, crash recovery,
// dedup, snapshot+tail recovery) — never just "both strings look equal".

const { spawn } = require('node:child_process');
const http = require('node:http');
const path = require('node:path');
const assert = require('node:assert/strict');
const Y = require('yjs');
const crypto = require('node:crypto');

const db = require('../src/db');
const yutil = require('../src/yutil');
const { DocClient } = require('../scripts/lib-client');
const { setupTest, createDoc } = require('./fixtures');

const PORT = 8791;
const WS_URL = `ws://127.0.0.1:${PORT}/ws`;
const BASE = `http://127.0.0.1:${PORT}`;
const SERVER = path.join(__dirname, '..', 'src', 'server.js');

const results = [];
function test(name, fn) { return { name, fn }; }

function withTimeout(p, ms, label) {
  return Promise.race([
    p,
    new Promise((_, reject) => setTimeout(
      () => reject(new Error(`TIMEOUT after ${ms}ms: ${label}`)), ms)),
  ]);
}

let serverProc = null;

async function waitHealthy(timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const body = await httpGet('/healthz');
      if (JSON.parse(body).ok) return;
    } catch { /* retry */ }
    await sleep(100);
  }
  throw new Error('server did not become healthy');
}

async function startServer({ crashAfterCommit = false } = {}) {
  await stopServer();
  const env = {
    ...process.env,
    PORT: String(PORT),
    HOST: '127.0.0.1',
    PGHOST: '/tmp',
    PGPORT: '55432',
    PGDATABASE: 'collab',
    PGUSER: 'collab',
    LOG_LEVEL: 'warn',
    CRASH_AFTER_COMMIT: crashAfterCommit ? '1' : '0',
  };
  serverProc = spawn(process.execPath, [SERVER], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  serverProc.stdout.on('data', (d) => process.env.VERBOSE && process.stdout.write(`[srv] ${d}`));
  serverProc.stderr.on('data', (d) => process.env.VERBOSE && process.stderr.write(`[srv!] ${d}`));
  await waitHealthy();
  return serverProc;
}

async function stopServer() {
  if (!serverProc) return null;
  if (serverProc.exitCode !== null) {
    const c = serverProc.exitCode;
    serverProc = null;
    return c;
  }
  const proc = serverProc;
  const codeP = new Promise((res) => proc.on('exit', (code) => res(code)));
  proc.kill('SIGTERM');
  const code = await Promise.race([
    codeP,
    new Promise((res) => setTimeout(() => res('timeout'), 3000)),
  ]);
  if (code === 'timeout') proc.kill('SIGKILL');
  if (serverProc === proc) serverProc = null;
  return code;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function httpGet(p, token) {
  return new Promise((resolve, reject) => {
    const req = http.get(`${BASE}${p}`, {
      headers: token ? { 'x-auth-token': token } : {},
    }, (res) => {
      let body = '';
      res.on('data', (d) => { body += d; });
      res.on('end', () => resolve(body));
    });
    req.on('error', reject);
  });
}

function httpPost(p, token, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body || {});
    const req = http.request(`${BASE}${p}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-auth-token': token,
        'content-length': Buffer.byteLength(data),
      },
    }, (res) => {
      let b = '';
      res.on('data', (d) => { b += d; });
      res.on('end', () => resolve({ status: res.statusCode, body: b }));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function recovered(docId, token = 'user-owner') {
  const j = JSON.parse(await httpGet(`/v1/docs/${docId}/recovered-state`, token));
  return j;
}

async function sqlCount(table, where = '', params = []) {
  const r = await db.query(`SELECT count(*)::int AS n FROM ${table} ${where}`, params);
  return r.rows[0].n;
}

async function rebuildAndHash(docId) {
  const doc = await require('../src/compaction').recoverFromStore(docId);
  return {
    hash: yutil.sha256(yutil.encodeState(doc)),
    text: doc.getText('content').toString(),
    sv: yutil.decodeStateVector(yutil.stateVector(doc)),
  };
}

function hashEqual(...clients) {
  const hashes = clients.map((c) => c.stateHash());
  return new Set(hashes).size === 1 ? hashes[0] : null;
}

async function settleConvergence(clients, docId, { rounds = 40 } = {}) {
  // Give the network room to drain, then compare structural state.
  for (let i = 0; i < rounds; i++) {
    await sleep(50);
    const h = hashEqual(...clients);
    if (h) {
      const r = await rebuildAndHash(docId);
      if (r.hash === h) return { hash: h, text: clients[0].text };
    }
  }
  const dump = clients.map((c, i) => ({ i, h: c.stateHash(), text: c.text }));
  throw new Error(`no convergence: ${JSON.stringify(dump, null, 2)}`);
}

// ---------------------------------------------------------------------------
// T1 concurrent inserts + deletes + overlapping edits converge
// ---------------------------------------------------------------------------
const t1 = test('T1 concurrent insert/delete across two writers converges structurally', async () => {
  await createDoc('t1', { writers: ['user-alice', 'user-bob'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't1' });
  const b = new DocClient({ url: WS_URL, token: 'user-bob', docId: 't1' });
  await Promise.all([a.connect(), b.connect()]);

  // Prime with distinctly marked regions so deletions are unambiguous.
  a.localEdit((t) => t.insert(0, 'AAA-BBB-CCC-DDD'));
  await a.flush();
  await sleep(100);

  // Concurrent round: both edits are generated from the same base state and
  // raced onto the wire. A deletes region AAA and appends; B inserts around
  // region BBB. Yjs merges them deterministically regardless of arrival.
  a.localEdit((t) => {
    const i = t.toString().indexOf('AAA');
    t.delete(i, 4); // remove 'AAA-'
    t.insert(t.length, 'TAIL-A');
  });
  b.localEdit((t) => {
    t.insert(0, 'B-START>');
    const i = t.toString().indexOf('CCC');
    t.insert(i, '[before-CCC]');
  });
  await Promise.all([a.flush({ concurrent: true }), b.flush({ concurrent: true })]);

  // Second concurrent round based on the merged state: overlapping appends.
  a.localEdit((t) => t.insert(t.length, '|a2'));
  b.localEdit((t) => t.insert(t.length, '|b2'));
  await Promise.all([a.flush({ concurrent: true }), b.flush({ concurrent: true })]);

  const conv = await settleConvergence([a, b], 't1');
  assert.ok(conv.hash, 'converged structural hash');
  // A's delete of 'AAA' is replicated everywhere (no last-write-wins loss);
  // both writers' inserts survive the merge.
  assert.ok(!conv.text.includes('AAA'), `A deletion must replicate, got ${conv.text}`);
  assert.ok(conv.text.includes('BBB') && conv.text.includes('CCC') && conv.text.includes('DDD'),
    `remaining regions must survive, got ${conv.text}`);
  assert.ok(conv.text.includes('[before-CCC]'));
  assert.ok(conv.text.includes('B-START>'));
  assert.ok(conv.text.includes('TAIL-A'));
  assert.ok(conv.text.includes('|a2'));
  assert.ok(conv.text.includes('|b2'));
  a.close(); b.close();
});

// ---------------------------------------------------------------------------
// T2 out-of-order arrival
// ---------------------------------------------------------------------------
const t2 = test('T2 updates delivered out of order by the network still converge', async () => {
  await createDoc('t2', { writers: ['user-alice'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't2' });
  await a.connect();

  // Build three dependent edits locally, then deliberately send them
  // reversed. A fresh second client must arrive at the same state via seq
  // replay from PostgreSQL.
  a.localEdit((t) => t.insert(0, 'one '));
  a.localEdit((t) => t.insert(t.length, 'two '));
  a.localEdit((t) => t.insert(t.length, 'three'));
  assert.equal(a.updateQueue.length, 3);
  const [u1, u2, u3] = a.updateQueue;
  a.updateQueue = [];
  // Reverse the wire order; server assigns seq in arrival order — Yjs is
  // order-independent because structs are causal, not positional.
  await a.sendUpdate(u3);
  await a.sendUpdate(u2);
  await a.sendUpdate(u1);

  const fresh = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't2' });
  await fresh.connect();
  const conv = await settleConvergence([a, fresh], 't2');
  assert.equal(conv.text, 'one two three');

  // seqs in the log are arrival order, but replay is seq ascending; both
  // paths must reproduce the identical document.
  const rows = (await db.query(
    `SELECT seq FROM doc_updates WHERE doc_id='t2' ORDER BY seq`,
  )).rows;
  assert.equal(rows.length, 3);
  a.close(); fresh.close();
});

// ---------------------------------------------------------------------------
// T3 duplicate frame dedup
// ---------------------------------------------------------------------------
const t3 = test('T3 duplicate client_msg_id is persisted once and acked as duplicate', async () => {
  await createDoc('t3', { writers: ['user-alice', 'user-bob'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't3' });
  const b = new DocClient({ url: WS_URL, token: 'user-bob', docId: 't3' });
  await Promise.all([a.connect(), b.connect()]);

  const u = a.localEdit((t) => t.insert(0, 'DEDUP'));
  const msgId = crypto.randomBytes(8).toString('hex');
  const ack1 = await a.sendUpdate(u, 5000, msgId);
  assert.equal(ack1.ok, true);
  assert.equal(ack1.duplicated, false);
  const seq = ack1.seq;

  // Exact replay of the same frame (same msgId, same bytes).
  const ackColl = collect(a, 'ack');
  a.resendRaw(msgId, u);
  const ack2 = await ackColl.wait((m) => m.msgId === msgId);
  ackColl.stop();
  assert.equal(ack2.ok, true);
  assert.equal(ack2.duplicated, true);
  assert.equal(ack2.seq, seq);

  // Even a *different* socket (reconnect) replaying the same id dedups.
  a.hardClose();
  await sleep(100);
  const a2 = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't3' });
  await a2.connect();
  const ackColl2 = collect(a2, 'ack');
  a2.resendRaw(msgId, u);
  const ack3 = await ackColl2.wait((m) => m.msgId === msgId);
  ackColl2.stop();
  assert.equal(ack3.duplicated, true);
  assert.equal(ack3.seq, seq);

  await sleep(100);
  const n = await sqlCount('doc_updates', `WHERE doc_id='t3'`);
  assert.equal(n, 1, 'exactly one persisted row');

  const conv = await settleConvergence([a2, b], 't3');
  assert.equal(conv.text, 'DEDUP');
  a2.close(); b.close();
});

// ---------------------------------------------------------------------------
// T4 persistence boundary: crash AFTER commit, BEFORE ack
// ---------------------------------------------------------------------------
const t4 = test('T4 crash after durable commit: retry dedups, restarted room replays', async () => {
  await startServer({ crashAfterCommit: true });
  await createDoc('t4', { writers: ['user-alice', 'user-bob'] });

  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't4' });
  await a.connect();
  const u = a.localEdit((t) => t.insert(0, 'CRASH-BOUNDARY'));
  const msgId = crypto.randomBytes(8).toString('hex');

  // The process exits(17) after COMMIT. Observe BOTH the client side
  // (no ack, socket dies) and the server side (explicit exit code).
  const exitP = new Promise((res) => serverProc.on('exit', (code) => res(code)));
  let sendErr = null;
  const sendP = a.sendUpdate(u, 3000, msgId).catch((e) => { sendErr = e; });
  const exitCode = await Promise.race([
    exitP,
    new Promise((res) => setTimeout(() => res(null), 5000)),
  ]);
  await sendP;
  assert.equal(String(exitCode), '17', `server must hard-exit 17 after commit, got ${exitCode}`);
  assert.ok(sendErr, 'client must observe failure (no ack)');

  // Durable BEFORE the crash: row exists with a seq.
  const rows = (await db.query(
    `SELECT seq, client_msg_id FROM doc_updates WHERE doc_id='t4' ORDER BY seq`,
  )).rows;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].client_msg_id, msgId);

  // Restart without the crash hook. The room is rebuilt from PostgreSQL.
  await startServer({ crashAfterCommit: false });

  const a2 = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't4' });
  await a2.connect();
  // A's local doc already contains the edit; the hello state must not
  // re-introduce a duplicate (idempotent Yjs merge).
  assert.equal(a2.text, 'CRASH-BOUNDARY');

  // Retry the SAME msgId: dedup ack, no second row.
  const ack = await a2.sendUpdate(u, 5000, msgId);
  assert.equal(ack.ok, true);
  assert.equal(ack.duplicated, true);
  assert.equal(Number(ack.seq), Number(rows[0].seq));

  // A genuinely new edit flows after recovery, and a fresh client converges.
  a2.localEdit((t) => t.insert(t.length, '+after'));
  await a2.flush();
  const fresh = new DocClient({ url: WS_URL, token: 'user-bob', docId: 't4' });
  await fresh.connect();
  const conv = await settleConvergence([a2, fresh], 't4');
  assert.equal(conv.text, 'CRASH-BOUNDARY+after');
  assert.equal(await sqlCount('doc_updates', `WHERE doc_id='t4'`), 2);
  a2.close(); fresh.close();
});

// ---------------------------------------------------------------------------
// T5 offline gap repair by state vector
// ---------------------------------------------------------------------------
const t5 = test('T5 disconnected client catches up exactly the missing diff by state vector', async () => {
  await createDoc('t5', { writers: ['user-alice', 'user-bob'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't5' });
  const b = new DocClient({ url: WS_URL, token: 'user-bob', docId: 't5' });
  await Promise.all([a.connect(), b.connect()]);
  a.localEdit((t) => t.insert(0, 'base'));
  await a.flush();
  await sleep(100);

  // B goes offline (hard disconnect). A keeps mutating.
  b.hardClose();
  await sleep(150);
  for (let i = 0; i < 5; i++) {
    a.localEdit((t) => t.insert(t.length, `#${i}`));
    await a.flush();
  }
  await sleep(100);

  // Reconnect the SAME document identity presenting its state vector: the
  // server returns only missing structs (the diff), not a forced full
  // snapshot. A wrong-vector scenario (fresh doc identity, stale vector) is
  // intentionally NOT used here: diffs are keyed by Yjs client ids.
  const staleSv = Y.encodeStateVector(b.doc);
  await b.reconnectWithStateVector();
  assert.equal(b.text, a.text);

  const diffStale = await b.requestSync(staleSv);
  const staleBytes = Buffer.from(diffStale.update, 'base64');
  assert.ok(staleBytes.length > 2, 'stale SV must produce a non-empty diff');

  const currentSv = Y.encodeStateVector(b.doc);
  const diffCurrent = await b.requestSync(currentSv);
  assert.equal(Buffer.from(diffCurrent.update, 'base64').length, 2,
    'up-to-date SV must produce an empty diff (2-byte empty update)');

  // Re-applying a stale diff is idempotent and changes nothing.
  const before = b.stateHash();
  Y.applyUpdate(b.doc, new Uint8Array(staleBytes), 're-diff');
  assert.equal(b.stateHash(), before);

  // A brand-new client joining with an empty SV gets full state and
  // converges structurally with both.
  const fresh = new DocClient({ url: WS_URL, token: 'user-bob', docId: 't5' });
  await fresh.connect();
  const conv = await settleConvergence([a, b, fresh], 't5');
  assert.match(conv.text, /base#0#1#2#3#4/);
  a.close(); b.close(); fresh.close();
});

// ---------------------------------------------------------------------------
// T6 compaction preserves recoverability, including delete-folded path
// ---------------------------------------------------------------------------
const t6 = test('T6 snapshot compaction: snapshot+tail and rebuild both recover identical doc', async () => {
  await createDoc('t6', { writers: ['user-alice'], owners: ['user-owner'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't6' });
  await a.connect();
  for (let i = 0; i < 12; i++) {
    a.localEdit((t) => t.insert(t.length, `chunk${i};`));
    await a.flush();
  }
  await sleep(100);
  const before = await rebuildAndHash('t6');

  // Compact (fold updates into a snapshot; rows retained first).
  const r1 = await httpPost('/v1/docs/t6/compact', 'user-owner', { minUpdates: 1 });
  assert.equal(r1.status, 200);
  const c1 = JSON.parse(r1.body);
  assert.equal(c1.compacted, true);
  const after1 = await rebuildAndHash('t6');
  assert.equal(after1.hash, before.hash, 'snapshot+tail replay == full log replay');

  const snap = (await db.query(
    `SELECT state_hash, through_seq, update_count FROM doc_snapshots WHERE doc_id='t6' ORDER BY id DESC LIMIT 1`,
  )).rows[0];
  assert.equal(snap.state_hash, before.hash);
  assert.equal(Number(snap.update_count), 12);

  // More updates land after compaction; compact again, then DELETE folded
  // rows: recovery must still be byte-identical.
  for (let i = 0; i < 5; i++) {
    a.localEdit((t) => t.insert(t.length, `post${i};`));
    await a.flush();
  }
  await sleep(100);
  const before2 = await rebuildAndHash('t6');
  const r2 = await httpPost('/v1/docs/t6/compact', 'user-owner',
    { minUpdates: 1, deleteFolded: true });
  assert.equal(r2.status, 200);
  const c2 = JSON.parse(r2.body);
  assert.equal(c2.compacted, true);
  assert.equal(c2.deleted, true);
  const after2 = await rebuildAndHash('t6');
  assert.equal(after2.hash, before2.hash, 'recovery after folded-row deletion');

  // A brand-new client joining now loads snapshot+tail from storage and
  // converges structurally.
  const fresh = new DocClient({ url: WS_URL, token: 'user-owner', docId: 't6' });
  await fresh.connect();
  const conv = await settleConvergence([a, fresh], 't6');
  assert.ok(conv.text.includes('chunk0;') && conv.text.includes('post4;'));
  a.close(); fresh.close();
});

// ---------------------------------------------------------------------------
// T7 corrupt / unknown frames land in locatable error storage
// ---------------------------------------------------------------------------
const t7 = test('T7 malformed, corrupt and unknown frames are rejected and logged, not applied', async () => {
  await createDoc('t7', { writers: ['user-alice'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't7' });
  await a.connect();

  const before = await sqlCount('doc_updates', `WHERE doc_id='t7'`);

  const errs = collect(a, 'error');
  const acks = collect(a, 'ack');

  // Register frame waiters BEFORE sending so no response can race the send.
  // 1. Not JSON.
  a.sendRaw('this-is-not-json{');
  // 2. Valid JSON, unknown frame type.
  a.sendRaw(JSON.stringify({ type: 'teleport', weird: 1 }));
  // 3. Update with bytes that are structurally not a Yjs update.
  a.sendRaw(JSON.stringify({ type: 'update', msgId: 'corrupt-1',
    update: Buffer.from([0xde, 0xad, 0xbe, 0xef, 0, 1, 2, 99]).toString('base64') }));
  // 4. Update with empty payload.
  a.sendRaw(JSON.stringify({ type: 'update', msgId: 'corrupt-2', update: '' }));

  const e1 = await errs.wait((m) => m.code === 'BAD_JSON');
  const e2 = await errs.wait((m) => m.code === 'UNKNOWN_TYPE');
  const n3 = await acks.wait((m) => m.msgId === 'corrupt-1');
  const n4 = await acks.wait((m) => m.msgId === 'corrupt-2');
  assert.equal(e1.code, 'BAD_JSON');
  assert.equal(e2.code, 'UNKNOWN_TYPE');
  assert.equal(n3.ok, false);
  assert.equal(n3.code, 'CORRUPT_UPDATE', `expected CORRUPT_UPDATE got ${n3.code}`);
  assert.equal(n4.ok, false);
  assert.equal(n4.code, 'BAD_ENVELOPE');
  errs.stop(); acks.stop();

  // A good update after the garbage still works; corrupt frames never apply.
  a.localEdit((t) => t.insert(0, 'still-works'));
  await a.flush();
  const after = await sqlCount('doc_updates', `WHERE doc_id='t7'`);
  assert.equal(after - before, 1, 'only the valid update persisted');

  const errRows = (await db.query(
    `SELECT error_code, raw_prefix_hex IS NOT NULL AS has_raw, doc_id, user_id
       FROM update_errors ORDER BY id`,
  )).rows;
  const codes = errRows.map((e) => e.error_code);
  assert.ok(codes.includes('BAD_JSON'));
  assert.ok(codes.includes('UNKNOWN_TYPE'));
  assert.ok(codes.includes('CORRUPT_UPDATE'));
  assert.ok(codes.includes('BAD_ENVELOPE'));
  // Locatable: each row carries doc/user context or raw hex for pre-auth.
  assert.ok(errRows.every((e) => e.has_raw || e.doc_id));
  a.close();
});

// ---------------------------------------------------------------------------
// T8 tenant + document authorization on connect and on every update
// ---------------------------------------------------------------------------
const t8 = test('T8 room id is never trusted: cross-tenant, non-member, reader, revoked all denied', async () => {
  await createDoc('t8', { writers: ['user-alice'], readers: ['user-carol'] });
  // user-dave belongs to another tenant; user-nobody is a valid ACME user
  // with no membership in t8.

  // Non-member from same tenant: connect refused.
  const nobody = new DocClient({ url: WS_URL, token: 'user-nobody', docId: 't8' });
  await assert.rejects(nobody.connect(), /FORBIDDEN/);

  // Cross-tenant: Dave (Globex) claiming an ACME room id.
  const dave = new DocClient({ url: WS_URL, token: 'user-dave', docId: 't8' });
  await assert.rejects(dave.connect(), /FORBIDDEN/);

  // Unknown token.
  const ghost = new DocClient({ url: WS_URL, token: 'no-such-user', docId: 't8' });
  await assert.rejects(ghost.connect(), /BAD_TOKEN/);

  // Reader may connect/sync but every write is denied.
  const carol = new DocClient({ url: WS_URL, token: 'user-carol', docId: 't8' });
  await carol.connect();
  assert.equal(carol.role, 'reader');
  const u = carol.localEdit((t) => t.insert(0, 'reader-write'));
  await assert.rejects(carol.sendUpdate(u, 3000), /READ_ONLY/);

  // Writer connects, then membership is REVOKED mid-session: the next
  // update must be rejected (per-update DB authorization, not cached).
  const alice = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't8' });
  await alice.connect();
  alice.localEdit((t) => t.insert(0, 'before-revoke '));
  await alice.flush();
  await db.query(
    `UPDATE document_members SET revoked_at=now() WHERE doc_id='t8' AND user_id='user-alice'`,
  );
  const u2 = alice.localEdit((t) => t.insert(0, 'after-revoke'));
  await assert.rejects(alice.sendUpdate(u2, 3000), /FORBIDDEN/);

  // The rejected post-revoke edit never persisted; recovered doc lacks it.
  const r = await rebuildAndHash('t8');
  assert.equal(r.text, 'before-revoke ');
  assert.ok(!r.text.includes('after-revoke'));

  // HTTP admin endpoints enforce the same boundary.
  const crossHttp = await httpPost('/v1/docs/t8/compact', 'user-dave', {});
  assert.equal(crossHttp.status, 403);
  const readerHttp = await httpPost('/v1/docs/t8/compact', 'user-carol', {});
  assert.equal(readerHttp.status, 403);

  carol.close(); alice.close();
});

// ---------------------------------------------------------------------------
// helpers for awaiting protocol frames
// ---------------------------------------------------------------------------
function collect(client, event) {
  const buf = [];
  const fn = (m) => buf.push(m);
  client.on(event, fn);
  return {
    async wait(pred, timeoutMs = 5000) {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const i = buf.findIndex(pred);
        if (i >= 0) return buf.splice(i, 1)[0];
        await sleep(20);
      }
      throw new Error(`timeout waiting for ${event}`);
    },
    stop() {
      const arr = client._waiters.get(event);
      if (arr) arr.splice(arr.indexOf(fn), 1);
    },
  };
}

// ---------------------------------------------------------------------------
// T9 high concurrency: many interleaved inserts and deletes on shared ranges
// ---------------------------------------------------------------------------
const t9 = test('T9 heavy concurrent inserts/deletes: N clients converge to one hash', async () => {
  await createDoc('t9', { writers: ['user-alice', 'user-bob', 'user-owner'] });
  const tokens = ['user-alice', 'user-bob', 'user-owner'];
  const clients = [];
  for (const tok of tokens) {
    const c = new DocClient({ url: WS_URL, token: tok, docId: 't9' });
    await c.connect();
    clients.push(c);
  }

  // Seed a shared line so concurrent deletions have live targets.
  clients[0].localEdit((t) => t.insert(0, 'SEED-'.repeat(20)));
  await clients[0].flush();
  await sleep(100);

  // 60 edits fired with maximal concurrency. Deletions target only the
  // shared SEED- region (positions 0..99); the appended labels go at the
  // end so they are never deleted and can be asserted afterwards.
  const SEED_LEN = 100;
  const jobs = [];
  for (let i = 0; i < 60; i++) {
    const c = clients[i % clients.length];
    c.localEdit((t) => {
      if (i % 3 === 0) {
        // Delete within the surviving prefix of the seed; find the first
        // still-present seed character structurally by scanning for '-'.
        const str = t.toString();
        let dash = -1;
        for (let k = 0; k < Math.min(str.length, SEED_LEN + 1); k++) {
          if (str[k] === '-') { dash = k; break; }
        }
        if (dash >= 0 && dash < SEED_LEN) t.delete(dash, 1);
      }
      t.insert(t.length, `<${i % clients.length}:${i}>`);
    });
    jobs.push(c.flush({ concurrent: true }));
  }
  await Promise.all(jobs);

  const conv = await settleConvergence(clients, 't9', { rounds: 100 });
  // Every insert token i=0..59 must be present exactly as a label; the
  // delete operations must be identically applied at all replicas.
  for (let i = 0; i < 60; i++) {
    assert.ok(conv.text.includes(`:${i}>`), `insert ${i} missing`);
  }
  // The independent PostgreSQL rebuild hash already matched inside
  // settleConvergence; also assert the raw log row count is exactly 61
  // (1 seed + 60) — no silently dropped or duplicated writes.
  assert.equal(await sqlCount('doc_updates', `WHERE doc_id='t9'`), 61);
  for (const c of clients) c.close();
});

// ---------------------------------------------------------------------------
// T10 graceful restart after durable writes: reconnect convergence, no crash
// ---------------------------------------------------------------------------
const t10 = test('T10 normal restart after persisted traffic: reconnecting clients converge', async () => {
  await createDoc('t10', { writers: ['user-alice', 'user-bob'] });
  const a = new DocClient({ url: WS_URL, token: 'user-alice', docId: 't10' });
  const b = new DocClient({ url: WS_URL, token: 'user-bob', docId: 't10' });
  await Promise.all([a.connect(), b.connect()]);
  for (let i = 0; i < 8; i++) {
    a.localEdit((t) => t.insert(t.length, `a${i}`));
    b.localEdit((t) => t.insert(t.length, `b${i}`));
    await Promise.all([a.flush({ concurrent: true }), b.flush({ concurrent: true })]);
  }
  await settleConvergence([a, b], 't10');
  const beforeHash = a.stateHash();
  const persisted = await sqlCount('doc_updates', `WHERE doc_id='t10'`);
  assert.equal(persisted, 16);

  // Clean shutdown (SIGTERM) like a deploy; in-memory rooms are discarded.
  const code = await stopServer();
  assert.notEqual(code, 'timeout');
  a.hardClose(); b.hardClose();
  await startServer({ crashAfterCommit: false });

  // Both come back online. A rejoins with its stale SV (gap repair); B
  // joins as a cold replica (empty SV, full state). All three views —
  // A, B and a PostgreSQL-only rebuild — must be byte-identical to the
  // state before the restart.
  await a.reconnectWithStateVector();
  await b.connect();
  const conv = await settleConvergence([a, b], 't10');
  assert.equal(conv.hash, beforeHash, 'post-restart hash must equal pre-restart hash');
  assert.equal(await sqlCount('doc_updates', `WHERE doc_id='t10'`), 16,
    'reconnect must not re-insert persisted updates');

  // New edits keep flowing after the restart and converge again.
  a.localEdit((t) => t.insert(t.length, 'post-restart'));
  await a.flush();
  const conv2 = await settleConvergence([a, b], 't10');
  assert.ok(conv2.text.endsWith('post-restart'));
  a.close(); b.close();
});

// ---------------------------------------------------------------------------
// runner
// ---------------------------------------------------------------------------
async function main() {
  await setupTest();
  await startServer({ crashAfterCommit: false });

  const tests = [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10];
  let pass = 0;
  const failures = [];
  for (const t of tests) {
    process.stdout.write(`  ${t.name} ... `);
    try {
      await withTimeout(t.fn(), 25000, t.name);
      console.log('PASS');
      pass += 1;
    } catch (e) {
      console.log('FAIL');
      failures.push({ name: t.name, err: e });
      console.log('    ' + String(e.message || e).split('\n').join('\n    '));
    }
  }

  await stopServer();
  await db.close();

  console.log('\n========================================');
  console.log(`RESULT: ${pass}/${tests.length} passed`);
  for (const f of failures) {
    console.log(`\n--- FAIL: ${f.name}`);
    console.log(f.err.stack || f.err.message || f.err);
  }
  process.exit(failures.length ? 1 : 0);
}

main().catch(async (e) => {
  console.error('test runner crashed:', e);
  if (serverProc) serverProc.kill('SIGKILL');
  await db.close().catch(() => {});
  process.exit(2);
});
