'use strict';

// Minimal Yjs-over-WebSocket client used by the demo and test scripts.
// It is deliberately controllable (manual flush, ordered/async send modes)
// so tests can drive concurrency, reordering, retries and disconnects.

const WebSocket = require('ws');
const Y = require('yjs');
const { LocalOutbox, newMsgId } = require('./outbox');

class DocClient {
  constructor({
    url, token, docId, name, autoApply = true, verbose = false,
    outboxDir = null, autoDrainOutbox = true,
  } = {}) {
    this.url = url;
    this.token = token;
    this.docId = docId;
    this.name = name || token;
    this.autoApply = autoApply;
    this.verbose = verbose;
    this.autoDrainOutbox = autoDrainOutbox;

    this.doc = new Y.Doc({ gc: false });
    this.ws = null;
    this.seq = 0;
    this.connected = false;
    this.helloDone = false;
    this.role = null;

    // Optional durable offline outbox. When enabled, local edits are
    // persisted (document state + stable msgId + bytes) before being sent,
    // and are only removed on a successful ack. A restart restores the
    // local document and outstanding items from disk.
    this.outbox = outboxDir
      ? new LocalOutbox({ dir: outboxDir, token, docId })
      : null;
    this.restored = false;
    if (this.outbox) {
      this.restored = this.outbox.restoreInto(this.doc);
    }
    this._drainPromise = null;

    this.pending = new Map(); // msgId -> { resolve, reject, timer, bytes }
    this.updateQueue = [];   // locally generated, unflushed updates (b64)
    this._queueMsgIds = [];  // aligned with updateQueue when using the outbox
    this.pendingUpdates = []; // received but not auto-applied (raw bytes)
    this.serverSv = null;
    this.acks = 0;
    this.dupAcks = 0;
    this.receivedUpdates = 0;
    this.errors = [];

    this._waiters = new Map(); // event -> [fn]
  }

  on(event, fn) {
    if (!this._waiters.has(event)) this._waiters.set(event, []);
    this._waiters.get(event).push(fn);
  }

  emit(event, arg) {
    for (const fn of this._waiters.get(event) || []) {
      try { fn(arg); } catch (e) { console.error('listener error', e); }
    }
  }

  log(...args) {
    if (this.verbose) console.log(`[client ${this.name}]`, ...args);
  }

  connect({ sv } = {}) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.url);
      this.ws = ws;
      let settled = false;
      const fail = (err) => { if (!settled) { settled = true; reject(err); } };
      const done = (v) => { if (!settled) { settled = true; resolve(v); } };
      ws.on('error', (err) => {
        if (!this.helloDone) fail(err);
      });
      ws.on('close', () => {
        this.connected = false;
        this.helloDone = false;
        this._failAllPending('DISCONNECTED', 'connection closed before ack');
        if (this._helloReject) {
          const e = this._helloReject;
          this._helloReject = null;
          fail(e);
        }
        this.emit('close');
      });
      ws.on('open', () => {
        this.connected = true;
        const hello = { type: 'hello', token: this.token, docId: this.docId };
        // Explicit sv wins; a process restarted with a restored outbox next
        // presents its existing state vector by default (offline gap repair),
        // instead of forcing a full snapshot.
        let helloSv = sv;
        if (!helloSv && this.outbox && this.restored) {
          helloSv = Y.encodeStateVector(this.doc);
        }
        if (helloSv) hello.sv = Buffer.from(helloSv).toString('base64');
        ws.send(JSON.stringify(hello));
      });
      ws.on('message', (data) => this._onMessage(data, done, () => {}));
    });
  }

  async reconnectWithStateVector() {
    // Reconnect presenting the local Yjs state vector: server sends only
    // the missing difference (offline gap repair).
    return this.connect({ sv: Y.encodeStateVector(this.doc) });
  }

  _onMessage(data, resolveHello, connected) {
    let msg;
    try {
      msg = JSON.parse(data.toString('utf8'));
    } catch (e) {
      this.errors.push(['BAD_JSON', e.message]);
      return;
    }
    this.log('recv', msg.type, msg.msgId || '', msg.code || '');
    switch (msg.type) {
      case 'hello-ok': {
        this.helloDone = true;
        this.role = msg.role;
        this.seq = msg.seq;
        this.serverSv = Buffer.from(msg.sv, 'base64');
        const state = Buffer.from(msg.state, 'base64');
        if (state.length && this.autoApply) {
          Y.applyUpdate(this.doc, new Uint8Array(state), 'hello-state');
        } else if (state.length) {
          this.pendingUpdates.push(state);
        }
        // The state-vector hello has repaired the offline gap; remember the
        // merged local state, then resend the outbox in saved order.
        if (this.outbox && state.length && this.autoApply) this.outbox.saveDocState(this.doc);
        connected();
        this.emit('hello', msg);
        if (this.autoDrainOutbox) {
          setImmediate(() => { this.drainOutbox().catch(() => {}); });
        }
        resolveHello(msg);
        break;
      }
      case 'hello-err':
        this.errors.push([msg.code, msg.message]);
        this.emit('hello-err', msg);
        if (!this.helloDone) {
          const e = new Error(`${msg.code}: ${msg.message}`);
          e.code = msg.code;
          // reject via close soon
          this.ws.close();
          this._helloReject = e;
        }
        break;
      case 'ack': {
        const p = this.pending.get(msg.msgId);
        if (msg.ok) {
          this.acks += 1;
          if (msg.duplicated) this.dupAcks += 1;
          this.seq = Math.max(this.seq, msg.seq || 0);
          // Durable boundary reached: the server committed (possibly via a
          // dedup hit). Only now may the outbox entry be dropped.
          if (this.outbox) this.outbox.markAcked(msg.msgId);
          if (p) {
            clearTimeout(p.timer);
            this.pending.delete(msg.msgId);
            p.resolve(msg);
          }
        } else {
          // Negative ack: keep the outbox item, stamped with the server's
          // error (READ_ONLY / FORBIDDEN / CORRUPT_UPDATE / ...). It must
          // remain visible instead of being silently discarded.
          if (this.outbox) this.outbox.markFailed(msg.msgId, msg.code, msg.message);
          if (p) {
            clearTimeout(p.timer);
            this.pending.delete(msg.msgId);
            const e = new Error(`${msg.code}: ${msg.message}`);
            e.code = msg.code;
            p.reject(e);
          }
          this.errors.push([msg.code, msg.message]);
        }
        this.emit('ack', msg);
        break;
      }
      case 'update': {
        this.receivedUpdates += 1;
        this.seq = Math.max(this.seq, msg.seq || 0);
        const bytes = Buffer.from(msg.update, 'base64');
        if (this.autoApply) {
          Y.applyUpdate(this.doc, new Uint8Array(bytes), 'remote');
          if (this.outbox) this.outbox.saveDocState(this.doc);
        } else {
          this.pendingUpdates.push(bytes);
        }
        this.emit('update', msg);
        break;
      }
      case 'sync-diff': {
        const bytes = Buffer.from(msg.update, 'base64');
        if (bytes.length) {
          if (this.autoApply) {
            Y.applyUpdate(this.doc, new Uint8Array(bytes), 'sync');
            if (this.outbox) this.outbox.saveDocState(this.doc);
          } else {
            this.pendingUpdates.push(bytes);
          }
        }
        this.seq = Math.max(this.seq, msg.seq || 0);
        this.emit('sync-diff', msg);
        break;
      }
      case 'pong':
        this.emit('pong');
        break;
      case 'error':
        this.errors.push([msg.code, msg.message]);
        this.emit('error', msg);
        break;
      default:
        this.emit('other', msg);
    }
  }

  // Edit helper: runs fn inside ONE Yjs transaction so delete+insert style
  // edits emit exactly one update event. Without transact(), Yjs emits one
  // update per op and capturing "the last event" silently drops deletes.
  localEdit(fn) {
    let updateB64 = null;
    const handler = (u) => { updateB64 = Buffer.from(u).toString('base64'); };
    this.doc.on('update', handler);
    try {
      this.doc.transact(() => fn(this.doc.getText('content')), 'local');
    } finally {
      this.doc.off('update', handler);
    }
    if (updateB64) {
      // Persist BEFORE anything is sent: a hard exit after this point still
      // leaves both the merged document state and the update on disk.
      if (this.outbox) {
        const item = this.outbox.add(updateB64);
        this._queueMsgIds.push(item.msgId);
        this.outbox.saveDocState(this.doc);
      }
      this.updateQueue.push(updateB64);
    }
    return updateB64;
  }

  queueUpdate(updateB64, msgId) {
    this.updateQueue.push(updateB64);
    if (this.outbox) {
      const id = msgId || newMsgId();
      this.outbox.add(updateB64, id);
      this._queueMsgIds.push(id);
      this.outbox.saveDocState(this.doc);
    }
  }

  // Flush queued updates with a deterministic ordering function.
  // orderFn(queue) returns the sequence of b64 strings to send.
  // With an outbox, each queued update already owns a stable msgId and a
  // durable record; orderFn is ignored there (saved order is the order).
  async flush({ orderFn = null, concurrent = false, timeoutMs = 5000 } = {}) {
    let items;
    if (this.outbox) {
      items = this.updateQueue.map((b64, i) => ({
        b64,
        msgId: this._queueMsgIds[i],
      }));
      this.updateQueue = [];
      this._queueMsgIds = [];
    } else {
      const picked = orderFn ? orderFn(this.updateQueue) : this.updateQueue.slice();
      this.updateQueue = [];
      items = picked.map((b64) => ({ b64, msgId: undefined }));
    }
    const sends = items.map(({ b64, msgId }) => () => this.sendUpdate(b64, timeoutMs, msgId));
    if (concurrent) {
      // Fire all writes without waiting: tests use this to race the server.
      return Promise.all(sends.map((s) => s()));
    }
    const out = [];
    for (const s of sends) out.push(await s());
    return out;
  }

  sendUpdate(updateB64, timeoutMs = 5000, msgId = newMsgId()) {
    if (this.outbox) {
      // Re-attach to the durable record (created in localEdit), or create one
      // for bytes produced outside localEdit. The same msgId is what makes a
      // post-restart replay dedup on the server.
      if (!this.outbox.get(msgId)) this.outbox.add(updateB64, msgId);
      this.outbox.markPending(msgId);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(msgId);
        if (this.outbox) this.outbox.markFailed(msgId, 'ACK_TIMEOUT', 'no ack before timeout');
        reject(new Error('ack timeout'));
      }, timeoutMs);
      this.pending.set(msgId, { resolve, reject, timer, bytes: updateB64 });
      this.ws.send(JSON.stringify({ type: 'update', msgId, update: updateB64 }));
      if (this.outbox) this.outbox.markAttempt(msgId);
      this.log('send update', msgId);
    });
  }

  // After a state-vector reconnect has repaired the offline gap, resend all
  // outstanding outbox entries in saved order. One failure must not block
  // the rest: server nacks are recorded on the item and iteration continues.
  // Runs at most once at a time; concurrent callers join the same run.
  drainOutbox({ timeoutMs = 5000 } = {}) {
    if (!this.outbox) return Promise.resolve({ sent: 0, acked: 0, duplicated: 0, failed: 0, results: [] });
    if (this._drainPromise) return this._drainPromise;
    this._drainPromise = (async () => {
      const report = { sent: 0, acked: 0, duplicated: 0, failed: 0, results: [] };
      if (!this.helloDone || !this.ws || this.ws.readyState !== this.ws.OPEN) {
        return report;
      }
      // Snapshot once: acks/errors mutate this.outbox.items while we iterate.
      for (const item of this.outbox.outstanding()) {
        if (!this.helloDone || this.ws.readyState !== this.ws.OPEN) break;
        // Already in flight (e.g. a concurrent flush using the same msgId):
        // never put a duplicate frame on the wire from this process.
        if (this.pending.has(item.msgId)) continue;
        report.sent += 1;
        this.outbox.markPending(item.msgId);
        try {
          const ack = await this.sendUpdate(item.update, timeoutMs, item.msgId);
          report.acked += 1;
          if (ack.duplicated) report.duplicated += 1;
          report.results.push({ msgId: item.msgId, ok: true, duplicated: !!ack.duplicated, seq: ack.seq });
        } catch (e) {
          // sendUpdate already stamped the durable item with the error code.
          report.failed += 1;
          report.results.push({
            msgId: item.msgId, ok: false,
            code: e.code || 'SEND_FAILED', message: e.message,
          });
        }
      }
      return report;
    })().finally(() => { this._drainPromise = null; });
    return this._drainPromise;
  }

  outboxSummary() {
    return this.outbox ? this.outbox.summary() : null;
  }

  // Reject every in-flight send promise on disconnect/teardown. Durable
  // items are marked failed (kept on disk, not deleted) so they are retried
  // after the next reconnect.
  _failAllPending(code, message) {
    for (const [msgId, p] of this.pending.entries()) {
      clearTimeout(p.timer);
      if (this.outbox) this.outbox.markFailed(msgId, code, message);
      const e = new Error(`${code}: ${message}`);
      e.code = code;
      p.reject(e);
    }
    this.pending.clear();
  }

  // Re-send an exact duplicate frame (same msgId + bytes), proving dedup.
  resend(msgId) {
    const p = this.pending.get(msgId);
    const bytes = p ? p.bytes : null;
    if (!bytes) throw new Error('no such pending msgId');
    this.ws.send(JSON.stringify({ type: 'update', msgId, update: bytes }));
  }

  // Re-send an already-acknowledged update with the same msgId: server must
  // report duplicated=true and the document must not change.
  resendRaw(msgId, updateB64) {
    this.ws.send(JSON.stringify({ type: 'update', msgId, update: updateB64 }));
  }

  // Forget a pending message without closing the socket (simulates the
  // client never having received an ack after a server crash).
  forgetPending(msgId) {
    const p = this.pending.get(msgId);
    if (p) { clearTimeout(p.timer); this.pending.delete(msgId); }
  }

  hardClose() {
    this._failAllPending('DISCONNECTED', 'hard close before ack');
    try { this.ws && this.ws.terminate(); } catch { try { this.ws && this.ws.close(); } catch {} }
  }

  // For tests: send arbitrary raw frame text (corruption / protocol abuse).
  sendRaw(text) {
    this.ws.send(text);
  }

  async requestSync(sv) {
    const svBytes = sv || Y.encodeStateVector(this.doc);
    return new Promise((resolve) => {
      const once = (msg) => {
        this._waiters.get('sync-diff')?.splice(
          this._waiters.get('sync-diff').indexOf(once), 1);
        resolve(msg);
      };
      this.on('sync-diff', once);
      this.ws.send(JSON.stringify({ type: 'sync-req', sv: Buffer.from(svBytes).toString('base64') }));
    });
  }

  applyPending() {
    for (const b of this.pendingUpdates) {
      Y.applyUpdate(this.doc, new Uint8Array(b), 'manual');
    }
    this.pendingUpdates = [];
  }

  get text() {
    return this.doc.getText('content').toString();
  }

  stateHash() {
    return require('node:crypto')
      .createHash('sha256')
      .update(Buffer.from(Y.encodeStateAsUpdate(this.doc)))
      .digest('hex');
  }

  stateVectorMap() {
    const m = {};
    for (const [c, clock] of this.doc.store.clients) m[String(c)] = clock;
    return m;
  }

  close() {
    this._failAllPending('CLOSED', 'client closed before ack');
    if (this.ws) this.ws.close();
  }
}

module.exports = { DocClient, newMsgId };
