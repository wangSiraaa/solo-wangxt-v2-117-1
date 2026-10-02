'use strict';

// Minimal Yjs-over-WebSocket client used by the demo and test scripts.
// It is deliberately controllable (manual flush, ordered/async send modes)
// so tests can drive concurrency, reordering, retries and disconnects.

const WebSocket = require('ws');
const Y = require('yjs');
const crypto = require('node:crypto');

function newMsgId() {
  return crypto.randomBytes(8).toString('hex');
}

class DocClient {
  constructor({ url, token, docId, name, autoApply = true, verbose = false }) {
    this.url = url;
    this.token = token;
    this.docId = docId;
    this.name = name || token;
    this.autoApply = autoApply;
    this.verbose = verbose;

    this.doc = new Y.Doc({ gc: false });
    this.ws = null;
    this.seq = 0;
    this.connected = false;
    this.helloDone = false;
    this.role = null;

    this.pending = new Map(); // msgId -> { resolve, reject, timer, bytes }
    this.updateQueue = [];   // locally generated, unflushed updates (b64)
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
        if (sv) hello.sv = sv.toString('base64');
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
        connected();
        this.emit('hello', msg);
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
          if (p) {
            clearTimeout(p.timer);
            this.pending.delete(msg.msgId);
            p.resolve(msg);
          }
        } else {
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
        if (this.autoApply) Y.applyUpdate(this.doc, new Uint8Array(bytes), 'remote');
        else this.pendingUpdates.push(bytes);
        this.emit('update', msg);
        break;
      }
      case 'sync-diff': {
        const bytes = Buffer.from(msg.update, 'base64');
        if (bytes.length) {
          if (this.autoApply) Y.applyUpdate(this.doc, new Uint8Array(bytes), 'sync');
          else this.pendingUpdates.push(bytes);
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
    if (updateB64) this.updateQueue.push(updateB64);
    return updateB64;
  }

  queueUpdate(updateB64) {
    this.updateQueue.push(updateB64);
  }

  // Flush queued updates with a deterministic ordering function.
  // orderFn(queue) returns the sequence of b64 strings to send.
  async flush({ orderFn = null, concurrent = false, timeoutMs = 5000 } = {}) {
    const items = orderFn ? orderFn(this.updateQueue) : this.updateQueue.slice();
    this.updateQueue = [];
    const sends = items.map((b64) => () => this.sendUpdate(b64, timeoutMs));
    if (concurrent) {
      // Fire all writes without waiting: tests use this to race the server.
      return Promise.all(sends.map((s) => s()));
    }
    const out = [];
    for (const s of sends) out.push(await s());
    return out;
  }

  sendUpdate(updateB64, timeoutMs = 5000, msgId = newMsgId()) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(msgId);
        reject(new Error('ack timeout'));
      }, timeoutMs);
      this.pending.set(msgId, { resolve, reject, timer, bytes: updateB64 });
      this.ws.send(JSON.stringify({ type: 'update', msgId, update: updateB64 }));
      this.log('send update', msgId);
    });
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
    for (const p of this.pending.values()) clearTimeout(p.timer);
    this.pending.clear();
    try { this.ws.terminate(); } catch { this.ws.close(); }
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
    for (const p of this.pending.values()) clearTimeout(p.timer);
    this.pending.clear();
    if (this.ws) this.ws.close();
  }
}

module.exports = { DocClient, newMsgId };
