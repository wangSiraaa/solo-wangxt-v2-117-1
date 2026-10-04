'use strict';

// Optional local offline outbox for the demo clients.
//
// Durability story (client side, mirrors the server's commit-before-ack
// boundary):
//
//   local edit -> append/update outbox file BEFORE the frame leaves the
//                 machine -> only an ack {ok:true} removes the entry.
//
// The file records:
//   - document identity (docId / token) so a restart rejoins the same room
//   - the full local Yjs document state (so offline edits survive restart)
//   - every outstanding update: stable msgId (= server client_msg_id),
//     base64 Yjs bytes, status ('pending' | 'error') and the last failure
//
// Reconnect flow (see DocClient.drainOutbox): state-vector hello first
// (server fills the offline gap), then outstanding items are resent in
// saved order. Successful acks (including duplicated:true, the server's
// existing dedup protocol) remove the item; failed items keep their error
// message and stay visible — they are never silently cleared.
//
// Persistence format is one JSON file per (doc, user), rewritten atomically
// (tmp file + rename). Good enough for a single-process script client; it
// is deliberately not a write-ahead log.

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Y = require('yjs');

const FORMAT_VERSION = 1;

function newMsgId() {
  return crypto.randomBytes(8).toString('hex');
}

// Filesystem-safe key for one (user, document) outbox.
function keyFor(token, docId) {
  const safe = (s) => String(s).replace(/[^A-Za-z0-9._-]/g, '_');
  return `outbox_${safe(token)}_${safe(docId)}.json`;
}

class LocalOutbox {
  constructor({ dir, token, docId } = {}) {
    if (!dir) throw new Error('LocalOutbox requires dir');
    if (!token || !docId) throw new Error('LocalOutbox requires token and docId');
    this.dir = dir;
    this.token = token;
    this.docId = docId;
    this.file = path.join(this.dir, keyFor(token, docId));
    this.seq = 0;          // monotonic per file: preserves saved send order
    this.items = [];       // [{ msgId, update(b64), status, attempts, lastError: {code,message,at}|null, createdAt }]
    this.stateB64 = '';    // full local Y.Doc state as a Yjs state update
    this.loaded = false;
    this.restored = false;
  }

  // Load the file if present. Returns true when an outbox was restored.
  load() {
    if (this.loaded) return this.restored;
    this.loaded = true;
    let raw;
    try {
      raw = fs.readFileSync(this.file, 'utf8');
    } catch (e) {
      if (e.code === 'ENOENT') return false;
      throw e;
    }
    const data = JSON.parse(raw);
    if (data.version !== FORMAT_VERSION) {
      throw new Error(`unsupported outbox version: ${data.version}`);
    }
    if (data.docId !== this.docId || data.token !== this.token) {
      throw new Error('outbox file identity mismatch');
    }
    this.items = Array.isArray(data.items) ? data.items : [];
    this.stateB64 = data.stateB64 || '';
    this.seq = data.seq || this.items.length;
    this.restored = this.items.length > 0 || !!this.stateB64;
    return this.restored;
  }

  // Restore the persisted document state into the given (fresh) Y.Doc.
  // Missing/empty state is a no-op (first run on a new machine).
  restoreInto(doc) {
    if (!this.loaded) this.load();
    if (this.stateB64) {
      Y.applyUpdate(doc, new Uint8Array(Buffer.from(this.stateB64, 'base64')), 'outbox-restore');
    }
    return !!this.stateB64;
  }

  // Persist the full local Y.Doc state. Called after local edits and after
  // applying remote/hello updates, so a restart resumes the merged state.
  saveDocState(doc) {
    this.stateB64 = Buffer.from(Y.encodeStateAsUpdate(doc)).toString('base64');
    this.persist();
  }

  // Record an update BEFORE it is sent. Keyed by msgId, and the same bytes
  // must never produce a parallel entry: a duplicate add (e.g. a flush racing
  // the auto-drain, or a replay across a restart) returns the existing item
  // so the ack always reconciles exactly one durable record.
  add(updateB64, msgId = newMsgId()) {
    const byId = this.items.find((i) => i.msgId === msgId);
    if (byId) return byId;
    const sameBytes = this.items.find((i) => i.update === updateB64);
    if (sameBytes) return sameBytes;
    this.seq += 1;
    const item = {
      msgId,
      update: updateB64,
      status: 'pending',
      seq: this.seq,
      attempts: 0,
      lastError: null,
      createdAt: new Date().toISOString(),
    };
    this.items.push(item);
    this.persist();
    return item;
  }

  markAttempt(msgId) {
    const item = this._get(msgId);
    if (item) {
      item.attempts += 1;
      this.persist();
    }
  }

  // Only a successful ack (dedup acks included) is allowed to remove an
  // entry. Everything else stays in the box.
  markAcked(msgId) {
    const before = this.items.length;
    this.items = this.items.filter((i) => i.msgId !== msgId);
    if (this.items.length !== before) this.persist();
    return before !== this.items.length;
  }

  // Negative ack / timeout / disconnect while waiting. The item is retained
  // with status 'error' and the server's code+message, so the failure is
  // locatable on the next run instead of vanishing.
  markFailed(msgId, code, message) {
    const item = this._get(msgId);
    if (item) {
      item.status = 'error';
      item.lastError = {
        code: code || 'UNKNOWN',
        message: message || '',
        at: new Date().toISOString(),
      };
      this.persist();
    }
  }

  // A retry is in flight: flip back to pending while keeping the history.
  markPending(msgId) {
    const item = this._get(msgId);
    if (item) {
      item.status = 'pending';
      this.persist();
    }
  }

  // Outstanding items in saved order. Both 'pending' and 'error' items are
  // resent on reconnect; transient failures (revocation lifted, reader
  // promoted, server restarted) then recover automatically, and hard
  // failures simply refresh their visible error message.
  outstanding() {
    return this.items.slice().sort((a, b) => a.seq - b.seq);
  }

  get(msgId) { return this._get(msgId); }

  _get(msgId) { return this.items.find((i) => i.msgId === msgId) || null; }

  summary() {
    const pending = this.items.filter((i) => i.status === 'pending').length;
    const failed = this.items.filter((i) => i.status === 'error').length;
    return { total: this.items.length, pending, failed, hasState: !!this.stateB64 };
  }

  persist() {
    fs.mkdirSync(this.dir, { recursive: true });
    const payload = {
      version: FORMAT_VERSION,
      docId: this.docId,
      token: this.token,
      seq: this.seq,
      stateB64: this.stateB64,
      items: this.items,
    };
    const tmp = `${this.file}.tmp-${process.pid}-${crypto.randomBytes(3).toString('hex')}`;
    fs.writeFileSync(tmp, JSON.stringify(payload));
    fs.renameSync(tmp, this.file);
  }
}

module.exports = { LocalOutbox, newMsgId, keyFor };
