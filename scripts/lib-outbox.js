'use strict';

// File-backed offline outbox for the demo clients.
//
// One JSON file per client identity records:
//   - document identity (docId + token): a file is only ever loaded by the
//     identity it was written for; a mismatch is a loud error, never a
//     silent cross-document apply;
//   - the stable Yjs clientID and the full local document state, so a
//     restarted process resumes the same document identity, including
//     edits that were never acknowledged;
//   - the un-acked items in save order: stable msgId (assigned once, at
//     enqueue time), Yjs update bytes and delivery status
//     ('pending' | 'failed' + the server's error code/message).
//
// Removal is ack-gated: an item leaves the outbox only when the server
// acks it with ok:true — including duplicated:true, which means the row
// was already durable from an earlier attempt (the server dedups on
// (doc_id, client_msg_id)). A server nack keeps the item and records the
// error; failed items are retried on the next flush but never silently
// cleared.
//
// Writes are atomic (tmp file + rename) and synchronous: outbox mutations
// are rare at demo scale and a crash must never leave a half-written file.

const fs = require('node:fs');
const path = require('node:path');
const Y = require('yjs');

class Outbox {
  constructor(file, { docId, token }) {
    this.file = file;
    this.identity = { docId, token };
    this.items = [];
    this.clientID = null;
    this._doc = null;
    this._docState = null;
  }

  // Load and validate the file. Returns null when no outbox exists yet.
  load() {
    let raw;
    try {
      raw = fs.readFileSync(this.file, 'utf8');
    } catch (e) {
      if (e.code === 'ENOENT') return null;
      throw e;
    }
    const data = JSON.parse(raw);
    if (data.docId !== this.identity.docId || data.token !== this.identity.token) {
      throw new Error(
        `outbox identity mismatch: ${this.file} belongs to ` +
        `${data.token}@${data.docId}, not ${this.identity.token}@${this.identity.docId}`,
      );
    }
    this.clientID = typeof data.clientID === 'number' ? data.clientID : null;
    this.items = Array.isArray(data.items) ? data.items : [];
    this._docState = data.docState ? Buffer.from(data.docState, 'base64') : null;
    return { clientID: this.clientID, docState: this._docState, items: this.items };
  }

  // Bind to the client's Y.Doc: restore the stable client identity (new
  // local edits continue this client's clock instead of fragmenting into a
  // fresh client id per restart) and persist the full local state on every
  // change, local or remote.
  attachDoc(doc) {
    this._doc = doc;
    if (this.clientID != null) doc.clientID = this.clientID;
    doc.on('update', () => this.save());
  }

  get restoredState() {
    return this._docState;
  }

  save() {
    if (!this._doc) return;
    const data = {
      version: 1,
      docId: this.identity.docId,
      token: this.identity.token,
      clientID: this._doc.clientID,
      savedAt: new Date().toISOString(),
      docState: Buffer.from(Y.encodeStateAsUpdate(this._doc)).toString('base64'),
      items: this.items,
    };
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(data));
    fs.renameSync(tmp, this.file);
  }

  // msgId is assigned here, once, and survives restarts: a retry after a
  // crash reuses it and hits the server's (doc_id, client_msg_id) dedup.
  enqueue(msgId, updateB64) {
    this.items.push({
      msgId,
      update: updateB64,
      status: 'pending',
      error: null,
      attempts: 0,
      createdAt: new Date().toISOString(),
    });
    this.save();
  }

  noteAttempt(msgId) {
    const it = this.items.find((i) => i.msgId === msgId);
    if (it) {
      it.attempts += 1;
      this.save();
    }
  }

  // Ack-gated removal: only ever called after ack { ok: true }.
  markAcked(msgId) {
    const i = this.items.findIndex((it) => it.msgId === msgId);
    if (i >= 0) {
      this.items.splice(i, 1);
      this.save();
    }
  }

  // A server nack is retained with its error, never silently dropped.
  markFailed(msgId, code, message) {
    const it = this.items.find((i) => i.msgId === msgId);
    if (it) {
      it.status = 'failed';
      it.error = { code, message };
      this.save();
    }
  }

  counts() {
    let pending = 0;
    let failed = 0;
    for (const it of this.items) {
      if (it.status === 'failed') failed += 1;
      else pending += 1;
    }
    return { pending, failed, total: this.items.length };
  }
}

module.exports = { Outbox };
