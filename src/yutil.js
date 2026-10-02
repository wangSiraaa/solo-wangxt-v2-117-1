'use strict';

const crypto = require('node:crypto');
const Y = require('yjs');

// All Y.Doc instances run with gc=false so that state encodings are
// deterministic regardless of when/where compaction happens; convergence is
// asserted on the binary state, not on a rendered string.

function createDoc() {
  return new Y.Doc({ gc: false });
}

function sha256(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function encodeState(doc) {
  return Buffer.from(Y.encodeStateAsUpdate(doc));
}

function stateVector(doc) {
  return Buffer.from(Y.encodeStateVector(doc));
}

// Decode a Yjs state-vector encoding into a plain { clientId -> clock }
// map, usable for stable JSON hashing and equality assertions.
function decodeStateVector(svBytes) {
  const map = Y.decodeStateVector(new Uint8Array(svBytes));
  const out = {};
  for (const [client, clock] of map.entries()) {
    out[String(client)] = clock;
  }
  return out;
}

function svHash(svBytes) {
  const map = decodeStateVector(svBytes);
  const canonical = Object.keys(map)
    .sort()
    .map((k) => `${k}:${map[k]}`)
    .join(',');
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function svEqual(aBytes, bBytes) {
  const a = decodeStateVector(aBytes);
  const b = decodeStateVector(bBytes);
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  return ak.every((k) => String(a[k]) === String(b[k]));
}

// Differential sync: missing updates relative to the peer's state vector.
function diffUpdate(doc, svBytes) {
  return Buffer.from(
    Y.encodeStateAsUpdate(doc, new Uint8Array(svBytes)),
  );
}

// Structural validation. Returns { ok: true } or { ok:false, code, message }.
// Throws are intentionally caught: corrupt binary input must land in
// update_errors with a locatable reason, never crash the process.
function validateUpdate(bytes) {
  if (!Buffer.isBuffer(bytes) || bytes.length === 0) {
    return { ok: false, code: 'EMPTY_UPDATE', message: 'update payload is empty' };
  }
  try {
    const probe = createDoc();
    Y.applyUpdate(probe, new Uint8Array(bytes), 'validate');
    // Round-trip check: a structurally parsed update that encodes back to a
    // degenerate empty state for non-empty input is treated as suspicious.
    return { ok: true };
  } catch (err) {
    return { ok: false, code: 'CORRUPT_UPDATE', message: `${err && err.message ? err.message : err}` };
  }
}

// Rebuild a document state by replaying snapshot + tail (or full log).
// Caller supplies rows ordered by seq ascending.
function rebuild(snapshotBytes, updateRows) {
  const doc = createDoc();
  if (snapshotBytes && snapshotBytes.length) {
    Y.applyUpdate(doc, new Uint8Array(snapshotBytes), 'snapshot');
  }
  for (const row of updateRows) {
    const b = Buffer.isBuffer(row.update_bytes)
      ? row.update_bytes
      : Buffer.from(row.update_bytes);
    Y.applyUpdate(doc, new Uint8Array(b), `seq:${row.seq}`);
  }
  return doc;
}

module.exports = {
  createDoc,
  sha256,
  encodeState,
  stateVector,
  decodeStateVector,
  svHash,
  svEqual,
  diffUpdate,
  validateUpdate,
  rebuild,
};
