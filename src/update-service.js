'use strict';

const db = require('./db');
const yutil = require('./yutil');
const { getActiveRole } = require('./permissions');
const { logError } = require('./errorlog');

// Persistence boundary for an update frame:
//   1. resolve & validate envelope (malformed -> update_errors, nack)
//   2. re-check authorization for THIS user x THIS doc in THIS tenant
//   3. structurally validate the Yjs binary (corrupt -> update_errors, nack)
//   4. INSERT ... ON CONFLICT DO NOTHING (dedup on doc_id+client_msg_id)
//      inside the same per-doc serial queue that applies memory state
//   5. apply to in-memory doc, broadcast to peers
//   6. only THEN send ack to origin
// Crash between 4 and 6: the client retries with the same client_msg_id and
// gets a dedup ack; no update can be lost or applied twice as new.

function makeNack(ws, msgId, code, message) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({ type: 'ack', msgId, ok: false, code, message }));
  }
}

async function processUpdate(ws, room, ctx, payload) {
  const msgId = payload && payload.msgId;
  const docId = room.docId;

  if (!msgId || typeof msgId !== 'string') {
    await logError({ docId, userId: ctx.userId, tenantId: ctx.tenantId,
      raw: safeRaw(payload), code: 'BAD_ENVELOPE', message: 'missing msgId' });
    return makeNack(ws, msgId || null, 'BAD_ENVELOPE', 'missing msgId');
  }
  const updateB64 = payload.update;
  if (!updateB64 || typeof updateB64 !== 'string') {
    await logError({ docId, userId: ctx.userId, tenantId: ctx.tenantId,
      clientMsgId: msgId, code: 'BAD_ENVELOPE', message: 'missing update' });
    return makeNack(ws, msgId, 'BAD_ENVELOPE', 'missing update');
  }

  let bytes;
  try {
    bytes = Buffer.from(updateB64, 'base64');
  } catch (e) {
    await logError({ docId, userId: ctx.userId, tenantId: ctx.tenantId,
      clientMsgId: msgId, code: 'BAD_ENCODING', message: e.message });
    return makeNack(ws, msgId, 'BAD_ENCODING', 'update is not valid base64');
  }

  // Authorization is not cached on the socket for writes.
  const role = await getActiveRole(ctx.userId, docId);
  if (!role || role.tenant_id !== ctx.tenantId) {
    await logError({ docId, userId: ctx.userId, tenantId: ctx.tenantId,
      clientMsgId: msgId, raw: bytes, code: 'FORBIDDEN',
      message: 'no active membership for document' });
    return makeNack(ws, msgId, 'FORBIDDEN', 'no write access to this document');
  }
  if (role.role === 'reader') {
    return makeNack(ws, msgId, 'READ_ONLY', 'reader role cannot send updates');
  }

  const validation = yutil.validateUpdate(bytes);
  if (!validation.ok) {
    await logError({ docId, userId: ctx.userId, tenantId: ctx.tenantId,
      clientMsgId: msgId, raw: bytes, code: validation.code,
      message: validation.message });
    return makeNack(ws, msgId, validation.code, validation.message);
  }

  // Serialized per document: seq assignment, persistence and memory apply
  // happen atomically w.r.t. other updates for the same doc.
  const result = await room.enqueue(async () => {
    const client = await db.getClient();
    let seq;
    let duplicated = false;
    try {
      await client.query('BEGIN');
      // nextval on a per-doc counter emulated by max+1 row lock.
      const lock = await client.query(
        `SELECT id FROM documents WHERE id = $1 FOR UPDATE`,
        [docId],
      );
      if (lock.rowCount === 0) throw new Error('document vanished');
      const maxRes = await client.query(
        `SELECT COALESCE(MAX(seq), 0) AS max_seq FROM doc_updates WHERE doc_id = $1`,
        [docId],
      );
      seq = Number(maxRes.rows[0].max_seq) + 1;

      const ins = await client.query(
        `INSERT INTO doc_updates
           (doc_id, seq, client_msg_id, origin_user_id, origin_sv_hash, update_bytes)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (doc_id, client_msg_id) DO NOTHING
         RETURNING seq`,
        [docId, seq, msgId, ctx.userId,
         payload.svHash || null, bytes],
      );
      if (ins.rowCount === 0) {
        duplicated = true;
        const prior = await client.query(
          `SELECT seq FROM doc_updates WHERE doc_id = $1 AND client_msg_id = $2`,
          [docId, msgId],
        );
        seq = Number(prior.rows[0].seq);
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }

    if (!duplicated) {
      // Crash-injection for persistence-boundary tests: the transaction has
      // committed (fsync done) but no in-memory apply / broadcast / ack has
      // happened. A restart must still see the update, and the client's
      // retry must dedup instead of double-applying.
      if (process.env.CRASH_AFTER_COMMIT === '1') {
        console.error('[crash-injector] committed seq %s for %s, exiting before ack',
          seq, msgId);
        process.exit(17);
      }
      // Persistence first (fsync+COMMIT done), then in-memory apply.
      const Y = require('yjs');
      Y.applyUpdate(room.doc, new Uint8Array(bytes), {
        user: ctx.userId, msgId,
      });
      room.loadedSeq = seq;
      // Broadcast to peers only. The origin already holds this change in
      // its local Y.Doc; echoing its own update back would double-apply and
      // diverge the origin from the canonical state (a real bug we caught
      // by asserting binary state equality, not just rendered text).
      room.broadcast({
        type: 'update',
        docId,
        seq,
        msgId,
        origin: ctx.userId,
        update: updateB64,
      }, ws);
    }
    return { seq, duplicated };
  });

  // Ack is the durable boundary. A client is allowed to forget the message
  // only after receiving this; a duplicate retries and gets `duplicated`.
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({
      type: 'ack',
      msgId,
      ok: true,
      seq: result.seq,
      duplicated: result.duplicated,
    }));
  }
}

function safeRaw(payload) {
  try {
    return Buffer.from(JSON.stringify(payload));
  } catch {
    return null;
  }
}

module.exports = { processUpdate };
