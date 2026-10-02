'use strict';

const db = require('./db');

// Persist a rejected frame so "unknown or corrupt update" is locatable:
// which doc/user/connection context, which client message id, raw length and
// a hex prefix, plus the classified error code.
async function logError(entry) {
  const {
    docId = null,
    userId = null,
    tenantId = null,
    clientMsgId = null,
    raw = null,
    code,
    message,
  } = entry;
  const rawBuf = Buffer.isBuffer(raw) ? raw : (raw != null ? Buffer.from(String(raw)) : null);
  const rawPrefixHex = rawBuf ? rawBuf.subarray(0, 32).toString('hex') : null;
  try {
    await db.query(
      `INSERT INTO update_errors
         (doc_id, user_id, tenant_id, client_msg_id, raw_len, raw_prefix_hex, error_code, error_message)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [docId, userId, tenantId, clientMsgId, rawBuf ? rawBuf.length : null,
       rawPrefixHex, code, String(message).slice(0, 2000)],
    );
  } catch (e) {
    // Last-resort: never let error logging mask the original failure.
    console.error('[update_errors] failed to persist:', e.message);
  }
}

module.exports = { logError };
