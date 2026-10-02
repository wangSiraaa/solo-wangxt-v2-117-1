'use strict';

const db = require('./db');

// Resolve (token -> session) and (user, doc -> active role). Every call
// hits the database: we do not cache authorization across updates, so a
// revoked member loses write access immediately, and a client claiming a
// room id it does not belong to is rejected with that exact id.

async function resolveToken(token) {
  if (!token || typeof token !== 'string') return null;
  const { rows } = await db.query(
    `SELECT u.id AS user_id, u.tenant_id, u.name
       FROM users u
      WHERE u.id = $1`,
    [token],
  );
  return rows[0] || null;
}

async function getActiveRole(userId, docId) {
  const { rows } = await db.query(
    `SELECT m.role, d.tenant_id, d.title
       FROM document_members m
       JOIN documents d ON d.id = m.doc_id
       JOIN users u ON u.id = m.user_id
      WHERE m.doc_id = $1
        AND m.user_id = $2
        AND m.revoked_at IS NULL
        AND u.tenant_id = d.tenant_id`,
    [docId, userId],
  );
  return rows[0] || null;
}

async function listActiveDocs(userId) {
  const { rows } = await db.query(
    `SELECT m.doc_id, m.role
       FROM document_members m
       JOIN documents d ON d.id = m.doc_id
       JOIN users u ON u.id = m.user_id
      WHERE m.user_id = $1
        AND m.revoked_at IS NULL
        AND u.tenant_id = d.tenant_id`,
    [userId],
  );
  return rows;
}

async function revokeMember(docId, userId) {
  await db.query(
    `UPDATE document_members SET revoked_at = now()
      WHERE doc_id = $1 AND user_id = $2 AND revoked_at IS NULL`,
    [docId, userId],
  );
}

module.exports = { resolveToken, getActiveRole, listActiveDocs, revokeMember };
