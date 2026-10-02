'use strict';

// Create (or reset) demo tenant/users/document with deterministic ids.
// User ids double as demo bearer tokens (see resolveToken) — adequate for a
// local demo; production would map opaque tokens to users.

const db = require('../src/db');

async function seed({ reset = false } = {}) {
  if (reset) {
    await db.query(`TRUNCATE update_errors, doc_snapshots, doc_updates,
                    document_members, documents, users, tenants RESTART IDENTITY CASCADE`);
  }

  await db.query(
    `INSERT INTO tenants (id, name) VALUES ($1,$2)
     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
    ['tenant-acme', 'ACME Research'],
  );
  await db.query(
    `INSERT INTO tenants (id, name) VALUES ($1,$2)
     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
    ['tenant-globex', 'Globex (separate tenant)'],
  );

  for (const [id, tenant, name] of [
    ['user-alice', 'tenant-acme', 'Alice'],
    ['user-bob', 'tenant-acme', 'Bob'],
    ['user-carol', 'tenant-acme', 'Carol (reader)'],
    ['user-dave', 'tenant-globex', 'Dave (other tenant)'],
    ['user-owner', 'tenant-acme', 'Owner'],
  ]) {
    await db.query(
      `INSERT INTO users (id, tenant_id, name) VALUES ($1,$2,$3)
       ON CONFLICT (id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, name = EXCLUDED.name`,
      [id, tenant, name],
    );
  }

  await db.query(
    `INSERT INTO documents (id, tenant_id, title) VALUES ($1,$2,$3)
     ON CONFLICT (id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, title = EXCLUDED.title`,
    ['doc-demo', 'tenant-acme', 'Concurrent editing demo'],
  );
  await db.query(
    `INSERT INTO documents (id, tenant_id, title) VALUES ($1,$2,$3)
     ON CONFLICT (id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, title = EXCLUDED.title`,
    ['doc-other', 'tenant-globex', 'Globex internal doc'],
  );

  const members = [
    ['doc-demo', 'user-owner', 'owner'],
    ['doc-demo', 'user-alice', 'writer'],
    ['doc-demo', 'user-bob', 'writer'],
    ['doc-demo', 'user-carol', 'reader'],
    ['doc-other', 'user-dave', 'writer'],
  ];
  for (const [doc, user, role] of members) {
    await db.query(
      `INSERT INTO document_members (doc_id, user_id, role)
       VALUES ($1,$2,$3)
       ON CONFLICT (doc_id, user_id)
       DO UPDATE SET role = EXCLUDED.role, revoked_at = NULL`,
      [doc, user, role],
    );
  }

  const r = await db.query('SELECT count(*)::int AS n FROM documents');
  return { documents: r.rows[0].n };
}

if (require.main === module) {
  const reset = process.argv.includes('--reset');
  seed({ reset })
    .then((r) => { console.log('seeded', r); return db.close(); })
    .then(() => process.exit(0))
    .catch(async (e) => { console.error(e); await db.close().catch(() => {}); process.exit(1); });
}

module.exports = { seed };
