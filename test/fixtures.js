'use strict';

// Test fixture: reset operational tables and ensure all test users/docs
// exist. Tests create fresh documents on demand; members here cover the
// permission scenarios.

const db = require('../src/db');

async function setupTest() {
  await db.query(`TRUNCATE update_errors, doc_snapshots, doc_updates,
                  document_members, documents, users, tenants RESTART IDENTITY CASCADE`);

  await db.query(`INSERT INTO tenants (id,name) VALUES
    ('tenant-acme','ACME'),('tenant-globex','Globex')`);
  await db.query(`INSERT INTO users (id,tenant_id,name) VALUES
    ('user-alice','tenant-acme','Alice'),
    ('user-bob','tenant-acme','Bob'),
    ('user-carol','tenant-acme','Carol'),
    ('user-owner','tenant-acme','Owner'),
    ('user-dave','tenant-globex','Dave'),
    ('user-nobody','tenant-acme','Nobody')`);
  await db.query(`INSERT INTO documents (id,tenant_id,title) VALUES
    ('doc-demo','tenant-acme','Demo')`);
  await db.query(`INSERT INTO document_members (doc_id,user_id,role) VALUES
    ('doc-demo','user-alice','writer'),
    ('doc-demo','user-bob','writer'),
    ('doc-demo','user-carol','reader'),
    ('doc-demo','user-owner','owner')`);
}

async function createDoc(docId, { writers = [], readers = [], owners = [] } = {}) {
  await db.query(
    `INSERT INTO documents (id, tenant_id, title) VALUES ($1,'tenant-acme',$1)
     ON CONFLICT (id) DO NOTHING`,
    [docId],
  );
  for (const u of writers) {
    await db.query(
      `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,$2,'writer')
       ON CONFLICT (doc_id,user_id) DO UPDATE SET role='writer', revoked_at=NULL`,
      [docId, u],
    );
  }
  for (const u of readers) {
    await db.query(
      `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,$2,'reader')
       ON CONFLICT (doc_id,user_id) DO UPDATE SET role='reader', revoked_at=NULL`,
      [docId, u],
    );
  }
  for (const u of owners) {
    await db.query(
      `INSERT INTO document_members (doc_id,user_id,role) VALUES ($1,$2,'owner')
       ON CONFLICT (doc_id,user_id) DO UPDATE SET role='owner', revoked_at=NULL`,
      [docId, u],
    );
  }
}

module.exports = { setupTest, createDoc };

if (require.main === module) {
  setupTest()
    .then(() => db.close())
    .then(() => console.log('test fixtures reset'))
    .catch(async (e) => { console.error(e); await db.close().catch(() => {}); process.exit(1); });
}
