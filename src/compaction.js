'use strict';

const db = require('./db');
const yutil = require('./yutil');

// Compaction contract:
//   After compaction through seq N, the document MUST remain recoverable as
//   snapshot(N) + updates(seq > N). The folded updates are marked
//   compressed_in (kept for audit by default; tests verify both the
//   "snapshot+tail" and the "tail deleted" recovery paths).
//
// Runs inside the per-doc serial queue, so no update can interleave between
// state computation and through_seq freezing.

async function compact(room, { deleteFolded = false, minUpdates = 1 } = {}) {
  return room.enqueue(async () => {
    const client = await db.getClient();
    try {
      await client.query('BEGIN');
      await client.query(
        `SELECT id FROM documents WHERE id = $1 FOR UPDATE`,
        [room.docId],
      );

      const sinceRes = await client.query(
        `SELECT COALESCE(MAX(through_seq), 0) AS since FROM doc_snapshots WHERE doc_id = $1`,
        [room.docId],
      );
      const since = Number(sinceRes.rows[0].since);

      const rowsRes = await client.query(
        `SELECT seq, update_bytes
           FROM doc_updates
          WHERE doc_id = $1 AND seq > $2 AND compressed_in IS NULL
          ORDER BY seq ASC`,
        [room.docId, since],
      );
      const rows = rowsRes.rows;
      if (rows.length < minUpdates) {
        await client.query('ROLLBACK');
        return { compacted: false, reason: 'not-enough-updates', count: rows.length };
      }

      // Fold onto the previous snapshot (room.doc already contains that
      // state because load() applied the same history).
      const Y = require('yjs');
      const work = yutil.createDoc();
      const prevSnap = await client.query(
        `SELECT state_bytes FROM doc_snapshots
          WHERE doc_id = $1 ORDER BY id DESC LIMIT 1`,
        [room.docId],
      );
      if (prevSnap.rows[0]) {
        Y.applyUpdate(work, new Uint8Array(prevSnap.rows[0].state_bytes), 'prev-snap');
      }
      for (const r of rows) {
        Y.applyUpdate(work, new Uint8Array(r.update_bytes), `seq:${r.seq}`);
      }

      const throughSeq = Number(rows[rows.length - 1].seq);
      const stateBytes = yutil.encodeState(work);
      const stateHash = yutil.sha256(stateBytes);

      // Independent verification #1 (in-transaction): the folded state must
      // equal the LIVE room document, which received every update in order
      // through normal traffic — a different construction path.
      const liveBytes = yutil.encodeState(room.doc);
      if (!liveBytes.equals(stateBytes)) {
        await client.query('ROLLBACK');
        throw new Error('compaction verification mismatch: folded replay != live document');
      }

      const snapIns = await client.query(
        `INSERT INTO doc_snapshots (doc_id, through_seq, state_bytes, state_hash, update_count)
         VALUES ($1,$2,$3,$4,$5)
         RETURNING id`,
        [room.docId, throughSeq, stateBytes, stateHash, rows.length],
      );
      const snapshotId = snapIns.rows[0].id;

      await client.query(
        `UPDATE doc_updates SET compressed_in = $1
          WHERE doc_id = $2 AND seq <= $3 AND compressed_in IS NULL`,
        [snapshotId, room.docId, throughSeq],
      );

      if (deleteFolded) {
        await client.query(
          `DELETE FROM doc_updates WHERE doc_id = $1 AND compressed_in = $2`,
          [room.docId, snapshotId],
        );
      }
      await client.query('COMMIT');

      // Independent verification #2 (post-commit): rebuild using ONLY what a
      // crashing process would have on restart — latest snapshot plus the
      // surviving tail — and demand byte-identical recovery. This is the
      // "compacted log must still restore the document" guarantee, checked
      // immediately after the destructive path.
      const recovered = await recoverFromStore(room.docId);
      const recoveredBytes = yutil.encodeState(recovered);
      const liveBytes2 = yutil.encodeState(room.doc);
      if (!recoveredBytes.equals(liveBytes2)) {
        throw new Error('post-compaction recovery mismatch: stored snapshot+tail != live document');
      }

      room.compactions += 1;
      return {
        compacted: true, throughSeq, stateHash,
        folded: rows.length, snapshotId, deleted: deleteFolded,
      };
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
  });
}

// Full-history rebuild (latest snapshot + all remaining tail). Used by
// compaction itself and by the recovery verifier after crash/compaction.
async function rebuildVerified(client, docId) {
  const snapRes = await client.query(
    `SELECT state_bytes, through_seq FROM doc_snapshots
      WHERE doc_id = $1 ORDER BY id DESC LIMIT 1`,
    [docId],
  );
  const snap = snapRes.rows[0] || null;
  const fromSeq = snap ? Number(snap.through_seq) : 0;
  const rowsRes = await client.query(
    `SELECT seq, update_bytes FROM doc_updates
      WHERE doc_id = $1 AND seq > $2 ORDER BY seq ASC
      FOR SHARE`,
    [docId, fromSeq],
  );
  return yutil.rebuild(snap ? snap.state_bytes : null, rowsRes.rows);
}

async function recoverFromStore(docId) {
  // Uses a pooled connection; independent of any in-memory room.
  const snapRes = await db.query(
    `SELECT state_bytes, through_seq FROM doc_snapshots
      WHERE doc_id = $1 ORDER BY id DESC LIMIT 1`,
    [docId],
  );
  const snap = snapRes.rows[0] || null;
  const fromSeq = snap ? Number(snap.through_seq) : 0;
  const rowsRes = await db.query(
    `SELECT seq, update_bytes FROM doc_updates
      WHERE doc_id = $1 AND seq > $2 ORDER BY seq ASC`,
    [docId, fromSeq],
  );
  return yutil.rebuild(snap ? snap.state_bytes : null, rowsRes.rows);
}

module.exports = { compact, rebuildVerified, recoverFromStore };
