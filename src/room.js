'use strict';

const Y = require('yjs');
const db = require('./db');
const yutil = require('./yutil');

// One Room per document:
//  - in-memory Y.Doc (gc=false) rebuilt from snapshot + tail on first use
//  - a strict serial async queue so apply order == persisted seq order
//  - set of connected sockets, each carrying authenticated context

class Room {
  constructor(docId) {
    this.docId = docId;
    this.doc = yutil.createDoc();
    this.sockets = new Set();
    this.tail = Promise.resolve();
    this.loadedSeq = 0;
    this.compactions = 0;
  }

  enqueue(job) {
    const run = this.tail.then(() => job());
    // Keep the chain alive even if this job rejects; the job itself is
    // expected to convert failures into error frames / rows.
    this.tail = run.then(
      () => {},
      () => {},
    );
    return run;
  }

  broadcast(message, exceptSocket = null) {
    const data = JSON.stringify(message);
    for (const ws of this.sockets) {
      if (ws === exceptSocket) continue;
      if (ws.readyState === ws.OPEN) {
        ws.send(data);
      }
    }
  }

  // Load authoritative state: latest snapshot + every update past it.
  async load() {
    const snapRes = await db.query(
      `SELECT id, through_seq, state_bytes
         FROM doc_snapshots
        WHERE doc_id = $1
        ORDER BY id DESC
        LIMIT 1`,
      [this.docId],
    );
    const snap = snapRes.rows[0] || null;
    const fromSeq = snap ? Number(snap.through_seq) : 0;
    const updRes = await db.query(
      `SELECT seq, update_bytes
         FROM doc_updates
        WHERE doc_id = $1 AND seq > $2
        ORDER BY seq ASC`,
      [this.docId, fromSeq],
    );
    if (snap) {
      Y.applyUpdate(this.doc, new Uint8Array(snap.state_bytes), 'snapshot');
    }
    for (const row of updRes.rows) {
      Y.applyUpdate(this.doc, new Uint8Array(row.update_bytes), `seq:${row.seq}`);
    }
    this.loadedSeq = updRes.rows.length
      ? Number(updRes.rows[updRes.rows.length - 1].seq)
      : fromSeq;
    return { fromSeq, applied: updRes.rows.length };
  }
}

const rooms = new Map();

async function getRoom(docId) {
  let room = rooms.get(docId);
  if (!room) {
    room = new Room(docId);
    rooms.set(docId, room);
    await room.load();
  }
  return room;
}

function roomCount() {
  return rooms.size;
}

module.exports = { getRoom, roomCount, Room };
