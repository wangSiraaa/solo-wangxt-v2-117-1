#!/usr/bin/env node
'use strict';

// Demo client B: connects, deletes/changes overlapping ranges with A, then
// prints final state. Both clients print the structural state hash, which
// must be identical (true CRDT convergence, not just equal-looking strings).
//
// Like client A, B keeps an optional local offline outbox (OUTBOX=off
// disables, OUTBOX_FILE overrides): on startup it restores its local
// document and un-acked items, syncs by state vector, resends anything
// still pending in saved order, and prints the outbox counts.

const path = require('node:path');
const { DocClient } = require('./lib-client');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN_B || 'user-bob';
const OUTBOX_FILE = process.env.OUTBOX === 'off'
  ? null
  : (process.env.OUTBOX_FILE || path.join(__dirname, '.outbox', `b-${DOC}.json`));

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function showOutbox(client, label) {
  if (!client.outbox) return;
  const c = client.outboxCounts();
  console.log(`B outbox ${label}: pending=${c.pending} failed=${c.failed}`);
  for (const it of client.outbox.items) {
    if (it.status === 'failed') {
      console.log(`  B outbox failed ${it.msgId}: ${it.error.code} — ${it.error.message}`);
    }
  }
}

async function main() {
  const b = new DocClient({
    url: URL, token: TOKEN, docId: DOC, name: 'B', verbose: true,
    outboxFile: OUTBOX_FILE,
  });
  if (b.outbox) {
    console.log('B outbox file:', b.outbox.file);
    showOutbox(b, 'restored');
  }

  // State-vector catch-up first, then ordered resend of saved items.
  await b.reconnectWithStateVector();
  console.log('B hello-ok role=', b.role, 'seq=', b.seq, 'text on join=', JSON.stringify(b.text));

  if (b.outbox && b.outbox.items.length) {
    const results = await b.flush();
    for (const r of results) {
      if (r.ok) {
        console.log(`B resent ${r.msgId}: ok seq=${r.seq}` +
          (r.duplicated ? ' (duplicate — already durable server-side)' : ''));
      } else {
        console.log(`B resend ${r.msgId}: FAILED ${r.code || ''} ${r.message || ''}`);
      }
    }
    showOutbox(b, 'after resend');
  }

  await sleep(150);
  b.localEdit((t) => t.insert(t.length, 'B was here too. '));
  await b.flush();
  showOutbox(b, 'after live edit');

  await sleep(300);
  // Concurrent edit against A's second insertion: replace prefix.
  b.localEdit((t) => {
    if (t.length >= 5) t.delete(0, 5);
    t.insert(0, 'HELLO');
  });
  await b.flush();

  await sleep(900);
  console.log('B final text:', JSON.stringify(b.text));
  console.log('B stateHash:', b.stateHash());
  showOutbox(b, 'final');
  b.close();
  await sleep(100);
}

main()
  .then(() => process.exit(0))
  .catch((e) => { console.error('B failed:', e); process.exit(1); });
