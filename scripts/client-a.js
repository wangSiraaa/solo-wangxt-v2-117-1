#!/usr/bin/env node
'use strict';

// Demo client A: connects, inserts a paragraph, then keeps editing while
// connected. Used together with client-b.js to show live convergence.
//
// Optional local offline outbox (on by default, OUTBOX=off disables,
// OUTBOX_FILE overrides the location): every edit is first recorded in a
// per-client JSON file with a stable msgId, the full local Yjs state and
// its delivery status. A restart restores the local document and whatever
// was never acked; the client then syncs by state vector first and resends
// the saved items in order — an item leaves the outbox only on a
// successful ack.
//
//   node scripts/client-a.js --offline   # edit with NO connection; the
//                                        # item stays pending, then exit
//   node scripts/client-a.js             # restore, SV-sync, resend, edit

const path = require('node:path');
const { DocClient } = require('./lib-client');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN_A || 'user-alice';
const OFFLINE = process.argv.includes('--offline');
const OUTBOX_FILE = process.env.OUTBOX === 'off'
  ? null
  : (process.env.OUTBOX_FILE || path.join(__dirname, '.outbox', `a-${DOC}.json`));

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function showOutbox(client, label) {
  if (!client.outbox) return;
  const c = client.outboxCounts();
  console.log(`A outbox ${label}: pending=${c.pending} failed=${c.failed}`);
  for (const it of client.outbox.items) {
    if (it.status === 'failed') {
      console.log(`  A outbox failed ${it.msgId}: ${it.error.code} — ${it.error.message}`);
    }
  }
}

async function main() {
  const a = new DocClient({
    url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: true,
    outboxFile: OUTBOX_FILE,
  });
  if (a.outbox) {
    console.log('A outbox file:', a.outbox.file);
    showOutbox(a, 'restored');
    if (a.text) console.log('A restored local text:', JSON.stringify(a.text));
  }

  if (OFFLINE) {
    // Offline editing: no connection at all. The edit is queued into the
    // outbox (and the local document persisted) and nothing is sent.
    if (!a.outbox) console.log('A: OUTBOX=off — this edit is lost on exit.');
    a.localEdit((t) => t.insert(t.length, `A offline edit ${new Date().toISOString()}. `));
    showOutbox(a, 'after offline edit (never connected)');
    console.log('A exits while offline; run again without --offline to sync and resend.');
    return;
  }

  // Reconnect: state-vector catch-up first (only the missing diff comes
  // back), then resend whatever the outbox still holds, in saved order.
  await a.reconnectWithStateVector();
  console.log('A hello-ok role=', a.role, 'seq=', a.seq);

  if (a.outbox && a.outbox.items.length) {
    const results = await a.flush();
    for (const r of results) {
      if (r.ok) {
        console.log(`A resent ${r.msgId}: ok seq=${r.seq}` +
          (r.duplicated ? ' (duplicate — already durable server-side)' : ''));
      } else {
        console.log(`A resend ${r.msgId}: FAILED ${r.code || ''} ${r.message || ''}`);
      }
    }
    showOutbox(a, 'after resend');
  }

  a.localEdit((t) => t.insert(0, 'Hello from A. '));
  await a.flush();
  showOutbox(a, 'after live edit');

  await sleep(400);
  a.localEdit((t) => t.insert(t.length, 'A adds line two.\n'));
  await a.flush();

  await sleep(800);
  console.log('A final text:', JSON.stringify(a.text));
  console.log('A stateHash:', a.stateHash());
  showOutbox(a, 'final');
  a.close();
  await sleep(100);
}

main()
  .then(() => process.exit(0))
  .catch((e) => { console.error('A failed:', e); process.exit(1); });
