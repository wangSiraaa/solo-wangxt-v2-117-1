#!/usr/bin/env node
'use strict';

// Demo client B with the same OPTIONAL durable local outbox (enabled by
// default). Two roles are demonstrated:
//
//   TOKEN_B=user-bob   (writer): online edits while A is offline; the state
//                      vector reconnect merges both sides on A's return.
//   TOKEN_B=user-carol (reader): connects/syncs fine, but every update is
//                      nacked READ_ONLY. Those items STAY in the outbox with
//                      the error printed — they are never silently cleared.
//
// Modes (env DEMO_SCENARIO):
//   online    online edits, prints convergence state (default)
//   offline   edit while disconnected, then reconnect/drain on phase 2
//   reader    reader-role failure retention (auto-selected for user-carol)
//   status    print the local outbox file only
//   reset     delete this (token, doc) outbox file

const fs = require('node:fs');
const path = require('node:path');
const { DocClient } = require('./lib-client');
const { keyFor } = require('./outbox');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN_B || 'user-bob';
const SCENARIO = process.env.DEMO_SCENARIO ||
  (TOKEN === 'user-carol' ? 'reader' : 'online');
const OUTBOX_DIR = process.env.OUTBOX_DIR || path.join(__dirname, '..', '.outbox');

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function boxFile() { return path.join(OUTBOX_DIR, keyFor(TOKEN, DOC)); }

function readBoxRaw() {
  try { return JSON.parse(fs.readFileSync(boxFile(), 'utf8')); }
  catch (e) {
    if (e.code === 'ENOENT') return null;
    throw e;
  }
}

function printBox(label) {
  const box = readBoxRaw();
  if (!box) {
    console.log(`[B outbox] (${label}) no file at ${boxFile()}`);
    return;
  }
  const errs = box.items.filter((i) => i.status === 'error');
  console.log(`[B outbox] (${label}) outstanding=${box.items.length} ` +
    `pending=${box.items.filter((i) => i.status === 'pending').length} ` +
    `error=${errs.length} localState=${box.stateB64 ? 'yes' : 'no'}`);
  for (const i of box.items) {
    const tail = i.lastError ? ` lastError=${i.lastError.code}: ${i.lastError.message}` : '';
    console.log(`    - msgId=${i.msgId} status=${i.status} attempts=${i.attempts}${tail}`);
  }
}

function printReconnectReport(label, b, report) {
  console.log(`[B reconnect:${label}] hello role=${b.role} serverSeq=${b.seq} ` +
    `resent=${report.sent} acked=${report.acked} duplicated=${report.duplicated} failed=${report.failed}`);
  for (const r of report.results) {
    if (r.ok) {
      console.log(`    msgId=${r.msgId} ACK${r.duplicated ? ' (duplicated, server dedup)' : ''} seq=${r.seq}`);
    } else {
      console.log(`    msgId=${r.msgId} FAILED ${r.code}: ${r.message}  [item kept with error]`);
    }
  }
}

async function scenarioOnline() {
  const b = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'B', verbose: true, outboxDir: OUTBOX_DIR });
  printBox('before connect');
  await b.connect();
  console.log('B hello-ok role=', b.role, 'seq=', b.seq, 'text on join=', JSON.stringify(b.text));

  await sleep(150);
  b.localEdit((t) => t.insert(t.length, 'B was here too. '));
  await b.flush();

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
  printBox('after flush');
  b.close();
  await sleep(100);
}

async function scenarioOffline() {
  const b = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'B', verbose: true, outboxDir: OUTBOX_DIR });
  const phase = process.env.OFFLINE_PHASE || '1';
  if (phase === '1') {
    console.log('B phase1: OFFLINE edit (no connection attempted)');
    b.localEdit((t) => t.insert(t.length, `[B-offline@${new Date().toISOString()}] `));
    printBox('offline edit persisted');
    console.log('B phase1 exits with', b.outboxSummary().pending, 'pending item(s)');
    return;
  }
  console.log('B phase2: restarting, restored local text=', JSON.stringify(b.text));
  printBox('restored from disk');
  await b.connect();
  const report = await b.drainOutbox();
  printReconnectReport('offline', b, report);
  await sleep(500);
  console.log('B merged text:', JSON.stringify(b.text));
  console.log('B stateHash:', b.stateHash());
  printBox('after reconnect');
  b.close();
  await sleep(100);
}

async function scenarioReader() {
  const b = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'B(reader)', verbose: true, outboxDir: OUTBOX_DIR });
  printBox('start');
  await b.connect();
  console.log('B hello-ok role=', b.role, '(reader may connect and sync, text=', JSON.stringify(b.text) + ')');

  // Two local writes; the server rejects every one with READ_ONLY. Drain
  // via the outbox (failure-tolerant, saved order) so both items are
  // attempted and both durable items survive with their error.
  b.localEdit((t) => t.insert(t.length, '[reader edit 1] '));
  b.localEdit((t) => t.insert(t.length, '[reader edit 2] '));
  const r1 = await b.drainOutbox();
  printReconnectReport('reader', b, r1);
  await sleep(200);
  console.log('B stateHash:', b.stateHash());
  printBox('after READ_ONLY nacks');

  // Reconnect and drain again: same items retried in saved order, same
  // failure, error refreshed — nothing disappears.
  b.hardClose();
  await sleep(200);
  await b.connect();
  const report = await b.drainOutbox();
  printReconnectReport('reader-retry', b, report);
  await sleep(200);
  printBox('after retry (failures retained)');
  console.log('B local text (rejected edits exist only on this client, never on the server):', JSON.stringify(b.text));
  b.close();
  await sleep(100);
}

async function scenarioStatus() {
  const b = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'B', verbose: false, outboxDir: OUTBOX_DIR });
  console.log('B status: restored text=', JSON.stringify(b.text));
  console.log('B status: stateHash=', b.stateHash());
  printBox('status');
}

async function scenarioReset() {
  try {
    fs.unlinkSync(boxFile());
    console.log('B reset: removed', boxFile());
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
    console.log('B reset: nothing to remove');
  }
}

async function main() {
  console.log(`=== Demo B scenario=${SCENARIO} doc=${DOC} user=${TOKEN} ===`);
  switch (SCENARIO) {
    case 'offline': return scenarioOffline();
    case 'reader': return scenarioReader();
    case 'status': return scenarioStatus();
    case 'reset': return scenarioReset();
    default: return scenarioOnline();
  }
}

main().catch((e) => { console.error('B failed:', e); process.exit(1); });
