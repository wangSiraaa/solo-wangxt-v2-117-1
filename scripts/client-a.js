#!/usr/bin/env node
'use strict';

// Demo client A with an OPTIONAL durable local outbox (enabled by default).
//
// The outbox (scripts/outbox.json/*.json) records document identity, a stable
// msgId, the Yjs update bytes and the pending/error state BEFORE anything is
// sent, plus the full local Y.Doc state. Entries are removed only after an
// ack {ok:true} (server-side dedup via duplicated:true is treated as success).
//
// Reconnect presents the saved state vector first (gap repair), then resends
// the outstanding entries in saved order.
//
// Modes (env DEMO_SCENARIO):
//   normal        online edit, normal ack flow (default)
//   offline       edit WITHOUT connecting, exit; second run reconnects with
//                 the saved state vector and drains the box
//   crash-ack     connect, send one update and hard-exit the process BEFORE
//                 the ack can arrive; second run resends the same msgId and
//                 the server persists exactly ONE row (dedup)
//   status        only print the outbox file (pending count / errors / text)
//   reset         delete this (token, doc) outbox file
//
// Pair "offline" with client-b.js editing online while A is away.

const fs = require('node:fs');
const path = require('node:path');
const { DocClient } = require('./lib-client');
const { keyFor } = require('./outbox');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN_A || 'user-alice';
const SCENARIO = process.env.DEMO_SCENARIO || 'normal';
const OUTBOX_DIR = process.env.OUTBOX_DIR || path.join(__dirname, '..', '.outbox');
const EXIT_DELAY = parseInt(process.env.EXIT_DELAY_MS || '0', 10);

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
    console.log(`[A outbox] (${label}) no file at ${boxFile()}`);
    return;
  }
  const pending = box.items.filter((i) => i.status !== 'acked').length;
  const errs = box.items.filter((i) => i.status === 'error');
  console.log(`[A outbox] (${label}) outstanding=${box.items.length} ` +
    `pending=${box.items.filter((i) => i.status === 'pending').length} ` +
    `error=${errs.length} localState=${box.stateB64 ? 'yes' : 'no'}`);
  for (const i of box.items) {
    const tail = i.lastError ? ` lastError=${i.lastError.code}: ${i.lastError.message}` : '';
    console.log(`    - msgId=${i.msgId} status=${i.status} attempts=${i.attempts}${tail}`);
  }
}

function printReconnectReport(label, a, report) {
  console.log(`[A reconnect:${label}] hello role=${a.role} serverSeq=${a.seq} ` +
    `resent=${report.sent} acked=${report.acked} duplicated=${report.duplicated} failed=${report.failed}`);
  for (const r of report.results) {
    if (r.ok) {
      console.log(`    msgId=${r.msgId} ACK${r.duplicated ? ' (duplicated, server dedup)' : ''} seq=${r.seq}`);
    } else {
      console.log(`    msgId=${r.msgId} FAILED ${r.code}: ${r.message}  [item kept with error]`);
    }
  }
}

async function scenarioNormal() {
  const a = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: true, outboxDir: OUTBOX_DIR });
  printBox('before connect');
  await a.connect();
  console.log('A hello-ok role=', a.role, 'seq=', a.seq);

  a.localEdit((t) => t.insert(0, 'Hello from A. '));
  await a.flush();

  await sleep(400);
  a.localEdit((t) => t.insert(t.length, 'A adds line two.\n'));
  await a.flush();

  await sleep(800);
  console.log('A final text:', JSON.stringify(a.text));
  console.log('A stateHash:', a.stateHash());
  printBox('after flush');
  a.close();
  await sleep(100);
}

async function scenarioOffline() {
  const a = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: true, outboxDir: OUTBOX_DIR });
  const phase = process.env.OFFLINE_PHASE || '1';
  if (phase === '1') {
    // No server contact at all: edit locally, the outbox holds everything.
    console.log('A phase1: OFFLINE edit (no connection attempted)');
    a.localEdit((t) => t.insert(t.length, `[A-offline@${new Date().toISOString()}] `));
    printBox('offline edit persisted');
    console.log('A phase1 exits with', a.outboxSummary().pending, 'pending item(s)');
    return;
  }
  // Phase 2: B has edited online meanwhile. Reconnect WITH the saved state
  // vector (done automatically for restored outboxes), then drain in order.
  console.log('A phase2: restarting, restored local text=', JSON.stringify(a.text));
  printBox('restored from disk');
  await a.connect();
  const report = await a.drainOutbox();
  printReconnectReport('offline', a, report);
  await sleep(500);
  console.log('A merged text:', JSON.stringify(a.text));
  console.log('A stateHash:', a.stateHash());
  printBox('after reconnect');
  a.close();
  await sleep(100);
}

async function scenarioCrashAck() {
  const phase = process.env.CRASH_PHASE || '1';
  if (phase === '1') {
    const a = new DocClient({
      url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: true,
      outboxDir: OUTBOX_DIR, autoDrainOutbox: false, // exactly one manual frame
    });
    await a.connect();
    const stamp = `[A-crashack@${Date.now()}] `;
    a.localEdit((t) => t.insert(t.length, stamp));
    // Take the durable item's stable msgId, put the bytes on the wire ONCE,
    // then hard-exit without waiting for the ack. The server may commit
    // before or after the exit — either way the retry must dedup to one row.
    const item = a.outbox.outstanding().slice(-1)[0];
    a.ws.send(JSON.stringify({ type: 'update', msgId: item.msgId, update: item.update }));
    a.outbox.markAttempt(item.msgId);
    console.log('A crash-ack phase1: sent msgId=', item.msgId, '- exiting NOW, no ack awaited');
    printBox('pre-exit');
    // Exit on the next tick so the frame leaves but no ack can be processed
    // (like SIGKILL right after the write syscall).
    setTimeout(() => process.exit(0), EXIT_DELAY);
    return;
  }
  const a = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: true, outboxDir: OUTBOX_DIR });
  const before = readBoxRaw();
  const pendingMsgIds = before.items.map((i) => i.msgId);
  console.log('A crash-ack phase2: restored', pendingMsgIds.length, 'unacked item(s):', pendingMsgIds.join(','));
  await a.connect();
  const report = await a.drainOutbox();
  printReconnectReport('crash-ack', a, report);
  await sleep(300);
  console.log('A stateHash:', a.stateHash());
  printBox('after resend');

  // Acceptance: the resent update exists exactly ONCE on the server.
  const db = require('../src/db');
  for (const msgId of pendingMsgIds) {
    const r = await db.query(
      'SELECT count(*)::int AS n, COALESCE(MAX(seq),0) AS seq FROM doc_updates WHERE doc_id=$1 AND client_msg_id=$2',
      [DOC, msgId],
    );
    const n = r.rows[0].n;
    console.log(`[A dedup check] msgId=${msgId} server_rows=${n} seq=${r.rows[0].seq} ` +
      (n === 1 ? 'OK (exactly one server update)' : 'FAIL'));
    if (n !== 1) process.exitCode = 1;
  }
  await db.close();
  a.close();
  await sleep(100);
}

async function scenarioStatus() {
  const a = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: false, outboxDir: OUTBOX_DIR });
  console.log('A status: restored text=', JSON.stringify(a.text));
  console.log('A status: stateHash=', a.stateHash());
  printBox('status');
}

async function scenarioReset() {
  try {
    fs.unlinkSync(boxFile());
    console.log('A reset: removed', boxFile());
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
    console.log('A reset: nothing to remove');
  }
}

async function main() {
  console.log(`=== Demo A scenario=${SCENARIO} doc=${DOC} user=${TOKEN} ===`);
  switch (SCENARIO) {
    case 'offline': return scenarioOffline();
    case 'crash-ack': return scenarioCrashAck();
    case 'status': return scenarioStatus();
    case 'reset': return scenarioReset();
    default: return scenarioNormal();
  }
}

main().catch((e) => { console.error('A failed:', e); process.exit(1); });
