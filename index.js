// poll-stripe.js
require('dotenv').config();
const Stripe = require('stripe');
const dayjs = require('dayjs');
const knex = require('knex');
const pino = require('pino');
const tz = require('dayjs/plugin/timezone');
const utc = require('dayjs/plugin/utc');

dayjs.extend(utc);
dayjs.extend(tz);

const log = pino({ level: process.env.LOG_LEVEL || 'info' });

// ========= Config =========
const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
if (!STRIPE_KEY) {
  log.error('Missing STRIPE_SECRET_KEY in .env');
  process.exit(1);
}
const stripe = new Stripe(STRIPE_KEY); // ใช้ค่าเวอร์ชันตามบัญชี

const db = knex({
  client: 'mysql2',
  connection: {
    host: process.env.MYSQL_HOST || '127.0.0.1',
    port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'test'
  },
  pool: { min: 0, max: 10 },
});

const TABLE = process.env.TABLE_NAME || 'payments';
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 60_000);
const TIMEOUT_MINUTES = Number(process.env.TIMEOUT_MINUTES || 15);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 3));

// ฟิลด์ที่สคริปต์นี้คาดหวังในตาราง
// id (PK), stripe_checkout_id (string), status (string), created_at (datetime)
// คุณสามารถเปลี่ยนชื่อฟิลด์ได้ด้านล่างในโค้ดตรงจุดที่อ่าน/อัปเดต

// ========= Utilities =========
const sleep = ms => new Promise(r => setTimeout(r, ms));

// limiter แบบง่าย
async function mapWithConcurrency(items, limit, worker) {
  const ret = [];
  let i = 0;
  let active = 0;
  let resolveAll;
  const done = new Promise(r => (resolveAll = r));

  function runNext() {
    if (i >= items.length && active === 0) {
      resolveAll();
      return;
    }
    while (active < limit && i < items.length) {
      const idx = i++;
      active++;
      Promise.resolve(worker(items[idx], idx))
        .then(res => (ret[idx] = res))
        .catch(err => {
          log.error({ err }, 'Worker error');
          ret[idx] = undefined;
        })
        .finally(() => {
          active--;
          runNext();
        });
    }
  }
  runNext();
  await done;
  return ret;
}

// แปลงผล Stripe -> สถานะใน DB
function deriveStatusFromStripe(session) {
  // session.status: 'open' | 'complete' | 'expired'
  // session.payment_status: 'paid' | 'unpaid' | 'no_payment_required'
  if (!session) return null;

  if (session.status === 'complete' || session.payment_status === 'paid') {
    return 'PAID';
  }
  if (session.status === 'expired') {
    return 'PAYMENT_EXP'; // หรือ 'EXPIRED' ตามที่ต้องการ
  }
  if (session.status === 'open') {
    return 'WAIT_PAYMENT';
  }
  return null;
}

// ========= Core logic =========
let looping = false;

async function fetchPendingRows(trx) {
  // รองรับสองค่าชื่อสถานะ
  return trx(TABLE)
    .select('id', 'checkout_id as stripe_checkout_id', 'status', 'created_date as created_at')
    .whereIn('status', ['WAIT_PAYMENT', 'WAITPAYMENT']);
}

async function updateStatus(trx, id, fromStatuses, toStatus, extra = {}) {
  const q = trx(TABLE).where('id', id).whereIn('status', fromStatuses);
  return q.update({ status: toStatus, updated_date: trx.fn.now(), ...extra });
}

async function handleRow(row) {
  const { id, stripe_checkout_id, status, created_at } = row;
  if (!stripe_checkout_id) {
    log.warn({ id }, 'Row missing stripe_checkout_id, marking CANCELED');
    await db.transaction(async trx => {
      await updateStatus(trx, id, ['WAIT_PAYMENT', 'WAITPAYMENT'], 'PAYMENT_EXP');
    });
    return;
  }

  // 1) TIMEOUT เชิงเวลา
  console.log('status', status);

  const ageMin = dayjs().diff(dayjs(created_at), "minute");
  console.log(ageMin);

  if (ageMin >= TIMEOUT_MINUTES) {
    log.info({ id, ageMin }, 'Timeout -> mark TIMEOUT');
    await db.transaction(async trx => {
      await updateStatus(trx, id, ['WAIT_PAYMENT', 'WAITPAYMENT'], 'PAYMENT_EXP');
    });
    const session = await stripe.checkout.sessions.expire(
      stripe_checkout_id
    );
    return;
  }

  // 2) เช็คกับ Stripe
  let session;
  try {
    session = await stripe.checkout.sessions.retrieve(stripe_checkout_id, {
      expand: ['payment_intent'],
    });
  } catch (err) {
    // ถ้า 404 ให้ถือว่า session ไม่อยู่แล้ว -> หมดอายุ/ยกเลิก
    if (err && err.statusCode === 404) {
      log.info({ id, stripe_checkout_id }, 'Stripe session not found -> CANCELED');
      await db.transaction(async trx => {
        await updateStatus(trx, id, ['WAIT_PAYMENT', 'WAITPAYMENT'], 'PAYMENT_EXP');
      });
      return;
    }
    log.error({ err, id, stripe_checkout_id }, 'Stripe retrieve error');
    return; // ข้ามรายการนี้ไปก่อน
  }

  const newStatus = deriveStatusFromStripe(session);
  if (!newStatus) return;

  if (newStatus === 'PAID') {
    // ตัวอย่าง: เก็บ paid_at ด้วย
    await db.transaction(async trx => {
      await updateStatus(trx, id, ['WAIT_PAYMENT', 'WAITPAYMENT'], 'ACTIVE', {
        paid_at: trx.fn.now(),
      });
    });
    log.info({ id }, 'Updated -> PAID');
    return;
  }

  if (newStatus === 'PAYMENT_EXP') {
    await db.transaction(async trx => {
      await updateStatus(trx, id, ['WAIT_PAYMENT', 'WAITPAYMENT'], 'PAYMENT_EXP');
    });
    log.info({ id }, 'Updated -> CANCELED');
    return;
  }

  // ถ้ายังเปิดอยู่ก็ปล่อยค้างไว้
  log.debug({ id, stripe_status: session.status, payment_status: session.payment_status }, 'Still waiting');
}

let timer = null;
let running = false;

async function tick() {
  if (running) {
    log.warn('Previous tick still running, skip this round.');
    return;
  }
  running = true;
  const startedAt = Date.now();
  try {
    await db.transaction(async trx => {
      const rows = await fetchPendingRows(trx);
      if (!rows.length) {
        log.info('No pending rows.');
        return;
      }
      log.info({ count: rows.length }, 'Processing pending rows...');
      await mapWithConcurrency(rows, CONCURRENCY, handleRow);
    });
  } catch (err) {
    log.error({ err }, 'Tick error');
  } finally {
    running = false;
    const took = Date.now() - startedAt;
    log.info({ tookMs: took }, 'Tick finished');
  }
}

function startLoop() {
  if (looping) return;
  looping = true;
  log.info({ everyMs: POLL_INTERVAL_MS, timeoutMin: TIMEOUT_MINUTES }, 'Stripe poller started');
  // run immediately once, แล้วค่อยตั้ง interval
  tick().finally(() => {
    timer = setInterval(tick, POLL_INTERVAL_MS);
  });
}

async function shutdown() {
  log.info('Shutting down...');
  if (timer) clearInterval(timer);
  // รอรอบที่กำลังทำงานจบ
  while (running) {
    log.info('Waiting current tick to end...');
    await sleep(500);
  }
  await db.destroy();
  log.info('Bye');
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startLoop();
