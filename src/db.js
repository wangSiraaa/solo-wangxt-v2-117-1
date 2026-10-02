'use strict';

const { Pool } = require('pg');
const config = require('./config');

const pool = new Pool({
  host: config.pg.host,
  port: config.pg.port,
  database: config.pg.database,
  user: config.pg.user,
  password: config.pg.password,
  max: config.pg.max,
});

pool.on('error', (err) => {
  // A pooled connection died; pg re-establishes, but log loudly because
  // silent pool failures can look like "lost updates".
  console.error('[pg] idle client error:', err.message);
});

module.exports = {
  pool,
  query: (text, params) => pool.query(text, params),
  getClient: () => pool.connect(),
  close: () => pool.end(),
};
