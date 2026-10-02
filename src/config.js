'use strict';

// Gateway configuration. In production tokens/secrets come from the
// environment; defaults point at the local user-space PostgreSQL used in
// development and tests.
const env = process.env;

module.exports = {
  http: {
    host: env.HOST || '127.0.0.1',
    port: parseInt(env.PORT || '7777', 10),
  },
  pg: {
    host: env.PGHOST || '/tmp',
    port: parseInt(env.PGPORT || '55432', 10),
    database: env.PGDATABASE || 'collab',
    user: env.PGUSER || 'collab',
    password: env.PGPASSWORD || '',
    max: parseInt(env.PGPOOL || '10', 10),
  },
};
