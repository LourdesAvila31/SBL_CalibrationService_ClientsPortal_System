const test = require('node:test');
const assert = require('node:assert/strict');

const { server } = require('../../tools/scripts/test-server');

const port = 3001;

// start server before tests

test.before(() => {
  return new Promise(resolve => server.listen(port, resolve));
});

// stop server after tests

test.after(() => {
  return new Promise(resolve => server.close(resolve));
});

// test endpoint

test('GET /api/instruments returns dataset', async () => {
  const res = await fetch(`http://localhost:${port}/api/instruments`);
  assert.equal(res.status, 200);
  const data = await res.json();
  assert.equal(Array.isArray(data), true);
  assert.equal(data.length, 12);
  assert.equal(data[0].instrumento, 'Aerotest');
  data.forEach((inst, idx) => {
    assert.equal(typeof inst.id, 'number');
    assert.equal(inst.id, idx + 1);
  });
});