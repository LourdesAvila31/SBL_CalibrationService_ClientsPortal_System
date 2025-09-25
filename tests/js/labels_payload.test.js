const test = require('node:test');
const assert = require('node:assert/strict');
const { createServer } = require('../../tools/scripts/test-server');
const LabelUtils = require('../../public/assets/scripts/labels.js');

const port = 3005;
const server = createServer();

test.before(() => new Promise(resolve => server.listen(port, resolve)));

test.after(() => new Promise(resolve => server.close(resolve)));

test('Label payload API returns normalized data', async () => {
  const res = await fetch(`http://localhost:${port}/api/label-payload?id=1`);
  assert.equal(res.status, 200);
  const data = await res.json();
  assert.equal(data.instrument_id, 1);
  assert.match(data.certificate_url, /share_certificate\.php\?token=/);
  const normalized = LabelUtils.normalizePayload(data);
  assert.equal(normalized.identifier, 'BAS-001');
  assert.equal(normalized.has_certificate, true);
  const baseUrl = `http://localhost:${port}/public`;
  const absolute = LabelUtils.buildCertificateUrl(baseUrl, normalized.certificate_url);
  assert.equal(absolute, `${baseUrl}/backend/calibraciones/share_certificate.php?token=abc.def`);
});

test('parseInstrumentId handles invalid values', () => {
  assert.equal(LabelUtils.parseInstrumentId('?instrument_id=12'), 12);
  assert.equal(LabelUtils.parseInstrumentId('?instrument_id=abc'), null);
  assert.equal(LabelUtils.parseInstrumentId(''), null);
});
