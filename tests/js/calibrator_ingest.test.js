const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { spawn } = require('node:child_process');
const crypto = require('node:crypto');

function computeSignature(uuid, timestamp, payload, token) {
  return crypto.createHmac('sha256', token).update(`${uuid}|${timestamp}|${JSON.stringify(payload)}`).digest('hex');
}

test('el script de ingesta firma y envía lecturas por HTTP', async () => {
  const token = 'demo-calibrator-token';
  const serverPort = 8123;
  let receivedBody = null;

  const server = http.createServer((req, res) => {
    if (req.method !== 'POST') {
      res.statusCode = 405;
      return res.end();
    }
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      receivedBody = JSON.parse(body);
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ success: true }));
    });
  });

  await new Promise((resolve) => server.listen(serverPort, resolve));

  const child = spawn(process.execPath, ['tools/scripts/calibrator_ingest.js'], {
    env: {
      ...process.env,
      CALIBRATOR_ID: '1',
      CALIBRATOR_TOKEN: token,
      CALIBRATOR_SOURCE: 'stdin',
      CALIBRATOR_ENDPOINT: `http://localhost:${serverPort}/ingest`
    },
    stdio: ['pipe', 'pipe', 'pipe']
  });

  const payload = { magnitud: 'temperatura', valor: 21.7, unidad: '°C' };
  child.stdin.write(JSON.stringify(payload) + '\n');

  await new Promise((resolve) => setTimeout(resolve, 200));

  child.kill();
  server.close();

  assert.ok(receivedBody, 'El servidor debe recibir la solicitud de ingesta');
  assert.equal(receivedBody.calibrador_id, 1, 'El calibrador enviado debe coincidir con la configuración');
  assert.deepEqual(receivedBody.payload, payload, 'La carga útil debe conservarse');
  assert.ok(receivedBody.measurement_uuid, 'Debe generarse un identificador de medición');
  const expectedSignature = computeSignature(receivedBody.measurement_uuid, receivedBody.timestamp, payload, token);
  assert.equal(receivedBody.firma, expectedSignature, 'La firma HMAC debe ser válida');
});
