const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const path = require('node:path');

const host = '127.0.0.1';
const port = 8095;
let phpServer;

const projectRoot = path.join(__dirname, '..', '..');
const tokensFile = path.join(projectRoot, 'storage', 'api_tokens.json');

test.before(async () => {
  try {
    fs.writeFileSync(tokensFile, '[]\n', 'utf8');
  } catch (error) {
    // Ignora si no existe todavía
  }

  phpServer = spawn('php', ['-S', `${host}:${port}`, '-t', 'public'], {
    cwd: projectRoot,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  await waitForServer(phpServer);
});

test.after(async () => {
  if (phpServer) {
    phpServer.kill('SIGTERM');
    try {
      await once(phpServer, 'exit');
    } catch (error) {
      // Ignorar salida forzada
    }
  }
});

async function waitForServer(proc) {
  return new Promise((resolve, reject) => {
    const onData = data => {
      const text = data.toString();
      if (text.includes('Development Server')) {
        cleanup();
        resolve();
      }
    };

    const onError = error => {
      cleanup();
      reject(error);
    };

    const onExit = code => {
      cleanup();
      reject(new Error(`El servidor PHP terminó inesperadamente con código ${code}`));
    };

    const cleanup = () => {
      proc.stdout.off('data', onData);
      proc.stdout.off('error', onError);
      proc.stderr.off('data', onData);
      proc.stderr.off('error', onError);
      proc.off('exit', onExit);
    };

    proc.stdout.on('data', onData);
    proc.stdout.on('error', onError);
    proc.stderr.on('data', onData);
    proc.stderr.on('error', onError);
    proc.on('exit', onExit);
  });
}

async function requestToken(scopes, rateLimit = { limit: 3, window: 60 }) {
  const res = await fetch(`http://${host}:${port}/backend/api/v1/tokens`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: 'suite-test',
      scopes,
      ttl: 300,
      rate_limit: rateLimit
    })
  });

  const body = await res.json();
  return { res, body };
}

test('crea un token y consume el endpoint de instrumentos', async () => {
  const { res: tokenRes, body: tokenBody } = await requestToken(['instrumentos.read']);
  assert.equal(tokenRes.status, 201, 'La creación del token debe responder 201.');
  assert.ok(tokenBody.access_token, 'La respuesta debe incluir access_token.');

  const instrumentosRes = await fetch(`http://${host}:${port}/backend/api/v1/instrumentos`, {
    headers: {
      Authorization: `Bearer ${tokenBody.access_token}`
    }
  });

  assert.equal(instrumentosRes.status, 200, 'El endpoint de instrumentos debe responder 200.');
  const data = await instrumentosRes.json();
  assert.equal(Array.isArray(data), true, 'La respuesta debe ser un arreglo.');
  assert.equal(data.length >= 2, true, 'La muestra debe contener registros.');
  assert.equal(data[0].codigo, 'EQ-001');
});

test('calibraciones requiere scope específico', async () => {
  const { body } = await requestToken(['instrumentos.read']);
  const res = await fetch(`http://${host}:${port}/backend/api/v1/calibraciones`, {
    headers: {
      Authorization: `Bearer ${body.access_token}`
    }
  });

  assert.equal(res.status, 403, 'Sin el scope de calibraciones la API debe rechazar la petición.');
  const payload = await res.json();
  assert.ok(payload.error.includes('permisos'));
});

test('el rate limit devuelve 429 al exceder la cuota', async () => {
  const { body } = await requestToken(['instrumentos.read'], { limit: 1, window: 60 });
  const url = `http://${host}:${port}/backend/api/v1/instrumentos`;
  const headers = { Authorization: `Bearer ${body.access_token}` };

  const first = await fetch(url, { headers });
  assert.equal(first.status, 200, 'La primera petición debe aceptarse.');

  const second = await fetch(url, { headers });
  assert.equal(second.status, 429, 'La segunda petición debe ser bloqueada por el rate limit.');
  const errorPayload = await second.json();
  assert.ok(errorPayload.error.includes('Límite'));
});
