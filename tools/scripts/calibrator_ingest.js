#!/usr/bin/env node
/**
 * Servicio de ingesta de lecturas de calibradores físicos.
 *
 * Permite leer datos desde un puerto serial (usando la biblioteca `serialport`)
 * o desde un socket TCP, firmar la carga útil con HMAC-SHA256 y enviarla al
 * backend PHP del sistema.
 */

const crypto = require('crypto');
const net = require('net');
const http = require('http');
const https = require('https');
const { URL } = require('url');
const { setTimeout: sleep } = require('timers/promises');
let SerialPort;
let ReadlineParser;

try {
    ({ SerialPort, ReadlineParser } = require('serialport'));
} catch (err) {
    SerialPort = null;
    ReadlineParser = null;
}

const endpoint = process.env.CALIBRATOR_ENDPOINT || 'http://localhost:8000/backend/integraciones/calibradores/store_measurement.php';
const calibradorId = parseInt(process.env.CALIBRATOR_ID || '1', 10);
const calibradorToken = process.env.CALIBRATOR_TOKEN || 'demo-calibrator-token';
const instrumentId = process.env.CALIBRATOR_INSTRUMENT_ID ? parseInt(process.env.CALIBRATOR_INSTRUMENT_ID, 10) : undefined;
const source = process.env.CALIBRATOR_SOURCE || 'tcp://localhost:9000';
const ackDelayMs = parseInt(process.env.CALIBRATOR_ACK_DELAY || '0', 10);
const pendingEnvios = new Set();

if (!Number.isInteger(calibradorId) || calibradorId <= 0) {
    console.error('[ingest] Debes definir CALIBRATOR_ID con el identificador del dispositivo.');
    process.exit(1);
}

if (!calibradorToken) {
    console.error('[ingest] Debes definir CALIBRATOR_TOKEN con el token del dispositivo.');
    process.exit(1);
}

function firmarLectura(uuid, timestamp, payload) {
    const mensaje = `${uuid}|${timestamp}|${JSON.stringify(payload)}`;
    return crypto.createHmac('sha256', calibradorToken).update(mensaje).digest('hex');
}

async function postJson(urlString, body) {
    const url = new URL(urlString);
    const data = JSON.stringify(body);

    const options = {
        method: 'POST',
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(data),
        },
        family: url.hostname === 'localhost' ? 4 : undefined,
    };

    const transport = url.protocol === 'https:' ? https : http;

    return new Promise((resolve, reject) => {
        const req = transport.request(options, (res) => {
            let responseBody = '';
            res.setEncoding('utf8');
            res.on('data', (chunk) => {
                responseBody += chunk;
            });
            res.on('end', () => {
                if (res.statusCode < 200 || res.statusCode >= 300) {
                    const error = new Error(`HTTP ${res.statusCode}`);
                    error.statusCode = res.statusCode;
                    error.responseBody = responseBody;
                    reject(error);
                    return;
                }

                if (!responseBody) {
                    resolve({});
                    return;
                }

                try {
                    resolve(JSON.parse(responseBody));
                } catch (parseError) {
                    parseError.responseBody = responseBody;
                    reject(parseError);
                }
            });
        });

        req.on('error', reject);
        req.setTimeout(2000, () => {
            req.destroy(new Error('timeout')); 
        });
        req.write(data);
        req.end();
    });
}

async function enviarLectura(payload) {
    const timestamp = new Date().toISOString();
    const measurementUuid = payload.measurement_uuid || crypto.randomUUID();

    const cuerpo = {
        calibrador_id: calibradorId,
        measurement_uuid: measurementUuid,
        timestamp,
        payload,
    };

    if (instrumentId && Number.isInteger(instrumentId)) {
        cuerpo.instrumento_id = instrumentId;
    }

    const firma = firmarLectura(measurementUuid, timestamp, payload);
    cuerpo.firma = firma;

    const maxIntentos = parseInt(process.env.CALIBRATOR_MAX_RETRIES || '3', 10);

    for (let intento = 1; intento <= maxIntentos; intento += 1) {
        try {
            const data = await postJson(endpoint, cuerpo);
            console.log('[ingest] Lectura registrada', data);
            return;
        } catch (error) {
            if (error && typeof error === 'object') {
                const detalles = [];
                if (error.statusCode) {
                    detalles.push(`HTTP ${error.statusCode}`);
                }
                if (error.responseBody) {
                    detalles.push(`cuerpo: ${error.responseBody}`);
                }
                detalles.push(`intento ${intento} de ${maxIntentos}`);
                console.error('[ingest] Error enviando la medición:', error.message, detalles.join(' | '));
            } else {
                console.error('[ingest] Error enviando la medición:', error);
            }

            if (intento < maxIntentos) {
                await sleep(50);
            }
        }
    }
}

async function procesarCadena(cadena) {
    const linea = cadena.trim();
    if (linea === '') {
        return;
    }
    try {
        const parsed = JSON.parse(linea);
        const envio = enviarLectura(parsed);
        pendingEnvios.add(envio);
        try {
            await envio;
        } finally {
            pendingEnvios.delete(envio);
        }
    } catch (err) {
        console.error('[ingest] No se pudo interpretar la lectura como JSON:', linea);
    }
}

async function iniciarTcp(url) {
    const [, host, portStr] = url.match(/^tcp:\/\/([^:]+):(\d+)$/i) || [];
    if (!host || !portStr) {
        console.error('[ingest] CALIBRATOR_SOURCE tcp inválido. Usa tcp://host:puerto');
        process.exit(1);
    }
    const puerto = parseInt(portStr, 10);
    const socket = net.createConnection({ host, port: puerto }, () => {
        console.log(`[ingest] Conectado al socket TCP ${host}:${puerto}`);
    });
    socket.setEncoding('utf8');
    let buffer = '';
    socket.on('data', (chunk) => {
        buffer += chunk;
        let indice;
        while ((indice = buffer.indexOf('\n')) !== -1) {
            const linea = buffer.slice(0, indice);
            buffer = buffer.slice(indice + 1);
            procesarCadena(linea).catch((err) => {
                console.error('[ingest] Error al procesar lectura TCP:', err && err.message ? err.message : err);
            });
        }
    });
    socket.on('error', (err) => {
        console.error('[ingest] Error TCP:', err.message);
    });
    socket.on('close', () => {
        console.warn('[ingest] Conexión TCP cerrada, reintentando en 5s...');
        sleep(5000).then(() => iniciarTcp(url));
    });
}

function iniciarSerial(path) {
    if (!SerialPort || !ReadlineParser) {
        console.error('[ingest] La dependencia serialport no está disponible. Ejecuta "npm install".');
        process.exit(1);
    }

    const baudRate = parseInt(process.env.CALIBRATOR_BAUD || '9600', 10);
    const puerto = new SerialPort({ path, baudRate });
    const parser = puerto.pipe(new ReadlineParser({ delimiter: '\n' }));
    parser.on('data', procesarCadena);
    puerto.on('open', () => {
        console.log(`[ingest] Puerto serial ${path} abierto a ${baudRate} baudios.`);
    });
    puerto.on('error', (err) => {
        console.error('[ingest] Error en puerto serial:', err.message);
    });
}

function iniciarStdin() {
    console.log('[ingest] Escuchando lecturas desde STDIN.');
    process.stdin.setEncoding('utf8');
    process.stdin.resume();
    let buffer = '';

    const procesarLineas = () => {
        let index;
        while ((index = buffer.indexOf('\n')) !== -1) {
            const linea = buffer.slice(0, index);
            buffer = buffer.slice(index + 1);
            procesarCadena(linea).catch((err) => {
                console.error('[ingest] Error al procesar lectura STDIN:', err && err.message ? err.message : err);
            });
        }
    };

    process.stdin.on('data', (chunk) => {
        buffer += chunk;
        procesarLineas();
    });

    process.stdin.on('end', () => {
        if (buffer.trim() !== '') {
            procesarCadena(buffer).catch((err) => {
                console.error('[ingest] Error al procesar lectura STDIN:', err && err.message ? err.message : err);
            });
        }
        finalizarProceso('STDIN end');
    });
}

let cerrando = false;

async function finalizarProceso(signal) {
    if (cerrando) {
        return;
    }
    cerrando = true;
    try {
        if (pendingEnvios.size > 0) {
            await Promise.allSettled([...pendingEnvios]);
        }
    } finally {
        process.exit(0);
    }
}

(async () => {
    console.log('[ingest] Servicio de ingesta iniciado. Fuente:', source);
    if (source.startsWith('serial://')) {
        const path = source.replace('serial://', '');
        iniciarSerial(path);
    } else if (source.startsWith('tcp://')) {
        iniciarTcp(source);
    } else if (source === 'stdin') {
        iniciarStdin();
    } else {
        console.warn('[ingest] Fuente desconocida, usando STDIN como respaldo.');
        iniciarStdin();
    }

    if (ackDelayMs > 0) {
        while (true) {
            await sleep(ackDelayMs);
            console.log('[ingest] Esperando lecturas...');
        }
    }
})();

process.once('SIGTERM', () => {
    finalizarProceso('SIGTERM');
});

process.once('SIGINT', () => {
    finalizarProceso('SIGINT');
});
