<?php
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/calibradores.php';

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

$sessionAutenticada = isset($_SESSION['usuario_id']);
if ($sessionAutenticada && !check_permission('integraciones_calibradores_ingestar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

header('Content-Type: application/json');

$empresaId = null;
if ($sessionAutenticada) {
    $empresaId = obtenerEmpresaId();
    if (!$empresaId) {
        http_response_code(400);
        echo json_encode(['success' => false, 'message' => 'Empresa no determinada']);
        exit;
    }
}

$rawInput = file_get_contents('php://input');
if ($rawInput === false || $rawInput === '') {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Datos no recibidos']);
    exit;
}

$data = json_decode($rawInput, true);
if (!is_array($data)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Formato JSON inválido']);
    exit;
}

$calibradorId = isset($data['calibrador_id']) ? filter_var($data['calibrador_id'], FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]) : false;
$measurementUuid = isset($data['measurement_uuid']) ? trim((string) $data['measurement_uuid']) : '';
$timestamp = isset($data['timestamp']) ? trim((string) $data['timestamp']) : '';
$firma = isset($data['firma']) ? trim((string) $data['firma']) : '';
$payload = $data['payload'] ?? [];
$calibracionIdRaw = $data['calibracion_id'] ?? null;
$instrumentoIdRaw = $data['instrumento_id'] ?? null;

if ($calibradorId === false || $measurementUuid === '' || $timestamp === '' || $firma === '') {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Datos incompletos']);
    exit;
}

$fechaLectura = date_create($timestamp);
if (!$fechaLectura) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Marca de tiempo inválida']);
    exit;
}

$calibracionId = null;
if ($calibracionIdRaw !== null && $calibracionIdRaw !== '') {
    $calibracionId = filter_var($calibracionIdRaw, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($calibracionId === false) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Calibración inválida']);
        exit;
    }
}

$instrumentoId = null;
if ($instrumentoIdRaw !== null && $instrumentoIdRaw !== '') {
    $instrumentoId = filter_var($instrumentoIdRaw, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($instrumentoId === false) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Instrumento inválido']);
        exit;
    }
}

global $conn;
$calibradorStmt = $conn->prepare('SELECT id, empresa_id, token_firma, instrumento_id_default FROM calibradores WHERE id = ? AND activo = 1 LIMIT 1');
if (!$calibradorStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo validar el calibrador']);
    exit;
}
$calibradorStmt->bind_param('i', $calibradorId);
$calibradorStmt->execute();
$result = $calibradorStmt->get_result();
$calibrador = $result->fetch_assoc();
$calibradorStmt->close();

if (!$calibrador) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Calibrador no encontrado o inactivo']);
    exit;
}

$empresaIdCalibrador = (int) $calibrador['empresa_id'];
if ($sessionAutenticada) {
    if ($empresaIdCalibrador !== (int) $empresaId) {
        http_response_code(403);
        echo json_encode(['success' => false, 'message' => 'El calibrador no pertenece a la empresa actual']);
        exit;
    }
} else {
    $empresaId = $empresaIdCalibrador;
}

if ($instrumentoId === null && isset($calibrador['instrumento_id_default'])) {
    $instrumentoId = $calibrador['instrumento_id_default'] !== null ? (int) $calibrador['instrumento_id_default'] : null;
}

if ($instrumentoId !== null) {
    $instrumentoStmt = $conn->prepare('SELECT id FROM instrumentos WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$instrumentoStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo validar el instrumento']);
        exit;
    }
    $instrumentoStmt->bind_param('ii', $instrumentoId, $empresaId);
    $instrumentoStmt->execute();
    $instrumentoStmt->store_result();
    if ($instrumentoStmt->num_rows === 0) {
        $instrumentoStmt->close();
        http_response_code(404);
        echo json_encode(['success' => false, 'message' => 'Instrumento no encontrado']);
        exit;
    }
    $instrumentoStmt->close();
}

if ($calibracionId !== null) {
    $calibracionStmt = $conn->prepare('SELECT id FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$calibracionStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo validar la calibración']);
        exit;
    }
    $calibracionStmt->bind_param('ii', $calibracionId, $empresaId);
    $calibracionStmt->execute();
    $calibracionStmt->store_result();
    if ($calibracionStmt->num_rows === 0) {
        $calibracionStmt->close();
        http_response_code(404);
        echo json_encode(['success' => false, 'message' => 'Calibración no encontrada']);
        exit;
    }
    $calibracionStmt->close();
}

$payloadString = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
if ($payloadString === false) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'No se pudo serializar la carga']);
    exit;
}

$tokenFirma = (string) $calibrador['token_firma'];
if (!calibrator_verify_signature($measurementUuid, $timestamp, $payloadString, $tokenFirma, $firma)) {
    http_response_code(401);
    echo json_encode(['success' => false, 'message' => 'Firma inválida']);
    exit;
}

$magnitud = null;
$valor = null;
$unidad = null;
if (is_array($payload)) {
    $magnitud = isset($payload['magnitud']) ? trim((string) $payload['magnitud']) : null;
    $unidad = isset($payload['unidad']) ? trim((string) $payload['unidad']) : null;
    if (isset($payload['valor'])) {
        $valorTmp = filter_var($payload['valor'], FILTER_VALIDATE_FLOAT);
        if ($valorTmp !== false) {
            $valor = $valorTmp;
        }
    }
}

$fechaLecturaStr = $fechaLectura->format('Y-m-d H:i:s');

$insertStmt = $conn->prepare('INSERT INTO calibraciones_lecturas (calibracion_id, instrumento_id, calibrador_id, empresa_id, measurement_uuid, fecha_lectura, magnitud, valor, unidad, payload_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
if (!$insertStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar el guardado']);
    exit;
}

$insertStmt->bind_param(
    'iiiisssdss',
    $calibracionId,
    $instrumentoId,
    $calibradorId,
    $empresaId,
    $measurementUuid,
    $fechaLecturaStr,
    $magnitud,
    $valor,
    $unidad,
    $payloadString
);

if (!$insertStmt->execute()) {
    $errno = $insertStmt->errno;
    $insertStmt->close();
    if ($errno === 1062) {
        http_response_code(200);
        echo json_encode(['success' => true, 'duplicate' => true, 'message' => 'Lectura ya registrada']);
        exit;
    }
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la lectura']);
    exit;
}

$measurementId = $insertStmt->insert_id;
$insertStmt->close();

$updateContacto = $conn->prepare('UPDATE calibradores SET ultimo_contacto = NOW() WHERE id = ?');
if ($updateContacto) {
    $updateContacto->bind_param('i', $calibradorId);
    $updateContacto->execute();
    $updateContacto->close();
}

echo json_encode([
    'success' => true,
    'data' => [
        'measurement_id' => $measurementId,
        'measurement_uuid' => $measurementUuid,
        'calibrador_id' => $calibradorId,
        'instrumento_id' => $instrumentoId,
        'calibracion_id' => $calibracionId,
        'fecha_lectura' => $fechaLecturaStr,
    ],
]);
