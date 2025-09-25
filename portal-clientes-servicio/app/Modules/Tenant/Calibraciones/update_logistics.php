<?php
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 2) . '/Internal/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/logistics.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_alerts.php';

session_start();
if (!check_permission('calibraciones_actualizar')) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no determinada']);
    exit;
}

$rawInput = file_get_contents('php://input');
$payload = [];
if ($rawInput !== false && $rawInput !== '') {
    $decoded = json_decode($rawInput, true);
    if (is_array($decoded)) {
        $payload = $decoded;
    }
}

if (!$payload) {
    $payload = $_POST;
}

$calibrationId = $payload['calibration_id'] ?? $payload['id'] ?? null;
$calibrationId = filter_var($calibrationId, FILTER_VALIDATE_INT);
if (!$calibrationId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Identificador de calibración inválido.']);
    exit;
}

$calibrationStmt = $conn->prepare('SELECT id, instrumento_id, tipo FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$calibrationStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la consulta de calibración.']);
    exit;
}

$calibrationStmt->bind_param('ii', $calibrationId, $empresaId);
$calibrationStmt->execute();
$result = $calibrationStmt->get_result();
$calibration = $result ? $result->fetch_assoc() : null;
$calibrationStmt->close();

if (!$calibration) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Calibración no encontrada.']);
    exit;
}

if (strtolower((string) $calibration['tipo']) !== 'externa') {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'La logística solo aplica para calibraciones externas.']);
    exit;
}

$existingStmt = $conn->prepare('SELECT * FROM logistica_calibraciones WHERE calibracion_id = ? LIMIT 1');
if (!$existingStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la consulta logística.']);
    exit;
}

$existingStmt->bind_param('i', $calibrationId);
$existingStmt->execute();
$existingResult = $existingStmt->get_result();
$existing = $existingResult ? $existingResult->fetch_assoc() : null;
$existingStmt->close();

$estadoInput = $payload['estado'] ?? $payload['logistica_estado'] ?? null;
$estado = logistics_normalize_state(is_string($estadoInput) ? $estadoInput : null);

$provider = isset($payload['proveedor_externo']) ? trim((string) $payload['proveedor_externo']) : '';
if ($provider === '' && isset($payload['proveedor'])) {
    $provider = trim((string) $payload['proveedor']);
}
$carrier = isset($payload['transportista']) ? trim((string) $payload['transportista']) : '';
$tracking = isset($payload['numero_guia']) ? trim((string) $payload['numero_guia']) : '';
if ($tracking === '' && isset($payload['tracking'])) {
    $tracking = trim((string) $payload['tracking']);
}
$order = isset($payload['orden_servicio']) ? trim((string) $payload['orden_servicio']) : '';
if ($order === '' && isset($payload['orden'])) {
    $order = trim((string) $payload['orden']);
}
$comments = isset($payload['comentarios']) ? trim((string) $payload['comentarios']) : '';

$dates = [
    'fecha_envio' => logistics_date_from_input($payload['fecha_envio'] ?? $payload['logistics_ship_date'] ?? null),
    'fecha_en_transito' => logistics_date_from_input($payload['fecha_en_transito'] ?? $payload['logistics_transit_date'] ?? null),
    'fecha_recibido' => logistics_date_from_input($payload['fecha_recibido'] ?? $payload['logistics_received_date'] ?? null),
    'fecha_retorno' => logistics_date_from_input($payload['fecha_retorno'] ?? $payload['logistics_return_date'] ?? null),
];

$milestone = isset($payload['milestone']) ? strtolower(trim((string) $payload['milestone'])) : '';
if ($milestone !== '') {
    $today = (new DateTime())->format('Y-m-d');
    switch ($milestone) {
        case 'enviado':
        case 'envio':
            $dates['fecha_envio'] = $dates['fecha_envio'] ?? $today;
            $estado = 'Enviado';
            break;
        case 'transito':
        case 'tránsito':
        case 'en_transito':
        case 'en tránsito':
            $dates['fecha_en_transito'] = $dates['fecha_en_transito'] ?? $today;
            $estado = 'En tránsito';
            break;
        case 'recibido':
        case 'retorno':
            $dates['fecha_retorno'] = $dates['fecha_retorno'] ?? $today;
            $estado = 'Recibido';
            break;
    }
}

$finalState = logistics_infer_state_from_dates($dates, $estado);

$providerParam = $provider !== '' ? $provider : null;
$carrierParam = $carrier !== '' ? $carrier : null;
$trackingParam = $tracking !== '' ? $tracking : null;
$orderParam = $order !== '' ? $order : null;
$commentsParam = $comments !== '' ? $comments : null;

if ($existing) {
    $updateStmt = $conn->prepare('UPDATE logistica_calibraciones SET estado = ?, proveedor_externo = ?, transportista = ?, numero_guia = ?, orden_servicio = ?, fecha_envio = ?, fecha_en_transito = ?, fecha_recibido = ?, fecha_retorno = ?, comentarios = ? WHERE calibracion_id = ?');
    if (!$updateStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo preparar la actualización logística.']);
        exit;
    }
    $updateStmt->bind_param(
        'ssssssssssi',
        $finalState,
        $providerParam,
        $carrierParam,
        $trackingParam,
        $orderParam,
        $dates['fecha_envio'],
        $dates['fecha_en_transito'],
        $dates['fecha_recibido'],
        $dates['fecha_retorno'],
        $commentsParam,
        $calibrationId
    );
    if (!$updateStmt->execute()) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo actualizar la logística.']);
        $updateStmt->close();
        exit;
    }
    $updateStmt->close();
} else {
    $insertStmt = $conn->prepare('INSERT INTO logistica_calibraciones (calibracion_id, estado, proveedor_externo, transportista, numero_guia, orden_servicio, fecha_envio, fecha_en_transito, fecha_recibido, fecha_retorno, comentarios) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    if (!$insertStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo preparar el registro logístico.']);
        exit;
    }
    $insertStmt->bind_param(
        'issssssssss',
        $calibrationId,
        $finalState,
        $providerParam,
        $carrierParam,
        $trackingParam,
        $orderParam,
        $dates['fecha_envio'],
        $dates['fecha_en_transito'],
        $dates['fecha_recibido'],
        $dates['fecha_retorno'],
        $commentsParam
    );
    if (!$insertStmt->execute()) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo registrar la logística.']);
        $insertStmt->close();
        exit;
    }
    $insertStmt->close();
}

$selectStmt = $conn->prepare("SELECT estado, proveedor_externo, transportista, numero_guia, orden_servicio, comentarios, DATE_FORMAT(fecha_envio, '%Y-%m-%d') AS fecha_envio, DATE_FORMAT(fecha_en_transito, '%Y-%m-%d') AS fecha_en_transito, DATE_FORMAT(fecha_recibido, '%Y-%m-%d') AS fecha_recibido, DATE_FORMAT(fecha_retorno, '%Y-%m-%d') AS fecha_retorno FROM logistica_calibraciones WHERE calibracion_id = ? LIMIT 1");
if (!$selectStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo leer la logística actualizada.']);
    exit;
}
$selectStmt->bind_param('i', $calibrationId);
$selectStmt->execute();
$logResult = $selectStmt->get_result();
$logRow = $logResult ? $logResult->fetch_assoc() : null;
$selectStmt->close();

if (!$logRow) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se encontró la logística actualizada.']);
    exit;
}

$payloadLogistica = logistics_response_payload($logRow, '');

$previousState = logistics_normalize_state($existing['estado'] ?? 'Pendiente');
if ($finalState !== $previousState && in_array($finalState, ['Enviado', 'Recibido'], true)) {
    try {
        calibration_alerts_send_logistics_event($conn, $calibrationId, $finalState);
    } catch (Throwable $e) {
        // Registrar pero no interrumpir la respuesta.
        error_log('[logistica_calibraciones] Error al enviar alerta logística: ' . $e->getMessage());
    }
}

$usuarioNombre = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
if ($usuarioNombre === '' && !empty($_SESSION['usuario_id'])) {
    $userStmt = $conn->prepare('SELECT nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
    if ($userStmt) {
        $userStmt->bind_param('i', $_SESSION['usuario_id']);
        if ($userStmt->execute()) {
            $userStmt->bind_result($nombreDb, $apellidosDb);
            if ($userStmt->fetch()) {
                $usuarioNombre = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
            }
        }
        $userStmt->close();
    }
}
if ($usuarioNombre === '') {
    $usuarioNombre = 'Sistema';
}
$usuarioCorreo = $_SESSION['usuario'] ?? null;
log_activity($usuarioNombre, 'Actualización logística calibración ' . $calibrationId, 'calibraciones', $usuarioCorreo);

echo json_encode([
    'success' => true,
    'message' => 'Logística actualizada correctamente.',
    'logistica' => $payloadLogistica,
]);
