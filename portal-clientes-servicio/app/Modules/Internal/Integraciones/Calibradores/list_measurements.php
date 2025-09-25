<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

if (!check_permission('integraciones_calibradores_ver')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no determinada']);
    exit;
}

$calibradorId = filter_input(INPUT_GET, 'calibrador_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if ($calibradorId === false || $calibradorId === null) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Calibrador inválido']);
    exit;
}

$instrumentoId = filter_input(INPUT_GET, 'instrumento_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if ($instrumentoId === false) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Instrumento inválido']);
    exit;
}

$limit = filter_input(INPUT_GET, 'limit', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'max_range' => 50]]) ?: 5;

$sql = 'SELECT id, calibracion_id, calibrador_id, instrumento_id, measurement_uuid, fecha_lectura, magnitud, valor, unidad, payload_json
        FROM calibraciones_lecturas
        WHERE empresa_id = ? AND calibrador_id = ?';
$params = [$empresaId, $calibradorId];
$types = 'ii';

if ($instrumentoId !== null) {
    $sql .= ' AND (instrumento_id = ? OR instrumento_id IS NULL)';
    $types .= 'i';
    $params[] = $instrumentoId;
}

$sql .= ' ORDER BY fecha_lectura DESC LIMIT ?';
$types .= 'i';
$params[] = $limit;

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la consulta de lecturas']);
    exit;
}

$stmt->bind_param($types, ...$params);
$stmt->execute();
$result = $stmt->get_result();
$lecturas = [];

while ($row = $result->fetch_assoc()) {
    $row['valor'] = $row['valor'] !== null ? (float) $row['valor'] : null;
    $row['calibracion_id'] = $row['calibracion_id'] !== null ? (int) $row['calibracion_id'] : null;
    $lecturas[] = $row;
}
$stmt->close();

echo json_encode(['success' => true, 'data' => $lecturas]);
