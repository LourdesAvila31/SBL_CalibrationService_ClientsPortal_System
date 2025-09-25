<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

ensure_portal_access('service');
if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_nonconformities.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$nonConformityId = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
if (!$nonConformityId) {
    http_response_code(400);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Identificador inválido']);
    $conn->close();
    exit;
}

try {
    calibration_nonconformities_ensure_table($conn);
} catch (Throwable $e) {
    error_log('[nc-calibraciones][ERROR] No se pudo asegurar la tabla de no conformidades: ' . $e->getMessage());
}

try {
    $record = calibration_nonconformities_get($conn, $nonConformityId, $empresaId);
} catch (Throwable $e) {
    http_response_code(500);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No se pudo obtener la no conformidad']);
    $conn->close();
    exit;
}

if (!$record) {
    http_response_code(404);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No conformidad no encontrada']);
    $conn->close();
    exit;
}

header('Content-Type: application/json');
echo json_encode($record);
$conn->close();
