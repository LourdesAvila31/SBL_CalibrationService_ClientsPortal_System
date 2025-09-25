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

global $conn;
if (!isset($conn) || !$conn) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'Conexión no disponible']);
    exit;
}

$sql = 'SELECT id, nombre, numero_serie, fabricante, modelo, tipo, descripcion, instrumento_id_default, activo, ultimo_contacto, creado_en, actualizado_en
        FROM calibradores
        WHERE empresa_id = ?
        ORDER BY nombre ASC';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la consulta']);
    exit;
}

$stmt->bind_param('i', $empresaId);
$stmt->execute();
$result = $stmt->get_result();
$rows = [];

while ($row = $result->fetch_assoc()) {
    $row['activo'] = (int) ($row['activo'] ?? 0);
    $rows[] = $row;
}
$stmt->close();

echo json_encode(['success' => true, 'data' => $rows]);
