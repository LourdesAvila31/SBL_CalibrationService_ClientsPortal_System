<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');
header('Content-Type: application/json; charset=utf-8');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

try {
    $empresaId = obtenerEmpresaId();
} catch (Throwable $e) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    exit;
}

if ($empresaId <= 0) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    exit;
}

$stmt = $conn->prepare(
    'SELECT i.id, ci.nombre AS nombre
     FROM instrumentos i
     LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
     WHERE i.empresa_id = ?
       AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> "baja")
     ORDER BY ci.nombre ASC'
);

if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta de instrumentos.']);
    exit;
}

$stmt->bind_param('i', $empresaId);

if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo ejecutar la consulta de instrumentos.']);
    exit;
}

$result = $stmt->get_result();
$instrumentos = [];

if ($result) {
    while ($row = $result->fetch_assoc()) {
        $instrumentos[] = [
            'id' => isset($row['id']) ? (int) $row['id'] : null,
            'nombre' => $row['nombre'] ?? '',
        ];
    }
    $result->free();
}

$stmt->close();
$conn->close();

echo json_encode($instrumentos, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
