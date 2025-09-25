<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

header('Content-Type: application/json');

ensure_portal_access('tenant');

$roleAlias = session_role_alias() ?? '';
if ($roleAlias !== 'cliente') {
    http_response_code(403);
    echo json_encode(['instruments' => [], 'message' => 'Acceso denegado.']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['instruments' => [], 'message' => 'No se pudo determinar la empresa asociada.']);
    exit;
}

$sql = "SELECT i.id, COALESCE(ci.nombre, '') AS nombre, i.codigo, i.serie\n        FROM instrumentos i\n        LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id\n        WHERE i.empresa_id = ?\n          AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')\n        ORDER BY ci.nombre ASC, i.codigo ASC, i.id ASC";
$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['instruments' => [], 'message' => 'No se pudo consultar la información.']);
    exit;
}
$stmt->bind_param('i', $empresaId);
$stmt->execute();
$rows = [];
if (method_exists($stmt, 'get_result')) {
    $res = $stmt->get_result();
    while ($row = $res->fetch_assoc()) {
        $rows[] = [
            'id' => (int)$row['id'],
            'nombre' => $row['nombre'],
            'codigo' => $row['codigo'],
            'serie' => $row['serie']
        ];
    }
} else {
    $stmt->bind_result($id, $nombre, $codigo, $serie);
    while ($stmt->fetch()) {
        $rows[] = [
            'id' => (int)$id,
            'nombre' => $nombre,
            'codigo' => $codigo,
            'serie' => $serie
        ];
    }
}
$stmt->close();
$conn->close();

echo json_encode(['instruments' => $rows]);
