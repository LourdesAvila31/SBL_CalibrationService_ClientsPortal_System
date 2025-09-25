<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');



if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

header('Content-Type: application/json');

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
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
$stmt->bind_param('i', $empresaId);
$stmt->execute();
$res = $stmt->get_result();
$options = [];
if ($res && $res->num_rows > 0) {
    while ($row = $res->fetch_assoc()) {
        $options[] = $row;
    }
}
$stmt->close();

echo json_encode($options, JSON_UNESCAPED_UNICODE);
$stmt->close();
$conn->close();
?>