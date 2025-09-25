<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

header('Content-Type: application/json');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$sql = <<<SQL
SELECT DISTINCT ubicacion AS nombre
FROM instrumentos
WHERE ubicacion IS NOT NULL
  AND ubicacion <> ''
  AND empresa_id = ?

$sql = <<<'SQL'
SELECT DISTINCT ubicacion AS nombre
FROM instrumentos
WHERE ubicacion IS NOT NULL AND ubicacion <> '' AND empresa_id = ?

ORDER BY nombre ASC
SQL;
$stmt = $conn->prepare($sql);
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