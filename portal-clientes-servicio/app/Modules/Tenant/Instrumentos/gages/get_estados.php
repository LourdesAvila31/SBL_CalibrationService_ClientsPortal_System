<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

header('Content-Type: application/json');




if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}


require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

header('Content-Type: application/json');


$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}


$stmt = $conn->prepare(
    "SELECT nombre FROM (
         SELECT DISTINCT estado AS nombre
         FROM instrumentos
         WHERE estado IS NOT NULL AND estado <> '' AND empresa_id = ?
         UNION
         SELECT 'En stock' AS nombre
     ) AS estados
     ORDER BY nombre ASC"
);
require_once dirname(__DIR__, 4) . '/Core/db.php';

$sql = <<<'SQL'
SELECT nombre FROM (
    SELECT DISTINCT estado AS nombre
    FROM instrumentos
    WHERE estado IS NOT NULL AND estado <> '' AND empresa_id = ?
    UNION
    SELECT 'En stock' AS nombre
) AS estados
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