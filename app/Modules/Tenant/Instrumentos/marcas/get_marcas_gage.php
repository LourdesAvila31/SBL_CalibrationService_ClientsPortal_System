<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}
// Devuelve la lista de marcas existentes
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';

$sql = "SELECT id, nombre FROM marcas ORDER BY nombre ASC";
$res = $conn->query($sql);
$options = [];
if ($res && $res->num_rows > 0) {
    while ($row = $res->fetch_assoc()) {
        $options[] = $row;
    }
}
echo json_encode($options, JSON_UNESCAPED_UNICODE);
$conn->close();
?>