<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}
// Devuelve la lista de modelos disponibles
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';

$marcaId = filter_input(INPUT_GET, 'marca_id', FILTER_VALIDATE_INT);
$sql = 'SELECT id, nombre, marca_id FROM modelos';
$params = [];
$types  = '';
if ($marcaId) {
    $sql   .= ' WHERE marca_id = ?';
    $params[] = $marcaId;
    $types   .= 'i';
}
$sql .= ' ORDER BY nombre ASC';

$stmt = $conn->prepare($sql);
if ($params) {
     $refs = [];
    foreach ($params as $key => $value) {
        $refs[$key] = &$params[$key];
    }
    $stmt->bind_param($types, ...$refs);
}
$stmt->execute();
$res = $stmt->get_result();
$options = [];
while ($row = $res->fetch_assoc()) {
    $options[] = $row;
}
$stmt->close();

echo json_encode($options, JSON_UNESCAPED_UNICODE);
$conn->close();
?>