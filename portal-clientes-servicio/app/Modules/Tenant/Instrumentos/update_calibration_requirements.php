<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}
// Actualiza los requerimientos de calibración de un instrumento
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

$instrumento_id  = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT);
$req = trim($_POST['requerimiento'] ?? '');

if (!$instrumento_id) {
    echo json_encode(['error' => 'ID inválido']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$check = $conn->prepare('SELECT 1 FROM instrumentos WHERE id = ? AND empresa_id = ?');
$check->bind_param('ii', $instrumento_id, $empresaId);
$check->execute();
$check->store_result();
if ($check->num_rows === 0) {

    http_response_code(404);

    echo json_encode(['error' => 'Instrumento no encontrado']);
    $check->close();
    $conn->close();
    exit;
}
$check->close();

$stmt = $conn->prepare(
    "INSERT INTO requerimientos_calibracion (instrumento_id, empresa_id, requerimiento, fecha_actualizacion)
     VALUES (?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE requerimiento=VALUES(requerimiento), fecha_actualizacion=NOW()"
);
$stmt->bind_param('iis', $instrumento_id, $empresaId, $req);
if ($stmt->execute()) {
    echo json_encode(['success' => true]);
} else {
    echo json_encode(['error' => 'No se pudo guardar']);
}
$stmt->close();
$conn->close();
?>
