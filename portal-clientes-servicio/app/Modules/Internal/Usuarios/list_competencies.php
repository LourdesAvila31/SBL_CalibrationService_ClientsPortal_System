<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

header('Content-Type: application/json; charset=UTF-8');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    exit;
}

$usuarioId = filter_input(INPUT_GET, 'usuario_id', FILTER_VALIDATE_INT);
if (!$usuarioId) {
    http_response_code(400);
    echo json_encode(['error' => 'Usuario inválido']);
    exit;
}

$check = $conn->prepare('SELECT empresa_id FROM usuarios WHERE id = ? LIMIT 1');
if (!$check) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo verificar el usuario']);
    exit;
}
$check->bind_param('i', $usuarioId);
$check->execute();
$check->bind_result($usuarioEmpresaId);
if (!$check->fetch()) {
    http_response_code(404);
    echo json_encode(['error' => 'Usuario no encontrado']);
    $check->close();
    exit;
}
$check->close();

if ((int) $usuarioEmpresaId !== (int) $empresaId) {
    http_response_code(403);
    echo json_encode(['error' => 'No puedes consultar usuarios de otra empresa']);
    exit;
}

$sql = 'SELECT id, descripcion, archivo, evidencia_fecha, catalogo_id, vigente_desde, vigente_hasta, created_at, updated_at
        FROM usuario_competencias
        WHERE usuario_id = ? AND empresa_id = ?
        ORDER BY created_at DESC, id DESC';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo obtener la información']);
    exit;
}
$stmt->bind_param('ii', $usuarioId, $empresaId);
$stmt->execute();
$result = $stmt->get_result();

$basePath = '/SISTEMA-COMPUTARIZADO-ISO-17025/storage/usuarios/competencias/';
$records = [];
while ($row = $result->fetch_assoc()) {
    $archivo = $row['archivo'] ?? null;
    $records[] = [
        'id' => (int) $row['id'],
        'descripcion' => $row['descripcion'] ?? '',
        'archivo' => $archivo,
        'evidencia_fecha' => $row['evidencia_fecha'] ?? null,
        'catalogo_id' => isset($row['catalogo_id']) ? (int) $row['catalogo_id'] : null,
        'vigente_desde' => $row['vigente_desde'] ?? null,
        'vigente_hasta' => $row['vigente_hasta'] ?? null,
        'created_at' => $row['created_at'] ?? null,
        'updated_at' => $row['updated_at'] ?? null,
        'url' => $archivo ? $basePath . ltrim($archivo, '/\\') : null,
    ];
}
$stmt->close();

echo json_encode(['competencias' => $records]);
$conn->close();
