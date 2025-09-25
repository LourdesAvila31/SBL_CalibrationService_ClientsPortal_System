<?php
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'msg' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once __DIR__ . '/tenant_roles_helpers.php';

$empresaSesion     = ensure_session_empresa_id();
$empresaSolicitada = requested_empresa_id();

if ($empresaSolicitada !== null) {
    if ($empresaSesion !== null && $empresaSolicitada !== $empresaSesion) {
        http_response_code(403);
        echo json_encode(['success' => false, 'msg' => 'Empresa no autorizada']);
        exit;
    }
    if ($empresaSesion === null) {
        $_SESSION['empresa_id'] = $empresaSolicitada;
        $empresaSesion          = $empresaSolicitada;
    }
}

$empresaId = $empresaSesion;
if ($empresaId === null) {
    http_response_code(400);
    echo json_encode(['success' => false, 'msg' => 'Empresa no disponible en la sesión']);
    exit;
}

$empresaSesion = ensure_session_empresa_id();
$empresaSolicitada = requested_empresa_id();
$empresaObjetivo = $empresaSesion;

if ($empresaSolicitada !== null) {
    if (!session_is_superadmin() && $empresaSesion !== null && $empresaSolicitada !== $empresaSesion) {
        http_response_code(403);
        echo json_encode([]);
        exit;
    }
    $empresaObjetivo = $empresaSolicitada;
}

if ($empresaObjetivo === null && !session_is_superadmin()) {
    http_response_code(400);
    echo json_encode([]);
    exit;
}

if ($empresaObjetivo !== null) {
    $stmt = $conn->prepare('SELECT id, nombre, empresa_id, delegated FROM roles WHERE empresa_id IS NULL OR empresa_id = ? ORDER BY empresa_id IS NULL DESC, nombre ASC');
    $stmt->bind_param('i', $empresaObjetivo);
} else {
    $stmt = $conn->prepare('SELECT id, nombre, empresa_id, delegated FROM roles ORDER BY empresa_id IS NULL DESC, nombre ASC');
}

if (!$stmt) {
    echo json_encode([]);
    exit;
}

$stmt->execute();
$result = $stmt->get_result();
$roles = [];
while ($row = $result->fetch_assoc()) {
    $empresaId = isset($row['empresa_id']) ? (int) $row['empresa_id'] : null;
    $delegated = (int) ($row['delegated'] ?? 0) === 1;
    $roles[] = [
        'id' => (int) $row['id'],
        'nombre' => $row['nombre'],
        'empresa_id' => $empresaId,
        'delegated' => $delegated,
        'scope' => $empresaId === null ? 'global' : 'delegado',
        'requires_permission' => $delegated ? 'usuarios_edit' : null,
    ];
}
$stmt->close();

echo json_encode($roles);
?>

