<?php
session_start();
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once __DIR__ . '/tenant_roles_helpers.php';

if (!check_permission('usuarios_add')) {
    http_response_code(403);
    echo json_encode([
        'success' => false,
        'msg'     => 'Acceso denegado',
    ]);
    exit;
}

$empresaSesion = ensure_session_empresa_id();
$empresaId = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT);
if ($empresaId === null || $empresaId <= 0) {
    $empresaId = $empresaSesion;
}

if ($empresaId === null || $empresaId <= 0) {
    http_response_code(400);
    echo json_encode([
        'success' => false,
        'msg'     => 'Empresa no válida.',
    ]);
    exit;
}

if (!session_is_superadmin() && $empresaSesion !== null && $empresaSesion !== $empresaId) {
    http_response_code(403);
    echo json_encode([
        'success' => false,
        'msg'     => 'Empresa no autorizada.',
    ]);
    exit;
}

$hasLegacyClienteColumn = false;
$columnsResult = $conn->query("SHOW COLUMNS FROM usuarios LIKE 'cliente_id'");
if ($columnsResult instanceof mysqli_result) {
    $hasLegacyClienteColumn = $columnsResult->num_rows > 0;
    $columnsResult->close();
}

if ($hasLegacyClienteColumn) {
    $stmt = $conn->prepare('SELECT COUNT(*) AS total FROM usuarios WHERE empresa_id = ? OR cliente_id = ?');
} else {
    $stmt = $conn->prepare('SELECT COUNT(*) AS total FROM usuarios WHERE empresa_id = ?');
}

if (!$stmt) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'msg'     => 'No se pudo consultar los usuarios registrados.',
    ]);
    exit;
}

if ($hasLegacyClienteColumn) {
    $stmt->bind_param('ii', $empresaId, $empresaId);
} else {
    $stmt->bind_param('i', $empresaId);
}

if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'msg'     => 'No se pudo consultar los usuarios registrados.',
    ]);
    exit;
}

$stmt->bind_result($totalRaw);
$stmt->fetch();
$stmt->close();

$usuarioCount = (int) ($totalRaw ?? 0);
$isFirstUser   = $usuarioCount === 0;
$responsableRoleAvailable = null;
if ($isFirstUser) {
    $responsableRoleAvailable = false;
    if (tenant_roles_table_exists($conn, 'tenant_roles')) {
        $stmtRole = $conn->prepare('SELECT id FROM tenant_roles WHERE empresa_id = ? AND LOWER(nombre) = LOWER(?) LIMIT 1');
        if ($stmtRole) {
            $slug = 'responsable';
            $stmtRole->bind_param('is', $empresaId, $slug);
            if ($stmtRole->execute()) {
                $resultRole = $stmtRole->get_result();
                $rowRole    = $resultRole ? $resultRole->fetch_assoc() : null;
                if ($resultRole instanceof mysqli_result) {
                    $resultRole->close();
                }
                if ($rowRole && isset($rowRole['id'])) {
                    $responsableRoleAvailable = true;
                }
            }
            $stmtRole->close();
        }
    }
}

$response = [
    'success'                  => true,
    'empresa_id'               => $empresaId,
    'user_count'               => $usuarioCount,
    'is_first_user'            => $isFirstUser,
    'forced_role'              => 'Cliente',
    'forced_tenant_role'       => 'responsable',
    'responsable_role_available' => $responsableRoleAvailable,
];

echo json_encode($response);
