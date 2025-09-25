<?php
session_start();
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_delete')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'msg' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once __DIR__ . '/tenant_roles_helpers.php';

function respond_error(string $message, int $status = 400): void
{
    http_response_code($status);
    echo json_encode(['success' => false, 'msg' => $message]);
    exit;
}

$roleId = filter_input(INPUT_POST, 'role_id', FILTER_VALIDATE_INT);
if (!$roleId) {
    respond_error('Identificador de rol inválido');
}

if (!tenant_roles_table_exists($conn, 'tenant_roles')) {
    respond_error('La configuración de roles delegados no está disponible', 500);
}

$empresaSesion = ensure_session_empresa_id();
$empresaInput  = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);

$stmtRole = $conn->prepare('SELECT empresa_id, nombre FROM tenant_roles WHERE id = ? LIMIT 1');
if (!$stmtRole) {
    respond_error('No se pudo consultar el rol delegado', 500);
}
$stmtRole->bind_param('i', $roleId);
$stmtRole->execute();
$resultRole = $stmtRole->get_result();
$roleData    = $resultRole->fetch_assoc();
$stmtRole->close();

if (!$roleData) {
    respond_error('Rol delegado no encontrado', 404);
}

$empresaIdRol = (int) $roleData['empresa_id'];
$roleName     = $roleData['nombre'];
if ($empresaSesion !== null && $empresaIdRol !== $empresaSesion) {
    respond_error('No puedes eliminar roles de otra empresa', 403);
}
if ($empresaInput !== null && $empresaInput !== $empresaIdRol) {
    respond_error('El rol no pertenece a la empresa indicada');
}

$assignedUsers = 0;
if (tenant_roles_table_exists($conn, 'tenant_user_roles')) {
    $stmtCount = $conn->prepare('SELECT COUNT(*) AS total FROM tenant_user_roles WHERE tenant_role_id = ?');
    if ($stmtCount) {
        $stmtCount->bind_param('i', $roleId);
        if ($stmtCount->execute()) {
            $resultCount = $stmtCount->get_result();
            if ($rowCount = $resultCount->fetch_assoc()) {
                $assignedUsers = (int) $rowCount['total'];
            }
        }
        $stmtCount->close();
    }
}

$conn->begin_transaction();
$stmtDelete = $conn->prepare('DELETE FROM tenant_roles WHERE id = ? LIMIT 1');
if (!$stmtDelete) {
    $conn->rollback();
    respond_error('No se pudo preparar la eliminación del rol delegado', 500);
}
$stmtDelete->bind_param('i', $roleId);
if (!$stmtDelete->execute() || $stmtDelete->affected_rows === 0) {
    $stmtDelete->close();
    $conn->rollback();
    respond_error('No se pudo eliminar el rol delegado', 500);
}
$stmtDelete->close();

if (!$conn->commit()) {
    $conn->rollback();
    respond_error('No se pudieron guardar los cambios', 500);
}

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
$detalle   = "Eliminó rol delegado $roleName (ID $roleId)";
if ($assignedUsers > 0) {
    $detalle .= " | Usuarios afectados: $assignedUsers";
}
log_activity($nombreAud, $detalle, null, $correoAud);

echo json_encode([
    'success'    => true,
    'msg'        => 'Rol delegado eliminado correctamente',
    'role_id'    => $roleId,
    'empresa_id' => $empresaIdRol,
]);

