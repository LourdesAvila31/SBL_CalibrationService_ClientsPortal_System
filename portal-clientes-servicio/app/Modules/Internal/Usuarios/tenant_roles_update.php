<?php
session_start();
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_edit')) {
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
if ($empresaSesion !== null && $empresaIdRol !== $empresaSesion) {
    respond_error('No puedes modificar roles de otra empresa', 403);
}
if ($empresaInput !== null && $empresaInput !== $empresaIdRol) {
    respond_error('El rol no pertenece a la empresa indicada');
}

$nombre = trim($_POST['nombre'] ?? '');
if ($nombre === '') {
    respond_error('El nombre del rol es obligatorio');
}
if (mb_strlen($nombre) > 100) {
    respond_error('El nombre del rol no puede exceder 100 caracteres');
}

$descripcion = trim($_POST['descripcion'] ?? '');
if (mb_strlen($descripcion) > 255) {
    respond_error('La descripción no puede exceder 255 caracteres');
}

$permissionIdsRaw = $_POST['permission_ids'] ?? null;
$permissionIds    = tenant_roles_normalize_int_list($permissionIdsRaw);
$validPermissions = tenant_roles_validate_permission_ids($conn, $permissionIds);
if (count($validPermissions) !== count($permissionIds)) {
    respond_error('Alguno de los permisos proporcionados es inválido');
}
$permissionNames = tenant_roles_fetch_permission_names($conn, $validPermissions);

$conn->begin_transaction();
$stmtUpdate = $conn->prepare('UPDATE tenant_roles SET nombre = ?, descripcion = NULLIF(?, ""), updated_at = NOW() WHERE id = ?');
if (!$stmtUpdate) {
    $conn->rollback();
    respond_error('No se pudo preparar la actualización del rol delegado', 500);
}
$stmtUpdate->bind_param('ssi', $nombre, $descripcion, $roleId);
if (!$stmtUpdate->execute()) {
    $errorCode = $conn->errno;
    $stmtUpdate->close();
    $conn->rollback();
    if ($errorCode === 1062) {
        respond_error('Ya existe otro rol delegado con ese nombre en la empresa', 409);
    }
    respond_error('Error al actualizar el rol delegado', 500);
}
$stmtUpdate->close();

if (!tenant_roles_table_exists($conn, 'tenant_role_permissions')) {
    $conn->rollback();
    respond_error('La tabla de permisos delegados no está disponible', 500);
}

$stmtDelete = $conn->prepare('DELETE FROM tenant_role_permissions WHERE tenant_role_id = ?');
if (!$stmtDelete) {
    $conn->rollback();
    respond_error('No se pudo limpiar los permisos del rol delegado', 500);
}
$stmtDelete->bind_param('i', $roleId);
if (!$stmtDelete->execute()) {
    $stmtDelete->close();
    $conn->rollback();
    respond_error('Error al limpiar los permisos actuales del rol', 500);
}
$stmtDelete->close();

if ($validPermissions !== []) {
    $stmtInsert = $conn->prepare('INSERT INTO tenant_role_permissions (tenant_role_id, permission_id) VALUES (?, ?)');
    if (!$stmtInsert) {
        $conn->rollback();
        respond_error('No se pudo preparar la asignación de nuevos permisos', 500);
    }
    foreach ($validPermissions as $permissionId) {
        $stmtInsert->bind_param('ii', $roleId, $permissionId);
        if (!$stmtInsert->execute()) {
            $stmtInsert->close();
            $conn->rollback();
            respond_error('Error al asignar los nuevos permisos del rol', 500);
        }
    }
    $stmtInsert->close();
}

if (!$conn->commit()) {
    $conn->rollback();
    respond_error('No se pudieron guardar los cambios del rol', 500);
}

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
$permSummary = $permissionNames !== [] ? implode(', ', $permissionNames) : 'sin permisos';
log_activity($nombreAud, "Actualizó rol delegado $nombre (ID $roleId) | Permisos: $permSummary", null, $correoAud);

echo json_encode([
    'success'     => true,
    'msg'         => 'Rol delegado actualizado correctamente',
    'role_id'     => $roleId,
    'empresa_id'  => $empresaIdRol,
    'permisos'    => $validPermissions,
]);

