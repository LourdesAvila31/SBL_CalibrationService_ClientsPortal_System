<?php
session_start();
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_add')) {
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

$empresaSesion = ensure_session_empresa_id();
$empresaInput  = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);
if ($empresaInput !== null) {
    if ($empresaSesion !== null && $empresaInput !== $empresaSesion) {
        respond_error('Empresa no autorizada', 403);
    }
    if ($empresaSesion === null) {
        $_SESSION['empresa_id'] = $empresaInput;
        $empresaSesion          = $empresaInput;
    }
}
$empresaId = $empresaInput ?? $empresaSesion;
if ($empresaId === null) {
    respond_error('No se pudo determinar la empresa de trabajo', 400);
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

if (!tenant_roles_table_exists($conn, 'tenant_roles')) {
    respond_error('La configuración de roles delegados no está disponible', 500);
}

$conn->begin_transaction();
$stmt = $conn->prepare('INSERT INTO tenant_roles (empresa_id, nombre, descripcion) VALUES (?, ?, NULLIF(?, ""))');
if (!$stmt) {
    $conn->rollback();
    respond_error('No se pudo preparar la creación del rol delegado', 500);
}
$stmt->bind_param('iss', $empresaId, $nombre, $descripcion);
if (!$stmt->execute()) {
    $errorCode = $conn->errno;
    $stmt->close();
    $conn->rollback();
    if ($errorCode === 1062) {
        respond_error('Ya existe un rol delegado con ese nombre en la empresa', 409);
    }
    respond_error('Error al crear el rol delegado', 500);
}
$roleId = (int) $conn->insert_id;
$stmt->close();

if ($validPermissions !== []) {
    if (!tenant_roles_table_exists($conn, 'tenant_role_permissions')) {
        $conn->rollback();
        respond_error('La tabla de permisos delegados no está disponible', 500);
    }
    $stmtPerm = $conn->prepare('INSERT INTO tenant_role_permissions (tenant_role_id, permission_id) VALUES (?, ?)');
    if (!$stmtPerm) {
        $conn->rollback();
        respond_error('No se pudo preparar la asignación de permisos', 500);
    }
    foreach ($validPermissions as $permissionId) {
        $stmtPerm->bind_param('ii', $roleId, $permissionId);
        if (!$stmtPerm->execute()) {
            $stmtPerm->close();
            $conn->rollback();
            respond_error('Error al asignar permisos al rol delegado', 500);
        }
    }
    $stmtPerm->close();
}

if (!$conn->commit()) {
    $conn->rollback();
    respond_error('No se pudieron guardar los cambios', 500);
}

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
$permSummary = $permissionNames !== [] ? implode(', ', $permissionNames) : 'sin permisos';
log_activity($nombreAud, "Creó rol delegado $nombre (ID $roleId) | Permisos: $permSummary", null, $correoAud);

echo json_encode([
    'success'     => true,
    'msg'         => 'Rol delegado creado correctamente',
    'role_id'     => $roleId,
    'empresa_id'  => $empresaId,
    'permisos'    => $validPermissions,
]);

