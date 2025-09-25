<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

header('Content-Type: application/json');

if(!isset($_SESSION['usuario_id'])){
    echo json_encode(['error' => 'No session']);
    exit;
}

$id = intval($_SESSION['usuario_id']);
$sql = "SELECT u.*, r.nombre AS role_name, e.nombre AS empresa_nombre FROM usuarios u "
     . "LEFT JOIN roles r ON u.role_id = r.id "
     . "LEFT JOIN empresas e ON u.empresa_id = e.id "
     . "WHERE u.id=?";
$stmt = $conn->prepare($sql);
$stmt->bind_param("i", $id);
$stmt->execute();
$res = $stmt->get_result();

if($u = $res->fetch_assoc()){
    $roleName = $u['role_name'] ?? '';
    $roleId   = $u['role_id'] ?? null;

    $canManageClientes = check_permission('clientes_gestionar');

    $permissionFlags = [
        'canManageInstrumentos' => check_permission('instrumentos_crear')
            || check_permission('instrumentos_actualizar')
            || check_permission('instrumentos_eliminar'),
        'canManagePlaneacion' => check_permission('planeacion_crear')
            || check_permission('planeacion_actualizar')
            || check_permission('planeacion_eliminar'),
        'canManageCalibraciones' => check_permission('calibraciones_crear')
            || check_permission('calibraciones_actualizar')
            || check_permission('calibraciones_eliminar'),
        'canManageUsuarios' => check_permission('usuarios_add')
            || check_permission('usuarios_edit')
            || check_permission('usuarios_delete'),
        'canReviewFeedback' => check_permission('feedback_leer'),
        'canManageClientesService' => $canManageClientes,
        'canManageClientesServicio' => $canManageClientes,
        'canViewApiTokens' => check_permission('api_tokens_leer')
            || check_permission('api_tokens_list')
            || check_permission('api_tokens_view'),
        'canManageApiTokens' => check_permission('api_tokens_crear')
            || check_permission('api_tokens_actualizar')
            || check_permission('api_tokens_eliminar')
            || check_permission('api_tokens_manage')
            || check_permission('api_tokens_update')
            || check_permission('api_tokens_delete')
            || check_permission('api_tokens_admin')
            || check_permission('api_tokens_write'),
    ];

    echo json_encode([
        'id'         => $u['id'],
        'correo'     => $u['correo'],
        'usuario'    => $u['usuario'],
        'nombre'     => $u['nombre'],
        'apellidos'  => $u['apellidos'],
        'puesto'     => $u['puesto'] ?? null,
        'telefono'   => $u['telefono'] ?? null,
        'role_id'    => $roleId,
        'role_name'  => $roleName,
        'activo'     => $u['activo'],
        'sso'        => $u['sso'],
        'ultima_ip'  => $u['ultima_ip'] ?? '',
        'last_login' => $u['last_login'] ?? null,
        'created_at' => $u['fecha_creacion'] ?? $u['created_at'] ?? null,
        'empresa_id' => isset($u['empresa_id']) ? (int) $u['empresa_id'] : null,
        'empresa_nombre' => $u['empresa_nombre'] ?? null,
        'permission_flags' => $permissionFlags,
    ]);
} else {
    echo json_encode(['error' => 'User not found']);
}
?>