<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_edit')) {
    http_response_code(403);
    echo json_encode(["success" => false, "msg" => "Acceso denegado"]);
    exit;
}
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once __DIR__ . '/tenant_roles_helpers.php';

$id        = filter_input(INPUT_POST, 'usuario_id', FILTER_VALIDATE_INT);
$correo    = filter_input(INPUT_POST, 'correo', FILTER_VALIDATE_EMAIL);
$nombre    = trim($_POST['nombre'] ?? '');
$apellidos = trim($_POST['apellidos'] ?? '');
$puesto    = trim($_POST['puesto'] ?? '');
$telefono  = trim($_POST['telefono'] ?? '');
$contrasena       = $_POST['contrasena'] ?? '';
$activo           = isset($_POST['activo']) ? 1 : 0;
$sso              = isset($_POST['sso']) ? 1 : 0;
$role_name        = trim($_POST['role_id'] ?? 'Operador');
$empresaId        = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);
$tenantRoleIdsRaw = $_POST['tenant_role_ids'] ?? null;

$empresaSesion = ensure_session_empresa_id();
if (!session_is_superadmin()) {
    if ($empresaSesion === null) {
        echo json_encode(["success" => false, "msg" => "Empresa no autorizada"]);
        exit;
    }
}

$stmtEmpresa = $conn->prepare('SELECT empresa_id FROM usuarios WHERE id=?');
$stmtEmpresa->bind_param('i', $id);
$stmtEmpresa->execute();
$stmtEmpresa->bind_result($empresaActual);
$tieneEmpresa = $stmtEmpresa->fetch();
$stmtEmpresa->close();

if (!$tieneEmpresa) {
    echo json_encode(["success" => false, "msg" => "Usuario no encontrado"]);
    exit;
}

$empresaActual = (int) $empresaActual;
if (!session_is_superadmin() && $empresaSesion !== null && $empresaActual !== $empresaSesion) {
    echo json_encode(["success" => false, "msg" => "No puedes editar usuarios de otra empresa"]);
    exit;
}

if ($empresaId === null || $empresaId <= 0) {
    $empresaId = $empresaActual ?: ($empresaSesion ?? 1);
}

if (!session_is_superadmin() && $empresaSesion !== null && $empresaId !== $empresaSesion) {
    echo json_encode(["success" => false, "msg" => "Empresa no autorizada"]);
    exit;
}

$shouldUpdateTenantRoles = $tenantRoleIdsRaw !== null;
$validTenantRoleIds      = [];
$tenantRoleNames         = [];
if ($shouldUpdateTenantRoles) {
    $tenantRoleIds = tenant_roles_normalize_int_list($tenantRoleIdsRaw);
    if ($tenantRoleIds !== []) {
        if (!tenant_roles_table_exists($conn, 'tenant_roles')) {
            echo json_encode(["success" => false, "msg" => "La configuración de roles delegados no está disponible"]);
            exit;
        }
        $validTenantRoleIds = tenant_roles_validate_ids($conn, $tenantRoleIds, $empresaId);
        if (count($validTenantRoleIds) !== count($tenantRoleIds)) {
            echo json_encode(["success" => false, "msg" => "Los roles delegados no pertenecen a la empresa indicada"]);
            exit;
        }
        $tenantRoleNames = tenant_roles_fetch_names($conn, $validTenantRoleIds);
    } else {
        $validTenantRoleIds = [];
        $tenantRoleNames    = [];
    }
}

if (!$id || !$correo || !$nombre || !$apellidos || !$puesto || !$telefono || !$role_name) {
    echo json_encode(["success" => false, "msg" => "Todos los campos son obligatorios"]);
    exit;
}

if (mb_strlen($puesto) > 150) {
    echo json_encode(["success" => false, "msg" => "El puesto no puede exceder 150 caracteres"]);
    exit;
}

if (mb_strlen($telefono) > 50) {
    echo json_encode(["success" => false, "msg" => "El teléfono no puede exceder 50 caracteres"]);
    exit;
}

$pwUpdate = false;
if ($contrasena) {
    $pwValid = strlen($contrasena) >= 8 && preg_match('/[A-Z]/', $contrasena) && preg_match('/[a-z]/', $contrasena) && preg_match('/\d/', $contrasena) && preg_match('/[^a-zA-Z0-9]/', $contrasena);
    if (!$pwValid) {
        echo json_encode(["success" => false, "msg" => "La contraseña no cumple los requisitos"]);
        exit;
    }
    $hash = password_hash($contrasena, PASSWORD_DEFAULT);
    $pwUpdate = true;
}

$stmt = $conn->prepare('SELECT id FROM usuarios WHERE correo=? AND id!=?');
$stmt->bind_param('si', $correo, $id);
$stmt->execute();
$existe = $stmt->get_result()->num_rows;
$stmt->close();
if ($existe) {
    echo json_encode(["success" => false, "msg" => "Ya existe otro usuario con este correo"]);
    exit;
}

$role_id = resolve_role_id($role_name, $empresaId);
if ($role_id === null) {
    echo json_encode(["success" => false, "msg" => "Rol inválido"]);
    exit;
}
$role_id = (int) $role_id;

$portalId = null;
$stmtRolePortal = $conn->prepare(
    'SELECT r.portal_id, r.delegated, LOWER(r.nombre) AS role_name, p.slug AS portal_slug'
    . ' FROM roles r'
    . ' LEFT JOIN portals p ON p.id = r.portal_id'
    . ' WHERE r.id = ?'
    . ' LIMIT 1'
);
if (!$stmtRolePortal) {
    echo json_encode(["success" => false, "msg" => "No se pudo validar el portal del rol"]);
    exit;
}
$stmtRolePortal->bind_param('i', $role_id);
if (!$stmtRolePortal->execute()) {
    $stmtRolePortal->close();
    echo json_encode(["success" => false, "msg" => "No se pudo validar el portal del rol"]);
    exit;
}
$resultRolePortal = $stmtRolePortal->get_result();
$rolePortalData   = $resultRolePortal ? $resultRolePortal->fetch_assoc() : null;
if ($resultRolePortal instanceof mysqli_result) {
    $resultRolePortal->close();
}
$stmtRolePortal->close();

if (!$rolePortalData) {
    echo json_encode(["success" => false, "msg" => "No se pudo determinar el portal para el rol seleccionado"]);
    exit;
}

if (isset($rolePortalData['portal_id']) && $rolePortalData['portal_id'] !== null) {
    $portalId = (int) $rolePortalData['portal_id'];
}

$portalSlug = isset($rolePortalData['portal_slug']) ? mb_strtolower((string) $rolePortalData['portal_slug'], 'UTF-8') : '';
$roleDelegated = isset($rolePortalData['delegated']) ? (int) $rolePortalData['delegated'] : 0;
$normalizedRoleName = isset($rolePortalData['role_name'])
    ? mb_strtolower((string) $rolePortalData['role_name'], 'UTF-8')
    : mb_strtolower($role_name, 'UTF-8');

if (($portalId === null || $portalId <= 0) && $portalSlug === '') {
    if ($roleDelegated === 1 || $normalizedRoleName === 'cliente') {
        $portalSlug = 'tenant';
    } elseif ($normalizedRoleName === 'sistemas') {
        $portalSlug = 'service';
    } else {
        $portalSlug = 'internal';
    }
}

if (($portalId === null || $portalId <= 0) && $portalSlug !== '') {
    $stmtPortal = $conn->prepare('SELECT id FROM portals WHERE slug = ? LIMIT 1');
    if (!$stmtPortal) {
        echo json_encode(["success" => false, "msg" => "No se pudo determinar el portal solicitado"]);
        exit;
    }
    $stmtPortal->bind_param('s', $portalSlug);
    if ($stmtPortal->execute()) {
        $stmtPortal->bind_result($portalIdDb);
        if ($stmtPortal->fetch()) {
            $portalId = (int) $portalIdDb;
        }
    }
    $stmtPortal->close();
}

if (!is_int($portalId) || $portalId <= 0) {
    echo json_encode(["success" => false, "msg" => "No se pudo determinar el portal para el rol seleccionado"]);
    exit;
}

$conn->begin_transaction();

if ($pwUpdate) {
    $stmt = $conn->prepare('UPDATE usuarios SET correo=?, nombre=?, apellidos=?, puesto=?, telefono=?, activo=?, sso=?, role_id=?, portal_id=?, empresa_id=?, contrasena=? WHERE id=?');
    if ($stmt === false) {
        $conn->rollback();
        echo json_encode(["success" => false, "msg" => "No se pudo preparar la actualización del usuario"]);
        exit;
    }
    $stmt->bind_param('sssssiiiiisi', $correo, $nombre, $apellidos, $puesto, $telefono, $activo, $sso, $role_id, $portalId, $empresaId, $hash, $id);
} else {
    $stmt = $conn->prepare('UPDATE usuarios SET correo=?, nombre=?, apellidos=?, puesto=?, telefono=?, activo=?, sso=?, role_id=?, portal_id=?, empresa_id=? WHERE id=?');
    if ($stmt === false) {
        $conn->rollback();
        echo json_encode(["success" => false, "msg" => "No se pudo preparar la actualización del usuario"]);
        exit;
    }
    $stmt->bind_param('sssssiiiiii', $correo, $nombre, $apellidos, $puesto, $telefono, $activo, $sso, $role_id, $portalId, $empresaId, $id);
}

if (!$stmt->execute()) {
    $stmt->close();
    $conn->rollback();
    echo json_encode(["success" => false, "msg" => "Error al modificar usuario"]);
    exit;
}
$stmt->close();

if ($shouldUpdateTenantRoles && tenant_roles_table_exists($conn, 'tenant_user_roles')) {
    $stmtDelete = $conn->prepare('DELETE FROM tenant_user_roles WHERE usuario_id = ?');
    if ($stmtDelete === false) {
        $conn->rollback();
        echo json_encode(["success" => false, "msg" => "No se pudo preparar la limpieza de roles delegados"]);
        exit;
    }
    $stmtDelete->bind_param('i', $id);
    if (!$stmtDelete->execute()) {
        $stmtDelete->close();
        $conn->rollback();
        echo json_encode(["success" => false, "msg" => "No se pudo limpiar roles delegados del usuario"]);
        exit;
    }
    $stmtDelete->close();

    if ($validTenantRoleIds !== []) {
        $stmtInsert = $conn->prepare('INSERT INTO tenant_user_roles (usuario_id, tenant_role_id) VALUES (?, ?)');
        if ($stmtInsert === false) {
            $conn->rollback();
            echo json_encode(["success" => false, "msg" => "No se pudo preparar la asignación de roles delegados"]);
            exit;
        }
        foreach ($validTenantRoleIds as $tenantRoleId) {
            $stmtInsert->bind_param('ii', $id, $tenantRoleId);
            if (!$stmtInsert->execute()) {
                $stmtInsert->close();
                $conn->rollback();
                echo json_encode(["success" => false, "msg" => "Error al asignar roles delegados"]);
                exit;
            }
        }
        $stmtInsert->close();
    }
}

if (!$conn->commit()) {
    $conn->rollback();
    echo json_encode(["success" => false, "msg" => "Error al confirmar la actualización del usuario"]);
    exit;
}

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
$detalleRoles = '';
if ($shouldUpdateTenantRoles) {
    $detalleRoles = ' | Roles delegados: ' . ($tenantRoleNames !== [] ? implode(', ', $tenantRoleNames) : 'sin asignación');
}

echo json_encode([
    "success"          => true,
    "msg"              => "Usuario modificado correctamente",
    "tenant_role_ids"  => $shouldUpdateTenantRoles ? $validTenantRoleIds : null,
]);
log_activity($nombreAud, "Editó usuario $id$detalleRoles", null, $correoAud);
?>
