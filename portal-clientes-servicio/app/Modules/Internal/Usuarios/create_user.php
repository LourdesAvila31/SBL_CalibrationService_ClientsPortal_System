<?php
session_start();
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_add')) {
    http_response_code(403);
    echo json_encode(["success" => false, "msg" => "Acceso denegado"]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once __DIR__ . '/tenant_roles_helpers.php';

/**
 * Envía una respuesta de error en formato JSON y detiene la ejecución.
 */
function respond_error(string $message, int $status = 400): void
{
    http_response_code($status);
    echo json_encode(["success" => false, "msg" => $message]);
    exit;
}

/**
 * Sanitiza y valida el nombre de usuario recibido en la petición.
 */
function normalize_username(?string $value): string
{
    $username = trim((string) $value);
    if ($username === '') {
        respond_error('El nombre de usuario es obligatorio.');
    }

    if (strlen($username) > 100) {
        respond_error('El nombre de usuario no puede exceder 100 caracteres.');
    }

    if (!preg_match('/^[a-zA-Z0-9._-]{3,}$/', $username)) {
        respond_error('El usuario debe tener al menos 3 caracteres y solo puede contener letras, números y ._-');
    }

    return $username;
}

$usuario    = normalize_username($_POST['usuario'] ?? null);
$correo     = filter_input(INPUT_POST, 'correo', FILTER_VALIDATE_EMAIL);
$nombre     = trim($_POST['nombre'] ?? '');
$apellidos  = trim($_POST['apellidos'] ?? '');
$puesto     = trim($_POST['puesto'] ?? '');
$telefono   = trim($_POST['telefono'] ?? '');
$contrasena          = $_POST['contrasena'] ?? '';
$activo              = isset($_POST['activo']) ? 1 : 0;
$sso                 = isset($_POST['sso']) ? 1 : 0;
$role_name           = trim($_POST['role_id'] ?? 'Operador');
$empresaId           = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);
$tenantRoleIdsRaw    = $_POST['tenant_role_ids'] ?? null;

$empresaSesion = ensure_session_empresa_id();
if (!session_is_superadmin()) {
    if ($empresaId === null || $empresaId <= 0 || $empresaSesion === null || $empresaSesion !== $empresaId) {
        echo json_encode(["success" => false, "msg" => "Empresa no autorizada"]);
        exit;
    }
}

if ($empresaId === null || $empresaId <= 0) {
    $empresaId = $empresaSesion ?? 1;
}

$tenantRoleIds      = tenant_roles_normalize_int_list($tenantRoleIdsRaw);
$validTenantRoleIds = [];
$tenantRoleNames    = [];
$empresaUserCount   = 0;
$isFirstCompanyUser = false;
$forcedTenantRoleId = null;
$hasLegacyClienteColumn = false;
$columnsResult = $conn->query("SHOW COLUMNS FROM usuarios LIKE 'cliente_id'");
if ($columnsResult instanceof mysqli_result) {
    $hasLegacyClienteColumn = $columnsResult->num_rows > 0;
    $columnsResult->close();
}

if ($empresaId !== null && $empresaId > 0) {
    if ($hasLegacyClienteColumn) {
        $stmtCount = $conn->prepare('SELECT COUNT(*) AS total FROM usuarios WHERE empresa_id = ? OR cliente_id = ?');
    } else {
        $stmtCount = $conn->prepare('SELECT COUNT(*) AS total FROM usuarios WHERE empresa_id = ?');
    }

    if (!$stmtCount) {
        respond_error('No se pudo verificar los usuarios registrados para la empresa.', 500);
    }

    if ($hasLegacyClienteColumn) {
        $stmtCount->bind_param('ii', $empresaId, $empresaId);
    } else {
        $stmtCount->bind_param('i', $empresaId);
    }

    if (!$stmtCount->execute()) {
        $stmtCount->close();
        respond_error('No se pudo verificar los usuarios registrados para la empresa.', 500);
    }

    $stmtCount->bind_result($empresaUserCountRaw);
    $stmtCount->fetch();
    $stmtCount->close();

    $empresaUserCount = (int) ($empresaUserCountRaw ?? 0);
    $isFirstCompanyUser = $empresaUserCount === 0;

    if ($isFirstCompanyUser) {
        $role_name = 'Cliente';

        if (!tenant_roles_table_exists($conn, 'tenant_roles')) {
            respond_error('La configuración de roles delegados no está disponible para la empresa seleccionada.', 500);
        }

        $stmtResponsable = $conn->prepare('SELECT id FROM tenant_roles WHERE empresa_id = ? AND LOWER(nombre) = LOWER(?) LIMIT 1');
        if (!$stmtResponsable) {
            respond_error('No se pudo validar el rol delegado "responsable".', 500);
        }

        $responsableSlug = 'responsable';
        $stmtResponsable->bind_param('is', $empresaId, $responsableSlug);
        if (!$stmtResponsable->execute()) {
            $stmtResponsable->close();
            respond_error('No se pudo validar el rol delegado "responsable".', 500);
        }

        $resultResponsable = $stmtResponsable->get_result();
        $rowResponsable    = $resultResponsable ? $resultResponsable->fetch_assoc() : null;
        if ($resultResponsable instanceof mysqli_result) {
            $resultResponsable->close();
        }
        $stmtResponsable->close();

        if (!$rowResponsable || !isset($rowResponsable['id'])) {
            respond_error('La empresa no tiene configurado el rol delegado "responsable".', 500);
        }

        $forcedTenantRoleId = (int) $rowResponsable['id'];
        $tenantRoleIds[]    = $forcedTenantRoleId;
        $tenantRoleIds      = tenant_roles_normalize_int_list($tenantRoleIds);
    }
}

if ($tenantRoleIds !== []) {
    if (!tenant_roles_table_exists($conn, 'tenant_roles')) {
        respond_error('La configuración de roles delegados no está disponible.');
    }
    $validTenantRoleIds = tenant_roles_validate_ids($conn, $tenantRoleIds, $empresaId);
    if (count($validTenantRoleIds) !== count($tenantRoleIds)) {
        respond_error('Los roles delegados no pertenecen a la empresa indicada.');
    }

    if ($isFirstCompanyUser && $forcedTenantRoleId !== null && !in_array($forcedTenantRoleId, $validTenantRoleIds, true)) {
        respond_error('No fue posible confirmar el rol delegado "responsable" para la empresa.', 500);
    }
    $tenantRoleNames = tenant_roles_fetch_names($conn, $validTenantRoleIds);
}

if (!$correo || !$nombre || !$apellidos || !$puesto || !$telefono || !$contrasena || !$role_name) {
    respond_error('Todos los campos son obligatorios.');
}

if (mb_strlen($puesto) > 150) {
    respond_error('El puesto no puede exceder 150 caracteres.');
}

if (mb_strlen($telefono) > 50) {
    respond_error('El teléfono no puede exceder 50 caracteres.');
}

$pwValid = strlen($contrasena) >= 8 && preg_match('/[A-Z]/', $contrasena) && preg_match('/[a-z]/', $contrasena) && preg_match('/\d/', $contrasena) && preg_match('/[^a-zA-Z0-9]/', $contrasena);
if (!$pwValid) {
    respond_error('La contraseña no cumple los requisitos.');
}
$hash = password_hash($contrasena, PASSWORD_DEFAULT);
$usuarioLookup = mb_strtolower($usuario, 'UTF-8');
$stmt = $conn->prepare('SELECT id FROM usuarios WHERE LOWER(usuario) = ? LIMIT 1');
$stmt->bind_param('s', $usuarioLookup);
$stmt->execute();
$existe = $stmt->get_result()->num_rows;
$stmt->close();
if ($existe) {
    respond_error('Ya existe un usuario con este nombre de usuario.');
}

$stmt = $conn->prepare('SELECT id FROM usuarios WHERE correo=? LIMIT 1');
$stmt->bind_param('s', $correo);
$stmt->execute();
$existe = $stmt->get_result()->num_rows;
$stmt->close();
if ($existe) {
    respond_error('Ya existe un usuario con este correo.');
}
$role_id = resolve_role_id($role_name, $empresaId);
if ($role_id === null) {
    respond_error('Rol inválido.');
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
    respond_error('No se pudo validar el portal asociado al rol.', 500);
}
$stmtRolePortal->bind_param('i', $role_id);
if (!$stmtRolePortal->execute()) {
    $stmtRolePortal->close();
    respond_error('No se pudo validar el portal asociado al rol.', 500);
}
$resultRolePortal = $stmtRolePortal->get_result();
$rolePortalData   = $resultRolePortal ? $resultRolePortal->fetch_assoc() : null;
if ($resultRolePortal instanceof mysqli_result) {
    $resultRolePortal->close();
}
$stmtRolePortal->close();

if (!$rolePortalData) {
    respond_error('No se pudo determinar el portal para el rol seleccionado.');
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
        respond_error('No se pudo ubicar el portal asociado al rol.', 500);
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
    respond_error('No se pudo determinar el portal para el rol seleccionado.');
}

$sql = "INSERT INTO usuarios (usuario, correo, nombre, apellidos, puesto, telefono, contrasena, empresa_id, role_id, portal_id, activo, sso) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
$stmt = $conn->prepare($sql);
if (!$stmt) {
    respond_error('Error al preparar registro de usuario.', 500);
}
$stmt->bind_param(
    "sssssssiiiii",
    $usuario,
    $correo,
    $nombre,
    $apellidos,
    $puesto,
    $telefono,
    $hash,
    $empresaId,
    $role_id,
    $portalId,
    $activo,
    $sso
);

$conn->begin_transaction();
$ok = $stmt->execute();
$nuevoUsuarioId = $ok ? (int) $conn->insert_id : 0;
$stmt->close();

if (!$ok || $nuevoUsuarioId <= 0) {
    $conn->rollback();
    respond_error('Error al registrar usuario.', 500);
}

if ($validTenantRoleIds !== []) {
    if (!tenant_roles_table_exists($conn, 'tenant_user_roles')) {
        $conn->rollback();
        respond_error('La asignación de roles delegados no está disponible.', 500);
    }
    $stmtTenantRole = $conn->prepare('INSERT INTO tenant_user_roles (usuario_id, tenant_role_id) VALUES (?, ?)');
    if (!$stmtTenantRole) {
        $conn->rollback();
        respond_error('No se pudo preparar la asignación de roles delegados.', 500);
    }
    foreach ($validTenantRoleIds as $tenantRoleId) {
        $stmtTenantRole->bind_param('ii', $nuevoUsuarioId, $tenantRoleId);
        if (!$stmtTenantRole->execute()) {
            $stmtTenantRole->close();
            $conn->rollback();
            respond_error('Error al asignar roles delegados.', 500);
        }
    }
    $stmtTenantRole->close();
}

if (!$conn->commit()) {
    $conn->rollback();
    respond_error('Error al confirmar cambios.', 500);
}

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
$detalleRoles = '';
if ($tenantRoleNames !== []) {
    $detalleRoles = ' | Roles delegados: ' . implode(', ', $tenantRoleNames);
}
if ($isFirstCompanyUser) {
    $detalleRoles .= ' | Primer usuario de la empresa';
}
log_activity($nombreAud, "Alta de usuario $usuario$detalleRoles", null, $correoAud);

echo json_encode([
    "success"          => true,
    "msg"              => "Usuario registrado correctamente",
    "usuario_id"       => $nuevoUsuarioId,
    "tenant_role_ids"  => $validTenantRoleIds,
    "empresa_user_count" => $empresaUserCount,
    "is_first_user"    => $isFirstCompanyUser,
    "forced_role_id"   => $isFirstCompanyUser ? $role_id : null,
    "forced_tenant_role_id" => $isFirstCompanyUser ? $forcedTenantRoleId : null,
]);
?>
