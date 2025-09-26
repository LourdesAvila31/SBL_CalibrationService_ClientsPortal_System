<?php
/**
 * Funciones auxiliares para la gestión de roles y empresas en sesión.
 */
function session_role_alias(): ?string
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $roleName = $_SESSION['rol'] ?? $_SESSION['role_name'] ?? $_SESSION['role_id'] ?? null;
    $roleNum  = $_SESSION['role_num'] ?? null;

    if (is_string($roleName)) {
        $map = [
            'superadministrador' => 'superadministrador',
            'super administrador' => 'superadministrador',
            'administrador' => 'administrador',
            'supervisor' => 'supervisor',
            'operador' => 'operador',
            'lector' => 'lector',
            'cliente' => 'cliente',
            'sistemas' => 'sistemas',
            'developer' => 'developer',
        ];
        $key = strtolower(trim($roleName));
        return $map[$key] ?? $key;
    }

    if ($roleNum !== null) {
        $numMap = [
            1 => 'superadministrador',
            2 => 'administrador',
            3 => 'supervisor',
            4 => 'operador',
            5 => 'lector',
            6 => 'cliente',
            7 => 'sistemas',
            8 => 'developer',
        ];
        return $numMap[(int) $roleNum] ?? null;
    }

    return null;
}

function session_is_superadmin(): bool
{
    $alias = session_role_alias();
    if ($alias !== null && in_array($alias, ['superadministrador', 'developer'], true)) {
        return true;
    }
    $roleNum = $_SESSION['role_num'] ?? null;
    return $roleNum !== null && (int) $roleNum === 1;
}

/**
 * Verifica si el usuario tiene permisos de developer con privilegios de superadministrador
 */
function session_is_developer_superadmin(): bool
{
    $alias = session_role_alias();
    return $alias === 'developer';
}

function session_empresa_id(): ?int
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $raw = $_SESSION['empresa_id'] ?? null;
    if ($raw === null) {
        return null;
    }
    $filtered = filter_var($raw, FILTER_VALIDATE_INT);
    if ($filtered && $filtered > 0) {
        $value = (int) $filtered;
        $_SESSION['empresa_id'] = $value;
        return $value;
    }

    return null;
}

function ensure_session_empresa_id(): ?int
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $empresa = session_empresa_id();
    if ($empresa !== null) {
        return $empresa;
    }

    if (!isset($_SESSION['usuario_id'])) {
        return null;
    }

    require_once __DIR__ . '/db.php';
    global $conn;

    if (!isset($conn) || !$conn) {
        return null;
    }

    if ($stmt = $conn->prepare('SELECT empresa_id FROM usuarios WHERE id = ? LIMIT 1')) {
        $stmt->bind_param('i', $_SESSION['usuario_id']);
        $stmt->execute();
        $stmt->bind_result($empresaDb);
        if ($stmt->fetch()) {
            $empresaDb = (int) $empresaDb;
            if ($empresaDb > 0) {
                $_SESSION['empresa_id'] = $empresaDb;
                $stmt->close();
                return $empresaDb;
            }
        }
        $stmt->close();
    }

    return null;
}

/**
 * @return array<int, array{id:int,slug:string,nombre:string}>
 */
function session_portal_options(): array
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $stored = $_SESSION['portal_options'] ?? [];
    if (!is_array($stored)) {
        return [];
    }

    $normalized = [];
    foreach ($stored as $option) {
        if (!is_array($option)) {
            continue;
        }
        $idRaw   = $option['id'] ?? null;
        $slugRaw = $option['slug'] ?? null;
        $nameRaw = $option['nombre'] ?? null;

        $id = filter_var($idRaw, FILTER_VALIDATE_INT);
        $slug = is_string($slugRaw) ? strtolower(trim($slugRaw)) : '';
        if ($id === false || $id === null || $id <= 0 || $slug === '') {
            continue;
        }

        $normalized[] = [
            'id'     => (int) $id,
            'slug'   => $slug,
            'nombre' => is_string($nameRaw) && $nameRaw !== '' ? $nameRaw : ucfirst($slug),
        ];
    }

    $_SESSION['portal_options'] = $normalized;

    return $normalized;
}

function session_portal_slug(): ?string
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $options = session_portal_options();
    $allowed = array_map(static fn (array $option): string => $option['slug'], $options);

    $rawSlug = $_SESSION['portal_slug'] ?? $_SESSION['app_context'] ?? null;
    $slug = is_string($rawSlug) ? strtolower(trim($rawSlug)) : '';

    if ($slug !== '' && (empty($allowed) || in_array($slug, $allowed, true))) {
        $_SESSION['portal_slug'] = $slug;
        $_SESSION['app_context'] = $slug;
        return $slug;
    }

    if (!empty($allowed)) {
        $slug = $allowed[0];
        $_SESSION['portal_slug'] = $slug;
        $_SESSION['app_context'] = $slug;
        return $slug;
    }

    $roleAlias = session_role_alias();
    if ($roleAlias === 'cliente') {
        $slug = 'tenant';
    } elseif ($roleAlias === 'sistemas') {
        $slug = 'service';
    } else {
        $slug = 'internal';
    }

    $_SESSION['portal_slug'] = $slug;
    $_SESSION['app_context'] = $slug;

    return $slug;
}

function session_portal_id(): ?int
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $raw = $_SESSION['portal_id'] ?? null;
    $portalId = filter_var($raw, FILTER_VALIDATE_INT);
    if ($portalId !== false && $portalId !== null && $portalId > 0) {
        $_SESSION['portal_id'] = (int) $portalId;
        return (int) $portalId;
    }

    $slug = session_portal_slug();
    $options = session_portal_options();
    foreach ($options as $option) {
        if ($option['slug'] === $slug) {
            $_SESSION['portal_id'] = $option['id'];
            return $option['id'];
        }
    }

    $usuarioPortal = $_SESSION['usuario_portal_id'] ?? null;
    $usuarioPortal = filter_var($usuarioPortal, FILTER_VALIDATE_INT);
    if ($usuarioPortal !== false && $usuarioPortal !== null && $usuarioPortal > 0) {
        $_SESSION['portal_id'] = (int) $usuarioPortal;
        return (int) $usuarioPortal;
    }

    return null;
}

function session_set_portal(string $slug): bool
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $slug = strtolower(trim($slug));
    if ($slug === '') {
        return false;
    }

    foreach (session_portal_options() as $option) {
        if ($option['slug'] === $slug) {
            $_SESSION['portal_slug'] = $slug;
            $_SESSION['app_context'] = $slug;
            $_SESSION['portal_id']   = $option['id'];
            return true;
        }
    }

    return false;
}

/**
 * @param string|array<int, string> $allowed
 */
function ensure_portal_access($allowed): void
{
    if (!is_array($allowed)) {
        $allowed = [$allowed];
    }

    $allowed = array_map(static function ($value): string {
        return strtolower(trim((string) $value));
    }, $allowed);

    $portalSlug = session_portal_slug();
    if ($portalSlug === null || !in_array($portalSlug, $allowed, true)) {
        http_response_code(403);
        exit('Portal no autorizado.');
    }
}

function requested_empresa_id(): ?int
{
    $fromGet = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT);
    if ($fromGet !== false && $fromGet !== null) {
        return (int) $fromGet;
    }
    $fromPost = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);
    if ($fromPost !== false && $fromPost !== null) {
        return (int) $fromPost;
    }
    return null;
}

/**
 * Verifica si el usuario autenticado tiene el permiso solicitado.
 *
 * La función debe invocarse antes de ejecutar acciones que requieran
 * autorización específica. Utiliza el rol almacenado en la sesión, las
 * relaciones rol-permiso definidas en la base de datos y, en caso de que el
 * usuario opere bajo un tenant (empresa) con rol delegado, los permisos
 * asignados mediante `tenant_roles`/`tenant_role_permissions`.
 *
 * El nombre del permiso sigue la convención `modulo_accion`, por ejemplo:
 * `instrumentos_crear`, `planeacion_leer`, `adjuntos_actualizar`,
 * `clientes_eliminar` o `auditoria_leer`. Los módulos disponibles coinciden
 * con los registrados en la tabla `permissions` durante la instalación del
 * sistema (`add_tables.sql`/`add_seed_data.sql` y opcionalmente `add_seed_data_demo.sql`).
 *
 * @param string $permiso Identificador del permiso en formato modulo_accion.
 * @return bool TRUE si el usuario posee el permiso, FALSE en caso contrario.
 */
function check_permission(string $permiso): bool
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    if (!isset($_SESSION['usuario_id'])) {
        return false;
    }

    $portalSlug = session_portal_slug();

    if ($permiso === 'clientes_gestionar' && $portalSlug !== 'service') {
        return false;
    }

    if (session_is_superadmin()) {
        return true;
    }

    // Developer con privilegios de superadministrador
    if (session_is_developer_superadmin()) {
        return true;
    }

    $roleAlias = session_role_alias() ?? '';

    $unrestrictedReads = [
        'instrumentos_leer',
        'planeacion_leer',
        'calibraciones_leer',
    ];
    if (in_array($permiso, $unrestrictedReads, true)) {
        return true;
    }

    if ($permiso === 'clientes_gestionar') {
        $serviceRoles = ['superadministrador', 'administrador', 'supervisor', 'sistemas'];
        return in_array($roleAlias, $serviceRoles, true);
    }

    $calibratorIntegrationPerms = [
        'integraciones_calibradores_ver',
        'integraciones_calibradores_configurar',
        'integraciones_calibradores_vincular',
        'integraciones_calibradores_ingestar',
    ];
    if (in_array($permiso, $calibratorIntegrationPerms, true)) {
        $integrationRoles = ['superadministrador', 'administrador', 'supervisor', 'sistemas'];
        return in_array($roleAlias, $integrationRoles, true);
    }

    $managementPerms = [
        'instrumentos_crear', 'instrumentos_actualizar', 'instrumentos_eliminar',
        'calibraciones_crear', 'calibraciones_actualizar', 'calibraciones_eliminar', 'calibraciones_aprobar',
        'planeacion_crear', 'planeacion_actualizar', 'planeacion_eliminar',
        'calidad_documentos_crear', 'calidad_documentos_actualizar', 'calidad_documentos_eliminar',
        'calidad_capacitaciones_crear', 'calidad_capacitaciones_actualizar', 'calidad_capacitaciones_eliminar',
        'calidad_nc_crear', 'calidad_nc_actualizar', 'calidad_nc_eliminar',
    ];
    if (in_array($permiso, $managementPerms, true)) {
        $managementRoles = ['superadministrador', 'administrador', 'supervisor'];
        $instrumentManagement = ['instrumentos_crear', 'instrumentos_actualizar', 'instrumentos_eliminar'];

        $tenantClientCanManageInstruments = (
            $portalSlug === 'tenant'
            && $roleAlias === 'cliente'
            && in_array($permiso, $instrumentManagement, true)
        );

        if ($tenantClientCanManageInstruments) {
            return true;
        }

        return in_array($roleAlias, $managementRoles, true);
    }

    $usuariosManage = ['usuarios_add', 'usuarios_edit', 'usuarios_delete'];
    if (in_array($permiso, $usuariosManage, true) && in_array($roleAlias, ['superadministrador', 'sistemas'], true)) {
        return true;
    }

    $empresaSesion    = ensure_session_empresa_id();
    $empresaSolicitada = requested_empresa_id();
    if ($empresaSolicitada !== null) {
        if ($empresaSesion !== null && $empresaSolicitada !== $empresaSesion) {
            return false;
        }
        if ($empresaSesion === null) {
            $_SESSION['empresa_id'] = $empresaSolicitada;
            $empresaSesion = $empresaSolicitada;
        }
    }

    require_once __DIR__ . '/db.php';
    global $conn;

    $usuarioId = (int) $_SESSION['usuario_id'];

    $permisos = permissions_fetch_user_permissions($conn, $usuarioId, $empresaSesion);

    if (in_array($permiso, $usuariosManage, true)) {
        // Los roles delegados pueden habilitar estos permisos incluso si el rol
        // global del usuario no es Sistemas/Superadministrador.
        return in_array($permiso, $permisos, true);
    }

    return in_array($permiso, $permisos, true);
}

/**
 * Obtiene y cachea en memoria los permisos disponibles para un usuario.
 */
function permissions_fetch_user_permissions(mysqli $conn, int $usuarioId, ?int $empresaId): array
{
    static $cache = [];

    $empresaKey = $empresaId !== null ? (string) $empresaId : 'null';
    $portalKey  = session_portal_slug() ?? 'null';
    $cacheKey   = $usuarioId . ':' . $empresaKey . ':' . $portalKey;

    if (array_key_exists($cacheKey, $cache)) {
        return $cache[$cacheKey];
    }

    $permisos = permissions_fetch_global_permissions($conn, $usuarioId, $empresaId);

    if ($empresaId !== null) {
        $permisosTenant = permissions_fetch_tenant_permissions($conn, $usuarioId, $empresaId);
        if ($permisosTenant) {
            $permisos = array_merge($permisos, $permisosTenant);
        }
    }

    if ($permisos) {
        $permisos = array_values(array_unique($permisos));
    }

    $cache[$cacheKey] = $permisos;

    return $permisos;
}

/**
 * Recupera los permisos asociados al rol global del usuario.
 */
function permissions_fetch_global_permissions(mysqli $conn, int $usuarioId, ?int $empresaId): array
{
    $sql = "SELECT DISTINCT p.nombre AS permission_name"
         . " FROM usuarios u"
         . " JOIN roles r ON u.role_id = r.id"
         . " JOIN role_permissions rp ON r.id = rp.role_id"
         . " JOIN permissions p ON p.id = rp.permission_id"
         . " WHERE u.id = ?";

    $types  = 'i';
    $params = [$usuarioId];

    if ($empresaId !== null) {
        $sql    .= " AND (u.empresa_id IS NULL OR u.empresa_id = ?)";
        $types  .= 'i';
        $params[] = $empresaId;
    }

    $portalId = session_portal_id();
    if ($portalId !== null) {
        $sql    .= " AND (r.portal_id IS NULL OR r.portal_id = ?)";
        $types  .= 'i';
        $params[] = $portalId;
    }

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return [];
    }

    $stmt->bind_param($types, ...$params);

    if (!$stmt->execute()) {
        $stmt->close();
        return [];
    }

    $permisos = [];
    if (method_exists($stmt, 'bind_result')) {
        $stmt->bind_result($permisoNombre);
        while ($stmt->fetch()) {
            if (is_string($permisoNombre) && $permisoNombre !== '') {
                $permisos[] = $permisoNombre;
            }
        }
    } elseif (method_exists($stmt, 'get_result')) {
        $res = $stmt->get_result();
        if ($res) {
            while ($row = $res->fetch_assoc()) {
                if (isset($row['permission_name']) && is_string($row['permission_name'])) {
                    $permisos[] = $row['permission_name'];
                }
            }
        }
    }

    $stmt->close();

    return $permisos;
}

/**
 * Recupera el rol delegado (tenant) de un usuario dentro de la empresa.
 */
function permissions_fetch_tenant_role_id(mysqli $conn, int $usuarioId, int $empresaId): ?int
{
    static $roleCache = [];

    $cacheKey = $usuarioId . ':' . $empresaId;
    if (array_key_exists($cacheKey, $roleCache)) {
        return $roleCache[$cacheKey];
    }

    $rolDelegado = null;

    if (isset($_SESSION['tenant_role_ids']) && is_array($_SESSION['tenant_role_ids'])) {
        $stored = $_SESSION['tenant_role_ids'][$empresaId] ?? null;
        if ($stored !== null) {
            $stored = filter_var($stored, FILTER_VALIDATE_INT);
            if ($stored !== false && $stored > 0) {
                $roleCache[$cacheKey] = (int) $stored;
                return $roleCache[$cacheKey];
            }
        }
    }

    $sqlDirect = 'SELECT tenant_role_id FROM usuarios WHERE id = ? AND empresa_id = ? LIMIT 1';
    $stmt = $conn->prepare($sqlDirect);
    if ($stmt) {
        $stmt->bind_param('ii', $usuarioId, $empresaId);
        if ($stmt->execute()) {
            if (method_exists($stmt, 'bind_result')) {
                $stmt->bind_result($tenantRoleId);
                if ($stmt->fetch() && $tenantRoleId !== null) {
                    $tenantRoleId = (int) $tenantRoleId;
                    if ($tenantRoleId > 0) {
                        $rolDelegado = $tenantRoleId;
                    }
                }
            } elseif (method_exists($stmt, 'get_result')) {
                $res = $stmt->get_result();
                if ($res && ($row = $res->fetch_assoc())) {
                    if (isset($row['tenant_role_id'])) {
                        $tenantRoleId = (int) $row['tenant_role_id'];
                        if ($tenantRoleId > 0) {
                            $rolDelegado = $tenantRoleId;
                        }
                    }
                }
            }
        }
        $stmt->close();
    }

    if ($rolDelegado === null) {
        $sqlJoin = 'SELECT tur.tenant_role_id FROM tenant_user_roles tur
                    INNER JOIN tenant_roles tr ON tr.id = tur.tenant_role_id
                    WHERE tur.usuario_id = ? AND tr.empresa_id = ?
                    ORDER BY tur.id DESC LIMIT 1';

        $stmt = $conn->prepare($sqlJoin);
        if ($stmt) {
            $stmt->bind_param('ii', $usuarioId, $empresaId);
            if ($stmt->execute()) {
                if (method_exists($stmt, 'bind_result')) {
                    $stmt->bind_result($tenantRoleId);
                    if ($stmt->fetch() && $tenantRoleId !== null) {
                        $tenantRoleId = (int) $tenantRoleId;
                        if ($tenantRoleId > 0) {
                            $rolDelegado = $tenantRoleId;
                        }
                    }
                } elseif (method_exists($stmt, 'get_result')) {
                    $res = $stmt->get_result();
                    if ($res && ($row = $res->fetch_assoc())) {
                        if (isset($row['tenant_role_id'])) {
                            $tenantRoleId = (int) $row['tenant_role_id'];
                            if ($tenantRoleId > 0) {
                                $rolDelegado = $tenantRoleId;
                            }
                        }
                    }
                }
            }
            $stmt->close();
        }
    }

    if ($rolDelegado !== null) {
        if (!isset($_SESSION['tenant_role_ids']) || !is_array($_SESSION['tenant_role_ids'])) {
            $_SESSION['tenant_role_ids'] = [];
        }
        $_SESSION['tenant_role_ids'][$empresaId] = $rolDelegado;
    }

    $roleCache[$cacheKey] = $rolDelegado;

    return $rolDelegado;
}

/**
 * Recupera los permisos delegados para el tenant (empresa) en sesión.
 */
function permissions_fetch_tenant_permissions(mysqli $conn, int $usuarioId, int $empresaId): array
{
    $rolDelegado = permissions_fetch_tenant_role_id($conn, $usuarioId, $empresaId);
    if ($rolDelegado === null) {
        return [];
    }

    $sql = 'SELECT DISTINCT p.nombre AS permission_name
            FROM tenant_role_permissions trp
            INNER JOIN permissions p ON p.id = trp.permission_id
            WHERE trp.tenant_role_id = ?';

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return [];
    }

    $stmt->bind_param('i', $rolDelegado);

    if (!$stmt->execute()) {
        $stmt->close();
        return [];
    }

    $permisos = [];
    if (method_exists($stmt, 'bind_result')) {
        $stmt->bind_result($permisoNombre);
        while ($stmt->fetch()) {
            if (is_string($permisoNombre) && $permisoNombre !== '') {
                $permisos[] = $permisoNombre;
            }
        }
    } elseif (method_exists($stmt, 'get_result')) {
        $res = $stmt->get_result();
        if ($res) {
            while ($row = $res->fetch_assoc()) {
                if (isset($row['permission_name']) && is_string($row['permission_name'])) {
                    $permisos[] = $row['permission_name'];
                }
            }
        }
    }

    $stmt->close();

    return $permisos;
}

/**
 * Resuelve el identificador de rol considerando el contexto de empresa.
 *
 * @param int|string $roleIdentifier
 */
function resolve_role_id($roleIdentifier, ?int $empresaId = null): ?int
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    require_once __DIR__ . '/db.php';
    global $conn;
    if (!isset($conn) || !$conn) {
        return null;
    }

    if ($empresaId === null) {
        $empresaId = session_empresa_id();
    }

    if (is_int($roleIdentifier) || ctype_digit((string) $roleIdentifier)) {
        $roleId = (int) $roleIdentifier;
        $stmt = $conn->prepare('SELECT id, empresa_id FROM roles WHERE id = ? LIMIT 1');
        if (!$stmt) {
            return null;
        }
        $stmt->bind_param('i', $roleId);
        $stmt->execute();
        $result = $stmt->get_result()->fetch_assoc();
        $stmt->close();
        if (!$result) {
            return null;
        }
        $empresaRol = isset($result['empresa_id']) ? (int) $result['empresa_id'] : null;
        if ($empresaRol !== null && $empresaId !== null && $empresaId !== $empresaRol) {
            return null;
        }
        return (int) $result['id'];
    }

    $roleName = trim((string) $roleIdentifier);
    if ($roleName === '') {
        return null;
    }

    $stmt = $conn->prepare(
        'SELECT id, empresa_id FROM roles WHERE nombre = ? ORDER BY (empresa_id IS NULL) DESC, id ASC'
    );
    if (!$stmt) {
        return null;
    }
    $stmt->bind_param('s', $roleName);
    $stmt->execute();
    $result = $stmt->get_result();

    $fallbackId = null;
    $matchId = null;
    while ($row = $result->fetch_assoc()) {
        $empresaRol = isset($row['empresa_id']) ? (int) $row['empresa_id'] : null;
        if ($empresaRol === null) {
            $fallbackId = (int) $row['id'];
        }
        if ($empresaId !== null && $empresaRol === $empresaId) {
            $matchId = (int) $row['id'];
            break;
        }
        if ($empresaId === null && $empresaRol === null) {
            $matchId = (int) $row['id'];
            break;
        }
    }
    $stmt->close();

    if ($matchId !== null) {
        return $matchId;
    }

    return $fallbackId;
}
