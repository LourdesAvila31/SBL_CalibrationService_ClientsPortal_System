<?php
declare(strict_types=1);

require_once __DIR__ . '/db.php';
require_once __DIR__ . '/permissions.php';
require_once dirname(__DIR__) . '/Modules/Internal/Auditoria/audit.php';

/**
 * Obtiene información básica del actor que ejecuta una operación de roles.
 *
 * @return array{id:int,empresa_id:?int,role_id:int,nombre:string,correo:string,es_superadmin:bool}
 */
function delegated_roles_fetch_actor(mysqli $conn, int $actorId): array
{
    $stmt = $conn->prepare(
        'SELECT u.id, u.empresa_id, u.role_id, u.nombre, u.apellidos, u.correo, '
        . 'r.nombre AS role_name '
        . 'FROM usuarios u '
        . 'LEFT JOIN roles r ON r.id = u.role_id '
        . 'WHERE u.id = ? LIMIT 1'
    );
    if (!$stmt) {
        throw new RuntimeException('No fue posible preparar la consulta de actor.');
    }
    $stmt->bind_param('i', $actorId);
    $stmt->execute();
    $res = $stmt->get_result();
    $actor = $res ? $res->fetch_assoc() : null;
    $stmt->close();

    if (!$actor) {
        throw new RuntimeException('El actor especificado no existe.');
    }

    $nombre = trim((string) ($actor['nombre'] ?? ''));
    $apellidos = trim((string) ($actor['apellidos'] ?? ''));
    $correo = trim((string) ($actor['correo'] ?? ''));

    return [
        'id' => (int) $actor['id'],
        'empresa_id' => isset($actor['empresa_id']) ? (int) $actor['empresa_id'] : null,
        'role_id' => (int) $actor['role_id'],
        'nombre' => trim($nombre . ' ' . $apellidos) ?: $nombre ?: 'Usuario sin nombre',
        'correo' => $correo !== '' ? $correo : 'desconocido@sistema.local',
        'es_superadmin' => strcasecmp((string) ($actor['role_name'] ?? ''), 'Superadministrador') === 0
            || (int) $actor['role_id'] === 1,
    ];
}

/**
 * Verifica si el actor puede gestionar roles delegados de la empresa.
 */
function delegated_roles_actor_can_manage(mysqli $conn, array $actor, int $empresaId): bool
{
    if ($actor['es_superadmin']) {
        return true;
    }

    if ($actor['empresa_id'] !== $empresaId) {
        return false;
    }

    $stmt = $conn->prepare(
        'SELECT 1 FROM role_permissions rp '
        . 'INNER JOIN permissions p ON p.id = rp.permission_id '
        . "WHERE rp.role_id = ? AND p.nombre IN ('usuarios_add','usuarios_edit','usuarios_delete') LIMIT 1"
    );
    if (!$stmt) {
        return false;
    }
    $stmt->bind_param('i', $actor['role_id']);
    $stmt->execute();
    $hasPerm = false;
    if (method_exists($stmt, 'store_result')) {
        $stmt->store_result();
        $hasPerm = $stmt->num_rows > 0;
    } else {
        $result = $stmt->get_result();
        $hasPerm = $result ? (bool) $result->fetch_assoc() : false;
    }
    $stmt->close();

    return $hasPerm;
}

/**
 * Obtiene los identificadores de permiso requeridos, validando su existencia.
 *
 * @param list<string> $permissionNames
 * @return array<int,string> Map permission_id => permission_name
 */
function delegated_roles_permission_map(mysqli $conn, array $permissionNames): array
{
    $filtered = [];
    foreach ($permissionNames as $perm) {
        $perm = trim((string) $perm);
        if ($perm !== '') {
            $filtered[$perm] = true;
        }
    }
    if ($filtered === []) {
        throw new RuntimeException('Debe especificar al menos un permiso.');
    }

    $placeholders = implode(',', array_fill(0, count($filtered), '?'));
    $stmt = $conn->prepare(
        "SELECT id, nombre FROM permissions WHERE nombre IN ($placeholders)"
    );
    if (!$stmt) {
        throw new RuntimeException('No fue posible preparar la consulta de permisos.');
    }
    $types = str_repeat('s', count($filtered));
    $values = array_keys($filtered);
    $stmt->bind_param($types, ...$values);
    $stmt->execute();
    $result = $stmt->get_result();
    $map = [];
    while ($row = $result->fetch_assoc()) {
        $map[(int) $row['id']] = (string) $row['nombre'];
    }
    $stmt->close();

    $missing = array_diff($values, array_values($map));
    if ($missing !== []) {
        throw new RuntimeException('Permisos inválidos: ' . implode(', ', $missing));
    }

    return $map;
}

/**
 * Crea un nuevo rol delegado para la empresa indicada.
 *
 * @param list<string> $permissionNames
 * @return array{id:int,nombre:string,permisos:list<string>}
 */
function create_delegated_role(int $empresaId, string $nombre, array $permissionNames, int $actorId): array
{
    global $conn;
    if (!isset($conn)) {
        $conn = DatabaseManager::getConnection();
    }

    $empresaId = (int) $empresaId;
    if ($empresaId <= 0) {
        throw new RuntimeException('La empresa especificada es inválida.');
    }

    $nombre = trim($nombre);
    if ($nombre === '') {
        throw new RuntimeException('El nombre del rol delegado es obligatorio.');
    }

    $actor = delegated_roles_fetch_actor($conn, $actorId);
    if (!delegated_roles_actor_can_manage($conn, $actor, $empresaId)) {
        throw new RuntimeException('El usuario no puede administrar roles delegados de esta empresa.');
    }

    $permMap = delegated_roles_permission_map($conn, $permissionNames);

    $conn->begin_transaction();
    try {
        $stmtEmpresa = $conn->prepare('SELECT id, nombre FROM empresas WHERE id = ? LIMIT 1');
        if (!$stmtEmpresa) {
            throw new RuntimeException('No fue posible verificar la empresa.');
        }
        $stmtEmpresa->bind_param('i', $empresaId);
        $stmtEmpresa->execute();
        $empresaInfo = $stmtEmpresa->get_result()->fetch_assoc();
        $stmtEmpresa->close();
        if (!$empresaInfo) {
            throw new RuntimeException('La empresa indicada no existe.');
        }

        $stmtRole = $conn->prepare('INSERT INTO roles (nombre, empresa_id, delegated) VALUES (?, ?, 1)');
        if (!$stmtRole) {
            throw new RuntimeException('No fue posible crear el rol delegado.');
        }
        $stmtRole->bind_param('si', $nombre, $empresaId);
        $stmtRole->execute();
        $roleId = (int) $stmtRole->insert_id;
        $stmtRole->close();

        foreach ($permMap as $permissionId => $permissionName) {
            $stmtPerm = $conn->prepare('INSERT INTO role_permissions (role_id, permission_id) VALUES (?, ?)');
            if ($stmtPerm) {
                $stmtPerm->bind_param('ii', $roleId, $permissionId);
                $stmtPerm->execute();
                $stmtPerm->close();
            }
        }

        $conn->commit();

        log_activity($actor['nombre'], [
            'seccion' => 'roles_delegados',
            'valor_nuevo' => sprintf(
                "Creó rol delegado '%s' para la empresa %s con permisos: %s",
                $nombre,
                $empresaInfo['nombre'] ?? $empresaId,
                implode(', ', array_values($permMap))
            ),
            'usuario_id' => $actor['id'],
            'usuario_correo' => $actor['correo'],
        ]);

        return [
            'id' => $roleId,
            'nombre' => $nombre,
            'permisos' => array_values($permMap),
        ];
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }
}

/**
 * Actualiza los metadatos y permisos de un rol delegado existente.
 *
 * @param list<string> $permissionNames
 */
function update_delegated_role(int $roleId, ?string $nuevoNombre, ?array $permissionNames, int $actorId): void
{
    global $conn;
    if (!isset($conn)) {
        $conn = DatabaseManager::getConnection();
    }

    $stmt = $conn->prepare('SELECT id, nombre, empresa_id FROM roles WHERE id = ? AND delegated = 1 LIMIT 1');
    if (!$stmt) {
        throw new RuntimeException('No fue posible preparar la consulta del rol.');
    }
    $stmt->bind_param('i', $roleId);
    $stmt->execute();
    $role = $stmt->get_result()->fetch_assoc();
    $stmt->close();

    if (!$role) {
        throw new RuntimeException('Rol delegado inexistente.');
    }

    $empresaId = (int) $role['empresa_id'];
    $actor = delegated_roles_fetch_actor($conn, $actorId);
    if (!delegated_roles_actor_can_manage($conn, $actor, $empresaId)) {
        throw new RuntimeException('El usuario no puede modificar roles delegados de esta empresa.');
    }

    $changes = [];
    $nuevoNombre = $nuevoNombre !== null ? trim($nuevoNombre) : null;
    $permMap = null;

    if ($permissionNames !== null) {
        $permMap = delegated_roles_permission_map($conn, $permissionNames);
    }

    $conn->begin_transaction();
    try {
        if ($nuevoNombre !== null && $nuevoNombre !== '' && $nuevoNombre !== $role['nombre']) {
            $stmtNombre = $conn->prepare('UPDATE roles SET nombre = ? WHERE id = ?');
            if (!$stmtNombre) {
                throw new RuntimeException('No fue posible actualizar el nombre del rol.');
            }
            $stmtNombre->bind_param('si', $nuevoNombre, $roleId);
            $stmtNombre->execute();
            $stmtNombre->close();
            $changes[] = sprintf("Nombre: '%s' → '%s'", $role['nombre'], $nuevoNombre);
            $role['nombre'] = $nuevoNombre;
        }

        if ($permMap !== null) {
            $conn->query('DELETE FROM role_permissions WHERE role_id = ' . (int) $roleId);
            foreach ($permMap as $permissionId => $permissionName) {
                $stmtPerm = $conn->prepare('INSERT INTO role_permissions (role_id, permission_id) VALUES (?, ?)');
                if ($stmtPerm) {
                    $stmtPerm->bind_param('ii', $roleId, $permissionId);
                    $stmtPerm->execute();
                    $stmtPerm->close();
                }
            }
            $changes[] = 'Permisos sincronizados: ' . implode(', ', array_values($permMap));
        }

        $conn->commit();

        if ($changes !== []) {
            log_activity($actor['nombre'], [
                'seccion' => 'roles_delegados',
                'valor_nuevo' => sprintf(
                    "Actualizó rol delegado '%s' (%s)",
                    $role['nombre'],
                    implode('; ', $changes)
                ),
                'usuario_id' => $actor['id'],
                'usuario_correo' => $actor['correo'],
            ]);
        }
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }
}

/**
 * Asigna un rol delegado como rol principal de un usuario.
 */
function assign_delegated_role_to_user(int $roleId, int $userId, int $actorId): void
{
    global $conn;
    if (!isset($conn)) {
        $conn = DatabaseManager::getConnection();
    }

    $stmtRole = $conn->prepare('SELECT id, nombre, empresa_id FROM roles WHERE id = ? AND delegated = 1 LIMIT 1');
    if (!$stmtRole) {
        throw new RuntimeException('No fue posible recuperar el rol delegado.');
    }
    $stmtRole->bind_param('i', $roleId);
    $stmtRole->execute();
    $role = $stmtRole->get_result()->fetch_assoc();
    $stmtRole->close();
    if (!$role) {
        throw new RuntimeException('Rol delegado inexistente.');
    }

    $stmtUsuario = $conn->prepare('SELECT id, usuario, nombre, apellidos, empresa_id, role_id FROM usuarios WHERE id = ? LIMIT 1');
    if (!$stmtUsuario) {
        throw new RuntimeException('No fue posible recuperar al usuario objetivo.');
    }
    $stmtUsuario->bind_param('i', $userId);
    $stmtUsuario->execute();
    $usuario = $stmtUsuario->get_result()->fetch_assoc();
    $stmtUsuario->close();
    if (!$usuario) {
        throw new RuntimeException('El usuario especificado no existe.');
    }

    $empresaId = (int) $role['empresa_id'];
    if ((int) $usuario['empresa_id'] !== $empresaId) {
        throw new RuntimeException('No es posible asignar roles delegados de otra empresa.');
    }

    $actor = delegated_roles_fetch_actor($conn, $actorId);
    if (!delegated_roles_actor_can_manage($conn, $actor, $empresaId)) {
        throw new RuntimeException('El usuario no puede asignar roles delegados de esta empresa.');
    }

    $rolAnterior = (int) $usuario['role_id'];

    $conn->begin_transaction();
    try {
        $stmtUpdate = $conn->prepare('UPDATE usuarios SET role_id = ? WHERE id = ?');
        if (!$stmtUpdate) {
            throw new RuntimeException('No fue posible actualizar el rol del usuario.');
        }
        $stmtUpdate->bind_param('ii', $roleId, $userId);
        $stmtUpdate->execute();
        $stmtUpdate->close();

        $conn->commit();

        $oldRoleName = null;
        if ($rolAnterior !== $roleId) {
            $stmtOld = $conn->prepare('SELECT nombre FROM roles WHERE id = ? LIMIT 1');
            if ($stmtOld) {
                $stmtOld->bind_param('i', $rolAnterior);
                $stmtOld->execute();
                $resultOld = $stmtOld->get_result()->fetch_assoc();
                $stmtOld->close();
                if ($resultOld) {
                    $oldRoleName = $resultOld['nombre'];
                }
            }
        }

        $usuarioNombre = trim(($usuario['nombre'] ?? '') . ' ' . ($usuario['apellidos'] ?? ''));
        $usuarioNombre = $usuarioNombre !== '' ? $usuarioNombre : ($usuario['usuario'] ?? (string) $userId);

        log_activity($actor['nombre'], [
            'seccion' => 'roles_delegados',
            'valor_anterior' => $oldRoleName ? "Rol anterior: {$oldRoleName}" : null,
            'valor_nuevo' => sprintf(
                "Asignó rol delegado '%s' al usuario %s (ID %d)",
                $role['nombre'],
                $usuarioNombre,
                $userId
            ),
            'usuario_id' => $actor['id'],
            'usuario_correo' => $actor['correo'],
        ]);
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }
}

