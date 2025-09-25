<?php
declare(strict_types=1);

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 4) . '/app/Core/delegated_roles.php';
require_once dirname(__DIR__, 4) . '/app/Core/permissions.php';

if (!function_exists('respond_json')) {
    function respond_json(array $payload, int $status = 200): void
    {
        http_response_code($status);
        echo json_encode($payload);
        exit;
    }
}

if (!function_exists('delegated_roles_column_exists')) {
    function delegated_roles_column_exists(mysqli $conn, string $column): bool
    {
        static $cache = [];
        if (array_key_exists($column, $cache)) {
            return $cache[$column];
        }

        $stmt = $conn->prepare('SHOW COLUMNS FROM roles LIKE ?');
        if (!$stmt) {
            return $cache[$column] = false;
        }
        $stmt->bind_param('s', $column);
        $stmt->execute();
        $result = $stmt->get_result();
        $exists = $result ? $result->num_rows > 0 : false;
        $stmt->close();

        return $cache[$column] = $exists;
    }
}

if (!function_exists('delegated_roles_update_metadata')) {
    function delegated_roles_update_metadata(mysqli $conn, int $roleId, string $visibleName, string $descripcion): void
    {
        $hasVisible = delegated_roles_column_exists($conn, 'nombre_visible');
        $hasDescription = delegated_roles_column_exists($conn, 'descripcion');

        if (!$hasVisible && !$hasDescription) {
            return;
        }

        if ($hasVisible) {
            $nombreVisible = $visibleName !== '' ? $visibleName : null;
            $stmt = $conn->prepare('UPDATE roles SET nombre_visible = ? WHERE id = ?');
            if ($stmt) {
                $stmt->bind_param('si', $nombreVisible, $roleId);
                $stmt->execute();
                $stmt->close();
            }
        }

        if ($hasDescription) {
            $descripcionValue = $descripcion !== '' ? $descripcion : null;
            $stmt = $conn->prepare('UPDATE roles SET descripcion = ? WHERE id = ?');
            if ($stmt) {
                $stmt->bind_param('si', $descripcionValue, $roleId);
                $stmt->execute();
                $stmt->close();
            }
        }
    }
}

if (!function_exists('delegated_roles_fetch_payload')) {
    function delegated_roles_fetch_payload(mysqli $conn, int $roleId): ?array
    {
        $columns = ['id', 'nombre', 'empresa_id'];
        if (delegated_roles_column_exists($conn, 'nombre_visible')) {
            $columns[] = 'nombre_visible';
        }
        if (delegated_roles_column_exists($conn, 'descripcion')) {
            $columns[] = 'descripcion';
        }
        $sql = 'SELECT ' . implode(', ', $columns) . ' FROM roles WHERE id = ? AND delegated = 1 LIMIT 1';
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return null;
        }
        $stmt->bind_param('i', $roleId);
        $stmt->execute();
        $result = $stmt->get_result();
        $role = $result ? $result->fetch_assoc() : null;
        $stmt->close();
        if (!$role) {
            return null;
        }

        $permStmt = $conn->prepare(
            'SELECT p.nombre FROM role_permissions rp '
            . 'INNER JOIN permissions p ON p.id = rp.permission_id '
            . 'WHERE rp.role_id = ? ORDER BY p.nombre ASC'
        );
        $permisos = [];
        if ($permStmt) {
            $permStmt->bind_param('i', $roleId);
            $permStmt->execute();
            $permResult = $permStmt->get_result();
            while ($permRow = $permResult->fetch_assoc()) {
                $nombrePerm = (string) ($permRow['nombre'] ?? '');
                if ($nombrePerm !== '') {
                    $permisos[] = $nombrePerm;
                }
            }
            $permStmt->close();
        }

        return [
            'id' => (int) $role['id'],
            'nombre' => (string) $role['nombre'],
            'nombre_visible' => isset($role['nombre_visible']) && $role['nombre_visible'] !== ''
                ? (string) $role['nombre_visible']
                : (string) $role['nombre'],
            'descripcion' => isset($role['descripcion']) && $role['descripcion'] !== null
                ? (string) $role['descripcion']
                : '',
            'empresa_id' => isset($role['empresa_id']) ? (int) $role['empresa_id'] : null,
            'permisos' => $permisos,
        ];
    }
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    respond_json(['success' => false, 'msg' => 'Método no permitido.'], 405);
}

if (!check_permission('usuarios_add')) {
    respond_json(['success' => false, 'msg' => 'No cuentas con permisos para crear roles delegados.'], 403);
}

global $conn;
if (!isset($conn)) {
    $conn = DatabaseManager::getConnection();
}

$actorId = $_SESSION['usuario_id'] ?? null;
if (!is_int($actorId) && !ctype_digit((string) $actorId)) {
    respond_json(['success' => false, 'msg' => 'Sesión no válida.'], 401);
}
$actorId = (int) $actorId;

$empresaSesion = ensure_session_empresa_id();
$empresaSolicitada = requested_empresa_id();
$empresaFormulario = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);
$empresaObjetivo = $empresaSesion;

$empresaReferencia = $empresaFormulario !== false && $empresaFormulario !== null ? $empresaFormulario : null;
if ($empresaReferencia === null) {
    $empresaReferencia = $empresaSolicitada;
}

if ($empresaReferencia !== null) {
    if ($empresaSesion !== null && $empresaReferencia !== $empresaSesion && !session_is_superadmin()) {
        respond_json(['success' => false, 'msg' => 'Empresa no autorizada.'], 403);
    }
    if ($empresaSesion === null) {
        $_SESSION['empresa_id'] = $empresaReferencia;
    }
    $empresaObjetivo = $empresaReferencia;
}

if ($empresaObjetivo === null) {
    respond_json(['success' => false, 'msg' => 'No se pudo determinar la empresa de trabajo.'], 400);
}

$visibleName = trim((string) ($_POST['nombre_visible'] ?? ''));
if ($visibleName === '') {
    respond_json(['success' => false, 'msg' => 'Ingresa un nombre visible para el rol.'], 422);
}

$internalName = trim((string) ($_POST['nombre'] ?? ''));
if ($internalName === '') {
    $normalized = strtolower($visibleName);
    $normalized = iconv('UTF-8', 'ASCII//TRANSLIT', $normalized) ?: $normalized;
    $normalized = preg_replace('/[^a-z0-9]+/i', '_', $normalized ?? '');
    $normalized = trim((string) $normalized, '_');
    if ($normalized === '') {
        $normalized = 'delegado_' . $empresaObjetivo . '_' . time();
    }
    $internalName = $normalized;
}

$descripcion = trim((string) ($_POST['descripcion'] ?? ''));

$permisosRaw = $_POST['permisos'] ?? [];
if (!is_array($permisosRaw)) {
    $permisosRaw = [$permisosRaw];
}
$permissionNames = [];
foreach ($permisosRaw as $permiso) {
    if (is_string($permiso)) {
        $permiso = trim($permiso);
        if ($permiso !== '') {
            $permissionNames[] = $permiso;
        }
    }
}

if ($permissionNames === []) {
    respond_json(['success' => false, 'msg' => 'Selecciona al menos un permiso para el rol.'], 422);
}

try {
    $roleData = create_delegated_role($empresaObjetivo, $internalName, $permissionNames, $actorId);
    $roleId = (int) ($roleData['id'] ?? 0);
    if ($roleId <= 0) {
        respond_json(['success' => false, 'msg' => 'No se pudo registrar el rol delegado.'], 500);
    }

    delegated_roles_update_metadata($conn, $roleId, $visibleName, $descripcion);

    $payload = delegated_roles_fetch_payload($conn, $roleId);
    if ($payload === null) {
        $payload = [
            'id' => $roleId,
            'nombre' => $internalName,
            'nombre_visible' => $visibleName,
            'descripcion' => $descripcion,
            'empresa_id' => $empresaObjetivo,
            'permisos' => $permissionNames,
        ];
    }

    respond_json([
        'success' => true,
        'msg' => 'Rol delegado creado correctamente.',
        'role' => $payload,
    ]);
} catch (RuntimeException $e) {
    respond_json(['success' => false, 'msg' => $e->getMessage()], 400);
} catch (Throwable $e) {
    respond_json(['success' => false, 'msg' => 'Ocurrió un error al registrar el rol delegado.'], 500);
}
