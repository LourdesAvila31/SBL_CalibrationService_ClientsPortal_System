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
$empresaObjetivo = $empresaSesion;

if ($empresaSolicitada !== null) {
    if ($empresaSesion !== null && $empresaSolicitada !== $empresaSesion && !session_is_superadmin()) {
        respond_json(['success' => false, 'msg' => 'Empresa no autorizada'], 403);
    }
    if ($empresaSesion === null) {
        $_SESSION['empresa_id'] = $empresaSolicitada;
    }
    $empresaObjetivo = $empresaSolicitada;
}

if ($empresaObjetivo === null) {
    respond_json(['success' => false, 'msg' => 'No se pudo determinar la empresa en sesión'], 400);
}

try {
    $actor = delegated_roles_fetch_actor($conn, $actorId);
    if (!delegated_roles_actor_can_manage($conn, $actor, $empresaObjetivo)) {
        respond_json(['success' => false, 'msg' => 'No cuentas con permisos para administrar roles delegados.'], 403);
    }
} catch (Throwable $e) {
    respond_json(['success' => false, 'msg' => $e->getMessage()], 400);
}

$columns = ['id', 'nombre', 'empresa_id'];
if (delegated_roles_column_exists($conn, 'nombre_visible')) {
    $columns[] = 'nombre_visible';
}
if (delegated_roles_column_exists($conn, 'descripcion')) {
    $columns[] = 'descripcion';
}
$select = 'SELECT ' . implode(', ', $columns) . ' FROM roles WHERE delegated = 1 AND empresa_id = ? ORDER BY nombre ASC';
$stmt = $conn->prepare($select);
if (!$stmt) {
    respond_json(['success' => false, 'msg' => 'No fue posible consultar los roles delegados.'], 500);
}
$stmt->bind_param('i', $empresaObjetivo);
$stmt->execute();
$result = $stmt->get_result();
$roles = [];
while ($row = $result->fetch_assoc()) {
    $roles[(int) $row['id']] = [
        'id' => (int) $row['id'],
        'nombre' => (string) $row['nombre'],
        'nombre_visible' => isset($row['nombre_visible']) && $row['nombre_visible'] !== ''
            ? (string) $row['nombre_visible']
            : (string) $row['nombre'],
        'descripcion' => isset($row['descripcion']) && $row['descripcion'] !== null
            ? (string) $row['descripcion']
            : '',
        'empresa_id' => isset($row['empresa_id']) ? (int) $row['empresa_id'] : null,
        'permisos' => [],
    ];
}
$stmt->close();

if ($roles !== []) {
    $roleIds = array_keys($roles);
    $placeholders = implode(',', array_fill(0, count($roleIds), '?'));
    $permSql = 'SELECT rp.role_id, p.nombre FROM role_permissions rp '
        . 'INNER JOIN permissions p ON p.id = rp.permission_id '
        . "WHERE rp.role_id IN ($placeholders) ORDER BY p.nombre ASC";
    $permStmt = $conn->prepare($permSql);
    if ($permStmt) {
        $types = str_repeat('i', count($roleIds));
        $permStmt->bind_param($types, ...$roleIds);
        $permStmt->execute();
        $permResult = $permStmt->get_result();
        while ($permRow = $permResult->fetch_assoc()) {
            $roleKey = (int) $permRow['role_id'];
            if (!isset($roles[$roleKey])) {
                continue;
            }
            $permName = (string) ($permRow['nombre'] ?? '');
            if ($permName !== '') {
                $roles[$roleKey]['permisos'][] = $permName;
            }
        }
        $permStmt->close();
    }
}

respond_json([
    'success' => true,
    'roles' => array_values($roles),
]);
