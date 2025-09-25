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

if (!check_permission('usuarios_add') && !check_permission('usuarios_edit')) {
    respond_json(['success' => false, 'msg' => 'Acceso denegado.'], 403);
}

global $conn;
if (!isset($conn)) {
    $conn = DatabaseManager::getConnection();
}

$stmt = $conn->prepare('SELECT nombre, descripcion FROM permissions ORDER BY nombre ASC');
if (!$stmt) {
    respond_json(['success' => false, 'msg' => 'No fue posible recuperar los permisos disponibles.'], 500);
}
$stmt->execute();
$result = $stmt->get_result();
$permissions = [];
while ($row = $result->fetch_assoc()) {
    $permissions[] = [
        'nombre' => (string) ($row['nombre'] ?? ''),
        'descripcion' => isset($row['descripcion']) && $row['descripcion'] !== null
            ? (string) $row['descripcion']
            : '',
    ];
}
$stmt->close();

respond_json([
    'success' => true,
    'permissions' => $permissions,
]);
