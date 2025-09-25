<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

header('Content-Type: application/json');
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';

$esSuper = session_is_superadmin();
$empresaSesion = ensure_session_empresa_id();
$sqlBase = "SELECT u.id, u.nombre, u.correo, u.telefono, r.nombre AS role_name, u.fecha_creacion, u.empresa_id, e.nombre AS empresa_nombre "
         . "FROM usuarios u LEFT JOIN roles r ON u.role_id = r.id "
         . "LEFT JOIN empresas e ON u.empresa_id = e.id";

if ($esSuper || $empresaSesion === null) {
    $result = $conn->query($sqlBase);
} else {
    $stmt = $conn->prepare($sqlBase . " WHERE u.empresa_id = ?");
    $stmt->bind_param('i', $empresaSesion);
    $stmt->execute();
    $result = $stmt->get_result();
}

$usuarios = array();

if ($result->num_rows > 0) {
    while ($row = $result->fetch_assoc()) {
        $row['role_id'] = $row['role_name'] ?? $row['role_id'] ?? null;
        $row['empresa_id'] = isset($row['empresa_id']) ? (int) $row['empresa_id'] : null;
        $row['telefono'] = $row['telefono'] ?? null;
        $row['empresa_nombre'] = $row['empresa_nombre'] ?? null;
        $usuarios[] = $row;
    }
}

echo json_encode($usuarios);

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
log_activity($nombreAud, 'Consultó lista de usuarios', null, $correoAud);

if (isset($stmt) && $stmt) {
    $stmt->close();
}
$conn->close();
?>
