<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('planeacion_crear')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

header('Content-Type: application/json');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

$sql = "SELECT u.id, u.nombre, u.apellidos, u.usuario, u.correo "
     . "FROM usuarios u "
     . "INNER JOIN roles r ON u.role_id = r.id "
     . "WHERE r.nombre = 'Administrador' AND u.activo = 1";

$params = [];
$types = '';
if ($empresaId > 0) {
    $sql .= " AND (u.empresa_id IS NULL OR u.empresa_id = ?)";
    $types .= 'i';
    $params[] = $empresaId;
}

$sql .= ' ORDER BY u.nombre ASC, u.apellidos ASC';

if ($types !== '') {
    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        http_response_code(500);
        echo json_encode(['error' => 'No se pudo preparar la consulta']);
        exit;
    }
    $stmt->bind_param($types, ...$params);
    $stmt->execute();
    $result = $stmt->get_result();
} else {
    $result = $conn->query($sql);
}

$usuarios = [];
if ($result) {
    while ($row = $result->fetch_assoc()) {
        $usuarios[] = [
            'id'        => (int) ($row['id'] ?? 0),
            'nombre'    => $row['nombre'] ?? '',
            'apellidos' => $row['apellidos'] ?? '',
            'usuario'   => $row['usuario'] ?? '',
            'correo'    => $row['correo'] ?? '',
        ];
    }
}

echo json_encode($usuarios);

$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
log_activity($nombreAud, 'Consultó responsables administradores', null, $correoAud);

if (isset($stmt) && $stmt instanceof mysqli_stmt) {
    $stmt->close();
}
if (isset($result) && $result instanceof mysqli_result) {
    $result->free();
}
$conn->close();
