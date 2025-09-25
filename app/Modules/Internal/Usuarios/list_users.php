<?php
// Devuelve la lista de usuarios en formato JSON para su consumo desde el frontend.
// Requiere una conexión válida a la base de datos y no utiliza datasets de respaldo.

session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

header('Content-Type: application/json');

try {
    require_once dirname(__DIR__, 3) . '/Core/db.php';
    require_once dirname(__DIR__) . '/Auditoria/audit.php';
} catch (Throwable $e) {
    error_log('Error de conexión a la base de datos: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo conectar a la base de datos.']);
    exit;
}

$esSuper = session_is_superadmin();
$empresaSesion = ensure_session_empresa_id();
$sqlBase = "SELECT u.*, r.nombre AS role_name, e.nombre AS empresa_nombre FROM usuarios u "
         . "LEFT JOIN roles r ON u.role_id = r.id "
         . "LEFT JOIN empresas e ON u.empresa_id = e.id";

if ($esSuper || $empresaSesion === null) {
    $res = $conn->query($sqlBase . " ORDER BY u.id DESC");
    $stmtListado = null;
} else {
    $stmtListado = $conn->prepare($sqlBase . " WHERE u.empresa_id = ? ORDER BY u.id DESC");
    $stmtListado->bind_param('i', $empresaSesion);
    $stmtListado->execute();
    $res = $stmtListado->get_result();
}

$usuarios = [];
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $empresaId = null;
        if (!empty($row['empresa_id'])) {
            $empresaId = (int) $row['empresa_id'];
        } elseif (!empty($row['cliente_id'])) {
            // Compatibilidad con instalaciones antiguas donde se usaba cliente_id.
            $empresaId = (int) $row['cliente_id'];
        }

        $usuarios[] = [
            'id'             => $row['id'],
            'correo'         => $row['correo'],
            'usuario'        => $row['usuario'],
            'nombre'         => $row['nombre'],
            'apellidos'      => $row['apellidos'],
            'puesto'         => $row['puesto'] ?? null,
            'telefono'       => $row['telefono'] ?? null,
            'activo'         => $row['activo'],
            'sso'            => $row['sso'],
            'verificado'     => $row['verificado'] ?? 1,
            'dosfa'          => $row['dosfa'] ?? 0,
            'ultima_ip'      => $row['ultima_ip'] ?? '',
            'last_login'     => $row['last_login'] ?? null,
            'created_at'     => $row['fecha_creacion'] ?? $row['created_at'] ?? null,
            'role_id'        => $row['role_id'] ?? null,
            'role_name'      => $row['role_name'] ?? null,
            'empresa_id'     => $empresaId,
            'empresa_nombre' => $row['empresa_nombre'] ?? null,
        ];
    }
}

echo json_encode($usuarios);
$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
log_activity($nombreAud, 'Consultó lista de usuarios', null, $correoAud);
if (isset($stmtListado) && $stmtListado) {
    $stmtListado->close();
}
$conn->close();
?>
