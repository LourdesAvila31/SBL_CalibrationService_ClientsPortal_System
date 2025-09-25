<?php
session_start();

header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';

if (!isset($_SESSION['usuario_id'])) {
    http_response_code(401);
    echo json_encode(['error' => 'Sesión no válida']);
    exit;
}

$empresaSesion = ensure_session_empresa_id();
$esSuperadmin = session_is_superadmin();
$empresaObjetivo = $empresaSesion;

if ($esSuperadmin) {
    $empresaParam = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($empresaParam) {
        $empresaObjetivo = $empresaParam;
    }
}

$sql = "SELECT u.id, u.nombre, u.apellidos, u.puesto, u.correo, u.telefono, r.nombre AS role_name "
     . "FROM usuarios u "
     . "LEFT JOIN roles r ON u.role_id = r.id "
     . "WHERE u.activo = 1";

$types = '';
$params = [];

if ($empresaObjetivo) {
    $sql .= ' AND u.empresa_id = ?';
    $types .= 'i';
    $params[] = $empresaObjetivo;
} elseif (!$esSuperadmin) {
    // Los clientes deben tener una empresa asociada; si no existe, devolver lista vacía.
    echo json_encode([]);
    exit;
}

$sql .= " AND (r.nombre IS NULL OR r.nombre <> 'Cliente')";
$sql .= ' ORDER BY u.nombre ASC, u.apellidos ASC';

try {
    if ($types !== '') {
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No fue posible preparar la consulta.');
        }
        $stmt->bind_param($types, ...$params);
        $stmt->execute();
        $result = $stmt->get_result();
    } else {
        $result = $conn->query($sql);
    }

    $contactos = [];
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $contactos[] = [
                'id'        => (int) ($row['id'] ?? 0),
                'nombre'    => $row['nombre'] ?? '',
                'apellidos' => $row['apellidos'] ?? '',
                'puesto'    => $row['puesto'] ?? '',
                'correo'    => $row['correo'] ?? '',
                'telefono'  => isset($row['telefono']) ? trim((string) $row['telefono']) : '',
                'role_name' => $row['role_name'] ?? '',
            ];
        }
    }

    echo json_encode($contactos);

    $nombreAud = $_SESSION['nombre'] ?? '';
    $correoAud = $_SESSION['usuario'] ?? null;
    log_activity($nombreAud, 'Consultó contactos de servicio', null, $correoAud);

    if (isset($stmt) && $stmt instanceof mysqli_stmt) {
        $stmt->close();
    }
    if (isset($result) && $result instanceof mysqli_result) {
        $result->free();
    }
    $conn->close();
} catch (Throwable $e) {
    error_log('Error al obtener contactos de servicio: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible obtener los contactos.']);
}
