<?php

declare(strict_types=1);

require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

session_start();

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 2) . '/Internal/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

header('Content-Type: application/json');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no determinada']);
    exit;
}

$sql = "SELECT
            nc.id,
            nc.calibracion_id,
            nc.instrumento_id,
            nc.estado,
            nc.detected_at,
            nc.notified_at,
            nc.notes,
            i.codigo,
            i.serie,
            ci.nombre AS instrumento_nombre,
            cal.fecha_calibracion,
            cal.resultado
        FROM calibration_nonconformities nc
        INNER JOIN instrumentos i ON i.id = nc.instrumento_id
        LEFT JOIN catalogo_instrumentos ci ON ci.id = i.catalogo_id
        LEFT JOIN calibraciones cal ON cal.id = nc.calibracion_id
        WHERE nc.empresa_id = ? AND nc.estado = 'abierta'
        ORDER BY nc.detected_at DESC";

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta.']);
    exit;
}

$stmt->bind_param('i', $empresaId);
if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo ejecutar la consulta.']);
    exit;
}

$result = $stmt->get_result();
$registros = [];
while ($row = $result->fetch_assoc()) {
    $registros[] = [
        'id' => (int) $row['id'],
        'calibracion_id' => $row['calibracion_id'] !== null ? (int) $row['calibracion_id'] : null,
        'instrumento_id' => (int) $row['instrumento_id'],
        'estado' => $row['estado'],
        'detected_at' => $row['detected_at'],
        'notified_at' => $row['notified_at'],
        'notes' => $row['notes'],
        'codigo' => $row['codigo'],
        'serie' => $row['serie'],
        'instrumento_nombre' => $row['instrumento_nombre'],
        'fecha_calibracion' => $row['fecha_calibracion'],
        'resultado' => $row['resultado'],
    ];
}
$stmt->close();

$nombreOperador = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
if ($nombreOperador === '' && !empty($_SESSION['usuario_id'])) {
    $usuarioStmt = $conn->prepare('SELECT nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
    if ($usuarioStmt) {
        $usuarioStmt->bind_param('i', $_SESSION['usuario_id']);
        if ($usuarioStmt->execute()) {
            $usuarioStmt->bind_result($nombreDb, $apellidosDb);
            if ($usuarioStmt->fetch()) {
                $nombreOperador = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
            }
        }
        $usuarioStmt->close();
    }
}
if ($nombreOperador === '') {
    $nombreOperador = 'Desconocido';
}
$correoOperador = $_SESSION['usuario'] ?? null;

log_activity($nombreOperador, 'Consulta de no conformidades abiertas', 'calibraciones', $correoOperador);

echo json_encode($registros);
