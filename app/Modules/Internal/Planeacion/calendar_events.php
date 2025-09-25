<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
header('Content-Type: application/json');

$roleName = $_SESSION['role_id'] ?? '';
if (!in_array($roleName, ['Superadministrador', 'Administrador', 'Supervisor'], true)) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/logistics.php';

$empresaId = obtenerEmpresaId();

$year = isset($_GET['year']) ? intval($_GET['year']) : intval(date('Y'));
$month = isset($_GET['month']) ? intval($_GET['month']) : intval(date('n'));
$firstDay = sprintf('%04d-%02d-01', $year, $month);
$lastDay = date('Y-m-t', strtotime($firstDay));

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$events = [];

// Calibraciones próximas a vencer
$sqlVencer = <<<'SQL'
SELECT
    i.id,
    COALESCE(ci.nombre, i.codigo) AS instrumento,
    i.codigo,
    i.proxima_calibracion
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
WHERE i.estado <> 'stock'
  AND i.proxima_calibracion BETWEEN ? AND ?
  AND i.empresa_id = ?
SQL;
$stmt = $conn->prepare($sqlVencer);
$stmt->bind_param('ssi', $firstDay, $lastDay, $empresaId);

$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $events[] = [
        'id' => 'cal-v-' . $row['id'],
        'title' => 'VENCE: ' . $row['instrumento'],
        'date' => $row['proxima_calibracion'],
        'description' => 'La calibración de este instrumento vence en esta fecha.',
        'type' => 'calibracion-pendiente',
        'info' => 'Haz clic para ver detalles o reprogramar.',
        'instrumento' => $row['instrumento'],
        'codigo' => $row['codigo']
    ];
}
$stmt->close();

// Calibraciones programadas
$sqlProgramadas = <<<'SQL'
SELECT
    c.id,
    COALESCE(ci.nombre, i.codigo) AS instrumento,
    i.codigo,
    c.fecha_calibracion,
    c.fecha_proxima,
    c.resultado,
    p.nombre AS proveedor,
    log.estado AS log_estado,
    log.proveedor_externo AS log_proveedor_externo,
    log.transportista AS log_transportista,
    log.numero_guia AS log_numero_guia,
    log.orden_servicio AS log_orden_servicio,
    log.comentarios AS log_comentarios,
    DATE_FORMAT(log.fecha_envio, '%Y-%m-%d') AS log_fecha_envio,
    DATE_FORMAT(log.fecha_en_transito, '%Y-%m-%d') AS log_fecha_en_transito,
    DATE_FORMAT(log.fecha_recibido, '%Y-%m-%d') AS log_fecha_recibido,
    DATE_FORMAT(log.fecha_retorno, '%Y-%m-%d') AS log_fecha_retorno
FROM calibraciones c
JOIN instrumentos i ON i.id = c.instrumento_id
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN proveedores p ON c.proveedor_id = p.id
LEFT JOIN logistica_calibraciones log ON log.calibracion_id = c.id
WHERE c.fecha_calibracion BETWEEN ? AND ?
  AND i.empresa_id = ?
SQL;

$stmt = $conn->prepare($sqlProgramadas);
$stmt->bind_param('ssi', $firstDay, $lastDay, $empresaId);
$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $logistica = logistics_response_payload($row, 'log_');
    $events[] = [
        'id' => 'cal-p-' . $row['id'],
        'title' => 'Programado: ' . $row['instrumento'],
        'date' => $row['fecha_calibracion'],
        'description' => 'Calibración programada.',
        'type' => 'calibracion-programada',
        'instrumento' => $row['instrumento'],
        'proveedor' => $row['proveedor'],
        'modalidad' => null,
        'codigo' => $row['codigo'],
        'fecha_proxima' => $row['fecha_proxima'],
        'resultado' => $row['resultado'],
        'logistica' => $logistica,
        'logistica_estado' => $logistica['estado'],
        'logistica_envio' => $logistica['fecha_envio'],
        'logistica_retorno' => $logistica['fecha_retorno']
    ];
}
$stmt->close();

// Calibraciones vencidas antes del mes consultado
$sqlVencidas = <<<'SQL'
SELECT
    i.id,
    COALESCE(ci.nombre, i.codigo) AS instrumento,
    i.codigo,
    i.proxima_calibracion
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
WHERE i.estado <> 'stock'
  AND i.proxima_calibracion < ?
  AND i.empresa_id = ?
SQL;
$stmt = $conn->prepare($sqlVencidas);
$stmt->bind_param('si', $firstDay, $empresaId);

$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $events[] = [
        'id' => 'cal-x-' . $row['id'],
        'title' => 'VENCIDA: ' . $row['instrumento'],
        'date' => $row['proxima_calibracion'],
        'description' => 'La calibración está vencida.',
        'type' => 'calibracion-vencida',
        'info' => 'Requiere atención inmediata.',
        'instrumento' => $row['instrumento'],
        'codigo' => $row['codigo']
    ];
}
$stmt->close();

$conn->close();
echo json_encode($events);
?>
