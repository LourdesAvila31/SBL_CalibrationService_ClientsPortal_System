<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');
require_once dirname(__DIR__, 4) . '/Core/helpers/calibration_results.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';
if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

try {
    require_once dirname(__DIR__, 4) . '/Core/db.php';
    require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

    $empresaId = obtenerEmpresaId();
} catch (Throwable $e) {
    file_put_contents('php://stderr', 'Error de conexión a la base de datos: ' . $e->getMessage() . PHP_EOL);
    http_response_code(500);
    echo json_encode(['error' => 'Error de conexión a la base de datos']);
    exit;
}

$instrumentId = filter_input(INPUT_GET, 'instrument_id', FILTER_VALIDATE_INT);
if (!$instrumentId) {
    echo json_encode(['error' => 'Instrumento inválido']);
    $conn->close();
    exit;
}
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$checkStmt = $conn->prepare('SELECT 1 FROM instrumentos WHERE id = ? AND empresa_id = ?');
$checkStmt->bind_param('ii', $instrumentId, $empresaId);
$checkStmt->execute();
$checkStmt->store_result();
if ($checkStmt->num_rows === 0) {
    http_response_code(404);
    echo json_encode(['error' => 'Instrumento no encontrado']);
    $checkStmt->close();
    $conn->close();
    exit;
}
$checkStmt->close();

$changes = [];

// Historial de departamentos
$stmt = $conn->prepare(
    "SELECT d.nombre AS value, hd.fecha, 'department' AS type
     FROM historial_departamentos hd
     JOIN departamentos d ON hd.departamento_id = d.id
     WHERE hd.instrumento_id = ? AND hd.empresa_id = ?
     ORDER BY hd.fecha DESC"
);
$stmt->bind_param('ii', $instrumentId, $empresaId);
$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $changes[] = ['type' => $row['type'], 'value' => $row['value'], 'date' => $row['fecha']];
}
$stmt->close();

// Historial de ubicaciones
$stmt = $conn->prepare(
    "SELECT ubicacion AS value, fecha, 'location' AS type
     FROM historial_ubicaciones
     WHERE instrumento_id = ? AND empresa_id = ?
     ORDER BY fecha DESC"
);
$stmt->bind_param('ii', $instrumentId, $empresaId);
$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $changes[] = ['type' => $row['type'], 'value' => $row['value'], 'date' => $row['fecha']];
}
$stmt->close();

// Historial de estado
$stmt = $conn->prepare(
    "SELECT estado AS value, fecha, 'state' AS type
     FROM historial_tipos_instrumento
     WHERE instrumento_id = ? AND empresa_id = ?
     ORDER BY fecha DESC"
);
$stmt->bind_param('ii', $instrumentId, $empresaId);
$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $estadoNormalizado = instrumento_normalizar_estado_operativo($row['value'] ?? null);
    $changes[] = [
        'type' => $row['type'],
        'value' => $row['value'],
        'date' => $row['fecha'],
        'operational_state' => $estadoNormalizado,
        'operational_state_label' => instrumento_estado_operativo_label($row['value'] ?? null, $estadoNormalizado),
    ];
}
$stmt->close();

// Ordenar todos los cambios por fecha descendente
usort($changes, function ($a, $b) {
    return strcmp($b['date'], $a['date']);
});

// Historial de calibraciones estructurado por año y periodo
$periodos = ['P1', 'P2', 'EXTRA'];
$calendar = [];

$stmt = $conn->prepare(
    "SELECT cal.id,
            cal.fecha_calibracion,
            cal.fecha_proxima,
            cal.resultado,
            cal.resultado_preliminar,
            cal.estado,
            cal.periodo,
            cal.tipo,
            cal.duracion_horas,
            cal.costo_total,
            cal.liberado_por,
            cal.fecha_liberacion,
            YEAR(cal.fecha_calibracion) AS year_value,
            cert.archivo AS certificado,
            cal.usuario_id AS tecnico_id,
            CONCAT_WS(' ', tech.nombre, tech.apellidos) AS tecnico_nombre

     FROM calibraciones cal
     LEFT JOIN (
         SELECT c1.*
         FROM certificados c1
         JOIN (
             SELECT calibracion_id, MAX(id) AS max_id
             FROM certificados
             GROUP BY calibracion_id
         ) latest ON latest.calibracion_id = c1.calibracion_id AND latest.max_id = c1.id
     ) cert ON cert.calibracion_id = cal.id
     LEFT JOIN usuarios tech ON tech.id = cal.usuario_id
     WHERE cal.instrumento_id = ? AND cal.empresa_id = ?
     ORDER BY cal.fecha_calibracion DESC, cal.id DESC"
);
$stmt->bind_param('ii', $instrumentId, $empresaId);
$stmt->execute();
$res = $stmt->get_result();
while ($row = $res->fetch_assoc()) {
    $fechaCalibracion = $row['fecha_calibracion'];
    $periodo = strtoupper($row['periodo'] ?? '');
    if (!in_array($periodo, $periodos, true)) {
        $periodo = 'P1';
    }

    if (!$fechaCalibracion) {
        continue;
    }

    $yearValue = $row['year_value'];
    if ($yearValue === null) {
        $yearValue = (int) date('Y', strtotime($fechaCalibracion));
    }
    $yearKey = (string) $yearValue;

    if (!isset($calendar[$yearKey])) {
        $calendar[$yearKey] = array_fill_keys($periodos, null);
    }

    if ($calendar[$yearKey][$periodo] === null) {
        $archivo = $row['certificado'] ?? null;
        $liberadoPor = $row['liberado_por'] ?? null;
        $fechaLiberacion = $row['fecha_liberacion'] ?? null;
        $aprobado = $archivo !== null && $archivo !== '' && $liberadoPor !== null && $fechaLiberacion !== null;

        $entry = [
            'id' => (int) $row['id'],
            'fecha' => $fechaCalibracion,
            'fecha_proxima' => $row['fecha_proxima'],
            'resultado' => $decision['resultado'],
            'resultado_preliminar' => $decision['resultado_preliminar'],
            'resultado_liberado' => $decision['resultado_liberado'],
            'estado' => $decision['estado'],
            'estado_resultado' => $decision['estado_resultado'],
            'periodo' => $periodo,
            'tipo' => $row['tipo'] ?? null,
            'duracion_horas' => $row['duracion_horas'],
            'costo_total' => $row['costo_total'],
            'certificado_archivo' => $archivo,
            'certificado_url' => ($aprobado ? '/backend/calibraciones/certificates/' . ltrim($archivo, '/\\') : null),
            'certificado_disponible' => $aprobado,
            'certificado_estado' => $archivo ? ($aprobado ? 'liberado' : 'pendiente') : 'no_registrado',
            'liberado_por' => $liberadoPor !== null ? (int) $liberadoPor : null,
            'liberado_por_nombre' => $row['liberado_por_nombre'] !== null ? trim((string) $row['liberado_por_nombre']) : null,
            'fecha_liberacion' => $fechaLiberacion
        ];

        $tecnicoId = isset($row['tecnico_id']) ? (int) $row['tecnico_id'] : null;
        if ($tecnicoId !== null && $tecnicoId <= 0) {
            $tecnicoId = null;
        }

        $entry['tecnico_id'] = $tecnicoId;
        $entry['tecnico_nombre'] = $row['tecnico_nombre'] ?? null;

        $calendar[$yearKey][$periodo] = $entry;
    }
}
$stmt->close();

krsort($calendar, SORT_NUMERIC);

$response = [
    'changes' => $changes,
    'calendar' => $calendar,
    'periods' => $periodos
];

echo json_encode($response);
$conn->close();

?>
