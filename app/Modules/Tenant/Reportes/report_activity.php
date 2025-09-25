<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    exit;
}

require_once __DIR__ . '/report_helpers.php';

global $conn;
$empresaId = require_empresa_id();
$format = strtolower((string) request_input('format', 'pdf'));

$sql = <<<SQL
SELECT
    COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) AS instrumento,
    i.codigo,
    DATE_FORMAT(ult.fecha_calibracion, '%Y-%m-%d') AS ultima_calibracion,
    DATE_FORMAT(ult.fecha_proxima, '%Y-%m-%d') AS proxima_calibracion,
    COALESCE(ult.resultado, 'Sin resultado') AS resultado,
    DATE_FORMAT(plan.fecha_programada, '%Y-%m-%d') AS fecha_programada,
    COALESCE(plan.estado, 'Sin plan') AS estado_plan
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN (
    SELECT c1.instrumento_id, c1.fecha_calibracion, c1.fecha_proxima, c1.resultado
    FROM calibraciones c1
    JOIN (
        SELECT instrumento_id, MAX(fecha_calibracion) AS max_fecha
        FROM calibraciones
        WHERE empresa_id = $empresaId
        GROUP BY instrumento_id
    ) ult ON ult.instrumento_id = c1.instrumento_id AND ult.max_fecha = c1.fecha_calibracion
    WHERE c1.empresa_id = $empresaId
) AS ult ON ult.instrumento_id = i.id
LEFT JOIN (
    SELECT p1.instrumento_id, p1.fecha_programada, p1.estado
    FROM planes p1
    JOIN (
        SELECT instrumento_id, MAX(fecha_programada) AS max_fecha
        FROM planes
        WHERE empresa_id = $empresaId
        GROUP BY instrumento_id
    ) planmax ON planmax.instrumento_id = p1.instrumento_id AND planmax.max_fecha = p1.fecha_programada
    WHERE p1.empresa_id = $empresaId
) AS plan ON plan.instrumento_id = i.id
WHERE i.empresa_id = $empresaId
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
ORDER BY instrumento ASC, i.codigo ASC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    $rows[] = [
        $row['instrumento'],
        $row['codigo'],
        $row['ultima_calibracion'] ?? '-',
        $row['proxima_calibracion'] ?? '-',
        $row['resultado'],
        $row['fecha_programada'] ?? '-',
        $row['estado_plan'],
    ];
}

$headers = ['Instrumento', 'Código', 'Última Calibración', 'Próxima Calibración', 'Resultado', 'Fecha Planificada', 'Estado del Plan'];

switch ($format) {
    case 'excel':
        stream_excel_report('instrument_activity.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('instrument_activity.pdf', 'Seguimiento de actividad de instrumentos', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('instrument_activity.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
