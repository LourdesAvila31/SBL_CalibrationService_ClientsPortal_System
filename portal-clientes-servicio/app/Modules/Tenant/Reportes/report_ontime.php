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
    DATE_FORMAT(c.fecha_calibracion, '%Y-%m-%d') AS fecha_calibracion,
    DATE_FORMAT(c.fecha_proxima, '%Y-%m-%d') AS fecha_proxima,
    CASE
        WHEN c.fecha_calibracion IS NULL OR c.fecha_proxima IS NULL THEN 'Sin datos'
        WHEN c.fecha_calibracion <= c.fecha_proxima THEN 'A tiempo'
        ELSE 'Fuera de tiempo'
    END AS cumplimiento,
    DATEDIFF(c.fecha_proxima, c.fecha_calibracion) AS dias_margen
FROM calibraciones c
JOIN instrumentos i ON i.id = c.instrumento_id AND i.empresa_id = $empresaId
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
WHERE c.empresa_id = $empresaId
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
ORDER BY c.fecha_proxima ASC, c.fecha_calibracion ASC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    $margen = $row['dias_margen'];
    if ($margen === null || $row['cumplimiento'] === 'Sin datos') {
        $margenTexto = '-';
    } else {
        $margenTexto = $margen . ' días';
    }
    $rows[] = [
        $row['instrumento'],
        $row['codigo'],
        $row['fecha_calibracion'] ?? '-',
        $row['fecha_proxima'] ?? '-',
        $row['cumplimiento'],
        $margenTexto,
    ];
}

$headers = ['Instrumento', 'Código', 'Fecha de Calibración', 'Fecha Compromiso', 'Estado', 'Margen'];

switch ($format) {
    case 'excel':
        stream_excel_report('calibration_on_time.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('calibration_on_time.pdf', 'Calibraciones realizadas a tiempo', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('calibration_on_time.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
