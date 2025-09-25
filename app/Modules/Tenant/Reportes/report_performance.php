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
$format = strtolower((string) request_input('format', 'excel'));

$sql = <<<SQL
SELECT
    i.id,
    COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) AS instrumento,
    COUNT(c.id) AS total_calibraciones,
    SUM(CASE WHEN LOWER(COALESCE(c.resultado, '')) IN ('aprobado','aprobada','ok','cumple','pass','satisfactorio') THEN 1 ELSE 0 END) AS aprobadas,
    SUM(CASE WHEN LOWER(COALESCE(c.resultado, '')) IN ('no cumple','rechazado','rechazada','fail','fuera de tolerancia') THEN 1 ELSE 0 END) AS rechazadas,
    MIN(c.fecha_calibracion) AS primera_calibracion,
    MAX(c.fecha_calibracion) AS ultima_calibracion
FROM instrumentos i
LEFT JOIN calibraciones c ON c.instrumento_id = i.id AND c.empresa_id = $empresaId
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
WHERE i.empresa_id = $empresaId
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
GROUP BY i.id, instrumento
HAVING COUNT(c.id) > 0
ORDER BY instrumento ASC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    $total = (int) $row['total_calibraciones'];
    $aprobadas = (int) $row['aprobadas'];
    $rechazadas = (int) $row['rechazadas'];
    $porcentaje = $total > 0 ? round(($aprobadas / $total) * 100, 2) : 0;
    $rows[] = [
        $row['instrumento'],
        $total,
        $aprobadas,
        $rechazadas,
        $porcentaje . '%',
        $row['primera_calibracion'] ?? '-',
        $row['ultima_calibracion'] ?? '-',
    ];
}

$headers = ['Instrumento', 'Calibraciones', 'Aprobadas', 'Rechazadas', '% Cumplimiento', 'Primera Calibración', 'Última Calibración'];

switch ($format) {
    case 'excel':
        stream_excel_report('calibration_performance.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('calibration_performance.pdf', 'Desempeño de calibración', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('calibration_performance.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
