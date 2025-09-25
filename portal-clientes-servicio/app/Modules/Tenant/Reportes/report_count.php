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
    DATE_FORMAT(c.fecha_calibracion, '%Y-%m') AS periodo,
    COUNT(*) AS total,
    SUM(CASE WHEN LOWER(COALESCE(c.resultado, '')) IN ('aprobado','aprobada','ok','cumple','pass','satisfactorio') THEN 1 ELSE 0 END) AS aprobadas,
    SUM(CASE WHEN LOWER(COALESCE(c.resultado, '')) IN ('no cumple','rechazado','rechazada','fail','fuera de tolerancia') THEN 1 ELSE 0 END) AS rechazadas
FROM calibraciones c
WHERE c.empresa_id = $empresaId
GROUP BY periodo
ORDER BY periodo DESC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    $total = (int) $row['total'];
    $aprobadas = (int) $row['aprobadas'];
    $porcentaje = $total > 0 ? round(($aprobadas / $total) * 100, 2) : 0;
    $rows[] = [
        $row['periodo'] ?? 'Sin fecha',
        $total,
        $aprobadas,
        (int) $row['rechazadas'],
        $porcentaje . '%',
    ];
}

$headers = ['Periodo', 'Total', 'Aprobadas', 'Rechazadas', '% Cumplimiento'];

switch ($format) {
    case 'excel':
        stream_excel_report('calibration_count.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('calibration_count.pdf', 'Conteo de calibraciones por periodo', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('calibration_count.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
