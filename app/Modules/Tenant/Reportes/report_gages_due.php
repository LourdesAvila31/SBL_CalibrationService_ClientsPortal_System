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
$format = strtolower((string) request_input('format', 'csv'));

$sql = <<<SQL
SELECT
    COALESCE(ci.nombre, CONCAT('Instrumento #', i.id)) AS instrumento,
    DATE_FORMAT(i.proxima_calibracion, '%Y-%m-%d') AS fecha_vencimiento,
    COALESCE(d.nombre, 'Sin departamento') AS departamento
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN departamentos d ON i.departamento_id = d.id
WHERE i.empresa_id = $empresaId
  AND i.proxima_calibracion IS NOT NULL
  AND i.proxima_calibracion <= DATE_ADD(CURDATE(), INTERVAL 60 DAY)
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
ORDER BY i.proxima_calibracion ASC, instrumento ASC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    $rows[] = [
        $row['instrumento'],
        $row['fecha_vencimiento'],
        $row['departamento'],
    ];
}

$headers = ['Instrumento', 'Fecha de Vencimiento', 'Departamento'];

switch ($format) {
    case 'excel':
        stream_excel_report('gages_due.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('gages_due.pdf', 'Instrumentos próximos a calibrar', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('gages_due.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
