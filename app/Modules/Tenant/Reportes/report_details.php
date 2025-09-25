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
    COALESCE(c.resultado, 'Sin resultado') AS resultado,
    COALESCE(CONCAT_WS(' ', tech.nombre, tech.apellidos), 'Sin asignar') AS tecnico,
    COALESCE(p.nombre, CONCAT('Proveedor #', c.proveedor_id)) AS proveedor,
    COALESCE(c.observaciones, '-') AS observaciones
FROM calibraciones c
JOIN instrumentos i ON i.id = c.instrumento_id AND i.empresa_id = $empresaId
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN proveedores p ON p.id = c.proveedor_id
LEFT JOIN usuarios tech ON tech.id = c.usuario_id
WHERE c.empresa_id = $empresaId
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
ORDER BY c.fecha_calibracion DESC, instrumento ASC
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
        $row['fecha_calibracion'] ?? '-',
        $row['fecha_proxima'] ?? '-',
        $row['resultado'],
        $row['tecnico'] ?? 'Sin asignar',
        $row['proveedor'] ?? '-',
        $row['observaciones'],
    ];
}

$headers = ['Instrumento', 'Código', 'Fecha de Calibración', 'Próxima Calibración', 'Resultado', 'Técnico', 'Proveedor', 'Observaciones'];

switch ($format) {
    case 'excel':
        stream_excel_report('calibration_details.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('calibration_details.pdf', 'Informe de detalles de calibración', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('calibration_details.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
