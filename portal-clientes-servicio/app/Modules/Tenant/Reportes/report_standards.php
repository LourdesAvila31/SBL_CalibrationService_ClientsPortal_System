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
$format = strtolower((string) request_input('format', 'list'));

$sql = <<<SQL
SELECT
    COALESCE(c.proveedor_id, 0) AS estandar_id,
    COALESCE(p.nombre, 'Sin estándar definido') AS estandar_nombre,
    COUNT(*) AS total_calibraciones,
    GROUP_CONCAT(DISTINCT COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) ORDER BY ci.nombre SEPARATOR ', ') AS instrumentos
FROM calibraciones c
JOIN instrumentos i ON i.id = c.instrumento_id AND i.empresa_id = $empresaId
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN proveedores p ON p.id = c.proveedor_id
WHERE c.empresa_id = $empresaId
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
GROUP BY estandar_id, estandar_nombre
ORDER BY estandar_nombre ASC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$entries = [];
while ($row = $result->fetch_assoc()) {
    $entries[] = [
        'estandar' => $row['estandar_nombre'] ?? 'Sin estándar definido',
        'total' => (int) ($row['total_calibraciones'] ?? 0),
        'instrumentos' => $row['instrumentos'] ?: 'Sin instrumentos asociados',
    ];
}

$headers = ['Estándar', 'Total calibraciones', 'Instrumentos'];
$tabularRows = array_map(static function (array $entry): array {
    return [$entry['estandar'], $entry['total'], $entry['instrumentos']];
}, $entries);

switch ($format) {
    case 'list':
        $lines = [];
        if (empty($entries)) {
            $lines[] = 'No hay calibraciones registradas para la empresa seleccionada.';
        } else {
            foreach ($entries as $entry) {
                $lines[] = 'Estándar: ' . $entry['estandar'];
                $lines[] = '  Total de calibraciones: ' . $entry['total'];
                $lines[] = '  Instrumentos: ' . $entry['instrumentos'];
                $lines[] = '';
            }
        }
        stream_list_report('calibraciones_por_estandar.txt', $lines);
        break;
    case 'excel':
        stream_excel_report('calibraciones_por_estandar.xls', $headers, $tabularRows);
        break;
    case 'pdf':
        stream_pdf_report('calibraciones_por_estandar.pdf', 'Calibraciones por estándar de referencia', $headers, $tabularRows);
        break;
    case 'csv':
        stream_csv_report('calibraciones_por_estandar.csv', $headers, $tabularRows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
