<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once __DIR__ . '/report_helpers.php';
require_once __DIR__ . '/custom_report_definitions.php';

/**
 * @return bool
 */
function custom_report_is_valid_iso_date(string $value): bool
{
    $dt = \DateTime::createFromFormat('Y-m-d', $value);
    return $dt instanceof \DateTime && $dt->format('Y-m-d') === $value;
}

/**
 * @param string $datasetKey
 * @param array<string,mixed> $filters
 * @param array<string,array<string,mixed>> $definitions
 * @return array<string,string>
 */
function custom_report_sanitize_filters(string $datasetKey, array $filters, array $definitions): array
{
    $clean = [];
    $definition = $definitions[$datasetKey] ?? [];
    $filterDefinitions = $definition['filters'] ?? [];

    foreach ($filterDefinitions as $key => $meta) {
        if (!array_key_exists($key, $filters)) {
            continue;
        }

        $value = $filters[$key];
        if (is_array($value)) {
            $value = reset($value);
        }
        $value = trim((string) $value);
        if ($value === '') {
            continue;
        }

        if (strpos($key, 'fecha_') === 0) {
            if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $value) || !custom_report_is_valid_iso_date($value)) {
                continue;
            }
        }

        if ($key === 'estado') {
            $value = mb_substr($value, 0, 100, 'UTF-8');
        }

        $clean[$key] = $value;
    }

    return $clean;
}

$reportId = (int) request_input('id', 0);
if ($reportId <= 0) {
    http_response_code(400);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'Identificador de reporte no válido';
    exit;
}

$empresaId = require_empresa_id();

$sql = 'SELECT nombre, configuracion, filtros, formato_preferido FROM reportes_personalizados '
    . 'WHERE id = ? AND empresa_id = ? LIMIT 1';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    respond_database_error($conn);
}

$stmt->bind_param('ii', $reportId, $empresaId);
$stmt->execute();
$result = $stmt->get_result();
$configRow = $result ? $result->fetch_assoc() : null;
$stmt->close();

if (!$configRow) {
    http_response_code(404);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'Reporte no encontrado';
    exit;
}

$configData = json_decode($configRow['configuracion'] ?? '', true);
if (!is_array($configData)) {
    http_response_code(400);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'La configuración almacenada del reporte es inválida.';
    exit;
}

$definitions = custom_report_definitions();
$datasetKey = (string) ($configData['dataset'] ?? '');
$datasetDefinition = $definitions[$datasetKey] ?? null;
if (!$datasetDefinition) {
    http_response_code(400);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'El dataset del reporte ya no está disponible.';
    exit;
}

$columns = $configData['columnas'] ?? [];
if (!is_array($columns) || empty($columns)) {
    $columns = array_values($datasetDefinition['default_columns'] ?? []);
}

$columns = array_values(array_unique(array_intersect(
    array_map('strval', $columns),
    array_keys($datasetDefinition['columns'] ?? [])
)));

if (empty($columns)) {
    http_response_code(400);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'El reporte no tiene columnas válidas configuradas.';
    exit;
}

$filtersData = json_decode($configRow['filtros'] ?? '', true);
if (!is_array($filtersData)) {
    $filtersData = [];
}

$cleanFilters = custom_report_sanitize_filters($datasetKey, $filtersData, $definitions);

$requestedFormat = strtolower((string) request_input('format', ''));
$storedFormat = strtolower((string) ($configRow['formato_preferido'] ?? ''));
$allowedFormats = custom_report_allowed_formats();

if (!in_array($requestedFormat, $allowedFormats, true)) {
    $requestedFormat = $storedFormat;
}

if (!in_array($requestedFormat, $allowedFormats, true)) {
    $requestedFormat = $allowedFormats[0];
}

$selectParts = [];
$columnOrder = [];
$headers = [];

foreach ($columns as $columnKey) {
    $columnDefinition = $datasetDefinition['columns'][$columnKey] ?? null;
    if (!$columnDefinition) {
        continue;
    }
    $selectParts[] = $columnDefinition['select'] . ' AS `' . $columnKey . '`';
    $headers[] = $columnDefinition['label'] ?? $columnKey;
    $columnOrder[] = $columnKey;
}

if (empty($selectParts)) {
    http_response_code(400);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'No hay columnas disponibles para generar el reporte.';
    exit;
}

$query = 'SELECT ' . implode(', ', $selectParts)
    . ' FROM ' . $datasetDefinition['table'];

$joins = $datasetDefinition['joins'] ?? [];
if (!empty($joins)) {
    $query .= ' ' . implode(' ', $joins);
}

$where = [$datasetDefinition['empresa_column'] . ' = ?'];
$types = 'i';
$params = [$empresaId];

foreach ($cleanFilters as $key => $value) {
    $meta = $datasetDefinition['filters'][$key] ?? null;
    if (!$meta) {
        continue;
    }
    $where[] = $meta['column'] . ' ' . $meta['operator'] . ' ?';
    $types .= $meta['type'];
    $params[] = $value;
}

if (!empty($where)) {
    $query .= ' WHERE ' . implode(' AND ', $where);
}

if (!empty($datasetDefinition['order_by'])) {
    $query .= ' ORDER BY ' . $datasetDefinition['order_by'];
}

$stmt = $conn->prepare($query);
if (!$stmt) {
    respond_database_error($conn);
}

$bindParams = [];
$bindParams[] = $types;
foreach ($params as $index => $paramValue) {
    $bindParams[] = &$params[$index];
}

call_user_func_array([$stmt, 'bind_param'], $bindParams);
$stmt->execute();
$result = $stmt->get_result();

$rows = [];
if ($result) {
    while ($row = $result->fetch_assoc()) {
        $formattedRow = [];
        foreach ($columnOrder as $columnKey) {
            $formattedRow[] = value_or_dash($row[$columnKey] ?? null);
        }
        $rows[] = $formattedRow;
    }
}

$stmt->close();

$reportName = (string) $configRow['nombre'];
$filenameBase = preg_replace('/[^A-Za-z0-9_-]+/', '_', strtolower($reportName));
if ($filenameBase === '') {
    $filenameBase = 'reporte_personalizado';
}

switch ($requestedFormat) {
    case 'excel':
        stream_excel_report($filenameBase . '.xls', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report($filenameBase . '.csv', $headers, $rows);
        break;
    case 'pdf':
        $title = 'Reporte personalizado: ' . ($reportName !== '' ? $reportName : $filenameBase);
        stream_pdf_report($filenameBase . '.pdf', $title, $headers, $rows);
        break;
}

http_response_code(400);
header('Content-Type: text/plain; charset=UTF-8');
echo 'Formato no soportado';
exit;
