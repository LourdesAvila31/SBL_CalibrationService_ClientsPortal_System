<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('reportes_crear')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'msg' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once __DIR__ . '/custom_report_definitions.php';

/**
 * @return bool
 */
function is_valid_iso_date(string $value): bool
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
function sanitize_custom_report_filters(string $datasetKey, array $filters, array $definitions): array
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
            if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $value) || !is_valid_iso_date($value)) {
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

$nombre = trim((string) ($_POST['nombre'] ?? ''));
$configJson = (string) ($_POST['configuracion'] ?? '');
$filtrosJson = (string) ($_POST['filtros'] ?? '');
$formato = strtolower(trim((string) ($_POST['formato_preferido'] ?? '')));
$usuarioId = (int) ($_SESSION['usuario_id'] ?? 0);
$empresaId = (int) (obtenerEmpresaId() ?: 0);
$fechaCreacion = date('Y-m-d');

if ($nombre === '') {
    echo json_encode(['success' => false, 'msg' => 'El nombre del reporte es obligatorio.']);
    exit;
}

if ($usuarioId <= 0 || $empresaId <= 0) {
    echo json_encode(['success' => false, 'msg' => 'No se pudo determinar el usuario o la empresa.']);
    exit;
}

if ($configJson === '') {
    echo json_encode(['success' => false, 'msg' => 'La configuración del reporte es obligatoria.']);
    exit;
}

$configData = json_decode($configJson, true);
if (!is_array($configData)) {
    echo json_encode(['success' => false, 'msg' => 'La configuración recibida no es válida.']);
    exit;
}

$definitions = custom_report_definitions();
$datasetKey = (string) ($configData['dataset'] ?? '');

if (!isset($definitions[$datasetKey])) {
    echo json_encode(['success' => false, 'msg' => 'El dataset seleccionado no es válido.']);
    exit;
}

$allowedColumns = array_keys($definitions[$datasetKey]['columns'] ?? []);
$requestedColumns = $configData['columnas'] ?? [];
if (!is_array($requestedColumns)) {
    $requestedColumns = [];
}

$selectedColumns = array_values(array_unique(array_intersect(
    array_map('strval', $requestedColumns),
    $allowedColumns
)));

if (empty($selectedColumns)) {
    $selectedColumns = array_values($definitions[$datasetKey]['default_columns'] ?? []);
}

if (empty($selectedColumns)) {
    echo json_encode(['success' => false, 'msg' => 'Selecciona al menos una columna para el reporte.']);
    exit;
}

if (count($selectedColumns) > 25) {
    echo json_encode(['success' => false, 'msg' => 'Selecciona un máximo de 25 columnas.']);
    exit;
}

$cleanConfig = [
    'dataset' => $datasetKey,
    'columnas' => $selectedColumns,
];

$filtersData = [];
if ($filtrosJson !== '') {
    $decodedFilters = json_decode($filtrosJson, true);
    if (is_array($decodedFilters)) {
        $filtersData = $decodedFilters;
    }
}

$cleanFilters = sanitize_custom_report_filters($datasetKey, $filtersData, $definitions);

$allowedFormats = custom_report_allowed_formats();
if (!in_array($formato, $allowedFormats, true)) {
    $formato = $allowedFormats[0];
}

$configEncoded = json_encode($cleanConfig, JSON_UNESCAPED_UNICODE);
$filtersEncoded = json_encode($cleanFilters, JSON_UNESCAPED_UNICODE);

if ($configEncoded === false || $filtersEncoded === false) {
    echo json_encode(['success' => false, 'msg' => 'No fue posible preparar la configuración del reporte.']);
    exit;
}

$sql = 'INSERT INTO reportes_personalizados '
    . '(nombre, instrumento_id, fecha_creacion, usuario_id, empresa_id, configuracion, formato_preferido, filtros) '
    . 'VALUES (?, NULL, ?, ?, ?, ?, ?, ?)';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    echo json_encode(['success' => false, 'msg' => 'No fue posible preparar la operación.']);
    exit;
}

$stmt->bind_param(
    'ssiisss',
    $nombre,
    $fechaCreacion,
    $usuarioId,
    $empresaId,
    $configEncoded,
    $formato,
    $filtersEncoded
);

if ($stmt->execute()) {
    echo json_encode(['success' => true, 'msg' => 'Reporte personalizado guardado correctamente.']);
} else {
    echo json_encode(['success' => false, 'msg' => 'Error al guardar el reporte personalizado.']);
}

$stmt->close();
$conn->close();
