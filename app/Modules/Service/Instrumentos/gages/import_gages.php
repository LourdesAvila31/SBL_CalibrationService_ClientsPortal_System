<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

header('Content-Type: application/json; charset=utf-8');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['error' => 'Método no permitido']);
    exit;
}

if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

try {
    require_once dirname(__DIR__, 4) . '/Core/db.php';
    require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
    require_once __DIR__ . '/gages_import_helpers.php';
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo inicializar la importación.']);
    exit;
}

if (!isset($_FILES['inventory_file']) || !is_array($_FILES['inventory_file'])) {
    http_response_code(400);
    echo json_encode(['error' => 'No se recibió ningún archivo.']);
    exit;
}

$file = $_FILES['inventory_file'];
if (($file['error'] ?? UPLOAD_ERR_NO_FILE) !== UPLOAD_ERR_OK) {
    http_response_code(400);
    $message = 'Error al recibir el archivo.';
    if (($file['error'] ?? 0) === UPLOAD_ERR_INI_SIZE || ($file['error'] ?? 0) === UPLOAD_ERR_FORM_SIZE) {
        $message = 'El archivo excede el tamaño máximo permitido.';
    }
    echo json_encode(['error' => $message]);
    exit;
}

$maxBytes = 5 * 1024 * 1024; // 5 MB
if (($file['size'] ?? 0) > $maxBytes) {
    http_response_code(400);
    echo json_encode(['error' => 'El archivo excede el tamaño máximo permitido (5 MB).']);
    exit;
}

$empresaId = obtenerEmpresaId();
if ($empresaId <= 0) {
    http_response_code(400);
    echo json_encode(['error' => 'No se encontró la empresa asociada a la sesión.']);
    exit;
}

try {
    $parsed = parse_gages_inventory_csv($file['tmp_name']);
} catch (Throwable $e) {
    http_response_code(400);
    echo json_encode(['error' => $e->getMessage()]);
    exit;
}

$records = $parsed['records'] ?? [];
$rowErrors = $parsed['errors'] ?? [];

if (!$records) {
    http_response_code(422);
    echo json_encode([
        'error' => 'No se encontraron filas válidas en el archivo.',
        'row_errors' => $rowErrors,
    ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

try {
    $conn->begin_transaction();
    $summary = process_gages_import($conn, $empresaId, $records, $rowErrors);
    $conn->commit();
} catch (Throwable $e) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode([
        'error' => 'No se pudo completar la importación de instrumentos.',
        'details' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

$response = [
    'success' => true,
    'created' => $summary['created'] ?? 0,
    'updated' => $summary['updated'] ?? 0,
    'processed' => count($records),
    'row_errors' => $rowErrors,
];

$json = json_encode($response, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
if ($json === false) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo generar la respuesta de importación.']);
    exit;
}

echo $json;
