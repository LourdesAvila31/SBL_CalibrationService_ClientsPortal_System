<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    exit('Acceso denegado');
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    exit('Empresa no especificada');
}

$patternId = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
if (!$patternId) {
    http_response_code(400);
    exit('Patrón inválido');
}

$stmt = $conn->prepare('SELECT certificado_archivo FROM patrones WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$stmt) {
    http_response_code(500);
    exit('No se pudo preparar la consulta');
}

$stmt->bind_param('ii', $patternId, $empresaId);
$stmt->execute();
$stmt->bind_result($fileName);
if (!$stmt->fetch() || !$fileName) {
    $stmt->close();
    $conn->close();
    http_response_code(404);
    exit('Certificado no disponible');
}
$stmt->close();
$conn->close();

$storageDir = dirname(__DIR__, 4) . '/storage/patrones';
$filePath = realpath($storageDir . '/' . $fileName);
if ($filePath === false || strpos($filePath, realpath($storageDir)) !== 0 || !is_file($filePath)) {
    http_response_code(404);
    exit('Archivo no encontrado');
}

$mimeType = mime_content_type($filePath) ?: 'application/octet-stream';
header('Content-Type: ' . $mimeType);
header('Content-Disposition: attachment; filename="' . basename($filePath) . '"');
header('Content-Length: ' . filesize($filePath));
readfile($filePath);
