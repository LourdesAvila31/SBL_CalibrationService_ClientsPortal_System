<?php
session_start();

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';

if (!session_is_superadmin()) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}

$attachmentId = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);

if ($attachmentId === false || $attachmentId === null) {
    http_response_code(400);
    echo 'Identificador no válido';
    exit;
}

$stmt = $conn->prepare('SELECT fa.id, fa.nombre_original, fa.archivo, fa.mime_type FROM feedback_attachments fa WHERE fa.id = ? LIMIT 1');
if (!$stmt) {
    http_response_code(500);
    echo 'No se pudo preparar la consulta';
    exit;
}

$stmt->bind_param('i', $attachmentId);

if (!$stmt->execute()) {
    http_response_code(500);
    echo 'No se pudo obtener la información solicitada';
    $stmt->close();
    exit;
}

$result = $stmt->get_result();
$attachment = $result ? $result->fetch_assoc() : null;
$stmt->close();

if (!$attachment) {
    http_response_code(404);
    echo 'Archivo no encontrado';
    exit;
}

$filePath = __DIR__ . '/uploads/' . $attachment['archivo'];
if (!is_file($filePath)) {
    http_response_code(404);
    echo 'El archivo ya no está disponible';
    exit;
}

$originalName = trim((string) ($attachment['nombre_original'] ?? ''));
if ($originalName === '') {
    $originalName = $attachment['archivo'];
}
$originalName = preg_replace('/[\r\n]+/', ' ', $originalName);
if ($originalName === null || $originalName === '') {
    $originalName = 'archivo';
}

$mimeType = (string) ($attachment['mime_type'] ?? '');
if ($mimeType === '') {
    $detectedMime = function_exists('mime_content_type') ? @mime_content_type($filePath) : false;
    $mimeType = $detectedMime ?: 'application/octet-stream';
}

$size = @filesize($filePath);
if ($size !== false) {
    header('Content-Length: ' . $size);
}

header('Content-Type: ' . $mimeType);
header('X-Content-Type-Options: nosniff');

$encodedName = rawurlencode($originalName);
$printableName = str_replace(['"', '\\'], '', $originalName);
if ($printableName === '') {
    $printableName = 'archivo';
    $encodedName = rawurlencode($printableName);
}
header('Content-Disposition: attachment; filename="' . $printableName . '"; filename*=UTF-8\'\'' . $encodedName);

readfile($filePath);
exit;
