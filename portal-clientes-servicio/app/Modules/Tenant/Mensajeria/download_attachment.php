<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once __DIR__ . '/helpers.php';

$messageId = filter_input(INPUT_GET, 'message_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$index = filter_input(INPUT_GET, 'index', FILTER_VALIDATE_INT, ['options' => ['min_range' => 0]]);

if (!$messageId || $index === false || $index === null) {
    http_response_code(400);
    echo 'Parámetros inválidos';
    exit;
}

if (!check_permission('mensajeria_leer')) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}

$roleAlias = mensajeria_role_alias();
$empresaSolicitada = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$empresaId = mensajeria_resolve_empresa_id($empresaSolicitada !== false ? $empresaSolicitada : null);

$sql = <<<'SQL'
    SELECT tm.id, tm.conversation_id, tm.adjuntos, tc.empresa_id
    FROM tenant_messages tm
    INNER JOIN tenant_conversations tc ON tc.id = tm.conversation_id
    WHERE tm.id = ?
    LIMIT 1
SQL;

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo 'No se pudo preparar la consulta.';
    exit;
}

$stmt->bind_param('i', $messageId);
if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo 'No se pudo obtener el adjunto.';
    exit;
}

$result = $stmt->get_result();
$message = $result ? $result->fetch_assoc() : null;
$stmt->close();

if (!$message) {
    http_response_code(404);
    echo 'Adjunto no encontrado';
    exit;
}

if (!mensajeria_conversation_accessible(['empresa_id' => $message['empresa_id'] ?? null], $roleAlias, $empresaId)) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}

$attachments = mensajeria_decode_attachments($message['adjuntos'] ?? null);
if (!isset($attachments[$index])) {
    http_response_code(404);
    echo 'Adjunto no encontrado';
    exit;
}

$attachment = $attachments[$index];
$storedName = $attachment['stored_name'] ?? '';
if ($storedName === '') {
    http_response_code(404);
    echo 'Adjunto no encontrado';
    exit;
}

$path = rtrim(mensajeria_storage_dir(), '/\\') . DIRECTORY_SEPARATOR . $storedName;
if (!is_file($path)) {
    http_response_code(404);
    echo 'Archivo no disponible';
    exit;
}

$mimeType = $attachment['mime_type'] ?? null;
if (!$mimeType) {
    $mimeType = 'application/octet-stream';
}

$filename = $attachment['original_name'] ?? basename($storedName);
$filename = preg_replace('/[\r\n\t]+/', ' ', (string) $filename);
$filename = $filename !== null ? trim($filename) : '';
if ($filename === '') {
    $filename = basename($storedName);
}
$filename = str_replace('"', '', $filename);
$filesize = filesize($path);

header('Content-Type: ' . $mimeType);
header('Content-Disposition: attachment; filename="' . $filename . '"');
if ($filesize !== false) {
    header('Content-Length: ' . $filesize);
}
header('Cache-Control: private');
header('Pragma: private');

$handle = fopen($path, 'rb');
if ($handle === false) {
    http_response_code(500);
    echo 'No se pudo abrir el adjunto.';
    exit;
}

while (!feof($handle)) {
    echo fread($handle, 8192);
    if (ob_get_level()) {
        ob_flush();
    }
    flush();
}

fclose($handle);
exit;
