<?php
require_once dirname(__DIR__, 3) . '/app/Core/helpers/qr_links.php';
require_once dirname(__DIR__, 3) . '/app/Core/db.php';


// Nota: el enlace compartido debe funcionar incluso si el navegador ya tiene una sesión
// autenticada sin el permiso `calibraciones_leer`. Por ello evitamos iniciar o validar la
// sesión aquí y confiamos únicamente en el token firmado.

$token = filter_input(INPUT_GET, 'token', FILTER_UNSAFE_RAW);
$payload = qr_links_decode_token($token);

if (!$payload) {
    http_response_code(400);
    echo 'Token inválido';
    exit;
}

if (qr_links_token_is_expired($payload)) {
    http_response_code(410);
    echo 'El enlace ha expirado';
    exit;
}

// El token firmado es la autorización para consultar el certificado compartido

$empresaId = (int) ($payload['empresa_id'] ?? 0);
$instrumentoId = (int) ($payload['instrument_id'] ?? 0);
$calibracionId = (int) ($payload['calibration_id'] ?? 0);
$archivo = $payload['file'] ?? '';

if ($empresaId <= 0 || $instrumentoId <= 0 || $calibracionId <= 0 || $archivo === '') {
    http_response_code(400);
    echo 'Payload incompleto';
    exit;
}

$stmt = $conn->prepare(
    'SELECT cert.archivo
       FROM calibraciones cal
       JOIN instrumentos i ON i.id = cal.instrumento_id
       JOIN certificados cert ON cert.calibracion_id = cal.id
      WHERE cal.id = ? AND cal.instrumento_id = ? AND i.empresa_id = ? AND cert.archivo = ?
      LIMIT 1'
);

if (!$stmt) {
    http_response_code(500);
    echo 'No se pudo validar el certificado';
    exit;
}

$stmt->bind_param('iiis', $calibracionId, $instrumentoId, $empresaId, $archivo);
$stmt->execute();
$stmt->store_result();

if ($stmt->num_rows === 0) {
    $stmt->close();
    http_response_code(404);
    echo 'Certificado no encontrado';
    exit;
}

$stmt->close();

$certDir = realpath(dirname(__DIR__) . '/calibraciones/certificates');
if (!$certDir) {
    http_response_code(404);
    echo 'Directorio de certificados no disponible';
    exit;
}

$certDir = rtrim($certDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
$fullPath = realpath($certDir . $archivo);

if ($fullPath === false || strpos($fullPath, $certDir) !== 0 || !is_file($fullPath)) {
    http_response_code(404);
    echo 'Archivo de certificado no disponible';
    exit;
}

$mime = function_exists('mime_content_type') ? mime_content_type($fullPath) : 'application/octet-stream';
$filename = basename($fullPath);

header('Content-Type: ' . $mime);
header('Content-Disposition: inline; filename="' . str_replace('"', '', $filename) . '"');
header('Content-Length: ' . filesize($fullPath));
header('Cache-Control: private, max-age=600');

readfile($fullPath);
exit;
