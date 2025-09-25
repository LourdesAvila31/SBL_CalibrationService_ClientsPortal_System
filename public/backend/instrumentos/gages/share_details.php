<?php
require_once dirname(__DIR__, 4) . '/app/Core/helpers/qr_links.php';
require_once dirname(__DIR__, 4) . '/app/Core/permissions.php';

$portalSlug = session_portal_slug();
$modulesBase = dirname(__DIR__, 4) . '/app/Modules/';

header('Content-Type: application/json');

$token = filter_input(INPUT_GET, 'token', FILTER_UNSAFE_RAW);
$payload = qr_links_decode_token($token);

if (!$payload) {
    http_response_code(400);
    echo json_encode(['error' => 'Token inválido']);
    exit;
}

if (qr_links_token_is_expired($payload)) {
    http_response_code(410);
    echo json_encode(['error' => 'El enlace ha expirado']);
    exit;
}

require_once dirname(__DIR__, 4) . '/app/Core/db.php';
if ($portalSlug === 'service') {
    require_once $modulesBase . 'Service/Instrumentos/gages/certificate_helpers.php';
    require_once $modulesBase . 'Service/Instrumentos/gages/share_helpers.php';
} else {
    require_once $modulesBase . 'Tenant/Instrumentos/gages/certificate_helpers.php';
    require_once $modulesBase . 'Tenant/Instrumentos/gages/share_helpers.php';
}

if (!function_exists('qr_share_details_error')) {
    function qr_share_details_error(mysqli $conn, int $status, string $message): void
    {
        http_response_code($status);
        echo json_encode(['error' => $message]);
        $conn->close();
        exit;
    }
}

$empresaId = (int) ($payload['empresa_id'] ?? 0);
$instrumentoId = (int) ($payload['instrument_id'] ?? 0);
$calibracionId = (int) ($payload['calibration_id'] ?? 0);
$archivo = $payload['file'] ?? '';

if ($empresaId <= 0 || $instrumentoId <= 0 || $calibracionId <= 0 || $archivo === '') {
    qr_share_details_error($conn, 400, 'Datos incompletos en el enlace');
}

$instrumento = tenant_get_gage_share_summary($conn, $empresaId, $instrumentoId);
if (!$instrumento) {
    qr_share_details_error($conn, 404, 'Instrumento no encontrado');
}

$certificado = tenant_get_gage_share_certificate($conn, $empresaId, $instrumentoId, $calibracionId, $archivo);
if (!$certificado) {
    qr_share_details_error($conn, 404, 'Certificado no encontrado');
}

$ultimoCertificado = tenant_latest_certificate($conn, $instrumentoId, $empresaId);
if (!$ultimoCertificado || (int) ($ultimoCertificado['calibration_id'] ?? 0) !== $calibracionId || ($ultimoCertificado['archivo'] ?? '') !== $archivo) {
    qr_share_details_error($conn, 410, 'El enlace corresponde a un certificado desactualizado');
}

$calibracion = [
    'fecha_calibracion' => $certificado['fecha_calibracion'] ?? null,
    'fecha_proxima' => $certificado['fecha_proxima'] ?? $instrumento['proxima_calibracion'] ?? null,
    'resultado' => $certificado['resultado'] ?? '',
    'responsable' => $certificado['responsable'] ?? '',
];

$response = [
    'instrument' => [
        'id' => $instrumento['id'],
        'nombre' => $instrumento['nombre'],
        'codigo' => $instrumento['codigo'],
        'ubicacion' => $instrumento['ubicacion'],
        'estado' => $instrumento['estado'],
        'departamento' => $instrumento['departamento'],
        'proxima_calibracion' => $instrumento['proxima_calibracion'],
        'estado_calibracion' => $instrumento['estado_calibracion'] ?? null,
        'dias_restantes' => $instrumento['dias_restantes'],
    ],
    'calibration' => $calibracion,
    'certificate' => [
        'archivo' => $archivo,
        'issued_at' => $ultimoCertificado['fecha_calibracion'] ?? $certificado['fecha_calibracion'] ?? null,
        'share_url' => qr_links_certificate_relative_url($token),
    ],
    'token' => [
        'expires_at' => isset($payload['exp']) ? gmdate('c', (int) $payload['exp']) : null,
    ],
];

echo json_encode($response);

$conn->close();
