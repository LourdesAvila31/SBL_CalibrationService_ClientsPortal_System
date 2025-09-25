<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/qr_links.php';
require_once __DIR__ . '/certificate_helpers.php';

header('Content-Type: application/json');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$instrumentoId = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
if (!$instrumentoId) {
    http_response_code(422);
    echo json_encode(['error' => 'Identificador de instrumento no válido']);
    exit;
}

$stmt = $conn->prepare(
    "SELECT i.id, COALESCE(ci.nombre, '') AS nombre, COALESCE(m.nombre, '') AS marca, COALESCE(mo.nombre, '') AS modelo,\n            i.codigo, i.serie, COALESCE(d.nombre, '') AS departamento, COALESCE(i.ubicacion, '') AS ubicacion,\n            COALESCE(i.estado, '') AS estado, i.proxima_calibracion\n       FROM instrumentos i\n       LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id\n       LEFT JOIN marcas m ON i.marca_id = m.id\n       LEFT JOIN modelos mo ON i.modelo_id = mo.id\n       LEFT JOIN departamentos d ON i.departamento_id = d.id\n      WHERE i.id = ? AND i.empresa_id = ?\n      LIMIT 1"
);

if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible consultar el instrumento']);
    exit;
}

$stmt->bind_param('ii', $instrumentoId, $empresaId);
$stmt->execute();
$result = $stmt->get_result();
$instrumento = $result ? $result->fetch_assoc() : null;
$stmt->close();

if (!$instrumento) {
    http_response_code(404);
    echo json_encode(['error' => 'Instrumento no encontrado']);
    exit;
}

$nombre = trim((string) ($instrumento['nombre'] ?? ''));
$codigo = trim((string) ($instrumento['codigo'] ?? ''));
$serie = trim((string) ($instrumento['serie'] ?? ''));
$departamento = trim((string) ($instrumento['departamento'] ?? ''));
$ubicacion = trim((string) ($instrumento['ubicacion'] ?? ''));
$marca = trim((string) ($instrumento['marca'] ?? ''));
$modelo = trim((string) ($instrumento['modelo'] ?? ''));
$estado = trim((string) ($instrumento['estado'] ?? ''));
$proximaCalibracion = $instrumento['proxima_calibracion'] ?? null;
$proximaCalibracionIso = null;
if ($proximaCalibracion !== null && $proximaCalibracion !== '') {
    $timestamp = strtotime((string) $proximaCalibracion);
    if ($timestamp !== false) {
        $proximaCalibracionIso = gmdate('c', $timestamp);
    } else {
        $proximaCalibracionIso = (string) $proximaCalibracion;
    }
}

$displayName = $nombre !== '' ? $nombre : 'Instrumento';
$identifier = $codigo !== '' ? $codigo : ($serie !== '' ? $serie : 'ID ' . $instrumentoId);

$response = [
    'instrument_id' => (int) $instrumentoId,
    'name' => $displayName,
    'code' => $codigo,
    'serial' => $serie,
    'brand' => $marca,
    'model' => $modelo,
    'status' => $estado,
    'department' => $departamento,
    'location' => $ubicacion,
    'next_calibration' => $proximaCalibracionIso,
    'identifier' => $identifier,
    'certificate_url' => null,
    'certificate_issued_at' => null,
    'certificate_expires_at' => null,
    'detail_token' => null,
    'detail_url' => null,
    'detail_expires_at' => null,
    'generated_at' => gmdate('c'),
];

try {
    $ultimoCertificado = tenant_latest_certificate($conn, $instrumentoId, $empresaId);
    if ($ultimoCertificado && !empty($ultimoCertificado['calibration_id'])) {
        $tokenData = qr_links_generate_certificate_token(
            $empresaId,
            $instrumentoId,
            (int) $ultimoCertificado['calibration_id'],
            $ultimoCertificado['archivo']
        );
        $response['certificate_url'] = qr_links_certificate_relative_url($tokenData['token']);
        $issuedAt = $ultimoCertificado['fecha_calibracion'] ?? null;
        if ($issuedAt !== null && $issuedAt !== '') {
            $issuedTimestamp = strtotime((string) $issuedAt);
            if ($issuedTimestamp !== false) {
                $certificateIssuedIso = gmdate('c', $issuedTimestamp);
            } else {
                $certificateIssuedIso = (string) $issuedAt;
            }
        }
        $certificateExpiresIso = gmdate('c', $tokenData['expires_at']);
    }
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[instrumento_label] Error generando token: ' . $e->getMessage() . PHP_EOL);
}

try {
    $detailTokenData = qr_links_generate_instrument_detail_token($empresaId, (int) $response['instrument_id']);
    if (!empty($detailTokenData['token'])) {
        $response['detail_token'] = $detailTokenData['token'];
        $response['detail_url'] = qr_links_instrument_detail_relative_url($detailTokenData['token']);
        $response['detail_expires_at'] = gmdate('c', $detailTokenData['expires_at']);
    }
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[instrumento_label] Error generando token de detalle: ' . $e->getMessage() . PHP_EOL);
}

$response['resource_url'] = $response['detail_url'] ?? $response['certificate_url'];
$response['has_certificate'] = $response['certificate_url'] !== null;
$response['has_detail'] = $response['detail_url'] !== null;

echo json_encode($response);

$conn->close();
