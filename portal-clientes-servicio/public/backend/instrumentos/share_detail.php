<?php
require_once dirname(__DIR__, 3) . '/app/Core/helpers/qr_links.php';
require_once dirname(__DIR__, 3) . '/app/Core/db.php';
require_once dirname(__DIR__, 3) . '/app/Modules/Tenant/Instrumentos/gages/certificate_helpers.php';

header('Content-Type: application/json');

$token = filter_input(INPUT_GET, 'token', FILTER_UNSAFE_RAW);
$payload = qr_links_decode_detail_token($token);

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

$empresaId = (int) ($payload['empresa_id'] ?? 0);
$instrumentoId = (int) ($payload['instrument_id'] ?? 0);

if ($empresaId <= 0 || $instrumentoId <= 0) {
    http_response_code(400);
    echo json_encode(['error' => 'Payload incompleto']);
    exit;
}

$sql = "SELECT i.id, COALESCE(ci.nombre, '') AS nombre, COALESCE(m.nombre, '') AS marca, COALESCE(mo.nombre, '') AS modelo,
               i.codigo, i.serie, COALESCE(d.nombre, '') AS departamento, COALESCE(i.ubicacion, '') AS ubicacion,
               COALESCE(i.estado, '') AS estado, i.proxima_calibracion, COALESCE(emp.nombre, '') AS empresa
          FROM instrumentos i
          LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
          LEFT JOIN marcas m ON i.marca_id = m.id
          LEFT JOIN modelos mo ON i.modelo_id = mo.id
          LEFT JOIN departamentos d ON i.departamento_id = d.id
          JOIN empresas emp ON emp.id = i.empresa_id
         WHERE i.id = ? AND i.empresa_id = ?
         LIMIT 1";

$stmt = $conn->prepare($sql);

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
$empresaNombre = trim((string) ($instrumento['empresa'] ?? ''));
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
    'instrument_id' => $instrumentoId,
    'company' => $empresaNombre,
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
    'token_expires_at' => isset($payload['exp']) ? gmdate('c', (int) $payload['exp']) : null,
    'certificate_url' => null,
    'certificate_issued_at' => null,
    'certificate_expires_at' => null,
    'detail_token' => $token,
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
        $response['certificate_issued_at'] = $ultimoCertificado['fecha_calibracion'];
        $response['certificate_expires_at'] = gmdate('c', $tokenData['expires_at']);
    }
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[share_detail] Error generando token: ' . $e->getMessage() . PHP_EOL);
}

$response['has_certificate'] = $response['certificate_url'] !== null;

echo json_encode($response);

$conn->close();

