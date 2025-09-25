<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$sql = 'SELECT id, nombre, codigo, certificado_numero, certificado_archivo, fecha_vencimiento FROM patrones WHERE empresa_id = ? AND (activo IS NULL OR activo = 1) ORDER BY nombre';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No se pudo preparar la consulta de patrones.']);
    $conn->close();
    exit;
}

$stmt->bind_param('i', $empresaId);
if (!$stmt->execute()) {
    http_response_code(500);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No se pudo obtener el catálogo de patrones.']);
    $stmt->close();
    $conn->close();
    exit;
}

$result = $stmt->get_result();
$patterns = [];
$downloadBase = '/backend/patrones/download_certificate.php?id=';
while ($row = $result->fetch_assoc()) {
    $pattern = [
        'id' => (int) $row['id'],
        'nombre' => $row['nombre'],
        'codigo' => $row['codigo'],
        'certificado_numero' => $row['certificado_numero'],
        'certificado_archivo' => $row['certificado_archivo'],
        'fecha_vencimiento' => $row['fecha_vencimiento'],
    ];
    if (!empty($row['certificado_archivo'])) {
        $pattern['certificado_url'] = $downloadBase . (int) $row['id'];
    }
    $patterns[] = $pattern;
}
$stmt->close();

header('Content-Type: application/json');
echo json_encode($patterns);
$conn->close();
