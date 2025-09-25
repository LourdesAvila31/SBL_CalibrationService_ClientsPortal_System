<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

ensure_portal_access('tenant');

$roleAlias = session_role_alias() ?? '';
if ($roleAlias !== 'cliente') {
    http_response_code(403);
    echo 'Acceso denegado.';
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo 'Empresa no identificada.';
    exit;
}

$certificateId = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
if (!$certificateId) {
    http_response_code(400);
    echo 'Identificador de certificado inválido.';
    exit;
}

$sql = 'SELECT c.archivo, cal.estado, cal.liberado_por, cal.fecha_liberacion'
     . ' FROM certificados c JOIN calibraciones cal ON cal.id = c.calibracion_id'
     . ' WHERE c.id = ? AND cal.empresa_id = ? LIMIT 1';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo 'No se pudo validar el certificado solicitado.';
    exit;
}
$stmt->bind_param('ii', $certificateId, $empresaId);
$stmt->execute();
$fileName = null;
$estado = null;
$liberadoPor = null;
$fechaLiberacion = null;
if (method_exists($stmt, 'get_result')) {
    $res = $stmt->get_result();
    $row = $res ? $res->fetch_assoc() : null;
    if ($row) {
        $fileName = $row['archivo'];
        $estado = $row['estado'] ?? null;
        $liberadoPor = $row['liberado_por'] ?? null;
        $fechaLiberacion = $row['fecha_liberacion'] ?? null;
    }
} else {
    $stmt->bind_result($archivo, $estadoDb, $liberadoDb, $fechaDb);
    if ($stmt->fetch()) {
        $fileName = $archivo;
        $estado = $estadoDb;
        $liberadoPor = $liberadoDb;
        $fechaLiberacion = $fechaDb;
    }
}
$stmt->close();
$conn->close();

if (!$fileName) {
    http_response_code(404);
    echo 'Certificado no encontrado o aún en revisión.';
    exit;
}

if ($estado !== 'Aprobado') {
    http_response_code(423);
    echo 'El certificado continúa bloqueado porque la calibración no ha sido aprobada.';
    exit;
}

if (empty($liberadoPor) || empty($fechaLiberacion)) {
    http_response_code(423);
    echo 'El certificado sigue en revisión y se habilitará cuando calidad complete la liberación.';
    exit;
}

$baseDir = realpath(__DIR__ . '/../calibraciones/certificates');
if ($baseDir === false) {
    http_response_code(404);
    echo 'Directorio de certificados no disponible.';
    exit;
}

$basename = basename($fileName);
$target = realpath($baseDir . DIRECTORY_SEPARATOR . $basename);
if ($target === false || strpos($target, $baseDir) !== 0 || !is_file($target)) {
    http_response_code(404);
    echo 'Archivo no disponible.';
    exit;
}

$mime = function_exists('mime_content_type') ? mime_content_type($target) : 'application/octet-stream';
header('Content-Type: ' . $mime);
header('Content-Disposition: attachment; filename="' . $basename . '"');
header('Content-Length: ' . filesize($target));
readfile($target);
