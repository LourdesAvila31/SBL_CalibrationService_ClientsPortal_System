<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'error' => 'Acceso denegado']);
    exit;
}
// Actualiza la información del plan basado en riesgos de un instrumento
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

$id = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT);
if (!$id) {
    echo json_encode(['success' => false, 'error' => 'ID inválido']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$check = $conn->prepare('SELECT TRIM(LOWER(estado)) AS estado FROM instrumentos WHERE id = ? AND empresa_id = ?');
$check->bind_param('ii', $id, $empresaId);
$check->execute();
$check->bind_result($estado);
if (!$check->fetch()) {
    echo json_encode(['success' => false, 'error' => 'Instrumento no encontrado']);
    $check->close();
    exit;
}
$check->close();
if (!in_array($estado, ['activo', 'stock', 'en stock'], true)) {
    echo json_encode(['success' => false, 'error' => 'Instrumento inactivo']);
    exit;
}

$requerimiento       = $_POST['requerimiento'] ?? null;
$impacto             = filter_input(INPUT_POST, 'impacto_falla', FILTER_VALIDATE_INT);
$consideraciones     = filter_input(INPUT_POST, 'consideraciones_falla', FILTER_VALIDATE_INT);
$capacidad           = filter_input(INPUT_POST, 'capacidad_deteccion', FILTER_VALIDATE_INT);
$observaciones       = $_POST['observaciones'] ?? null;
$tipo_calibracion    = $_POST['tipo_calibracion'] ?? null;

// Obtener valores actuales para completar los faltantes
$stmt = $conn->prepare("SELECT requerimiento, impacto_falla, consideraciones_falla, capacidad_deteccion, observaciones, tipo_calibracion FROM plan_riesgos WHERE instrumento_id = ? LIMIT 1");
$stmt->bind_param('i', $id);
$stmt->execute();
$res = $stmt->get_result();
if (!$res || !$row = $res->fetch_assoc()) {
    echo json_encode(['success' => false, 'error' => 'Plan de riesgos no encontrado']);
    exit;
}
$stmt->close();

$requerimiento   = $requerimiento   ?? $row['requerimiento'];
$impacto         = $impacto         ?? (int)$row['impacto_falla'];
$consideraciones = $consideraciones ?? (int)$row['consideraciones_falla'];
$capacidad       = $capacidad       ?? (int)$row['capacidad_deteccion'];
$observaciones   = $observaciones   ?? $row['observaciones'];
$tipo_calibracion= $tipo_calibracion?? $row['tipo_calibracion'];

// Calcular clase de riesgo y frecuencia
$score = (int)$impacto + (int)$consideraciones;
if ($score >= 5) {
    $clase = 'Alto';
    $descripcion = 'Alta';
    $frecuencia = 6;
} elseif ($score == 4) {
    $clase = 'Medio';
    $descripcion = 'Media';
    $frecuencia = 12;
} else {
    $clase = 'Bajo';
    $descripcion = 'Baja';
    $frecuencia = 18;
}

$stmt = $conn->prepare("UPDATE plan_riesgos SET requerimiento=?, impacto_falla=?, consideraciones_falla=?, clase_riesgo=?, capacidad_deteccion=?, frecuencia=?, observaciones=?, tipo_calibracion=?, fecha_actualizacion=NOW() WHERE instrumento_id=?");
$stmt->bind_param('siisiissi', $requerimiento, $impacto, $consideraciones, $clase, $capacidad, $frecuencia, $observaciones, $tipo_calibracion, $id);
$success = $stmt->execute();
$stmt->close();
$conn->close();

echo json_encode([
    'success' => $success,
    'clase_riesgo' => $clase,
    'frecuencia' => $frecuencia,
    'descripcion' => $descripcion
]);

?>
