<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';

header('Content-Type: application/json');

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Auditoria/audit.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no identificada']);
    exit;
}

$payload = json_decode(file_get_contents('php://input'), true);
if (!is_array($payload)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Solicitud inválida']);
    exit;
}

$noConformidadId = isset($payload['no_conformidad_id']) ? (int)$payload['no_conformidad_id'] : 0;
$accion = trim((string)($payload['accion'] ?? ''));
$responsableId = isset($payload['responsable_id']) ? (int)$payload['responsable_id'] : 0;
$fechaCompromiso = trim((string)($payload['fecha_compromiso'] ?? ''));
$estadoSeguimiento = strtolower(trim((string)($payload['estado'] ?? 'en_proceso')));

if ($noConformidadId <= 0 || $accion === '' || $responsableId <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Acción, no conformidad y responsable son obligatorios']);
    exit;
}

$fechaCompromisoValida = null;
if ($fechaCompromiso !== '') {
    $fechaCompromisoDate = DateTime::createFromFormat('Y-m-d', $fechaCompromiso);
    if (!$fechaCompromisoDate) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Fecha de compromiso inválida']);
        exit;
    }
    $fechaCompromisoValida = $fechaCompromisoDate->format('Y-m-d');
}

$estadoAccion = in_array($estadoSeguimiento, ['pendiente', 'en_proceso', 'cerrada'], true) ? $estadoSeguimiento : 'en_proceso';

$responsableStmt = $conn->prepare('SELECT TRIM(CONCAT(COALESCE(nombre, ""), " ", COALESCE(apellidos, ""))) FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$responsableStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al responsable de la acción']);
    exit;
}
$responsableStmt->bind_param('ii', $responsableId, $empresaId);
$responsableStmt->execute();
$responsableStmt->bind_result($responsableNombre);
if (!$responsableStmt->fetch()) {
    $responsableStmt->close();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'El responsable indicado no pertenece a la empresa']);
    exit;
}
$responsableStmt->close();
$responsableNombre = trim((string)$responsableNombre) ?: null;

$conn->begin_transaction();

$selectNc = $conn->prepare('SELECT estado FROM calidad_no_conformidades WHERE id = ? AND empresa_id = ? FOR UPDATE');
if (!$selectNc) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar la no conformidad']);
    exit;
}

$selectNc->bind_param('ii', $noConformidadId, $empresaId);
$selectNc->execute();
$selectNc->bind_result($estadoActual);
if (!$selectNc->fetch()) {
    $selectNc->close();
    $conn->rollback();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'No conformidad no encontrada']);
    exit;
}
$selectNc->close();

$insertAccion = $conn->prepare(
    'INSERT INTO calidad_acciones_correctivas (no_conformidad_id, descripcion, estado, responsable_id, responsable, fecha_compromiso, creado_por)
     VALUES (?, ?, ?, ?, ?, ?, ?)' 
);
if (!$insertAccion) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar la acción de seguimiento']);
    exit;
}

$creadoPor = isset($_SESSION['usuario_id']) ? (int)$_SESSION['usuario_id'] : 0;
$creadoPorParam = $creadoPor > 0 ? $creadoPor : null;

$insertAccion->bind_param(
    'ississi',
    $noConformidadId,
    $accion,
    $estadoAccion,
    $responsableId,
    $responsableNombre,
    $fechaCompromisoValida,
    $creadoPorParam
);

if (!$insertAccion->execute()) {
    $insertAccion->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la acción de seguimiento']);
    exit;
}
$insertAccion->close();

$estadoPrincipal = $estadoAccion === 'cerrada' ? 'cerrada' : 'en_proceso';

$updateNc = $conn->prepare(
    'UPDATE calidad_no_conformidades SET estado = ?, responsable_id = ?, responsable = ?, actualizado_en = NOW()
     WHERE id = ? AND empresa_id = ?'
);
if (!$updateNc) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible actualizar el estado de la no conformidad']);
    exit;
}

$updateNc->bind_param(
    'sissi',
    $estadoPrincipal,
    $responsableId,
    $responsableNombre,
    $noConformidadId,
    $empresaId
);

if (!$updateNc->execute()) {
    $updateNc->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo actualizar el seguimiento']);
    exit;
}
$updateNc->close();

$conn->commit();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? ''));

log_activity($nombreAud, [
    'seccion' => 'calidad_no_conformidades',
    'valor_nuevo' => sprintf('Seguimiento no conformidad #%d (%s)', $noConformidadId, $estadoPrincipal),
    'valor_anterior' => $estadoActual,
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'Acción de seguimiento registrada',
    'data' => [
        'no_conformidad_id' => $noConformidadId,
        'estado' => $estadoPrincipal,
    ],
]);

$conn->close();
