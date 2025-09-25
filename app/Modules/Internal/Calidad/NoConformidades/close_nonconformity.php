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
$resultado = trim((string)($payload['resultado'] ?? ''));
$verificacion = trim((string)($payload['verificacion'] ?? ''));
$fechaCierre = trim((string)($payload['fecha_cierre'] ?? date('Y-m-d')));
$cerradaPorId = isset($payload['cerrada_por_id']) ? (int)$payload['cerrada_por_id'] : (int)($_SESSION['usuario_id'] ?? 0);

if ($noConformidadId <= 0 || $resultado === '' || $cerradaPorId <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'No conformidad, resultado y responsable de cierre son obligatorios']);
    exit;
}

$fechaCierreValida = DateTime::createFromFormat('Y-m-d', $fechaCierre);
if (!$fechaCierreValida) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Fecha de cierre inválida']);
    exit;
}
$fechaCierreFormat = $fechaCierreValida->format('Y-m-d');

$responsableStmt = $conn->prepare('SELECT TRIM(CONCAT(COALESCE(nombre, ""), " ", COALESCE(apellidos, ""))) FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$responsableStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al responsable de cierre']);
    exit;
}
$responsableStmt->bind_param('ii', $cerradaPorId, $empresaId);
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

$insertCierre = $conn->prepare(
    'INSERT INTO calidad_acciones_correctivas (no_conformidad_id, descripcion, estado, responsable_id, responsable, fecha_cierre, resultado, seguimiento, creado_por)
     VALUES (?, ?, "cerrada", ?, ?, ?, ?, ?, ?)' 
);
if (!$insertCierre) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible registrar el cierre en las acciones correctivas']);
    exit;
}

$creadoPor = isset($_SESSION['usuario_id']) ? (int)$_SESSION['usuario_id'] : 0;
$creadoPorParam = $creadoPor > 0 ? $creadoPor : null;
$descripcionCierre = 'Cierre de la no conformidad';
$verificacionParam = $verificacion !== '' ? $verificacion : null;

$insertCierre->bind_param(
    'isissssi',
    $noConformidadId,
    $descripcionCierre,
    $cerradaPorId,
    $responsableNombre,
    $fechaCierreFormat,
    $resultado,
    $verificacionParam,
    $creadoPorParam
);

if (!$insertCierre->execute()) {
    $insertCierre->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo guardar el detalle del cierre']);
    exit;
}
$insertCierre->close();

$updateNc = $conn->prepare(
    'UPDATE calidad_no_conformidades
     SET estado = "cerrada", cerrado_en = ?, cerrado_por = ?, actualizado_en = NOW()
     WHERE id = ? AND empresa_id = ?'
);
if (!$updateNc) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar el cierre de la no conformidad']);
    exit;
}

$updateNc->bind_param('siii', $fechaCierreFormat, $cerradaPorId, $noConformidadId, $empresaId);

if (!$updateNc->execute()) {
    $updateNc->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo cerrar la no conformidad']);
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
    'valor_nuevo' => sprintf('Cierre no conformidad #%d', $noConformidadId),
    'valor_anterior' => $estadoActual,
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'No conformidad cerrada',
    'data' => [
        'no_conformidad_id' => $noConformidadId,
        'estado' => 'cerrada',
        'fecha_cierre' => $fechaCierreFormat,
    ],
]);

$conn->close();
