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

$tema = trim((string)($payload['tema'] ?? ''));
$fechaProgramada = trim((string)($payload['fecha_programada'] ?? ''));
$responsableId = isset($payload['responsable_id']) ? (int)$payload['responsable_id'] : 0;
$modalidad = trim((string)($payload['modalidad'] ?? 'Presencial'));
$duracionHoras = isset($payload['duracion_horas']) ? (float)$payload['duracion_horas'] : null;
$ubicacion = trim((string)($payload['ubicacion'] ?? ''));
$descripcion = trim((string)($payload['descripcion'] ?? ''));

if ($tema === '' || $fechaProgramada === '' || $responsableId <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Tema, fecha programada y responsable son obligatorios']);
    exit;
}

$responsableStmt = $conn->prepare('SELECT TRIM(CONCAT(COALESCE(nombre, ""), " ", COALESCE(apellidos, ""))) AS nombre FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$responsableStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al responsable de la capacitación']);
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

$responsableNombre = trim((string)$responsableNombre);
if ($responsableNombre === '') {
    $responsableNombre = null;
}

$fechaValida = DateTime::createFromFormat('Y-m-d H:i:s', $fechaProgramada) ?: DateTime::createFromFormat('Y-m-d', $fechaProgramada);
if (!$fechaValida) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Formato de fecha inválido']);
    exit;
}

$programadaEn = $fechaValida->format('Y-m-d');
$creadorId = isset($_SESSION['usuario_id']) ? (int)$_SESSION['usuario_id'] : 0;

$descripcionExtras = [];
if ($modalidad !== '') {
    $descripcionExtras[] = 'Modalidad: ' . $modalidad;
}
if ($ubicacion !== '') {
    $descripcionExtras[] = 'Ubicación: ' . $ubicacion;
}

$descripcionCompuesta = trim($descripcion . (count($descripcionExtras) ? ("\n" . implode("\n", $descripcionExtras)) : ''));
$descripcionParam = $descripcionCompuesta !== '' ? $descripcionCompuesta : null;
$duracionParam = $duracionHoras !== null ? (string)$duracionHoras : null;
$creadorParam = $creadorId > 0 ? $creadorId : null;
$estadoInicial = 'borrador';

$stmt = $conn->prepare(
    'INSERT INTO calidad_capacitaciones (empresa_id, tema, descripcion, fecha_programada, duracion_horas, responsable_id, responsable, estado, creado_por)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible registrar la capacitación']);
    exit;
}

$stmt->bind_param(
    'issssissi',
    $empresaId,
    $tema,
    $descripcionParam,
    $programadaEn,
    $duracionParam,
    $responsableId,
    $responsableNombre,
    $estadoInicial,
    $creadorParam
);

if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo guardar la programación de la capacitación']);
    exit;
}

$capacitacionId = $stmt->insert_id;
$stmt->close();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? ''));

log_activity($nombreAud, [
    'seccion' => 'calidad_capacitaciones',
    'valor_nuevo' => sprintf('Programación capacitación #%d (%s)', $capacitacionId, $tema),
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'Capacitación programada',
    'data' => [
        'capacitacion_id' => $capacitacionId,
        'estado' => $estadoInicial,
    ],
]);

$conn->close();
