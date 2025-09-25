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

$titulo = trim((string)($payload['titulo'] ?? ''));
$responsableId = isset($payload['responsable_id']) ? (int)$payload['responsable_id'] : 0;
$descripcion = trim((string)($payload['descripcion'] ?? ''));
$contenido = trim((string)($payload['contenido'] ?? (string)($payload['ruta_archivo'] ?? '')));

if ($titulo === '' || $responsableId <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Título y responsable son obligatorios']);
    exit;
}

$responsableStmt = $conn->prepare('SELECT TRIM(CONCAT(COALESCE(nombre, ""), " ", COALESCE(apellidos, ""))) AS nombre FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$responsableStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al responsable indicado']);
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

$creadorId = isset($_SESSION['usuario_id']) ? (int)$_SESSION['usuario_id'] : 0;
$descripcionParam = $descripcion !== '' ? $descripcion : null;
$contenidoParam = $contenido !== '' ? $contenido : null;
$estadoInicial = 'borrador';

$stmt = $conn->prepare(
    'INSERT INTO calidad_documentos (empresa_id, titulo, descripcion, contenido, estado, creado_por, responsable_id, responsable) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar el registro de documento']);
    exit;
}

$creadorParam = $creadorId > 0 ? $creadorId : null;

$stmt->bind_param(
    'issssiis',
    $empresaId,
    $titulo,
    $descripcionParam,
    $contenidoParam,
    $estadoInicial,
    $creadorParam,
    $responsableId,
    $responsableNombre
);

if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar el documento de calidad']);
    exit;
}

$documentoId = $stmt->insert_id;
$stmt->close();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? '')); 

log_activity($nombreAud, [
    'seccion' => 'calidad_documentos',
    'valor_nuevo' => sprintf('Alta documento #%d (%s)', $documentoId, $titulo),
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

$response = [
    'success' => true,
    'message' => 'Documento registrado correctamente',
    'data' => [
        'documento_id' => $documentoId,
        'estado' => $estadoInicial,
    ],
];

echo json_encode($response);

$conn->close();
