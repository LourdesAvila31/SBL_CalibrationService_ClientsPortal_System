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

$documentoId = isset($payload['documento_id']) ? (int)$payload['documento_id'] : 0;
$publicadorId = isset($payload['publicador_id']) ? (int)$payload['publicador_id'] : (int)($_SESSION['usuario_id'] ?? 0);
$urlPublicacion = trim((string)($payload['url_publicacion'] ?? ''));

if ($documentoId <= 0 || $publicadorId <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Documento y publicador son obligatorios']);
    exit;
}

$publicadorStmt = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$publicadorStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al publicador']);
    exit;
}
$publicadorStmt->bind_param('ii', $publicadorId, $empresaId);
$publicadorStmt->execute();
$publicadorStmt->store_result();
if ($publicadorStmt->num_rows === 0) {
    $publicadorStmt->close();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'El publicador indicado no pertenece a la empresa']);
    exit;
}
$publicadorStmt->close();

$conn->begin_transaction();

$selectStmt = $conn->prepare('SELECT estado FROM calidad_documentos WHERE id = ? AND empresa_id = ? FOR UPDATE');
if (!$selectStmt) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar el documento']);
    exit;
}

$selectStmt->bind_param('ii', $documentoId, $empresaId);
$selectStmt->execute();
$selectStmt->bind_result($estadoActual);
if (!$selectStmt->fetch()) {
    $selectStmt->close();
    $conn->rollback();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Documento no encontrado']);
    exit;
}
$selectStmt->close();

if ($estadoActual !== 'borrador') {
    $conn->rollback();
    http_response_code(409);
    echo json_encode(['success' => false, 'message' => 'Solo documentos en borrador pueden publicarse']);
    exit;
}

$revisionStmt = $conn->prepare('SELECT decision FROM calidad_documentos_revision WHERE documento_id = ? AND empresa_id = ? LIMIT 1');
if (!$revisionStmt) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible verificar la revisión del documento']);
    exit;
}
$revisionStmt->bind_param('ii', $documentoId, $empresaId);
$revisionStmt->execute();
$revisionStmt->bind_result($decisionRevision);
if (!$revisionStmt->fetch()) {
    $revisionStmt->close();
    $conn->rollback();
    http_response_code(409);
    echo json_encode(['success' => false, 'message' => 'El documento debe contar con una revisión registrada antes de publicarse']);
    exit;
}
$revisionStmt->close();

if ($decisionRevision !== 'aprobado') {
    $conn->rollback();
    http_response_code(409);
    echo json_encode(['success' => false, 'message' => 'La revisión debe estar aprobada antes de publicar el documento']);
    exit;
}

$updateStmt = $conn->prepare(
    'UPDATE calidad_documentos
     SET estado = "publicado", publicado_por = ?, publicado_en = NOW(), actualizado_en = NOW()
     WHERE id = ? AND empresa_id = ?'
);
if (!$updateStmt) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar la publicación del documento']);
    exit;
}

$updateStmt->bind_param('iii', $publicadorId, $documentoId, $empresaId);

if (!$updateStmt->execute()) {
    $updateStmt->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo publicar el documento']);
    exit;
}
$updateStmt->close();

$conn->commit();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? ''));

log_activity($nombreAud, [
    'seccion' => 'calidad_documentos',
    'valor_nuevo' => sprintf('Publicación documento #%d', $documentoId),
    'valor_anterior' => $estadoActual,
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'Documento publicado correctamente',
    'data' => [
        'documento_id' => $documentoId,
        'estado' => 'publicado',
        'url_publicacion' => $urlPublicacion !== '' ? $urlPublicacion : null,
    ],
]);

$conn->close();
