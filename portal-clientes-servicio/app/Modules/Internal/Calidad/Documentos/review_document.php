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
$revisorId = isset($payload['revisor_id']) ? (int)$payload['revisor_id'] : 0;
$decision = strtolower(trim((string)($payload['decision'] ?? '')));
$comentarios = trim((string)($payload['comentarios'] ?? ''));

if ($documentoId <= 0 || $revisorId <= 0 || $decision === '') {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Documento, revisor y decisión son obligatorios']);
    exit;
}

$decisionNormalizada = match ($decision) {
    'aprobado', 'aprobada' => 'aprobado',
    'rechazado', 'rechazada', 'requiere ajustes', 'requiere_ajustes' => 'rechazado',
    default => 'en_revision',
};

$comentariosParam = $comentarios !== '' ? $comentarios : null;

$revisorStmt = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$revisorStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al revisor']);
    exit;
}
$revisorStmt->bind_param('ii', $revisorId, $empresaId);
$revisorStmt->execute();
$revisorStmt->store_result();
if ($revisorStmt->num_rows === 0) {
    $revisorStmt->close();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'El revisor indicado no pertenece a la empresa']);
    exit;
}
$revisorStmt->close();

$revisionTable = <<<'SQL'
CREATE TABLE IF NOT EXISTS calidad_documentos_revision (
    documento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    revisor_id INT NOT NULL,
    decision ENUM('en_revision','aprobado','rechazado') NOT NULL DEFAULT 'en_revision',
    comentarios TEXT NULL,
    revisado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (documento_id, empresa_id),
    KEY idx_calidad_doc_revision_revisor (revisor_id),
    CONSTRAINT fk_calidad_doc_revision_documento FOREIGN KEY (documento_id) REFERENCES calidad_documentos(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_doc_revision_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_doc_revision_revisor FOREIGN KEY (revisor_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SQL;

if (!$conn->query($revisionTable)) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar el historial de revisiones']);
    exit;
}

$conn->begin_transaction();

$validarStmt = $conn->prepare('SELECT estado FROM calidad_documentos WHERE id = ? AND empresa_id = ? FOR UPDATE');
if (!$validarStmt) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar la validación del documento']);
    exit;
}

$validarStmt->bind_param('ii', $documentoId, $empresaId);
$validarStmt->execute();
$validarStmt->bind_result($estadoActual);
if (!$validarStmt->fetch()) {
    $validarStmt->close();
    $conn->rollback();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Documento no encontrado']);
    exit;
}
$validarStmt->close();

$guardarRevision = $conn->prepare(
    'INSERT INTO calidad_documentos_revision (documento_id, empresa_id, revisor_id, decision, comentarios, revisado_en)
     VALUES (?, ?, ?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE revisor_id = VALUES(revisor_id), decision = VALUES(decision), comentarios = VALUES(comentarios), revisado_en = VALUES(revisado_en)'
);
if (!$guardarRevision) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar la actualización de revisión']);
    exit;
}

$guardarRevision->bind_param('iiiis', $documentoId, $empresaId, $revisorId, $decisionNormalizada, $comentariosParam);

if (!$guardarRevision->execute()) {
    $guardarRevision->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la revisión del documento']);
    exit;
}
$guardarRevision->close();

$actualizarDocumento = $conn->prepare('UPDATE calidad_documentos SET actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
if ($actualizarDocumento) {
    $actualizarDocumento->bind_param('ii', $documentoId, $empresaId);
    $actualizarDocumento->execute();
    $actualizarDocumento->close();
}

$conn->commit();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? ''));

$estadoRevisionLectura = match ($decisionNormalizada) {
    'aprobado' => 'Aprobado',
    'rechazado' => 'Requiere Ajustes',
    default => 'En Revisión',
};

log_activity($nombreAud, [
    'seccion' => 'calidad_documentos',
    'valor_nuevo' => sprintf('Revisión documento #%d (%s)', $documentoId, $estadoRevisionLectura),
    'valor_anterior' => $estadoActual,
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'Revisión registrada',
    'data' => [
        'documento_id' => $documentoId,
        'estado' => $estadoRevisionLectura,
    ],
]);

$conn->close();
