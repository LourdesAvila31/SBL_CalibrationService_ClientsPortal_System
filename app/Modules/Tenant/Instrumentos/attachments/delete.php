<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once __DIR__ . '/InstrumentAttachmentService.php';

header('Content-Type: application/json');

if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$attachmentId = filter_input(INPUT_POST, 'attachment_id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);
$instrumentoId = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);

if (!$attachmentId || !$instrumentoId) {
    http_response_code(422);
    echo json_encode(['error' => 'Solicitud inválida']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

try {
    InstrumentAttachmentService::deleteAttachment($conn, $attachmentId, $instrumentoId, $empresaId);
    echo json_encode([
        'success' => true,
        'total' => InstrumentAttachmentService::countByInstrument($conn, $instrumentoId, $empresaId),
    ]);
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[instrumento_adjuntos] Error al eliminar: ' . $e->getMessage() . PHP_EOL);
    http_response_code(500);
    echo json_encode(['error' => $e->getMessage()]);
}

$conn->close();

