<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once __DIR__ . '/InstrumentAttachmentService.php';

header('Content-Type: application/json');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$instrumentoId = filter_input(INPUT_GET, 'instrumento_id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);

if (!$instrumentoId) {
    http_response_code(422);
    echo json_encode(['error' => 'Instrumento no especificado']);
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
    $attachments = InstrumentAttachmentService::listAttachments($conn, $instrumentoId, $empresaId);
    $response = [
        'success' => true,
        'attachments' => $attachments,
        'total' => count($attachments),
        'can_manage' => check_permission('instrumentos_actualizar'),
    ];
    echo json_encode($response, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[instrumento_adjuntos] Error al listar: ' . $e->getMessage() . PHP_EOL);
    http_response_code(500);
    echo json_encode([
        'error' => 'No se pudieron obtener los adjuntos',
    ]);
}

$conn->close();

