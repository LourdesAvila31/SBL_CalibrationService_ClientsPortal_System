<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once __DIR__ . '/helpers.php';

header('Content-Type: application/json');

if (!check_permission('mensajeria_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$conversationId = filter_input(INPUT_POST, 'conversation_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if (!$conversationId) {
    http_response_code(400);
    echo json_encode(['error' => 'Conversación no válida']);
    exit;
}

$roleAlias = mensajeria_role_alias();
$empresaSolicitada = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$empresaId = mensajeria_resolve_empresa_id($empresaSolicitada !== false ? $empresaSolicitada : null);

$conversation = mensajeria_fetch_conversation($conn, $conversationId);
if (!$conversation) {
    http_response_code(404);
    echo json_encode(['error' => 'Conversación no encontrada']);
    exit;
}

if (!mensajeria_conversation_accessible($conversation, $roleAlias, $empresaId)) {
    http_response_code(403);
    echo json_encode(['error' => 'No tiene permiso para modificar esta conversación']);
    exit;
}

$empresaConversacion = (int) ($conversation['empresa_id'] ?? $empresaId ?? 0);

if ($roleAlias === 'cliente' || !$roleAlias || !mensajeria_is_support($roleAlias)) {
    $stmt = $conn->prepare("UPDATE tenant_messages SET leido_empresa = 1 WHERE conversation_id = ? AND autor_tipo = 'soporte'");
    if ($stmt) {
        $stmt->bind_param('i', $conversationId);
        $stmt->execute();
        $stmt->close();
    }
}

$response = [
    'success' => true,
    'conversation_id' => $conversationId,
];

if ($empresaConversacion > 0) {
    $response['unread'] = mensajeria_count_unread($conn, $empresaConversacion);
}

echo json_encode($response);
