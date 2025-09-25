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

$conversationId = filter_input(INPUT_GET, 'conversation_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if (!$conversationId) {
    http_response_code(400);
    echo json_encode(['error' => 'Conversación no válida']);
    exit;
}

$roleAlias = mensajeria_role_alias();
$empresaSolicitada = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$empresaId = mensajeria_resolve_empresa_id($empresaSolicitada !== false ? $empresaSolicitada : null);

$conversation = mensajeria_fetch_conversation($conn, $conversationId);
if (!$conversation) {
    http_response_code(404);
    echo json_encode(['error' => 'Conversación no encontrada']);
    exit;
}

$empresaConversacion = isset($conversation['empresa_id']) ? (int) $conversation['empresa_id'] : 0;
if (!mensajeria_conversation_accessible($conversation, $roleAlias, $empresaId)) {
    http_response_code(403);
    echo json_encode(['error' => 'No tiene permiso para ver esta conversación']);
    exit;
}

$sql = <<<'SQL'
    SELECT
        tm.id,
        tm.conversation_id,
        tm.autor_id,
        tm.autor_tipo,
        tm.mensaje,
        tm.adjuntos,
        tm.leido_empresa,
        tm.created_at,
        COALESCE(u.nombre, u.usuario) AS autor_nombre
    FROM tenant_messages tm
    LEFT JOIN usuarios u ON tm.autor_id = u.id
    WHERE tm.conversation_id = ?
    ORDER BY tm.created_at ASC, tm.id ASC
SQL;

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta de mensajes.']);
    exit;
}

$stmt->bind_param('i', $conversationId);
if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['error' => 'No se pudieron obtener los mensajes.']);
    exit;
}

$result = $stmt->get_result();
$mensajes = [];
while ($row = $result->fetch_assoc()) {
    $mensajes[] = mensajeria_normalize_message($row);
}
$stmt->close();

$response = [
    'conversation' => [
        'id' => (int) $conversation['id'],
        'asunto' => (string) ($conversation['asunto'] ?? ''),
        'estado' => (string) ($conversation['estado'] ?? ''),
        'empresa_id' => $empresaConversacion,
        'created_at' => (string) ($conversation['created_at'] ?? ''),
        'updated_at' => (string) ($conversation['updated_at'] ?? ''),
    ],
    'messages' => $mensajes,
];

$response['unread'] = mensajeria_count_unread($conn, $empresaConversacion);

echo json_encode($response);
