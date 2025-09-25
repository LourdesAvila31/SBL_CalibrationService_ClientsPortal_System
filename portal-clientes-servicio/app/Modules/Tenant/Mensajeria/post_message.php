<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once __DIR__ . '/helpers.php';

header('Content-Type: application/json');

if (!check_permission('mensajeria_responder')) {
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

$mensaje = trim((string) ($_POST['mensaje'] ?? ''));
if ($mensaje === '') {
    http_response_code(422);
    echo json_encode(['error' => 'El mensaje no puede estar vacío']);
    exit;
}

$estadoSolicitado = isset($_POST['estado']) ? trim((string) $_POST['estado']) : '';

$roleAlias = mensajeria_role_alias();
$usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;
if (!$usuarioId) {
    http_response_code(401);
    echo json_encode(['error' => 'Sesión no válida']);
    exit;
}

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
    echo json_encode(['error' => 'No tiene permiso para interactuar en esta conversación']);
    exit;
}

$adjuntosGuardados = [];
try {
    $adjuntosGuardados = mensajeria_save_attachments('adjuntos');
} catch (RuntimeException $e) {
    http_response_code(422);
    echo json_encode(['error' => $e->getMessage()]);
    exit;
}

$attachmentsPayload = mensajeria_strip_attachment_paths($adjuntosGuardados);
$attachmentsJson = $attachmentsPayload ? json_encode($attachmentsPayload) : null;

$autorTipo = mensajeria_author_type($roleAlias);
$leidoEmpresa = $autorTipo === 'soporte' ? 0 : 1;

try {
    $conn->begin_transaction();

    $stmtMsg = $conn->prepare('INSERT INTO tenant_messages (conversation_id, autor_id, autor_tipo, mensaje, adjuntos, leido_empresa) VALUES (?, ?, ?, ?, ?, ?)');
    if (!$stmtMsg) {
        throw new RuntimeException('No se pudo preparar el mensaje.');
    }
    $stmtMsg->bind_param('iisssi', $conversationId, $usuarioId, $autorTipo, $mensaje, $attachmentsJson, $leidoEmpresa);
    if (!$stmtMsg->execute()) {
        $stmtMsg->close();
        throw new RuntimeException('No se pudo guardar el mensaje.');
    }
    $messageId = (int) $stmtMsg->insert_id;
    $stmtMsg->close();

    $estadoActualizar = $estadoSolicitado !== '' ? $estadoSolicitado : $conversation['estado'];
    $stmtConv = $conn->prepare('UPDATE tenant_conversations SET estado = ?, updated_at = NOW() WHERE id = ?');
    if ($stmtConv) {
        $stmtConv->bind_param('si', $estadoActualizar, $conversationId);
        $stmtConv->execute();
        $stmtConv->close();
        $conversation['estado'] = $estadoActualizar;
    }

    if ($autorTipo !== 'soporte') {
        $marcarLeido = $conn->prepare("UPDATE tenant_messages SET leido_empresa = 1 WHERE conversation_id = ? AND autor_tipo = 'soporte'");
        if ($marcarLeido) {
            $marcarLeido->bind_param('i', $conversationId);
            $marcarLeido->execute();
            $marcarLeido->close();
        }
    }

    $conn->commit();
} catch (Throwable $exception) {
    $conn->rollback();
    mensajeria_cleanup_attachments($adjuntosGuardados);
    http_response_code(500);
    echo json_encode(['error' => $exception->getMessage()]);
    exit;
}

$messageRow = [
    'id' => $messageId,
    'conversation_id' => $conversationId,
    'autor_id' => $usuarioId,
    'autor_tipo' => $autorTipo,
    'mensaje' => $mensaje,
    'adjuntos' => $attachmentsJson,
    'leido_empresa' => $leidoEmpresa,
    'created_at' => date('Y-m-d H:i:s'),
    'autor_nombre' => $_SESSION['nombre'] ?? null,
];

$mensajeNormalizado = mensajeria_normalize_message($messageRow);

if ($autorTipo === 'soporte') {
    mensajeria_notify_new_support_message($conn, $conversation, $mensajeNormalizado);
}

$empresaConversacion = (int) ($conversation['empresa_id'] ?? $empresaId);

$response = [
    'conversation' => [
        'id' => $conversationId,
        'empresa_id' => $empresaConversacion,
        'asunto' => (string) ($conversation['asunto'] ?? ''),
        'estado' => (string) ($conversation['estado'] ?? ''),
    ],
    'message' => $mensajeNormalizado,
];

$response['unread'] = mensajeria_count_unread($conn, $empresaConversacion);

echo json_encode($response);
