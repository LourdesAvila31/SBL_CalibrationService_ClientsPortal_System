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

$roleAlias = mensajeria_role_alias();
$usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;
if (!$usuarioId) {
    http_response_code(401);
    echo json_encode(['error' => 'Sesión no válida']);
    exit;
}

$asunto = trim((string) ($_POST['asunto'] ?? ''));
$mensaje = trim((string) ($_POST['mensaje'] ?? ''));
$estado = isset($_POST['estado']) ? trim((string) $_POST['estado']) : 'abierto';

if ($asunto === '') {
    http_response_code(422);
    echo json_encode(['error' => 'El asunto es obligatorio']);
    exit;
}

if ($mensaje === '') {
    http_response_code(422);
    echo json_encode(['error' => 'El mensaje inicial es obligatorio']);
    exit;
}

$empresaSolicitada = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$empresaId = mensajeria_resolve_empresa_id($empresaSolicitada !== false ? $empresaSolicitada : null);

if ($roleAlias === 'cliente' && (!$empresaId || $empresaId <= 0)) {
    http_response_code(400);
    echo json_encode(['error' => 'No se pudo identificar la empresa.']);
    exit;
}

if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Debe indicar una empresa para crear la conversación.']);
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

try {
    $conn->begin_transaction();

    $estadoFinal = $estado !== '' ? $estado : 'abierto';
    $stmtConv = $conn->prepare('INSERT INTO tenant_conversations (empresa_id, asunto, estado) VALUES (?, ?, ?)');
    if (!$stmtConv) {
        throw new RuntimeException('No se pudo preparar la conversación.');
    }
    $stmtConv->bind_param('iss', $empresaId, $asunto, $estadoFinal);
    if (!$stmtConv->execute()) {
        $stmtConv->close();
        throw new RuntimeException('No se pudo crear la conversación.');
    }
    $conversationId = (int) $stmtConv->insert_id;
    $stmtConv->close();

    $autorTipo = mensajeria_author_type($roleAlias);
    $leidoEmpresa = $autorTipo === 'soporte' ? 0 : 1;

    $stmtMsg = $conn->prepare('INSERT INTO tenant_messages (conversation_id, autor_id, autor_tipo, mensaje, adjuntos, leido_empresa) VALUES (?, ?, ?, ?, ?, ?)');
    if (!$stmtMsg) {
        throw new RuntimeException('No se pudo preparar el mensaje.');
    }
    $stmtMsg->bind_param('iisssi', $conversationId, $usuarioId, $autorTipo, $mensaje, $attachmentsJson, $leidoEmpresa);
    if (!$stmtMsg->execute()) {
        $stmtMsg->close();
        throw new RuntimeException('No se pudo guardar el mensaje inicial.');
    }
    $messageId = (int) $stmtMsg->insert_id;
    $stmtMsg->close();

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

$conversationRow = mensajeria_fetch_conversation($conn, $conversationId);
if (!$conversationRow) {
    $conversationRow = [
        'id' => $conversationId,
        'empresa_id' => $empresaId,
        'asunto' => $asunto,
        'estado' => $estadoFinal,
        'created_at' => date('Y-m-d H:i:s'),
        'updated_at' => date('Y-m-d H:i:s'),
    ];
}

if ($autorTipo === 'soporte') {
    mensajeria_notify_new_support_message($conn, $conversationRow, $mensajeNormalizado);
}

$response = [
    'conversation' => [
        'id' => $conversationId,
        'empresa_id' => (int) ($conversationRow['empresa_id'] ?? $empresaId),
        'asunto' => (string) ($conversationRow['asunto'] ?? $asunto),
        'estado' => (string) ($conversationRow['estado'] ?? $estadoFinal),
        'created_at' => (string) ($conversationRow['created_at'] ?? date('Y-m-d H:i:s')),
        'updated_at' => (string) ($conversationRow['updated_at'] ?? date('Y-m-d H:i:s')),
    ],
    'message' => $mensajeNormalizado,
];

$response['unread'] = mensajeria_count_unread($conn, (int) ($conversationRow['empresa_id'] ?? $empresaId));

echo json_encode($response);
