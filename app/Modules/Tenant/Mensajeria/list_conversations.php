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

$roleAlias = mensajeria_role_alias();
$empresaSolicitada = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$empresaId = mensajeria_resolve_empresa_id($empresaSolicitada !== false ? $empresaSolicitada : null);

if ($roleAlias === 'cliente' && (!$empresaId || $empresaId <= 0)) {
    http_response_code(400);
    echo json_encode(['error' => 'No se pudo determinar la empresa asociada.']);
    exit;
}

if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Debe especificar una empresa para consultar las conversaciones.']);
    exit;
}

$sql = <<<'SQL'
    SELECT
        c.id,
        c.asunto,
        c.estado,
        c.created_at,
        c.updated_at,
        (SELECT COUNT(*) FROM tenant_messages tm WHERE tm.conversation_id = c.id) AS total_mensajes,
        (SELECT COUNT(*) FROM tenant_messages tm WHERE tm.conversation_id = c.id AND tm.autor_tipo = 'soporte' AND tm.leido_empresa = 0) AS sin_leer,
        (SELECT tm.mensaje FROM tenant_messages tm WHERE tm.conversation_id = c.id ORDER BY tm.created_at DESC, tm.id DESC LIMIT 1) AS ultimo_mensaje,
        (SELECT tm.autor_tipo FROM tenant_messages tm WHERE tm.conversation_id = c.id ORDER BY tm.created_at DESC, tm.id DESC LIMIT 1) AS ultimo_autor_tipo,
        (SELECT COALESCE(u.nombre, u.usuario) FROM tenant_messages tm LEFT JOIN usuarios u ON tm.autor_id = u.id WHERE tm.conversation_id = c.id ORDER BY tm.created_at DESC, tm.id DESC LIMIT 1) AS ultimo_autor,
        (SELECT tm.created_at FROM tenant_messages tm WHERE tm.conversation_id = c.id ORDER BY tm.created_at DESC, tm.id DESC LIMIT 1) AS ultimo_created_at
    FROM tenant_conversations c
    WHERE c.empresa_id = ?
    ORDER BY COALESCE(
        (SELECT tm.created_at FROM tenant_messages tm WHERE tm.conversation_id = c.id ORDER BY tm.created_at DESC, tm.id DESC LIMIT 1),
        c.updated_at,
        c.created_at
    ) DESC,
    c.id DESC
    LIMIT 100
SQL;

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta.']);
    exit;
}

$stmt->bind_param('i', $empresaId);
if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo obtener la información de mensajería.']);
    exit;
}

$result = $stmt->get_result();
$conversaciones = [];
while ($row = $result->fetch_assoc()) {
    $conversaciones[] = [
        'id' => (int) $row['id'],
        'asunto' => (string) ($row['asunto'] ?? ''),
        'estado' => (string) ($row['estado'] ?? ''),
        'created_at' => (string) ($row['created_at'] ?? ''),
        'updated_at' => (string) ($row['updated_at'] ?? ''),
        'total_mensajes' => isset($row['total_mensajes']) ? (int) $row['total_mensajes'] : 0,
        'sin_leer' => isset($row['sin_leer']) ? (int) $row['sin_leer'] : 0,
        'ultimo_mensaje' => (string) ($row['ultimo_mensaje'] ?? ''),
        'ultimo_autor_tipo' => (string) ($row['ultimo_autor_tipo'] ?? ''),
        'ultimo_autor' => (string) ($row['ultimo_autor'] ?? ''),
        'ultimo_created_at' => (string) ($row['ultimo_created_at'] ?? ''),
    ];
}
$stmt->close();

$response = [
    'empresa_id' => $empresaId,
    'conversaciones' => $conversaciones,
];

$counts = mensajeria_count_unread($conn, $empresaId);
$response['unread'] = $counts;

echo json_encode($response);
