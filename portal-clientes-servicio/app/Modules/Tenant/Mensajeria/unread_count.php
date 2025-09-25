<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once __DIR__ . '/helpers.php';

header('Content-Type: application/json');

if (!check_permission('mensajeria_leer')) {
    echo json_encode([
        'empresa_id' => null,
        'unread' => ['mensajes' => 0, 'conversaciones' => 0],
    ]);
    exit;
}

$empresaSolicitada = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$empresaId = mensajeria_resolve_empresa_id($empresaSolicitada !== false ? $empresaSolicitada : null);

if (!$empresaId || $empresaId <= 0) {
    echo json_encode([
        'empresa_id' => null,
        'unread' => ['mensajes' => 0, 'conversaciones' => 0],
    ]);
    exit;
}

$counts = mensajeria_count_unread($conn, $empresaId);

echo json_encode([
    'empresa_id' => $empresaId,
    'unread' => $counts,
]);
