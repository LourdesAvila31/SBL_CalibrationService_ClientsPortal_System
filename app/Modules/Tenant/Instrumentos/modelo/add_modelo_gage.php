<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo 'error';
    exit;
}
// Inserta un nuevo modelo para los instrumentos
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo 'error';
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';

$nombre  = trim($_POST['nombre'] ?? '');
$marcaId = filter_input(INPUT_POST, 'marca_id', FILTER_VALIDATE_INT);
if ($nombre !== '') {
    if ($marcaId) {
        $stmt = $conn->prepare('INSERT INTO modelos (nombre, marca_id) VALUES (?, ?)');
        $stmt->bind_param('si', $nombre, $marcaId);
    } else {
        $stmt = $conn->prepare('INSERT INTO modelos (nombre, marca_id) VALUES (?, NULL)');
        $stmt->bind_param('s', $nombre);
    }
    $ok = $stmt->execute();
    $stmt->close();
    echo $ok ? 'ok' : 'error';
} else {
    echo 'error';
}
$conn->close();
?>