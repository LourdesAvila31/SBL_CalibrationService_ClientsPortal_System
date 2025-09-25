<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}
// Conexión reutilizable a la base de datos
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo 'error';
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';

$nombre = trim($_POST['nombre'] ?? '');
if ($nombre !== '') {
    $stmt = $conn->prepare('INSERT INTO catalogo_instrumentos (nombre) VALUES (?)');
    $stmt->bind_param('s', $nombre);
    $ok = $stmt->execute();
    $stmt->close();
    echo $ok ? 'ok' : 'error';
} else {
    echo 'error';
}
$conn->close();
?>