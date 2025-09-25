<?php
$runFromCLI = (php_sapi_name() === 'cli');
if (!$runFromCLI) {
    require_once dirname(__DIR__, 3) . '/Core/permissions.php';
    header('Content-Type: application/json');
    if (!check_permission('backups_create')) {
        http_response_code(403);
        echo json_encode(['error' => 'Acceso denegado']);
        exit;
    }
    session_start();
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once __DIR__ . '/helpers.php';

$dir = __DIR__ . '/files';
$result = create_backup($conn, $dir);

if ($result['success']) {
    if ($runFromCLI) {
        $name = 'CLI';
        $email = null;
    } else {
        $nombre = trim((string) ($_SESSION['nombre'] ?? ''));
        $apellidos = trim((string) ($_SESSION['apellidos'] ?? ''));
        $name = trim($nombre . ' ' . $apellidos);
        $email = $_SESSION['usuario'] ?? null;
    }
    $filename = $result['filename'];
    $payload = [
        'seccion' => 'media',
        'valor_nuevo' => "Creó respaldo: {$filename}",
        'usuario_correo' => $email,
    ];
    if ($runFromCLI) {
        $payload['segmento_actor'] = 'Sistemas';
    }
    log_activity($name, $payload);
    if ($runFromCLI) {
        echo "Respaldo creado en {$result['filepath']}\n";
    } else {
        echo json_encode(['ok' => true, 'file' => $filename]);
    }
} else {
    if (!$runFromCLI) {
        http_response_code(500);
        echo json_encode(['error' => 'No se pudo crear el respaldo']);
    } else {
        $error = $result['error'] ?? 'Error desconocido';
        echo "Error al crear respaldo: {$error}\n";
    }
}

if (isset($conn) && $conn instanceof mysqli) {
    $conn->close();
}
?>
