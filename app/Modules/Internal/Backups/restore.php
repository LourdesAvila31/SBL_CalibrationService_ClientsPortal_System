<?php
$runFromCLI = (php_sapi_name() === 'cli');
if (!$runFromCLI) {
    require_once dirname(__DIR__, 3) . '/Core/permissions.php';
    header('Content-Type: application/json');
    if (!check_permission('backups_restore')) {
        http_response_code(403);
        echo json_encode(['error' => 'Acceso denegado']);
        exit;
    }
    session_start();
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';

$dir = realpath(__DIR__ . '/files');
$file = '';
if ($runFromCLI) {
    $file = $argv[1] ?? '';
} else {
    $file = $_POST['file'] ?? '';
}
$target = $dir && $file ? realpath($dir . '/' . basename($file)) : false;

if (!$target || strpos($target, $dir) !== 0 || !is_file($target)) {
    if (!$runFromCLI) {
        http_response_code(400);
        echo json_encode(['error' => 'Archivo inválido']);
    } else {
        echo "Archivo inválido\n";
    }
    exit;
}

$passPart = $password ? "-p{$password}" : '';
$command = "mysql -h {$servername} -u {$username} {$passPart} {$dbname} < " . escapeshellarg($target);
exec($command, $output, $retval);

if ($retval === 0) {
    if ($runFromCLI) {
        $name = 'CLI';
        $email = null;
    } else {
        $nombre = trim((string) ($_SESSION['nombre'] ?? ''));
        $apellidos = trim((string) ($_SESSION['apellidos'] ?? ''));
        $name = trim($nombre . ' ' . $apellidos);
        $email = $_SESSION['usuario'] ?? null;
    }
    $payload = [
        'seccion' => 'alta',
        'valor_nuevo' => 'Restauró respaldo: ' . basename($target),
        'usuario_correo' => $email,
    ];
    if ($runFromCLI) {
        $payload['segmento_actor'] = 'Sistemas';
    }
    log_activity($name, $payload);
    if ($runFromCLI) {
        echo "Respaldo restaurado desde {$target}\n";
    } else {
        echo json_encode(['ok' => true]);
    }
} else {
    if (!$runFromCLI) {
        http_response_code(500);
        echo json_encode(['error' => 'No se pudo restaurar el respaldo']);
    } else {
        echo "Error al restaurar respaldo\n";
    }
}

if (isset($conn) && $conn instanceof mysqli) {
    $conn->close();
}
?>
