<?php
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('backups_view')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$dir = __DIR__ . '/files';
$files = [];
if (is_dir($dir)) {
    foreach (glob($dir . '/*.sql') as $file) {
        $files[] = basename($file);
    }
}

echo json_encode($files);
?>
