<?php
require_once __DIR__ . '/permissions.php';
$permiso = $_GET['permiso'] ?? '';
header('Content-Type: application/json');
if (!$permiso) {
    echo json_encode(['allowed' => false]);
    exit;
}
$allowed = check_permission($permiso);
echo json_encode(['allowed' => $allowed]);
?>
