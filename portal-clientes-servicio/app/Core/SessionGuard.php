<?php

require_once __DIR__ . '/login_paths.php';

session_start();

if (!isset($_SESSION['usuario_id'])) {
    $fetchMode   = $_SERVER['HTTP_SEC_FETCH_MODE'] ?? '';
    $accept      = $_SERVER['HTTP_ACCEPT'] ?? '';
    $contentType = $_SERVER['CONTENT_TYPE'] ?? '';

    $expectsJson = stripos($accept, 'application/json') !== false
        || stripos($contentType, 'application/json') !== false
        || ($fetchMode && $fetchMode !== 'navigate');

    if ($expectsJson) {
        http_response_code(401);
        header('Content-Type: application/json');
        echo json_encode(['error' => 'Sesión no válida']);
        exit;
    }

    $portal = detect_portal_context('internal');
    $loginPath = login_path_for_context($portal);

    header('Location: ' . $loginPath);
    exit;
}
