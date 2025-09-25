<?php
session_start();
if (!isset($_SESSION['usuario'])) {
    require_once __DIR__ . '/login_paths.php';

    $portal = detect_portal_context('internal');
    $loginUrl = login_path_for_context($portal);

    header('Location: ' . $loginUrl);
    exit;
}

if (!isset($_SESSION['rol'])) {
    if (isset($_SESSION['role_name']) && is_string($_SESSION['role_name'])) {
        $_SESSION['rol'] = $_SESSION['role_name'];
    } elseif (isset($_SESSION['role_id']) && is_string($_SESSION['role_id'])) {
        $_SESSION['rol'] = $_SESSION['role_id'];
    }
}

require_once __DIR__ . '/permissions.php';

$empresaSesion  = ensure_session_empresa_id();
$empresaSolicitada = requested_empresa_id();
$esSuperAdmin   = session_is_superadmin();

if ($esSuperAdmin) {
    if ($empresaSolicitada !== null && $empresaSolicitada > 0) {
        $_SESSION['empresa_id'] = $empresaSolicitada;
        $empresaSesion = $empresaSolicitada;
    } elseif ($empresaSesion !== null && $empresaSesion > 0) {
        $_SESSION['empresa_id'] = $empresaSesion;
    }
} else {
    if ($empresaSesion === null || $empresaSesion <= 0) {
        http_response_code(403);
        exit('No se encontró una empresa asociada al usuario.');
    }
    if ($empresaSolicitada !== null && $empresaSolicitada !== $empresaSesion) {
        http_response_code(403);
        exit('Acceso denegado a la empresa solicitada.');
    }
}

$role_id = $_SESSION['rol'] ?? $_SESSION['role_id'] ?? '';
?>