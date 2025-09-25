<?php

session_start();

require_once dirname(__DIR__, 3) . '/Core/login_paths.php';

$context = $_SESSION['app_context'] ?? null;

$_SESSION = [];

if (ini_get('session.use_cookies')) {
    $params = session_get_cookie_params();
    setcookie(session_name(), '', time() - 42000, $params['path'], $params['domain'], $params['secure'], $params['httponly']);
}

session_destroy();

$redirects = [
    'tenant'   => login_path_for_context('tenant'),
    'service'  => login_path_for_context('service'),
    'internal' => login_path_for_context('internal'),
];

$target = $redirects['internal'];

if (is_string($context)) {
    $normalized = strtolower(trim($context));
    if (isset($redirects[$normalized])) {
        $target = $redirects[$normalized];
    }
}

header('Location: ' . $target);
exit;
