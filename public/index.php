<?php

require_once __DIR__ . '/../app/Core/Paths.php';
require_once __DIR__ . '/../app/Core/permissions.php';

session_start();

$scopes = ['internal', 'tenant', 'service'];

$requestedScope = $_GET['portal'] ?? $_GET['app'] ?? null;
if (is_string($requestedScope)) {
    $requestedScope = strtolower(trim($requestedScope));
    if (in_array($requestedScope, $scopes, true)) {
        if ($requestedScope === 'service' && !can_access_service_module()) {
            // Ignorar peticiones no autorizadas al contexto de servicio.
        } else {
            if (!session_set_portal($requestedScope)) {
                $_SESSION['portal_slug'] = $requestedScope;
                $_SESSION['app_context'] = $requestedScope;
                unset($_SESSION['portal_id']);
            }
        }
    }
}

$requestPath = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH) ?? '/';
$base = rtrim(Paths::baseUrl(), '/');
if ($base !== '' && $base !== '/' && strpos($requestPath, $base) === 0) {
    $requestPath = substr($requestPath, strlen($base));
}
$requestPath = '/' . ltrim($requestPath, '/');

if (PHP_SAPI === 'cli-server' && preg_match('#^/backend/api/#', $requestPath)) {
    return false;
}

if ($requestPath === '/index.php') {
    $requestPath = '/';
}

if (preg_match('#^/(internal|tenant|service)(/.*)?$#', $requestPath, $matches)) {
    $scope = $matches[1];
    $resource = ltrim($matches[2] ?? '', '/');
    if ($resource === '') {
        $resource = 'index.html';
    }
  
    if ($scope === 'service' && !can_access_service_module()) {
        $scope = 'internal';
        $resource = 'index.html';
    }

    $target = Paths::app($scope . '/' . $resource);
    header('Location: ' . $target);
    exit;
}

$context = resolve_app_context($scopes);

if ($requestPath === '/' || $requestPath === '') {
    if (isset($_SESSION['usuario_id'])) {
        if ($context === 'service' && can_access_service_module()) {
            $target = 'service/index.html';
        } else {
            $target = $context === 'tenant' ? 'tenant/index.html' : 'internal/index.html';
        }
    } else {
        $target = $context === 'tenant'
            ? 'tenant/usuarios/login.html'
            : 'internal/usuarios/login.html';
    }

    header('Location: ' . Paths::app($target));
    exit;
}

if ($requestPath === '/internal' || $requestPath === '/tenant') {
    $target = $context === 'tenant' ? 'tenant/index.html' : 'internal/index.html';
    header('Location: ' . Paths::app($target));
    exit;
}

if ($requestPath === '/service') {
    $target = can_access_service_module()
        ? 'service/index.html'
        : 'internal/index.html';
    header('Location: ' . Paths::app($target));
    exit;
}

if ($context === 'service' && can_access_service_module()) {
    header('Location: ' . Paths::app('service/index.html'));
    exit;
}

header('Location: ' . Paths::app($context === 'tenant' ? 'tenant/index.html' : 'internal/index.html'));
exit;

function resolve_app_context(array $scopes): string
{
    $portal = session_portal_slug();

    if ($portal === 'service' && !can_access_service_module()) {
        $portal = 'internal';
    }

    if (!in_array($portal, $scopes, true)) {
        $roleAlias = session_role_alias();
        if ($roleAlias === 'cliente' && in_array('tenant', $scopes, true)) {
            $portal = 'tenant';
        } elseif ($roleAlias === 'sistemas' && in_array('service', $scopes, true) && can_access_service_module()) {
            $portal = 'service';
        } else {
            $portal = in_array('internal', $scopes, true) ? 'internal' : ($scopes[0] ?? 'internal');
        }
    }

    $_SESSION['portal_slug'] = $portal;
    $_SESSION['app_context'] = $portal;

    return $portal;
}

function can_access_service_module(): bool
{
    if (!isset($_SESSION['usuario_id'])) {
        return false;
    }

    return check_permission('clientes_gestionar');
}
