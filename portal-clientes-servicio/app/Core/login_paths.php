<?php

declare(strict_types=1);

require_once __DIR__ . '/Paths.php';

/**
 * Normaliza y valida un portal permitido.
 */
function normalize_portal_slug(?string $portal): ?string
{
    if (!is_string($portal)) {
        return null;
    }

    $portal = strtolower(trim($portal));

    if ($portal === '') {
        return null;
    }

    $allowed = ['internal', 'tenant', 'service'];
    if (!in_array($portal, $allowed, true)) {
        return null;
    }

    return $portal;
}

/**
 * Intenta detectar el portal desde una ruta solicitada.
 */
function detect_portal_from_path(?string $path): ?string
{
    if (!is_string($path) || $path === '') {
        return null;
    }

    $path = strtolower($path);

    if (strpos($path, '/apps/tenant/') !== false || preg_match('#/apps/tenant$#', $path)) {
        return 'tenant';
    }

    if (strpos($path, '/apps/service/') !== false || preg_match('#/apps/service$#', $path)) {
        return 'service';
    }

    if (strpos($path, '/apps/internal/') !== false || preg_match('#/apps/internal$#', $path)) {
        return 'internal';
    }

    return null;
}

/**
 * Determina el portal solicitado a partir del request y la sesión.
 */
function detect_portal_context(string $default = 'internal'): string
{
    $candidates = [];

    $inputs = ['portal', 'portal_scope', 'portal_slug', 'context', 'app_context'];

    foreach ($inputs as $key) {
        if (isset($_POST[$key])) {
            $candidates[] = $_POST[$key];
        }
        if (isset($_GET[$key])) {
            $candidates[] = $_GET[$key];
        }
    }

    if (!empty($_SERVER['HTTP_REFERER'])) {
        $referer = parse_url((string) $_SERVER['HTTP_REFERER'], PHP_URL_PATH);
        $fromReferer = detect_portal_from_path($referer);
        if ($fromReferer !== null) {
            $candidates[] = $fromReferer;
        }
    }

    if (!empty($_SERVER['REQUEST_URI'])) {
        $fromRequest = detect_portal_from_path($_SERVER['REQUEST_URI']);
        if ($fromRequest !== null) {
            $candidates[] = $fromRequest;
        }
    }

    if (isset($_SESSION['app_context'])) {
        $candidates[] = $_SESSION['app_context'];
    }

    $candidates[] = $default;

    foreach ($candidates as $candidate) {
        $normalized = normalize_portal_slug(is_string($candidate) ? $candidate : null);
        if ($normalized !== null) {
            return $normalized;
        }
    }

    return 'internal';
}

/**
 * Devuelve la ruta absoluta al login correspondiente al portal.
 */
function login_path_for_context(string $context): string
{
    $normalized = normalize_portal_slug($context) ?? 'internal';

    $map = [
        'internal' => 'internal/usuarios/login.html',
        'tenant'   => 'tenant/usuarios/login.html',
        'service'  => 'service/usuarios/login.html',
    ];

    $target = $map[$normalized] ?? $map['internal'];

    return Paths::app($target);
}
