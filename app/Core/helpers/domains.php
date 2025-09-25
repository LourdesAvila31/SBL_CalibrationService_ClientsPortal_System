<?php

declare(strict_types=1);

/**
 * Loads the configured portal domain map from supported sources.
 *
 * The helper accepts PHP or JSON configuration files stored under the
 * application's storage directory as well as an optional environment
 * variable. The resulting map is normalized to lowercase keys and values.
 *
 * @return array<string, string>
 */
function portal_domain_map(): array
{
    static $cache;

    if ($cache !== null) {
        return $cache;
    }

    $cache = [];
    $basePath = dirname(__DIR__, 2);
    $candidates = [
        $basePath . '/storage/portal_domains.php',
        $basePath . '/storage/portal_domains.json',
    ];

    foreach ($candidates as $candidate) {
        if (!is_file($candidate)) {
            continue;
        }

        $data = null;
        if (substr($candidate, -4) === '.php') {
            $data = include $candidate;
        } else {
            $contents = @file_get_contents($candidate);
            if ($contents !== false) {
                $data = json_decode($contents, true);
            }
        }

        if (is_array($data)) {
            $cache = normalize_portal_domain_map($data);
            if (!empty($cache)) {
                return $cache;
            }
        }
    }

    $env = getenv('PORTAL_DOMAIN_MAP');
    if (is_string($env) && $env !== '') {
        $decoded = json_decode($env, true);
        if (is_array($decoded)) {
            $map = normalize_portal_domain_map($decoded);
            if (!empty($map)) {
                $cache = $map;
                return $cache;
            }
        }

        $pairs = array_map('trim', explode(',', $env));
        $map = [];
        foreach ($pairs as $pair) {
            if ($pair === '') {
                continue;
            }
            $parts = array_map('trim', explode(':', $pair, 2));
            if (count($parts) !== 2) {
                continue;
            }
            $map[$parts[0]] = $parts[1];
        }
        $map = normalize_portal_domain_map($map);
        if (!empty($map)) {
            $cache = $map;
            return $cache;
        }
    }

    return $cache;
}

/**
 * Normalizes a raw domain=>slug map ensuring lowercase keys and values.
 *
 * @param array<mixed> $raw
 * @return array<string, string>
 */
function normalize_portal_domain_map(array $raw): array
{
    $normalized = [];

    foreach ($raw as $key => $value) {
        if (is_array($value)) {
            $domain = $value['domain'] ?? $value['email_domain'] ?? null;
            $slug   = $value['slug'] ?? $value['portal'] ?? null;
        } else {
            $domain = $key;
            $slug   = $value;
        }

        if (!is_string($domain) || !is_string($slug)) {
            continue;
        }

        $domain = strtolower(trim($domain));
        $slug   = strtolower(trim($slug));

        if ($domain === '' || $slug === '') {
            continue;
        }

        $normalized[$domain] = $slug;
    }

    return $normalized;
}

/**
 * Attempts to resolve the target portal slug for a given email using
 * the configured domain map.
 */
function resolve_portal_slug_from_email(string $email, ?array $map = null): ?string
{
    if ($map === null) {
        $map = portal_domain_map();
    } else {
        $map = normalize_portal_domain_map($map);
    }

    if (empty($map)) {
        return null;
    }

    $email = strtolower(trim($email));
    if ($email === '' || strpos($email, '@') === false) {
        return null;
    }

    $domain = substr(strrchr($email, '@'), 1);
    if ($domain === false) {
        return null;
    }
    $domain = strtolower(trim($domain));
    if ($domain === '') {
        return null;
    }

    if (isset($map[$domain])) {
        return $map[$domain];
    }

    $patterns = [];
    $segments = explode('.', $domain);
    while (count($segments) > 1) {
        array_shift($segments);
        $suffix = implode('.', $segments);
        $patterns[] = '*.' . $suffix;
        $patterns[] = '.' . $suffix;
    }

    foreach ($patterns as $pattern) {
        if (isset($map[$pattern])) {
            return $map[$pattern];
        }
    }

    foreach ($map as $pattern => $slug) {
        if (strpos($pattern, '*') === false) {
            continue;
        }

        $regex = '#^' . str_replace(['\\*', '\\?'], ['.*', '.'], preg_quote($pattern, '#')) . '$#i';
        if (preg_match($regex, $domain) === 1) {
            return $slug;
        }
    }

    return null;
}
