<?php

class Paths
{
    private static ?string $baseUrl = null;

    public static function baseUrl(): string
    {
        if (self::$baseUrl !== null) {
            return self::$baseUrl;
        }

        $scriptName = $_SERVER['SCRIPT_NAME'] ?? '';
        if ($scriptName === '') {
            self::$baseUrl = '/';
            return self::$baseUrl;
        }

        $scriptName = str_replace('\\', '/', $scriptName);

        $markers = [
            '/public/' => function (string $name, int $position): string {
                return substr($name, 0, $position + strlen('/public'));
            },
            '/backend/' => function (string $name, int $position): string {
                return substr($name, 0, $position);
            },
            '/apps/' => function (string $name, int $position): string {
                return substr($name, 0, $position);
            },
            '/assets/' => function (string $name, int $position): string {
                return substr($name, 0, $position);
            },
        ];

        $base = null;
        foreach ($markers as $marker => $resolver) {
            $markerPosition = strpos($scriptName, $marker);
            if ($markerPosition !== false) {
                $base = $resolver($scriptName, $markerPosition);
                break;
            }
        }

        if ($base === null) {
            $base = str_replace('\\', '/', dirname($scriptName));
        }

        $base = rtrim($base, '/');

        if ($base === '') {
            $base = '/';
        } elseif ($base[0] !== '/') {
            $base = '/' . $base;
        }

        self::$baseUrl = $base;
        return self::$baseUrl;
    }

    private static function normalizedBase(): string
    {
        $base = self::baseUrl();
        return $base === '/' ? '' : $base;
    }

    private static function join(string $base, string $path): string
    {
        $base = rtrim($base, '/');
        $path = ltrim($path, '/');

        if ($base === '') {
            return '/' . $path;
        }

        return $base . '/' . $path;
    }

    public static function asset(string $path): string
    {
        return self::join(self::normalizedBase(), 'assets/' . ltrim($path, '/'));
    }

    public static function app(string $path): string
    {
        return self::join(self::normalizedBase(), 'apps/' . ltrim($path, '/'));
    }
}

if (!defined('BASE_URL')) {
    define('BASE_URL', Paths::baseUrl());
}
