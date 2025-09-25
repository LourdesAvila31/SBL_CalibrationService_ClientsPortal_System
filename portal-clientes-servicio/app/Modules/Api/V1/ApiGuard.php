<?php

require_once dirname(__DIR__, 3) . '/Core/db_config.php';

class ApiGuard
{
    /**
     * Obtiene y valida el identificador de empresa asociado a la petición actual.
     */
    public static function requireEmpresaId(mysqli $db): int
    {
        $empresaId = self::resolveEmpresaId($db);
        if ($empresaId === null) {
            self::deny('Empresa no especificada o credenciales inválidas.', 401);
        }

        $stmt = $db->prepare('SELECT id FROM empresas WHERE id = ? LIMIT 1');
        if (!$stmt) {
            error_log('ApiGuard: no fue posible preparar la validación de empresa.');
            self::deny('No fue posible validar la empresa.', 500);
        }

        $stmt->bind_param('i', $empresaId);
        $stmt->execute();
        $stmt->bind_result($foundId);
        $found = $stmt->fetch();
        $stmt->close();

        if (!$found || !$foundId) {
            self::deny('Empresa no encontrada.', 404);
        }

        return (int) $empresaId;
    }

    /**
     * Intenta resolver el identificador de empresa a partir del encabezado Authorization,
     * encabezados personalizados o parámetros de la URL.
     */
    public static function resolveEmpresaId(mysqli $db): ?int
    {
        $headers = self::getRequestHeaders();

        $empresaHeader = $headers['x-empresa-id'] ?? $headers['x-company-id'] ?? null;
        if ($empresaHeader !== null && $empresaHeader !== '') {
            $empresaId = filter_var($empresaHeader, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
            if ($empresaId !== false) {
                return (int) $empresaId;
            }
        }

        $authHeader = $headers['authorization'] ?? null;
        if ($authHeader) {
            $token = self::extractBearerToken($authHeader);
            if ($token !== null) {
                $empresaId = self::lookupEmpresaByToken($db, $token);
                if ($empresaId !== null) {
                    return $empresaId;
                }
            }
        }

        $tokenHeader = $headers['x-api-token'] ?? null;
        if ($tokenHeader) {
            $empresaId = self::lookupEmpresaByToken($db, $tokenHeader);
            if ($empresaId !== null) {
                return $empresaId;
            }
        }

        $empresaQuery = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
        if ($empresaQuery) {
            return (int) $empresaQuery;
        }

        $empresaBody = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
        if ($empresaBody) {
            return (int) $empresaBody;
        }

        return null;
    }

    public static function deny(string $message, int $status = 401): void
    {
        http_response_code($status);
        header('Content-Type: application/json');
        echo json_encode(['error' => $message], JSON_UNESCAPED_UNICODE);
        exit;
    }

    /**
     * @return array<string,string>
     */
    private static function getRequestHeaders(): array
    {
        if (function_exists('getallheaders')) {
            $headers = getallheaders();
            $normalized = [];
            foreach ($headers as $key => $value) {
                $normalized[strtolower((string) $key)] = (string) $value;
            }
            return $normalized;
        }

        $headers = [];
        foreach ($_SERVER as $key => $value) {
            if (strpos($key, 'HTTP_') === 0) {
                $headerName = strtolower(str_replace('_', '-', substr($key, 5)));
                $headers[$headerName] = (string) $value;
            }
        }
        return $headers;
    }

    private static function extractBearerToken(string $header): ?string
    {
        if (stripos($header, 'Bearer ') === 0) {
            $token = trim(substr($header, 7));
            return $token !== '' ? $token : null;
        }
        return null;
    }

    private static function lookupEmpresaByToken(mysqli $db, string $token): ?int
    {
        $stmt = $db->prepare('SELECT empresa_id FROM api_tokens WHERE token = ? AND activo = 1 LIMIT 1');
        if (!$stmt) {
            error_log('ApiGuard: no se pudo preparar la búsqueda de token.');
            return null;
        }

        $stmt->bind_param('s', $token);
        $stmt->execute();
        $stmt->bind_result($empresaId);
        $found = $stmt->fetch();
        $stmt->close();

        if ($found && $empresaId) {
            return (int) $empresaId;
        }

        return null;
    }
}
