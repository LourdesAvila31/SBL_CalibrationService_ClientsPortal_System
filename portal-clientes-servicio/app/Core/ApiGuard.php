<?php
declare(strict_types=1);

require_once __DIR__ . '/helpers/api_tokens.php';

class ApiGuard
{
    /** @var array<string, mixed>|null */
    private $tokenRow;

    /** @var string[] */
    private $scopes = [];

    /** @var int|null */
    private $empresaId;

    /** @var array<string, mixed> */
    private $rateLimitInfo = [];

    /** @var string|null */
    private $providedToken;

    public function __construct()
    {
        $this->authenticate();
    }

    /**
     * Devuelve el identificador del token autenticado.
     */
    public function tokenId(): ?int
    {
        return $this->tokenRow['id'] ?? null;
    }

    /**
     * Devuelve el identificador de la empresa asociada al token.
     */
    public function empresaId(): ?int
    {
        return $this->empresaId;
    }

    /**
     * Devuelve los scopes asociados al token.
     *
     * @return string[]
     */
    public function scopes(): array
    {
        return $this->scopes;
    }

    /**
     * Devuelve el token proporcionado originalmente por el cliente.
     */
    public function tokenSecret(): ?string
    {
        return $this->providedToken;
    }

    /**
     * Información completa del registro del token.
     *
     * @return array<string, mixed>
     */
    public function tokenRecord(): array
    {
        return $this->tokenRow ?? [];
    }

    /**
     * Valida que el token incluya todos los scopes requeridos.
     *
     * @param string[] $scopes
     */
    public function requireScopes(array $scopes): void
    {
        $missing = array_values(array_diff($scopes, $this->scopes));

        if (!empty($missing)) {
            $this->deny(403, 'El token no posee los scopes requeridos.', [
                'missing_scopes' => $missing,
            ]);
        }
    }

    /**
     * Información adicional sobre los límites aplicados en la petición actual.
     */
    public function rateLimitInfo(): array
    {
        return $this->rateLimitInfo;
    }

    /**
     * Obtiene el valor del header Authorization o X-API-Key.
     */
    private function extractTokenFromHeaders(): ?string
    {
        $authorization = $_SERVER['HTTP_AUTHORIZATION'] ?? '';
        $apiKey        = $_SERVER['HTTP_X_API_KEY'] ?? '';

        if (is_string($authorization) && stripos($authorization, 'Bearer ') === 0) {
            $token = trim(substr($authorization, 7));
            return $token !== '' ? $token : null;
        }

        if (is_string($apiKey) && $apiKey !== '') {
            return trim($apiKey);
        }

        return null;
    }

    private function authenticate(): void
    {
        $token = $this->extractTokenFromHeaders();

        if (!$token) {
            $this->deny(401, 'Cabecera de autenticación ausente.');
        }

        $tokenRow = api_token_find_by_secret($token);

        if (!$tokenRow) {
            $this->deny(401, 'Token no reconocido.');
        }

        if (!empty($tokenRow['revoked_at'])) {
            $this->deny(401, 'Token revocado.');
        }

        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        if (!empty($tokenRow['expires_at'])) {
            $expiresAt = api_token_parse_datetime($tokenRow['expires_at']);

            if (!$expiresAt || $expiresAt <= $now) {
                $this->deny(401, 'Token expirado.');
            }
        }

        $empresaId = isset($tokenRow['empresa_id']) ? (int) $tokenRow['empresa_id'] : null;
        if (!$empresaId) {
            $this->deny(403, 'El token no está asociado a ninguna empresa.');
        }

        $scopes = [];
        if (isset($tokenRow['scopes'])) {
            if (is_array($tokenRow['scopes'])) {
                $scopes = $tokenRow['scopes'];
            } elseif (is_string($tokenRow['scopes']) && $tokenRow['scopes'] !== '') {
                $decoded = json_decode($tokenRow['scopes'], true);
                if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                    $scopes = $decoded;
                } else {
                    $scopes = array_map('trim', explode(',', $tokenRow['scopes']));
                }
            }
        }

        $scopes = array_values(array_filter(array_unique($scopes), static function ($value) {
            return $value !== '' && $value !== null;
        }));

        $usage = api_token_register_usage((int) $tokenRow['id'], $now);

        if (!$usage['allowed']) {
            if ($usage['retry_after']) {
                header('Retry-After: ' . (int) $usage['retry_after']);
            }
            $this->deny(429, 'Límite de peticiones alcanzado para el token.', [
                'retry_after' => $usage['retry_after'],
            ]);
        }

        $empresa = api_empresa_register_usage($empresaId, $now);

        if (!$empresa['allowed']) {
            if ($empresa['retry_after']) {
                header('Retry-After: ' . (int) $empresa['retry_after']);
            }
            $this->deny(429, 'Límite de peticiones alcanzado para la empresa.', [
                'retry_after' => $empresa['retry_after'],
            ]);
        }

        if ($usage['remaining'] !== null) {
            header('X-RateLimit-Remaining-Token: ' . max(0, (int) $usage['remaining']));
        }

        if ($empresa['remaining'] !== null) {
            header('X-RateLimit-Remaining-Empresa: ' . max(0, (int) $empresa['remaining']));
        }

        $this->providedToken = $token;
        $this->tokenRow      = $tokenRow;
        $this->scopes        = $scopes;
        $this->empresaId     = $empresaId;
        $this->rateLimitInfo = [
            'token'   => $usage,
            'empresa' => $empresa,
        ];
    }

    private function deny(int $status, string $message, array $extra = []): void
    {
        http_response_code($status);
        header('Content-Type: application/json');

        $payload = array_merge(['error' => $message], $extra);
        echo json_encode($payload, JSON_UNESCAPED_UNICODE);
        exit;
    }
}

?>
