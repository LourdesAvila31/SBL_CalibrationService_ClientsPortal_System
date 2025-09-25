<?php

declare(strict_types=1);

require_once __DIR__ . '/ApiTokensHelper.php';

final class ApiGuardException extends RuntimeException
{
    private int $statusCode;

    public function __construct(string $message, int $statusCode)
    {
        parent::__construct($message, $statusCode);
        $this->statusCode = $statusCode;
    }

    public function getStatusCode(): int
    {
        return $this->statusCode;
    }
}

final class ApiGuard
{
    private ApiTokensHelper $tokensHelper;

    public function __construct(ApiTokensHelper $tokensHelper)
    {
        $this->tokensHelper = $tokensHelper;
    }

    /**
     * @param array<string, string> $headers
     * @param array<int, string> $requiredScopes
     *
     * @return array{token: array<mixed>, remaining: int|null}
     */
    public function authorize(array $headers, array $requiredScopes = [], ?int $now = null, ?string $fallbackToken = null): array
    {
        $tokenValue = $this->extractToken($headers, $fallbackToken);
        if ($tokenValue === null) {
            throw new ApiGuardException('Token no proporcionado.', 401);
        }

        $tokenData = $this->tokensHelper->getToken($tokenValue);
        if ($tokenData === null) {
            throw new ApiGuardException('Token inválido.', 401);
        }

        if ($this->tokensHelper->isExpired($tokenData, $now)) {
            throw new ApiGuardException('Token expirado.', 401);
        }

        if (!$this->tokensHelper->hasScopes($tokenData, $requiredScopes)) {
            throw new ApiGuardException('El token no tiene los permisos requeridos.', 403);
        }

        $result = $this->tokensHelper->consume($tokenData, $now);
        if ($result['allowed'] === false) {
            throw new ApiGuardException('Límite de peticiones excedido.', 429);
        }

        return [
            'token' => $result['token'],
            'remaining' => $result['remaining'],
        ];
    }

    /**
     * @param array<string, string> $headers
     */
    private function extractToken(array $headers, ?string $fallbackToken = null): ?string
    {
        $candidates = [];
        foreach ($headers as $name => $value) {
            $normalized = strtolower($name);
            if ($normalized === 'authorization' || $normalized === 'http_authorization') {
                $candidates[] = $value;
            } elseif ($normalized === 'x-api-token' || $normalized === 'x_api_token') {
                $candidates[] = $value;
            }
        }

        foreach ($candidates as $candidate) {
            if (!is_string($candidate) || $candidate === '') {
                continue;
            }

            if (stripos($candidate, 'Bearer ') === 0) {
                $token = trim(substr($candidate, 7));
                if ($token !== '') {
                    return $token;
                }
            }

            $candidate = trim($candidate);
            if ($candidate !== '') {
                return $candidate;
            }
        }

        if ($fallbackToken !== null && $fallbackToken !== '') {
            return $fallbackToken;
        }

        return null;
    }
}
