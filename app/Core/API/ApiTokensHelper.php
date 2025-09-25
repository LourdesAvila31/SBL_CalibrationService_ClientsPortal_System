<?php

declare(strict_types=1);

final class ApiTokensHelper
{
    private string $storageFile;

    public function __construct(?string $storageFile = null)
    {
        $baseDir = dirname(__DIR__, 2) . '/storage';
        $this->storageFile = $storageFile ?? $baseDir . '/api_tokens.json';
        $dir = dirname($this->storageFile);
        if (!is_dir($dir)) {
            if (!mkdir($dir, 0777, true) && !is_dir($dir)) {
                throw new RuntimeException(sprintf('No se pudo crear el directorio de almacenamiento "%s".', $dir));
            }
        }

        if (!file_exists($this->storageFile)) {
            $this->saveTokens([]);
        }
    }

    /**
     * @return array<string, array<mixed>>
     */
    public function loadTokens(): array
    {
        if (!file_exists($this->storageFile)) {
            return [];
        }

        $contents = file_get_contents($this->storageFile);
        if ($contents === false || $contents === '') {
            return [];
        }

        try {
            $decoded = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new RuntimeException('El archivo de tokens contiene JSON inválido: ' . $e->getMessage(), 0, $e);
        }

        if (!is_array($decoded)) {
            return [];
        }

        $tokens = [];
        foreach ($decoded as $entry) {
            if (!is_array($entry)) {
                continue;
            }
            $token = $entry['token'] ?? null;
            if (is_string($token) && $token !== '') {
                $tokens[$token] = $entry;
            }
        }

        return $tokens;
    }

    /**
     * @param array<string, array<mixed>> $tokens
     */
    private function saveTokens(array $tokens): void
    {
        $payload = array_values($tokens);
        $encoded = json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        if ($encoded === false) {
            throw new RuntimeException('No se pudo serializar el listado de tokens.');
        }

        $result = file_put_contents($this->storageFile, $encoded . PHP_EOL, LOCK_EX);
        if ($result === false) {
            throw new RuntimeException('No se pudo escribir el archivo de tokens.');
        }
    }

    /**
     * Genera y persiste un token nuevo.
     *
     * @param array<int, string> $scopes
     */
    public function issueToken(string $clientId, array $scopes, int $ttlSeconds, int $limit, int $windowSeconds = 60): array
    {
        $token = bin2hex(random_bytes(16));
        $issuedAt = time();
        $normalizedScopes = array_values(array_unique(array_map(static fn ($scope) => (string) $scope, $scopes)));

        $tokenData = [
            'token' => $token,
            'client_id' => $clientId,
            'scopes' => $normalizedScopes,
            'created_at' => $issuedAt,
            'expires_at' => $issuedAt + $ttlSeconds,
            'rate_limit' => [
                'limit' => max(0, $limit),
                'window' => max(1, $windowSeconds),
                'usage' => [],
            ],
        ];

        $tokens = $this->loadTokens();
        $tokens[$token] = $tokenData;
        $this->saveTokens($tokens);

        return $tokenData;
    }

    public function persistToken(array $tokenData): void
    {
        $token = $tokenData['token'] ?? null;
        if (!is_string($token) || $token === '') {
            throw new InvalidArgumentException('El token debe incluir la clave "token".');
        }

        $tokens = $this->loadTokens();
        $tokens[$token] = $tokenData;
        $this->saveTokens($tokens);
    }

    public function deleteToken(string $token): void
    {
        $tokens = $this->loadTokens();
        if (isset($tokens[$token])) {
            unset($tokens[$token]);
            $this->saveTokens($tokens);
        }
    }

    public function getToken(string $token): ?array
    {
        $tokens = $this->loadTokens();
        $data = $tokens[$token] ?? null;
        return is_array($data) ? $data : null;
    }

    public function isExpired(array $tokenData, ?int $now = null): bool
    {
        $now = $now ?? time();
        $expiresAt = isset($tokenData['expires_at']) ? (int) $tokenData['expires_at'] : 0;
        return $expiresAt > 0 ? $expiresAt <= $now : false;
    }

    /**
     * @param array<int, string> $requiredScopes
     */
    public function hasScopes(array $tokenData, array $requiredScopes): bool
    {
        if ($requiredScopes === []) {
            return true;
        }

        $available = $tokenData['scopes'] ?? [];
        if (!is_array($available)) {
            return false;
        }

        $available = array_map(static fn ($scope) => (string) $scope, $available);
        foreach ($requiredScopes as $scope) {
            if (!in_array($scope, $available, true)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Registra el uso del token respetando la ventana de rate limit.
     *
     * @return array{token: array<mixed>, allowed: bool, remaining: int|null}
     */
    public function consume(array $tokenData, ?int $now = null): array
    {
        $now = $now ?? time();
        $rateLimit = $tokenData['rate_limit'] ?? [];
        $limit = isset($rateLimit['limit']) ? (int) $rateLimit['limit'] : 0;
        $window = isset($rateLimit['window']) ? max(1, (int) $rateLimit['window']) : 60;
        $usage = [];
        if (isset($rateLimit['usage']) && is_array($rateLimit['usage'])) {
            foreach ($rateLimit['usage'] as $timestamp) {
                $timestamp = (int) $timestamp;
                if ($timestamp > 0 && ($now - $timestamp) < $window) {
                    $usage[] = $timestamp;
                }
            }
        }

        if ($limit > 0 && count($usage) >= $limit) {
            $tokenData['rate_limit']['usage'] = $usage;
            $this->persistToken($tokenData);
            return [
                'token' => $tokenData,
                'allowed' => false,
                'remaining' => 0,
            ];
        }

        $usage[] = $now;
        $tokenData['rate_limit']['usage'] = $usage;
        $this->persistToken($tokenData);

        $remaining = $limit > 0 ? max(0, $limit - count($usage)) : null;

        return [
            'token' => $tokenData,
            'allowed' => true,
            'remaining' => $remaining,
        ];
    }
}
