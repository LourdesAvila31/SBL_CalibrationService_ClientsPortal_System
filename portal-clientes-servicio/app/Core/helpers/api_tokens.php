<?php
declare(strict_types=1);

require_once __DIR__ . '/../db_config.php';

/**
 * Genera un token aleatorio en formato hexadecimal.
 */
function api_token_generate_secret(int $bytes = 32): string
{
    return bin2hex(random_bytes($bytes));
}

/**
 * Calcula el hash canónico utilizado para almacenar tokens.
 */
function api_token_hash(string $token): string
{
    return hash('sha256', $token);
}

/**
 * Intenta crear un objeto DateTimeImmutable en UTC a partir de un valor.
 */
function api_token_parse_datetime($value): ?DateTimeImmutable
{
    if (!$value) {
        return null;
    }

    try {
        if ($value instanceof DateTimeInterface) {
            return (new DateTimeImmutable('@' . $value->getTimestamp()))->setTimezone(new DateTimeZone('UTC'));
        }

        if (is_numeric($value)) {
            return (new DateTimeImmutable('@' . (int) $value))->setTimezone(new DateTimeZone('UTC'));
        }

        return (new DateTimeImmutable((string) $value, new DateTimeZone('UTC')))->setTimezone(new DateTimeZone('UTC'));
    } catch (Exception $e) {
        return null;
    }
}

/**
 * Obtiene el listado de columnas de la tabla api_tokens.
 *
 * @return array<string, bool>
 */
function api_token_table_columns(): array
{
    static $columns = null;

    if ($columns !== null) {
        return $columns;
    }

    $conn = DatabaseManager::getConnection();
    $result = $conn->query('SHOW COLUMNS FROM api_tokens');

    if (!$result) {
        throw new RuntimeException('No se pudieron obtener las columnas de la tabla api_tokens: ' . $conn->error);
    }

    $columns = [];
    while ($row = $result->fetch_assoc()) {
        $columns[$row['Field']] = true;
    }

    return $columns;
}

/**
 * Determina el tipo de parámetro para bind_param de mysqli.
 *
 * @param mixed $value
 */
function api_token_param_type($value): string
{
    if (is_int($value)) {
        return 'i';
    }

    if (is_float($value)) {
        return 'd';
    }

    return 's';
}

/**
 * Ejecuta una sentencia preparada manejando errores de forma uniforme.
 *
 * @param array<int, mixed>|array<int, array{0:mixed,1:string}> $params
 */
function api_token_execute(mysqli $conn, string $sql, array $params = []): mysqli_stmt
{
    $stmt = $conn->prepare($sql);

    if (!$stmt) {
        throw new RuntimeException('No se pudo preparar la consulta: ' . $conn->error);
    }

    if (!empty($params)) {
        $types = '';
        $values = [];

        foreach ($params as $param) {
            if (is_array($param)) {
                [$value, $type] = $param;
            } else {
                $value = $param;
                $type = api_token_param_type($value);
            }

            $types   .= $type;
            $values[] = $value;
        }

        $bindParams = [];
        $bindParams[] = &$types;

        foreach ($values as $index => $value) {
            $bindParams[] = &$values[$index];
        }

        call_user_func_array([$stmt, 'bind_param'], $bindParams);
    }

    if (!$stmt->execute()) {
        throw new RuntimeException('No se pudo ejecutar la consulta: ' . $stmt->error);
    }

    return $stmt;
}

/**
 * Crea un nuevo token de API para una empresa determinada.
 *
 * @param int $empresaId Identificador de la empresa propietaria del token.
 * @param string[] $scopes Listado de scopes asignados al token.
 * @param array $options Opciones adicionales como expiración o límites.
 *
 * @return array{id:int, token:string, hash:string}
 */
function api_token_create(int $empresaId, array $scopes = [], array $options = []): array
{
    $conn = DatabaseManager::getConnection();
    $columns = api_token_table_columns();

    $token     = api_token_generate_secret();
    $tokenHash = api_token_hash($token);

    $scopes = array_values(array_unique($scopes));
    sort($scopes);

    $scopePayload = json_encode($scopes, JSON_UNESCAPED_UNICODE);

    $expiresAt = $options['expires_at'] ?? null;
    if ($expiresAt instanceof DateTimeInterface || (is_string($expiresAt) && $expiresAt !== '') || is_numeric($expiresAt)) {
        $date = api_token_parse_datetime($expiresAt);
        $expiresAt = $date ? $date->format('Y-m-d H:i:s') : null;
    } else {
        $expiresAt = null;
    }

    $rateLimit       = $options['rate_limit'] ?? null;
    $rateLimitWindow = $options['rate_limit_window'] ?? 60;

    $rateLimit = $rateLimit !== null ? (int) $rateLimit : null;
    $rateLimitWindow = $rateLimitWindow !== null ? (int) $rateLimitWindow : null;

    $now    = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $nowStr = $now->format('Y-m-d H:i:s');

    $resetAt = null;
    if ($rateLimit && $rateLimit > 0 && $rateLimitWindow && $rateLimitWindow > 0) {
        $resetAt = $now->add(new DateInterval('PT' . max(1, $rateLimitWindow) . 'S'))->format('Y-m-d H:i:s');
    }

    $fields = [];
    $params = [];

    $appendField = function (string $field, $value) use (&$fields, &$params) {
        $fields[] = $field;
        $params[] = $value;
    };

    $appendField('empresa_id', [$empresaId, 'i']);

    if (isset($columns['token_hash'])) {
        $appendField('token_hash', $tokenHash);
    } elseif (isset($columns['token'])) {
        $appendField('token', $tokenHash);
    }

    if (isset($columns['token_preview'])) {
        $appendField('token_preview', substr($token, 0, 8));
    }

    if (isset($columns['scopes'])) {
        $appendField('scopes', $scopePayload);
    }

    if (isset($columns['expires_at'])) {
        $appendField('expires_at', $expiresAt);
    }

    if (isset($columns['revoked_at'])) {
        $appendField('revoked_at', null);
    }

    if (isset($columns['last_used_at'])) {
        $appendField('last_used_at', null);
    }

    if (isset($columns['rate_limit'])) {
        $appendField('rate_limit', $rateLimit !== null ? [$rateLimit, 'i'] : null);
    }

    if (isset($columns['rate_limit_window'])) {
        $appendField('rate_limit_window', $rateLimitWindow !== null ? [$rateLimitWindow, 'i'] : null);
    }

    if (isset($columns['rate_limit_count'])) {
        $appendField('rate_limit_count', [0, 'i']);
    }

    if (isset($columns['rate_limit_reset_at'])) {
        $appendField('rate_limit_reset_at', $resetAt);
    }

    if (isset($columns['created_at'])) {
        $appendField('created_at', $nowStr);
    }

    if (isset($columns['updated_at'])) {
        $appendField('updated_at', $nowStr);
    }

    $placeholders = implode(', ', array_fill(0, count($fields), '?'));
    $fieldList    = implode(', ', $fields);

    $conn->begin_transaction();

    try {
        api_token_execute($conn, "INSERT INTO api_tokens ($fieldList) VALUES ($placeholders)", $params);
        $tokenId = $conn->insert_id;
        $conn->commit();
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }

    return [
        'id'    => (int) $tokenId,
        'token' => $token,
        'hash'  => $tokenHash,
    ];
}

/**
 * Genera un nuevo secreto para un token existente.
 *
 * @return array{token:string, hash:string}
 */
function api_token_rotate(int $tokenId): array
{
    $conn    = DatabaseManager::getConnection();
    $columns = api_token_table_columns();

    $now    = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $nowStr = $now->format('Y-m-d H:i:s');

    $token     = api_token_generate_secret();
    $tokenHash = api_token_hash($token);

    $conn->begin_transaction();

    try {
        $stmt   = api_token_execute($conn, 'SELECT * FROM api_tokens WHERE id = ? FOR UPDATE', [[$tokenId, 'i']]);
        $result = $stmt->get_result();
        $row    = $result ? $result->fetch_assoc() : null;

        if (!$row) {
            $conn->rollback();
            throw new RuntimeException('Token no encontrado para rotar.');
        }

        $rateLimitWindow = isset($columns['rate_limit_window']) ? (int) ($row['rate_limit_window'] ?? 60) : null;
        $rateLimit       = isset($columns['rate_limit']) ? (int) ($row['rate_limit'] ?? 0) : null;

        $resetAt = null;
        if ($rateLimit && $rateLimit > 0 && $rateLimitWindow && $rateLimitWindow > 0) {
            $resetAt = $now->add(new DateInterval('PT' . max(1, $rateLimitWindow) . 'S'))->format('Y-m-d H:i:s');
        }

        $updates = [];
        $params  = [];

        if (isset($columns['token_hash'])) {
            $updates[] = 'token_hash = ?';
            $params[]  = $tokenHash;
        } elseif (isset($columns['token'])) {
            $updates[] = 'token = ?';
            $params[]  = $tokenHash;
        }

        if (isset($columns['token_preview'])) {
            $updates[] = 'token_preview = ?';
            $params[]  = substr($token, 0, 8);
        }

        if (isset($columns['last_used_at'])) {
            $updates[] = 'last_used_at = NULL';
        }

        if (isset($columns['rate_limit_count'])) {
            $updates[] = 'rate_limit_count = 0';
        }

        if (isset($columns['rate_limit_reset_at'])) {
            $updates[] = 'rate_limit_reset_at = ?';
            $params[]  = $resetAt;
        }

        if (isset($columns['updated_at'])) {
            $updates[] = 'updated_at = ?';
            $params[]  = $nowStr;
        }

        if (empty($updates)) {
            $conn->commit();
            return [
                'token' => $token,
                'hash'  => $tokenHash,
            ];
        }

        $setClause = implode(', ', $updates);

        $params[] = [$tokenId, 'i'];

        api_token_execute($conn, "UPDATE api_tokens SET $setClause WHERE id = ?", $params);
        $conn->commit();
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }

    return [
        'token' => $token,
        'hash'  => $tokenHash,
    ];
}

/**
 * Revoca un token estableciendo la fecha de revocación.
 */
function api_token_revoke(int $tokenId): void
{
    $conn    = DatabaseManager::getConnection();
    $columns = api_token_table_columns();

    $now    = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $nowStr = $now->format('Y-m-d H:i:s');

    $updates = [];
    $params  = [];

    if (isset($columns['revoked_at'])) {
        $updates[] = 'revoked_at = ?';
        $params[]  = $nowStr;
    }

    if (isset($columns['updated_at'])) {
        $updates[] = 'updated_at = ?';
        $params[]  = $nowStr;
    }

    if (empty($updates)) {
        return;
    }

    $params[] = [$tokenId, 'i'];

    api_token_execute($conn, 'UPDATE api_tokens SET ' . implode(', ', $updates) . ' WHERE id = ?', $params);
}

/**
 * Registra el uso de un token teniendo en cuenta los límites aplicables.
 *
 * @return array{allowed:bool, remaining:?int, retry_after:?int}
 */
function api_token_register_usage(int $tokenId, ?DateTimeImmutable $now = null): array
{
    $conn    = DatabaseManager::getConnection();
    $columns = api_token_table_columns();

    $now    = $now ? $now->setTimezone(new DateTimeZone('UTC')) : new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $nowStr = $now->format('Y-m-d H:i:s');

    $conn->begin_transaction();

    try {
        $stmt   = api_token_execute($conn, 'SELECT * FROM api_tokens WHERE id = ? FOR UPDATE', [[$tokenId, 'i']]);
        $result = $stmt->get_result();
        $row    = $result ? $result->fetch_assoc() : null;

        if (!$row) {
            $conn->rollback();
            throw new RuntimeException('Token no encontrado al registrar uso.');
        }

        $rateLimit       = isset($columns['rate_limit']) ? ($row['rate_limit'] !== null ? (int) $row['rate_limit'] : null) : null;
        $rateLimitWindow = isset($columns['rate_limit_window']) ? ($row['rate_limit_window'] !== null ? (int) $row['rate_limit_window'] : null) : null;
        $rateLimitCount  = isset($columns['rate_limit_count']) ? (int) ($row['rate_limit_count'] ?? 0) : 0;
        $resetAtRaw      = isset($columns['rate_limit_reset_at']) ? $row['rate_limit_reset_at'] : null;

        $rateLimited = false;
        $retryAfter  = null;

        if ($rateLimit !== null && $rateLimit > 0 && $rateLimitWindow !== null && $rateLimitWindow > 0) {
            $resetAt = api_token_parse_datetime($resetAtRaw);

            if (!$resetAt || $resetAt <= $now) {
                $rateLimitCount = 0;
                $resetAt        = $now->add(new DateInterval('PT' . max(1, $rateLimitWindow) . 'S'));
            }

            if ($rateLimitCount >= $rateLimit) {
                if (!$resetAt) {
                    $resetAt = $now->add(new DateInterval('PT' . max(1, $rateLimitWindow) . 'S'));
                }

                $rateLimited = true;
                $retryAfter  = max(1, $resetAt->getTimestamp() - $now->getTimestamp());
            } else {
                $rateLimitCount++;
                $resetAtRaw = $resetAt ? $resetAt->format('Y-m-d H:i:s') : null;
            }
        }

        if ($rateLimited) {
            $conn->rollback();

            return [
                'allowed'     => false,
                'remaining'   => 0,
                'retry_after' => $retryAfter,
            ];
        }

        $updates = [];
        $params  = [];

        if (isset($columns['last_used_at'])) {
            $updates[] = 'last_used_at = ?';
            $params[]  = $nowStr;
        }

        if (isset($columns['rate_limit_count'])) {
            $updates[] = 'rate_limit_count = ?';
            $params[]  = [$rateLimitCount, 'i'];
        }

        if (isset($columns['rate_limit_reset_at'])) {
            $updates[] = 'rate_limit_reset_at = ?';
            $params[]  = $rateLimit !== null && $rateLimit > 0 && $rateLimitWindow !== null && $rateLimitWindow > 0
                ? $resetAtRaw
                : null;
        }

        if (isset($columns['updated_at'])) {
            $updates[] = 'updated_at = ?';
            $params[]  = $nowStr;
        }

        if (!empty($updates)) {
            $params[] = [$tokenId, 'i'];
            api_token_execute($conn, 'UPDATE api_tokens SET ' . implode(', ', $updates) . ' WHERE id = ?', $params);
        }

        $conn->commit();

        return [
            'allowed'     => true,
            'remaining'   => $rateLimit !== null && $rateLimit > 0 ? max(0, $rateLimit - $rateLimitCount) : null,
            'retry_after' => null,
        ];
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }
}

/**
 * Actualiza el contador de rate limit para una empresa en la tabla `empresas` si las columnas existen.
 *
 * @return array{allowed:bool, remaining:?int, retry_after:?int}
 */
function api_empresa_register_usage(int $empresaId, ?DateTimeImmutable $now = null): array
{
    $conn = DatabaseManager::getConnection();
    static $columns = null;

    if ($columns === null) {
        $result = $conn->query('SHOW COLUMNS FROM empresas');
        if ($result) {
            $columns = [];
            while ($row = $result->fetch_assoc()) {
                $columns[$row['Field']] = true;
            }
        } else {
            $columns = [];
        }
    }

    $required = ['api_rate_limit', 'api_rate_limit_window', 'api_rate_limit_count', 'api_rate_limit_reset_at'];
    foreach ($required as $field) {
        if (!isset($columns[$field])) {
            return ['allowed' => true, 'remaining' => null, 'retry_after' => null];
        }
    }

    $now    = $now ? $now->setTimezone(new DateTimeZone('UTC')) : new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $nowStr = $now->format('Y-m-d H:i:s');

    $conn->begin_transaction();

    try {
        $stmt   = api_token_execute($conn, 'SELECT api_rate_limit, api_rate_limit_window, api_rate_limit_count, api_rate_limit_reset_at FROM empresas WHERE id = ? FOR UPDATE', [[$empresaId, 'i']]);
        $result = $stmt->get_result();
        $row    = $result ? $result->fetch_assoc() : null;

        if (!$row) {
            $conn->rollback();
            return ['allowed' => true, 'remaining' => null, 'retry_after' => null];
        }

        $limit       = $row['api_rate_limit'] !== null ? (int) $row['api_rate_limit'] : null;
        $window      = $row['api_rate_limit_window'] !== null ? (int) $row['api_rate_limit_window'] : null;
        $count       = $row['api_rate_limit_count'] !== null ? (int) $row['api_rate_limit_count'] : 0;
        $resetAtRaw  = $row['api_rate_limit_reset_at'] ?? null;
        $retryAfter  = null;
        $rateLimited = false;

        if ($limit !== null && $limit > 0 && $window !== null && $window > 0) {
            $resetAt = api_token_parse_datetime($resetAtRaw);

            if (!$resetAt || $resetAt <= $now) {
                $count    = 0;
                $resetAt  = $now->add(new DateInterval('PT' . max(1, $window) . 'S'));
                $resetAtRaw = $resetAt->format('Y-m-d H:i:s');
            }

            if ($count >= $limit) {
                $rateLimited = true;
                $retryAfter  = max(1, (api_token_parse_datetime($resetAtRaw) ?? $resetAt)->getTimestamp() - $now->getTimestamp());
            } else {
                $count++;
            }
        }

        if ($rateLimited) {
            $conn->rollback();
            return ['allowed' => false, 'remaining' => 0, 'retry_after' => $retryAfter];
        }

        $updates = [
            'api_rate_limit_count = ?',
            'api_rate_limit_reset_at = ?',
        ];

        $params = [
            [$count, 'i'],
            $resetAtRaw,
        ];

        if (isset($columns['updated_at'])) {
            $updates[] = 'updated_at = ?';
            $params[]  = $nowStr;
        }

        $params[] = [$empresaId, 'i'];

        api_token_execute(
            $conn,
            'UPDATE empresas SET ' . implode(', ', $updates) . ' WHERE id = ?',
            $params
        );

        $conn->commit();

        return [
            'allowed'     => true,
            'remaining'   => $limit !== null && $limit > 0 ? max(0, $limit - $count) : null,
            'retry_after' => null,
        ];
    } catch (Throwable $e) {
        $conn->rollback();
        throw $e;
    }
}

/**
 * Localiza un token a partir del secreto proporcionado por el cliente.
 *
 * @return array<string, mixed>|null
 */
function api_token_find_by_secret(string $token): ?array
{
    $conn    = DatabaseManager::getConnection();
    $columns = api_token_table_columns();

    $conditions = [];
    $params     = [];

    $hash = api_token_hash($token);

    if (isset($columns['token_hash'])) {
        $conditions[] = 'token_hash = ?';
        $params[]     = $hash;
    }

    if (isset($columns['token'])) {
        $conditions[] = 'token = ?';
        $params[]     = $token;
    }

    if (empty($conditions)) {
        throw new RuntimeException('La tabla api_tokens no dispone de columnas compatibles para buscar tokens.');
    }

    $sql = 'SELECT * FROM api_tokens WHERE ' . implode(' OR ', $conditions) . ' LIMIT 1';

    $stmt   = api_token_execute($conn, $sql, $params);
    $result = $stmt->get_result();

    if (!$result) {
        return null;
    }

    $row = $result->fetch_assoc();

    return $row ?: null;
}

?>
