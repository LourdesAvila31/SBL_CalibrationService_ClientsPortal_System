<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/app/Core/API/ApiGuard.php';

$tokensHelper = new ApiTokensHelper();
$guard = new ApiGuard($tokensHelper);

$method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
$uri = $_SERVER['REQUEST_URI'] ?? '/';
$path = parse_url($uri, PHP_URL_PATH) ?: '/';
$segments = array_values(array_filter(explode('/', $path)));

if (count($segments) < 4 || $segments[0] !== 'backend' || $segments[1] !== 'api' || $segments[2] !== 'v1') {
    http_response_code(404);
    echo json_encode(['error' => 'Ruta no encontrada.'], JSON_UNESCAPED_UNICODE);
    exit;
}

$resource = $segments[3];
$input = file_get_contents('php://input') ?: '';
$headers = function_exists('getallheaders') ? getallheaders() : [];
$headers = is_array($headers) ? $headers : [];

// El servidor embebido expone el header Authorization como HTTP_AUTHORIZATION
if (!isset($headers['Authorization']) && isset($_SERVER['HTTP_AUTHORIZATION'])) {
    $headers['Authorization'] = (string) $_SERVER['HTTP_AUTHORIZATION'];
}

$response = null;
$status = 200;

try {
    switch ($resource) {
        case 'tokens':
            if ($method !== 'POST') {
                $status = 405;
                $response = ['error' => 'Método no permitido.'];
                break;
            }

            $payload = $input !== '' ? json_decode($input, true) : [];
            if (!is_array($payload)) {
                throw new RuntimeException('El cuerpo debe ser JSON válido.');
            }

            $clientId = isset($payload['client_id']) ? (string) $payload['client_id'] : '';
            $scopes = isset($payload['scopes']) && is_array($payload['scopes']) ? array_map('strval', $payload['scopes']) : [];
            $ttl = isset($payload['ttl']) ? max(60, (int) $payload['ttl']) : 3600;
            $rate = $payload['rate_limit'] ?? [];
            $limit = isset($rate['limit']) ? max(0, (int) $rate['limit']) : 60;
            $window = isset($rate['window']) ? max(1, (int) $rate['window']) : 60;

            if ($clientId === '') {
                throw new RuntimeException('El campo client_id es obligatorio.');
            }

            if ($scopes === []) {
                throw new RuntimeException('Debes indicar al menos un scope.');
            }

            $tokenData = $tokensHelper->issueToken($clientId, $scopes, $ttl, $limit, $window);
            $status = 201;
            $response = [
                'access_token' => $tokenData['token'],
                'token_type' => 'Bearer',
                'expires_at' => $tokenData['expires_at'],
                'scopes' => $tokenData['scopes'],
                'rate_limit' => $tokenData['rate_limit'],
            ];
            break;

        case 'instrumentos':
            $auth = $guard->authorize($headers, ['instrumentos.read'], null, $_GET['token'] ?? null);
            $response = loadDataset('instrumentos');
            $headersOut = [
                'X-RateLimit-Remaining' => $auth['remaining'] !== null ? (string) $auth['remaining'] : '',
            ];
            sendJson($response, $status, $headersOut);
            exit;

        case 'calibraciones':
            $auth = $guard->authorize($headers, ['calibraciones.read'], null, $_GET['token'] ?? null);
            $response = loadDataset('calibraciones');
            $headersOut = [
                'X-RateLimit-Remaining' => $auth['remaining'] !== null ? (string) $auth['remaining'] : '',
            ];
            sendJson($response, $status, $headersOut);
            exit;

        default:
            $status = 404;
            $response = ['error' => 'Recurso no encontrado.'];
            break;
    }
} catch (ApiGuardException $e) {
    $status = $e->getStatusCode();
    $response = ['error' => $e->getMessage()];
} catch (Throwable $e) {
    $status = 400;
    $response = ['error' => $e->getMessage()];
}

offsetRateLimitHeader($status);
sendJson($response ?? [], $status);

function loadDataset(string $name): array
{
    $file = dirname(__DIR__, 4) . '/storage/api_v1/' . $name . '.json';
    if (!file_exists($file)) {
        return [];
    }

    $contents = file_get_contents($file);
    if ($contents === false || $contents === '') {
        return [];
    }

    $decoded = json_decode($contents, true);
    return is_array($decoded) ? $decoded : [];
}

function sendJson(array $payload, int $status, array $extraHeaders = []): void
{
    http_response_code($status);
    header('Content-Type: application/json; charset=utf-8');
    foreach ($extraHeaders as $name => $value) {
        if ($value === '') {
            continue;
        }
        header($name . ': ' . $value);
    }
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
}

function offsetRateLimitHeader(int $status): void
{
    if ($status === 405) {
        header('Allow: POST');
    }
}
