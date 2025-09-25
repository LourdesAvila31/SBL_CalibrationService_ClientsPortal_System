<?php

declare(strict_types=1);

require_once dirname(__DIR__, 2) . '/app/Core/API/ApiGuard.php';

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function expect_exception(callable $callable, string $message, int $status): void
{
    try {
        $callable();
        throw new RuntimeException('Se esperaba una excepción y no se lanzó.');
    } catch (ApiGuardException $e) {
        if ($e->getStatusCode() !== $status) {
            throw new RuntimeException($message . ' Código obtenido: ' . $e->getStatusCode());
        }
    }
}

$tempFile = tempnam(sys_get_temp_dir(), 'api_guard_');
if ($tempFile === false) {
    throw new RuntimeException('No se pudo crear el archivo temporal para las pruebas.');
}

register_shutdown_function(static function () use ($tempFile): void {
    if (file_exists($tempFile)) {
        unlink($tempFile);
    }
});

try {
    $helper = new ApiTokensHelper($tempFile);
    $guard = new ApiGuard($helper);

    $tokenData = $helper->issueToken('cliente-guard', ['instrumentos.read'], 120, 2, 60);
    $headers = ['Authorization' => 'Bearer ' . $tokenData['token']];
    $auth = $guard->authorize($headers, ['instrumentos.read'], time());
    assert_true($auth['token']['client_id'] === 'cliente-guard', 'El token autorizado debe pertenecer al cliente original.');
    assert_true($auth['remaining'] === 1, 'El límite restante debe reflejar la cuota disponible.');

    expect_exception(static function () use ($guard, $headers): void {
        $guard->authorize($headers, ['calibraciones.read'], time());
    }, 'El guard debe validar los scopes.', 403);

    $expired = $tokenData;
    $expired['expires_at'] = time() - 5;
    $helper->persistToken($expired);
    expect_exception(static function () use ($guard, $headers): void {
        $guard->authorize($headers, ['instrumentos.read'], time());
    }, 'Un token expirado no debe autenticarse.', 401);

    $limited = $helper->issueToken('cliente-rate', ['instrumentos.read'], 120, 1, 60);
    $headersLimited = ['Authorization' => 'Bearer ' . $limited['token']];
    $guard->authorize($headersLimited, ['instrumentos.read'], time());
    expect_exception(static function () use ($guard, $headersLimited): void {
        $guard->authorize($headersLimited, ['instrumentos.read'], time() + 1);
    }, 'El rate limit debe dispararse tras alcanzar el máximo permitido.', 429);

    echo "✓ ApiGuard: todas las aserciones pasaron" . PHP_EOL;
    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, '✗ ApiGuard falló: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
