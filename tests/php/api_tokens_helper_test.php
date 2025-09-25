<?php

declare(strict_types=1);

require_once dirname(__DIR__, 2) . '/app/Core/API/ApiTokensHelper.php';

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function assert_equals($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        throw new RuntimeException($message . ' Esperado: ' . var_export($expected, true) . ' Actual: ' . var_export($actual, true));
    }
}

$tempFile = tempnam(sys_get_temp_dir(), 'api_tokens_');
if ($tempFile === false) {
    throw new RuntimeException('No se pudo crear el archivo temporal para la prueba.');
}

register_shutdown_function(static function () use ($tempFile): void {
    if (file_exists($tempFile)) {
        unlink($tempFile);
    }
});

try {
    $helper = new ApiTokensHelper($tempFile);

    $tokenData = $helper->issueToken('cliente-test', ['instrumentos.read', 'calibraciones.read'], 120, 2, 60);
    $token = $tokenData['token'];
    assert_true(is_string($token) && strlen($token) >= 16, 'El token generado debe ser una cadena.');

    $persisted = $helper->getToken($token);
    assert_true($persisted !== null, 'El token recién emitido debe existir en el almacenamiento.');
    assert_true($helper->hasScopes($persisted, ['instrumentos.read']), 'El token debe incluir el scope solicitado.');
    assert_true(!$helper->isExpired($persisted, time()), 'El token recién creado no debe estar expirado.');

    $expired = $persisted;
    $expired['expires_at'] = time() - 10;
    $helper->persistToken($expired);
    $recuperado = $helper->getToken($token);
    assert_true($recuperado !== null && $helper->isExpired($recuperado, time()), 'Un token expirado debe marcarse como inválido.');

    $helper->persistToken($persisted);
    $now = time();
    $consumo1 = $helper->consume($persisted, $now);
    assert_true($consumo1['allowed'] === true, 'La primera petición debe permitirse.');
    assert_equals(1, $consumo1['remaining'], 'El límite restante debe decrementar.');

    $consumo2 = $helper->consume($consumo1['token'], $now + 1);
    assert_true($consumo2['allowed'] === true, 'La segunda petición dentro del límite debe permitirse.');
    assert_equals(0, $consumo2['remaining'], 'Ya no deben quedar peticiones disponibles en la ventana.');

    $consumo3 = $helper->consume($consumo2['token'], $now + 2);
    assert_true($consumo3['allowed'] === false, 'La tercera petición consecutiva debe bloquearse por rate limit.');

    $consumo4 = $helper->consume($consumo2['token'], $now + 65);
    assert_true($consumo4['allowed'] === true, 'Tras expirar la ventana el token debe reactivar su cuota.');

    echo "✓ ApiTokensHelper: todas las aserciones pasaron" . PHP_EOL;
    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, '✗ ApiTokensHelper falló: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
