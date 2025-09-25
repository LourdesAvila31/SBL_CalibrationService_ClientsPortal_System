<?php

declare(strict_types=1);

require_once __DIR__ . '/../../app/Core/services/AlertDispatcher.php';

class DummyMysqli extends mysqli
{
    public function __construct()
    {
        // Evita la conexión real al inicializar mysqli.
    }

    #[\ReturnTypeWillChange]
    public function query($query, $resultmode = MYSQLI_STORE_RESULT)
    {
        return true;
    }
}

/**
 * @param mixed  $expected
 * @param mixed  $actual
 * @param string $message
 */
function assertSameValue($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        fwrite(STDERR, sprintf(
            "Assertion failed: %s. Expected %s, got %s\n",
            $message,
            var_export($expected, true),
            var_export($actual, true)
        ));
        exit(1);
    }
}

$loggerCalls = [];
$logger = static function (string $level, string $message) use (&$loggerCalls): void {
    $loggerCalls[] = [$level, $message];
};

$emailCalls = 0;
$slackCalls = 0;
$emailSummaries = [
    [
        'attempted' => 1,
        'sent' => 1,
        'failures' => [],
    ],
    [
        'attempted' => 1,
        'sent' => 1,
        'failures' => [],
    ],
    [
        'attempted' => 1,
        'sent' => 0,
        'failures' => [['correo' => 'demo@example.com']],
    ],
];

$connectors = [
    'email' => static function (array $config, array $message, callable $logger) use (&$emailCalls, &$emailSummaries): array {
        $emailCalls++;
        $index = $emailCalls - 1;
        if ($index >= count($emailSummaries)) {
            $index = count($emailSummaries) - 1;
        }

        $summary = $emailSummaries[$index];
        $recipients = $message['recipients'] ?? [];
        $summary['attempted'] = count($recipients);

        return $summary;
    },
    'slack' => static function (array $config, array $message, callable $logger) use (&$slackCalls): array {
        $slackCalls++;
        return ['delivered' => true];
    },
];

$channelResolver = static function (int $empresaId): array {
    if ($empresaId === 7) {
        return [
            'email' => ['enabled' => true, 'config' => []],
            'slack' => ['enabled' => true, 'config' => ['webhook_url' => 'https://example.test']],
        ];
    }

    return [
        'email' => ['enabled' => true, 'config' => []],
        'slack' => ['enabled' => false, 'config' => []],
    ];
};

$dispatcher = new AlertDispatcher(new DummyMysqli(), $logger, $connectors, $channelResolver);

$message = [
    'subject' => 'Próximas calibraciones',
    'html_body' => '<p>Alerta</p>',
    'text_body' => 'Alerta de calibraciones',
    'recipients' => [
        ['nombre' => 'Demo', 'correo' => 'demo@example.com'],
    ],
];

$resultWithSlack = $dispatcher->dispatch(7, $message);
assertSameValue(1, $emailCalls, 'El conector de correo se invoca una vez');
assertSameValue(1, $slackCalls, 'El conector adicional se invoca cuando está habilitado');
assertSameValue('sent', $resultWithSlack['email']['status'], 'El canal de correo reporta envío exitoso');
assertSameValue('sent', $resultWithSlack['slack']['status'], 'El canal adicional reporta envío exitoso');

$resultWithoutSlack = $dispatcher->dispatch(9, $message);
assertSameValue(2, $emailCalls, 'El conector de correo se puede reutilizar para otras empresas');
assertSameValue(1, $slackCalls, 'El conector adicional no se invoca cuando está deshabilitado');
assertSameValue('sent', $resultWithoutSlack['email']['status'], 'El canal de correo sigue activo');
assertSameValue('skipped', $resultWithoutSlack['slack']['status'], 'El canal adicional se marca como omitido cuando está deshabilitado');

$resultWithEmailFailure = $dispatcher->dispatch(7, $message);
assertSameValue(3, $emailCalls, 'El conector de correo se invoca para el tercer envío');
assertSameValue(2, $slackCalls, 'El canal adicional vuelve a activarse cuando procede');
assertSameValue('failed', $resultWithEmailFailure['email']['status'], 'El canal de correo reporta fallo cuando no envía mensajes');
assertSameValue(true, isset($resultWithEmailFailure['email']['error']), 'El canal de correo informa del motivo del fallo');
assertSameValue('sent', $resultWithEmailFailure['slack']['status'], 'El canal adicional permanece operativo en fallos de correo');

echo "AlertDispatcher channel selection tests passed\n";
