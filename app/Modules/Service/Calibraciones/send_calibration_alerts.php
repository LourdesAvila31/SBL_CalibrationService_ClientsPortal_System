<?php

if (PHP_SAPI !== 'cli') {
    http_response_code(403);
    header('Content-Type: text/plain; charset=UTF-8');
    echo "Este script solo puede ejecutarse desde la línea de comandos.";
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/mail_helper.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_alerts.php';

$options = array_slice($argv, 1);

if (in_array('--help', $options, true) || in_array('-h', $options, true)) {
    echo "Uso: php backend/calibraciones/send_calibration_alerts.php [--days=N]\n";
    echo "Envía alertas multicanal (correo, Slack, etc.) de las calibraciones próximas a vencer (por defecto 60 días).\n";
    exit(0);
}

$daysAhead = 60;
foreach ($options as $option) {
    if (preg_match('/^--days=(\d{1,3})$/', $option, $matches)) {
        $daysAhead = (int) $matches[1];
        break;
    }
    if (ctype_digit($option)) {
        $daysAhead = (int) $option;
        break;
    }
}

$daysAhead = max($daysAhead, 1);

echo "Buscando calibraciones con vencimiento en los próximos {$daysAhead} días...\n";

try {
    $result = calibration_alerts_send_notifications(
        $conn,
        $daysAhead,
        static function (string $level, string $message): void {
            $timestamp = date('Y-m-d H:i:s');
            $output = '[' . $timestamp . '] [' . strtoupper($level) . '] ' . $message . PHP_EOL;

            if ($level === 'error') {
                fwrite(STDERR, $output);
                return;
            }

            echo $output;
        }
    );
} catch (RuntimeException $e) {
    fwrite(STDERR, 'Error al enviar las notificaciones: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}

$conn->close();

echo 'Proceso completado. Detalles:' . PHP_EOL;
echo '  Calibraciones detectadas: ' . $result['alerts_found'] . ' (próximas: ' . $result['alerts_found_upcoming'] . ', vencidas: ' . $result['alerts_found_overdue'] . ')' . PHP_EOL;
echo '  Calibraciones pendientes: ' . $result['pending_alerts'] . ' (próximas: ' . $result['pending_upcoming'] . ', vencidas: ' . $result['pending_overdue'] . ')' . PHP_EOL;
echo '  Empresas notificadas: ' . $result['companies_notified'] . ' de ' . $result['companies_attempted'] . PHP_EOL;
echo '  Instrumentos notificados: ' . $result['notifications_sent'] . ' (próximos: ' . $result['notifications_sent_upcoming'] . ', vencidos: ' . $result['notifications_sent_overdue'] . ')' . PHP_EOL;
echo '  Registros asentados: ' . $result['notifications_recorded'] . ' (próximos: ' . $result['notifications_recorded_upcoming'] . ', vencidos: ' . $result['notifications_recorded_overdue'] . ')' . PHP_EOL;

$channelFailures = $result['channel_failures'] ?? [];
if ($channelFailures) {
    echo '  Fallos por canal: ' . count($channelFailures) . PHP_EOL;
    foreach ($channelFailures as $failure) {
        $empresa = $failure['empresa_nombre'] ?? ('ID ' . ($failure['empresa_id'] ?? 'desconocido'));
        echo '    - ' . $empresa . ' (' . ($failure['channel'] ?? 'canal') . '): ' . ($failure['error'] ?? 'Error desconocido') . PHP_EOL;
    }
} else {
    echo '  Fallos por canal: 0' . PHP_EOL;
}

$channelFailures = $result['channel_failures'] ?? [];
if ($channelFailures) {
    echo '  Fallos por canal: ' . count($channelFailures) . PHP_EOL;
    foreach ($channelFailures as $failure) {
        $empresa = $failure['empresa_nombre'] ?? ('ID ' . ($failure['empresa_id'] ?? 'desconocido'));
        echo '    - ' . $empresa . ' (' . ($failure['channel'] ?? 'canal') . '): ' . ($failure['error'] ?? 'Error desconocido') . PHP_EOL;
    }
} else {
    echo '  Fallos por canal: 0' . PHP_EOL;
}

$channelFailures = $result['channel_failures'] ?? [];
if ($channelFailures) {
    echo '  Fallos por canal: ' . count($channelFailures) . PHP_EOL;
    foreach ($channelFailures as $failure) {
        $empresa = $failure['empresa_nombre'] ?? ('ID ' . ($failure['empresa_id'] ?? 'desconocido'));
        echo '    - ' . $empresa . ' (' . ($failure['channel'] ?? 'canal') . '): ' . ($failure['error'] ?? 'Error desconocido') . PHP_EOL;
    }
} else {
    echo '  Fallos por canal: 0' . PHP_EOL;
}

exit($result['notifications_sent'] > 0 ? 0 : 2);
