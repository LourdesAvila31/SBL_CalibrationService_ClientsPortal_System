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

$options = getopt('', [
    'interval-hours::',
    'days::',
    'once',
    'daemon',
    'help',
]);

if (isset($options['help'])) {
    echo "Uso: php backend/calibraciones/alert_scheduler.php [--interval-hours=N] [--days=N] [--once|--daemon]\n";
    echo "    --interval-hours   Horas a esperar entre ciclos (predeterminado: 24). Acepta decimales.\n";
    echo "    --days             Horizonte de días para buscar calibraciones (predeterminado: 60).\n";
    echo "    --once             Ejecuta un único ciclo y termina (modo cron).\n";
    echo "    --daemon           Mantiene el proceso en ejecución indefinidamente (predeterminado).\n";
    echo "    --help             Muestra esta ayuda.\n";
    echo "El envío es multicanal y respeta los conectores habilitados por empresa (correo, Slack, etc.).\n";
    exit(0);
}

$intervalHours = isset($options['interval-hours']) ? (float) $options['interval-hours'] : 24.0;
if (!is_finite($intervalHours) || $intervalHours <= 0) {
    fwrite(STDERR, "El intervalo de horas debe ser un número positivo.\n");
    exit(1);
}

$daysAhead = isset($options['days']) ? (int) $options['days'] : 60;
if ($daysAhead <= 0) {
    fwrite(STDERR, "El horizonte de días debe ser mayor que cero.\n");
    exit(1);
}

$mode = 'daemon';
if (isset($options['once']) && isset($options['daemon'])) {
    fwrite(STDERR, "No se puede usar --once y --daemon al mismo tiempo.\n");
    exit(1);
}
if (isset($options['once'])) {
    $mode = 'once';
} elseif (isset($options['daemon'])) {
    $mode = 'daemon';
}

$intervalSeconds = (int) round($intervalHours * 3600);
if ($intervalSeconds < 1) {
    $intervalSeconds = 1;
}

$logger = static function (string $level, string $message) {
    $timestamp = date('c');
    $level = strtoupper($level);
    $line = sprintf('[%s] [%s] %s', $timestamp, $level, $message) . PHP_EOL;

    if ($level === 'ERROR') {
        fwrite(STDERR, $line);
        return;
    }

    fwrite(STDOUT, $line);
};

$stopRequested = false;
if (function_exists('pcntl_async_signals') && function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, static function () use (&$stopRequested, $logger): void {
        $stopRequested = true;
        $logger('info', 'Recibida señal SIGTERM. Finalizando el scheduler...');
    });
    pcntl_signal(SIGINT, static function () use (&$stopRequested, $logger): void {
        $stopRequested = true;
        $logger('info', 'Interrupción solicitada. Cerrando el scheduler...');
    });
}

$logger('info', sprintf('Iniciando scheduler en modo %s (intervalo: %.2f h, horizonte: %d días)', $mode, $intervalHours, $daysAhead));

$cycle = 1;
$exitCode = 0;

while (!$stopRequested) {
    $logger('info', sprintf('Comenzando ciclo #%d', $cycle));

    try {
        $connection = DatabaseManager::getConnection();
    } catch (Throwable $e) {
        $logger('error', 'No se pudo obtener la conexión a la base de datos: ' . $e->getMessage());
        $exitCode = 1;

        if ($mode === 'once') {
            break;
        }

        $retrySeconds = min(max($intervalSeconds, 60), 300);
        $logger('info', 'Reintentando en ' . $retrySeconds . ' segundos...');
        $slept = 0;
        while ($slept < $retrySeconds && !$stopRequested) {
            $remaining = $retrySeconds - $slept;
            $chunk = $remaining > 30 ? 30 : $remaining;
            sleep($chunk);
            $slept += $chunk;
        }
        $cycle++;
        continue;
    }

    try {
        $result = calibration_alerts_send_notifications($connection, $daysAhead, $logger);
        $logger('info', sprintf(
            'Ciclo #%1$d completado: detectadas %2$d (próximas: %3$d, hoy: %4$d, vencidas: %5$d); pendientes %6$d (próximas: %7$d, hoy: %8$d, vencidas: %9$d); empresas notificadas %10$d/%11$d; instrumentos notificados %12$d (próximas: %13$d, hoy: %14$d, vencidas: %15$d); registros asentados %16$d; fallos de canal %17$d',
            $cycle,
            $result['alerts_found'],
            $result['alerts_found_upcoming'],
            $result['alerts_found_due_today'],
            $result['alerts_found_overdue'],
            $result['pending_alerts'],
            $result['pending_upcoming'],
            $result['pending_due_today'],
            $result['pending_overdue'],
            $result['companies_notified'],
            $result['companies_attempted'],
            $result['notifications_sent'],
            $result['notifications_sent_upcoming'],
            $result['notifications_sent_due_today'],
            $result['notifications_sent_overdue'],
            $result['notifications_recorded'],
            count($result['channel_failures'] ?? [])
        ));
        $exitCode = $result['notifications_sent'] > 0 ? 0 : 2;
    } catch (Throwable $e) {
        $logger('error', 'Error durante el ciclo #' . $cycle . ': ' . $e->getMessage());
        $exitCode = 1;
    }

    if ($mode === 'once' || $stopRequested) {
        break;
    }

    $logger('info', sprintf('Esperando %.2f horas (%d segundos) antes del próximo ciclo.', $intervalSeconds / 3600, $intervalSeconds));

    $elapsed = 0;
    while ($elapsed < $intervalSeconds && !$stopRequested) {
        $remaining = $intervalSeconds - $elapsed;
        $chunk = $remaining > 60 ? 60 : $remaining;
        sleep($chunk);
        $elapsed += $chunk;
    }

    $cycle++;
}

$logger('info', 'Scheduler finalizado.');
exit($exitCode);
