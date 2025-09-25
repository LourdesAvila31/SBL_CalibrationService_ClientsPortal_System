<?php
declare(strict_types=1);

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once __DIR__ . '/helpers.php';

$options = getopt('', ['interval-hours::', 'retention-days::', 'help']);

if (isset($options['help'])) {
    display_usage();
    exit(0);
}

$intervalHours = get_numeric_option($options, 'interval-hours', 24.0);
$retentionDays = (int) get_numeric_option($options, 'retention-days', 7.0);

if ($intervalHours < 0) {
    fwrite(STDERR, "El intervalo debe ser un número positivo.\n");
    exit(1);
}

if ($retentionDays < 0) {
    fwrite(STDERR, "La retención no puede ser negativa.\n");
    exit(1);
}

$intervalSeconds = (int) round($intervalHours * 3600);
$runOnce = $intervalSeconds <= 0;
$dir = __DIR__ . '/files';

$loggerName = 'Scheduler de respaldos';
$loggerEmail = 'scheduler@sistema.local';

if (!($conn instanceof mysqli)) {
    fwrite(STDERR, "No se pudo establecer la conexión a la base de datos.\n");
    exit(1);
}

$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_signal(SIGTERM, function () use (&$running) {
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running) {
        $running = false;
    });
}

do {
    $cycleStart = time();
    $timestamp = date('Y-m-d H:i:s', $cycleStart);

    $result = create_backup($conn, $dir);

    if ($result['success']) {
        $message = 'Respaldo automático creado: ' . $result['filename'];
        log_activity($loggerName, [
            'seccion' => 'Backups',
            'valor_nuevo' => $message,
            'usuario_correo' => $loggerEmail,
            'usuario_nombre' => $loggerName,
            'usuario_firma_interna' => $loggerName,
            'fecha_evento' => $timestamp,
            'usuario_id' => null,
            'segmento_actor' => 'Sistemas',
        ]);
        echo "[{$timestamp}] {$message}\n";
    } else {
        $error = $result['error'] ?? 'Error desconocido';
        log_activity($loggerName, [
            'seccion' => 'Backups',
            'valor_nuevo' => 'Error al crear respaldo automático: ' . $error,
            'usuario_correo' => $loggerEmail,
            'usuario_nombre' => $loggerName,
            'usuario_firma_interna' => $loggerName,
            'fecha_evento' => $timestamp,
            'usuario_id' => null,
            'segmento_actor' => 'Sistemas',
        ]);
        fwrite(STDERR, "[{$timestamp}] Error al crear respaldo automático: {$error}\n");
    }

    $deleted = cleanup_old_backups($dir, $retentionDays);
    if ($deleted > 0) {
        echo "[{$timestamp}] Se eliminaron {$deleted} respaldos antiguos.\n";
    }

    if ($runOnce) {
        break;
    }

    $elapsed = time() - $cycleStart;
    $sleepFor = $intervalSeconds - $elapsed;
    while ($sleepFor > 0 && $running) {
        sleep(min($sleepFor, 60));
        if (function_exists('pcntl_signal_dispatch')) {
            pcntl_signal_dispatch();
        }
        $sleepFor = $intervalSeconds - (time() - $cycleStart);
    }

    if (!$running) {
        echo "[" . date('Y-m-d H:i:s') . "] Finalizando scheduler de respaldos.\n";
        break;
    }

    if (function_exists('pcntl_signal_dispatch')) {
        pcntl_signal_dispatch();
    }
} while (true);

if (isset($conn) && $conn instanceof mysqli) {
    $conn->close();
}

die(0);

function display_usage(): void
{
    echo <<<TXT
Uso: php backend/backups/schedule_backups.php [--interval-hours=N] [--retention-days=N]

  --interval-hours   Horas entre ejecuciones. Usa 0 para ejecutar una sola vez (predeterminado: 24).
  --retention-days   Días a conservar respaldos antes de eliminarlos (predeterminado: 7).
  --help             Muestra esta ayuda.

TXT;
}

/**
 * Obtiene una opción numérica desde los argumentos CLI.
 */
function get_numeric_option(array $options, string $name, float $default): float
{
    if (!isset($options[$name])) {
        return $default;
    }

    if (!is_numeric($options[$name])) {
        fwrite(STDERR, "El parámetro --{$name} debe ser numérico.\n");
        exit(1);
    }

    return (float) $options[$name];
}

/**
 * Elimina archivos antiguos del directorio de respaldos.
 */
function cleanup_old_backups(string $dir, int $retentionDays): int
{
    if ($retentionDays <= 0 || !is_dir($dir)) {
        return 0;
    }

    $threshold = time() - ($retentionDays * 86400);
    $deleted = 0;

    $iterator = new DirectoryIterator($dir);
    foreach ($iterator as $fileInfo) {
        if ($fileInfo->isDot() || !$fileInfo->isFile()) {
            continue;
        }

        if (!preg_match('/^backup_\\d{8}_\\d{6}\\.sql$/', $fileInfo->getFilename())) {
            continue;
        }

        if ($fileInfo->getMTime() < $threshold) {
            if (@unlink($fileInfo->getPathname())) {
                $deleted++;
            }
        }
    }

    return $deleted;
}
