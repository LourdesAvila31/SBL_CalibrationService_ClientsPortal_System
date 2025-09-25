<?php
require_once __DIR__ . '/../../../app/Core/SessionGuard.php';
require_once __DIR__ . '/../../../app/Core/permissions.php';

if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode([
        'status' => 'forbidden',
        'message' => 'No cuentas con permisos para ejecutar las alertas.',
    ]);
    exit;
}

require_once __DIR__ . '/../../../app/Core/db.php';
require_once __DIR__ . '/../../../app/Core/helpers/calibration_alerts.php';
require_once __DIR__ . '/../../../app/Core/helpers/tenant_notifications.php';

header('Content-Type: application/json');

$daysAhead = filter_input(INPUT_GET, 'days', FILTER_VALIDATE_INT);
if ($daysAhead === false || $daysAhead === null || $daysAhead <= 0) {
    $daysAhead = 60;
}

$minIntervalMinutes = filter_input(INPUT_GET, 'interval', FILTER_VALIDATE_INT);
if ($minIntervalMinutes === false || $minIntervalMinutes === null || $minIntervalMinutes < 0) {
    $minIntervalMinutes = 1440; // 24 horas por omisión
}

$jobName = 'calibration_alerts';
$now = new DateTimeImmutable();

try {
    $shouldRun = tenant_notifications_jobs_should_run($conn, $jobName, $minIntervalMinutes * 60, $now);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'status' => 'error',
        'message' => 'No se pudo validar el estado del job: ' . $e->getMessage(),
    ]);
    $conn->close();
    exit;
}

if (!$shouldRun) {
    $state = null;
    try {
        $state = tenant_notifications_jobs_get($conn, $jobName);
    } catch (Throwable $e) {
        $state = null;
    }

    echo json_encode([
        'status' => 'skipped',
        'message' => 'Las alertas ya se ejecutaron recientemente.',
        'last_run' => $state['last_run_at'] ?? null,
        'last_status' => $state['last_status'] ?? null,
    ]);
    $conn->close();
    exit;
}

$logEntries = [];
$logger = static function (string $level, string $message) use (&$logEntries): void {
    $logEntries[] = [
        'timestamp' => (new DateTimeImmutable())->format('c'),
        'level' => strtoupper($level),
        'message' => $message,
    ];
};

try {
    $result = calibration_alerts_send_notifications($conn, $daysAhead, $logger);
    echo json_encode([
        'status' => 'ok',
        'result' => $result,
        'logs' => $logEntries,
    ]);
} catch (Throwable $e) {
    http_response_code(500);
    $logger('error', $e->getMessage());
    echo json_encode([
        'status' => 'error',
        'message' => $e->getMessage(),
        'logs' => $logEntries,
    ]);
}

$conn->close();
