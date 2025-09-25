<?php

declare(strict_types=1);

if (!defined('DB_CONFIG_AUTO_CONNECT')) {
    define('DB_CONFIG_AUTO_CONNECT', false);
}

$stubUpcomingRows = [];
$stubOverdueRows = [];

if (!function_exists('calibration_alerts_ensure_log_table')) {
    function calibration_alerts_ensure_log_table(mysqli $conn): void
    {
        // No-op en pruebas: la tabla se asume creada.
    }
}

if (!function_exists('calibration_alerts_fetch_upcoming')) {
    function calibration_alerts_fetch_upcoming(mysqli $conn, int $daysAhead = 60, ?int $empresaId = null): array
    {
        global $stubUpcomingRows;

        return $stubUpcomingRows;
    }
}

if (!function_exists('calibration_alerts_fetch_overdue')) {
    function calibration_alerts_fetch_overdue(mysqli $conn, ?int $empresaId = null): array
    {
        global $stubOverdueRows;

        return $stubOverdueRows;
    }
}

require_once __DIR__ . '/../fixtures/FakeCalibrationAlertDb.php';

$capturedMails = [];
$jobTouches = [];

if (!function_exists('tenant_notifications_jobs_touch')) {
    function tenant_notifications_jobs_touch(
        mysqli $conn,
        string $jobName,
        string $status,
        ?string $message,
        ?array $payload,
        ?\DateTimeImmutable $timestamp = null
    ): void {
        global $jobTouches;
        $jobTouches[] = [
            'job' => $jobName,
            'status' => $status,
            'message' => $message,
            'payload' => $payload,
        ];
    }
}

if (!function_exists('tenant_notifications_fetch_company_recipients')) {
    function tenant_notifications_fetch_company_recipients(mysqli $conn, int $empresaId, array $roleNames = []): array
    {
        return [
            ['nombre' => 'Tester Uno', 'correo' => 'tester@example.com'],
        ];
    }
}

if (!function_exists('tenant_notifications_send_bulk_mail')) {
    function tenant_notifications_send_bulk_mail(array $recipients, string $subject, string $htmlBody, ?string $textBody = null, ?callable $logger = null): array
    {
        global $capturedMails;
        $capturedMails[] = [
            'recipients' => $recipients,
            'subject' => $subject,
            'html' => $htmlBody,
            'text' => $textBody,
        ];

        return [
            'attempted' => count($recipients),
            'sent' => count($recipients),
            'failures' => [],
        ];
    }
}

require_once __DIR__ . '/../../app/Core/helpers/logistics.php';
require_once __DIR__ . '/../../app/Core/helpers/calibration_alerts.php';

function assertValue($condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, "Assertion failed: {$message}\n");
        exit(1);
    }
}

$upcomingRows = [
    [
        'instrumento_id' => 101,
        'empresa_id' => 10,
        'empresa_nombre' => 'Empresa Demo',
        'instrumento_nombre' => 'Termómetro A',
        'instrumento_codigo' => 'T-A',
        'fecha_calibracion' => '2024-02-01',
        'fecha_proxima' => '2024-04-10',
        'resultado' => 'OK',
        'dias_restantes' => 9,
        'log_estado' => null,
        'log_proveedor_externo' => null,
        'log_transportista' => null,
        'log_numero_guia' => null,
        'log_orden_servicio' => null,
        'log_comentarios' => null,
        'log_fecha_envio' => null,
        'log_fecha_en_transito' => null,
        'log_fecha_recibido' => null,
        'log_fecha_retorno' => null,
    ],
    [
        'instrumento_id' => 102,
        'empresa_id' => 10,
        'empresa_nombre' => 'Empresa Demo',
        'instrumento_nombre' => 'Termómetro B',
        'instrumento_codigo' => 'T-B',
        'fecha_calibracion' => '2024-02-15',
        'fecha_proxima' => '2024-04-20',
        'resultado' => 'OK',
        'dias_restantes' => 19,
        'log_estado' => null,
        'log_proveedor_externo' => null,
        'log_transportista' => null,
        'log_numero_guia' => null,
        'log_orden_servicio' => null,
        'log_comentarios' => null,
        'log_fecha_envio' => null,
        'log_fecha_en_transito' => null,
        'log_fecha_recibido' => null,
        'log_fecha_retorno' => null,
    ],
];

$overdueRows = [
    [
        'instrumento_id' => 201,
        'empresa_id' => 10,
        'empresa_nombre' => 'Empresa Demo',
        'instrumento_nombre' => 'Balanza C',
        'instrumento_codigo' => 'B-C',
        'fecha_calibracion' => '2023-09-01',
        'fecha_proxima' => '2024-03-15',
        'resultado' => 'OK',
        'dias_restantes' => -10,
        'log_estado' => null,
        'log_proveedor_externo' => null,
        'log_transportista' => null,
        'log_numero_guia' => null,
        'log_orden_servicio' => null,
        'log_comentarios' => null,
        'log_fecha_envio' => null,
        'log_fecha_en_transito' => null,
        'log_fecha_recibido' => null,
        'log_fecha_retorno' => null,
    ],
    [
        'instrumento_id' => 101,
        'empresa_id' => 10,
        'empresa_nombre' => 'Empresa Demo',
        'instrumento_nombre' => 'Termómetro A',
        'instrumento_codigo' => 'T-A',
        'fecha_calibracion' => '2023-12-01',
        'fecha_proxima' => '2024-04-10',
        'resultado' => 'OK',
        'dias_restantes' => -1,
        'log_estado' => null,
        'log_proveedor_externo' => null,
        'log_transportista' => null,
        'log_numero_guia' => null,
        'log_orden_servicio' => null,
        'log_comentarios' => null,
        'log_fecha_envio' => null,
        'log_fecha_en_transito' => null,
        'log_fecha_recibido' => null,
        'log_fecha_retorno' => null,
    ],
];

$fakeConn = new FakeCalibrationMysqli($upcomingRows, $overdueRows);
$stubUpcomingRows = $upcomingRows;
$stubOverdueRows = $overdueRows;
$fakeConn->existingNotifications = [FakeCalibrationMysqli::makeKeyValues(101, 10, '2024-04-10')];
$GLOBALS['conn'] = $fakeConn;

$logs = [];
$result = calibration_alerts_send_notifications(
    $fakeConn,
    60,
    function (string $level, string $message) use (&$logs): void {
        $logs[] = [$level, $message];
    }
);

assertValue($result['alerts_found'] === 4, 'Total alerts found');
assertValue($result['alerts_found_upcoming'] === 2, 'Upcoming alerts found');
assertValue($result['alerts_found_overdue'] === 2, 'Overdue alerts found');
assertValue($result['pending_alerts'] === 2, 'Pending alerts total');
assertValue($result['pending_upcoming'] === 1, 'Pending upcoming alerts');
assertValue($result['pending_overdue'] === 1, 'Pending overdue alerts');
assertValue($result['companies_attempted'] === 1, 'Companies attempted');
assertValue($result['companies_notified'] === 1, 'Companies notified');
assertValue($result['notifications_sent'] >= 0, 'Notifications sent counter present');
assertValue($result['notifications_recorded'] >= 0, 'Notifications recorded counter present');

assertValue(count($capturedMails) === 1, 'One email captured');
$mail = $capturedMails[0];
assertValue(stripos($mail['subject'], 'próximas y vencidas') !== false, 'Subject mentions upcoming and overdue');
assertValue(stripos($mail['html'], 'Calibraciones vencidas') !== false, 'HTML body includes overdue section');
assertValue(stripos($mail['html'], 'Calibraciones próximas') !== false, 'HTML body includes upcoming section');

assertValue(!empty($jobTouches), 'Job touch recorded');

fwrite(STDOUT, "Calibration alert notification tests passed\n");
