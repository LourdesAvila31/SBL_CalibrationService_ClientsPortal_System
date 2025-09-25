<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    exit;
}

require_once __DIR__ . '/report_helpers.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_alerts.php';

global $conn;
$empresaId = require_empresa_id();
$format = strtolower((string) request_input('format', 'excel'));

$rows = [];

try {
    $data = calibration_alerts_fetch_upcoming($conn, 60, $empresaId);
    foreach ($data as $row) {
        $logistica = $row['logistica'] ?? null;
        $estadoLogistica = $logistica['estado'] ?? ($row['logistica_estado'] ?? 'Pendiente');
        $fechaEnvio = $logistica['fecha_envio'] ?? ($row['logistica_fecha_envio'] ?? null);
        $fechaRetorno = $logistica['fecha_retorno'] ?? ($row['logistica_fecha_retorno'] ?? null);
        $rows[] = [
            $row['instrumento_nombre'] ?? '-',
            $row['fecha_calibracion'] ?? '-',
            $row['fecha_proxima'] ?? '-',
            $row['resultado'] !== null && $row['resultado'] !== '' ? $row['resultado'] : 'Sin resultado',
            $row['tecnico_nombre'] ?? 'Sin asignar',
            $estadoLogistica !== '' ? $estadoLogistica : 'Pendiente',
            $fechaEnvio ?? '-',
            $fechaRetorno ?? '-',
        ];
    }
} catch (RuntimeException $e) {
    http_response_code(500);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'Error al generar el reporte: ' . $e->getMessage();
    exit;
}

$headers = [
    'Instrumento',
    'Fecha de Calibración',
    'Próxima Calibración',
    'Resultado',
    'Técnico',
    'Estado logístico',
    'Fecha envío',
    'Fecha retorno',
];

switch ($format) {
    case 'excel':
        stream_excel_report('calibration_due.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('calibration_due.pdf', 'Calibraciones próximas a vencer', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('calibration_due.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
