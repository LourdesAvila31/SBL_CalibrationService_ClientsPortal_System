<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

ensure_portal_access('service');
if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No tienes permisos para consultar alertas de calibración.']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_alerts.php';

header('Content-Type: application/json');

try {
    $empresaId = obtenerEmpresaId();
    if ($empresaId <= 0) {
        http_response_code(400);
        echo json_encode(['error' => 'Empresa no especificada.']);
        $conn->close();
        exit;
    }

    $alertType = filter_input(INPUT_GET, 'alert_type', FILTER_SANITIZE_STRING) ?: null;
    $attentionStatus = filter_input(INPUT_GET, 'attention_status', FILTER_SANITIZE_STRING) ?: null;

    $filters = [];
    if ($alertType && in_array(strtolower($alertType), ['upcoming', 'due_today', 'overdue'], true)) {
        $filters['alert_type'] = strtolower($alertType);
    }

    if ($attentionStatus) {
        $normalizedStatus = strtolower($attentionStatus);
        if (in_array($normalizedStatus, calibration_alerts_valid_attention_statuses(), true)) {
            $filters['attention_status'] = $normalizedStatus;
        }
    }

    $notifications = calibration_alerts_list_notifications($conn, $empresaId, $filters);

    $summary = [
        'total' => count($notifications),
        'by_status' => [],
        'by_type' => [],
    ];

    foreach ($notifications as $row) {
        $status = strtolower((string) ($row['attention_status'] ?? 'desconocido'));
        $type = strtolower((string) ($row['alert_type'] ?? 'upcoming'));

        if (!isset($summary['by_status'][$status])) {
            $summary['by_status'][$status] = 0;
        }
        if (!isset($summary['by_type'][$type])) {
            $summary['by_type'][$type] = 0;
        }

        $summary['by_status'][$status]++;
        $summary['by_type'][$type]++;
    }

    echo json_encode([
        'data' => $notifications,
        'summary' => $summary,
    ]);
} catch (Throwable $exception) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudieron consultar las alertas de calibración.', 'details' => $exception->getMessage()]);
}

$conn->close();
