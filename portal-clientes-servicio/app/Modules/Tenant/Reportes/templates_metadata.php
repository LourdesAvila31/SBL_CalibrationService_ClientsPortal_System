<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

header('Content-Type: application/json; charset=UTF-8');

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    echo json_encode(['templates' => []]);
    exit;
}

$allRoles = [
    'superadministrador',
    'administrador',
    'supervisor',
    'operador',
    'lector',
    'cliente',
    'sistemas',
];

$templates = [
    [
        'id' => 'gages_due',
        'name' => 'Instrumentos por fecha de vencimiento',
        'description' => 'Listado de instrumentos próximos a vencer su calibración disponible en CSV, Excel o PDF.',
        'export_url' => '/backend/reportes/report_gages_due.php',
        'default_format' => 'csv',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'calibration_due',
        'name' => 'Lista de vencimiento de calibración',
        'description' => 'Reporte de calibraciones próximas a vencer disponible en Excel, PDF o CSV.',
        'export_url' => '/backend/reportes/report_calibration_due.php',
        'default_format' => 'excel',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'performance',
        'name' => 'Desempeño de calibración',
        'description' => 'Métricas de desempeño de calibraciones realizadas por instrumento.',
        'export_url' => '/backend/reportes/report_performance.php',
        'default_format' => 'excel',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'activity',
        'name' => 'Seguimiento de actividad de instrumentos',
        'description' => 'Detalle de actividad, planes y resultados recientes de cada instrumento.',
        'export_url' => '/backend/reportes/report_activity.php',
        'default_format' => 'pdf',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'details',
        'name' => 'Informe de detalles de calibración',
        'description' => 'Resumen de calibraciones con proveedor y observaciones asociadas.',
        'export_url' => '/backend/reportes/report_details.php',
        'default_format' => 'pdf',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'count',
        'name' => 'Informe de conteo de calibración',
        'description' => 'Cantidad de calibraciones realizadas por periodo y su porcentaje de aprobación.',
        'export_url' => '/backend/reportes/report_count.php',
        'default_format' => 'pdf',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'work_orders',
        'name' => 'Órdenes de trabajo de calibración',
        'description' => 'Órdenes programadas de calibración con técnico asignado, estado, fechas clave y observaciones.',
        'export_url' => '/backend/reportes/report_work_orders.php',
        'default_format' => 'pdf',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'standards',
        'name' => 'Calibraciones por estándar de referencia',
        'description' => 'Listado de calibraciones agrupadas por estándar de referencia utilizado con opciones tabulares.',
        'export_url' => '/backend/reportes/report_standards.php',
        'default_format' => 'list',
        'formats' => [
            ['id' => 'list', 'value' => 'list', 'label' => 'Listado'],
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
    [
        'id' => 'ontime',
        'name' => 'Calibración puntual',
        'description' => 'Reporte de cumplimiento de fechas de calibración comprometidas.',
        'export_url' => '/backend/reportes/report_ontime.php',
        'default_format' => 'pdf',
        'formats' => [
            ['id' => 'excel', 'value' => 'excel', 'label' => 'Excel'],
            ['id' => 'pdf', 'value' => 'pdf', 'label' => 'PDF'],
            ['id' => 'csv', 'value' => 'csv', 'label' => 'CSV'],
        ],
        'allowed_roles' => $allRoles,
    ],
];

echo json_encode([
    'templates' => $templates,
]);
