<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
header('Content-Type: application/json');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    if (isset($conn) && $conn instanceof mysqli) {
        $conn->close();
    }
    exit;
}

function normalizarFecha(string $valor, string $fallback): string
{
    $fecha = DateTimeImmutable::createFromFormat('Y-m-d', $valor);
    if (!$fecha) {
        return $fallback;
    }
    return $fecha->format('Y-m-d');
}

try {
    $empresaId = (int) $empresaId;
    $today = (new DateTimeImmutable('today'))->format('Y-m-d');

    $endParam = filter_input(INPUT_GET, 'end', FILTER_SANITIZE_STRING) ?: '';
    $startParam = filter_input(INPUT_GET, 'start', FILTER_SANITIZE_STRING) ?: '';

    $defaultEnd = (new DateTimeImmutable('last day of this month'))->format('Y-m-d');
    $defaultStart = (new DateTimeImmutable('first day of -11 months'))->format('Y-m-d');

    $endDate = normalizarFecha($endParam, $defaultEnd);
    $startDate = normalizarFecha($startParam, $defaultStart);

    if ($startDate > $endDate) {
        [$startDate, $endDate] = [$endDate, $startDate];
    }

    $startMonth = (new DateTimeImmutable($startDate))->modify('first day of this month');
    $endMonth = (new DateTimeImmutable($endDate))->modify('first day of this month');

    $months = [];
    $monthKeys = [];
    $current = $startMonth;
    while ($current <= $endMonth) {
        $key = $current->format('Y-m');
        $months[] = $key;
        $monthKeys[$key] = [
            'total' => 0,
            'approved' => 0,
            'rejected' => 0,
            'avg_interval_days' => null,
            'avg_remaining_days' => null,
            'delayed' => 0,
        ];
        $current = $current->modify('+1 month');
    }

    $calibrationSql = <<<SQL
        SELECT
            DATE_FORMAT(cal.fecha_calibracion, '%Y-%m') AS month_key,
            COUNT(*) AS total,
            SUM(CASE WHEN LOWER(TRIM(cal.resultado)) REGEXP '^(aprobado|aprobada|conforme|aceptado|aceptada|ok|satisfactorio|satisfactoria|passed)$' THEN 1 ELSE 0 END) AS approved,
            SUM(CASE WHEN LOWER(TRIM(cal.resultado)) REGEXP '^(rechazado|rechazada|no conforme|fallido|fallida|failed|cancelado|cancelada)$' THEN 1 ELSE 0 END) AS rejected,
            AVG(CASE WHEN cal.fecha_proxima IS NOT NULL THEN DATEDIFF(cal.fecha_proxima, cal.fecha_calibracion) END) AS avg_interval_days,
            AVG(CASE WHEN i.proxima_calibracion IS NOT NULL THEN DATEDIFF(i.proxima_calibracion, ?) END) AS avg_remaining_days,
            SUM(CASE WHEN cal.estado_ejecucion = 'Atrasada' THEN 1 ELSE 0 END) AS delayed
        FROM calibraciones cal
        LEFT JOIN instrumentos i ON i.id = cal.instrumento_id AND i.empresa_id = cal.empresa_id
        WHERE cal.empresa_id = ?
          AND cal.fecha_calibracion BETWEEN ? AND ?
        GROUP BY month_key
        ORDER BY month_key
    SQL;

    $calibrationStmt = $conn->prepare($calibrationSql);
    if (!$calibrationStmt) {
        throw new RuntimeException('No se pudo preparar la consulta de tendencias.');
    }

    $calibrationStmt->bind_param('siss', $today, $empresaId, $startDate, $endDate);
    $calibrationStmt->execute();
    $calibrationResult = $calibrationStmt->get_result();

    while ($calibrationResult && ($row = $calibrationResult->fetch_assoc())) {
        $monthKey = $row['month_key'];
        if (!array_key_exists($monthKey, $monthKeys)) {
            $monthKeys[$monthKey] = [
                'total' => 0,
                'approved' => 0,
                'rejected' => 0,
                'avg_interval_days' => null,
                'avg_remaining_days' => null,
                'delayed' => 0,
            ];
        }
        $monthKeys[$monthKey]['total'] = (int) ($row['total'] ?? 0);
        $monthKeys[$monthKey]['approved'] = (int) ($row['approved'] ?? 0);
        $monthKeys[$monthKey]['rejected'] = (int) ($row['rejected'] ?? 0);
        $monthKeys[$monthKey]['avg_interval_days'] = isset($row['avg_interval_days']) ? (float) $row['avg_interval_days'] : null;
        $monthKeys[$monthKey]['avg_remaining_days'] = isset($row['avg_remaining_days']) ? (float) $row['avg_remaining_days'] : null;
        $monthKeys[$monthKey]['delayed'] = (int) ($row['delayed'] ?? 0);
    }

    $calibrationStmt->close();

    $planSql = <<<SQL
        SELECT
            DATE_FORMAT(p.fecha_programada, '%Y-%m') AS month_key,
            SUM(CASE WHEN DATEDIFF(p.fecha_programada, ?) < 0 THEN 1 ELSE 0 END) AS bucket_overdue,
            SUM(CASE WHEN DATEDIFF(p.fecha_programada, ?) BETWEEN 0 AND 30 THEN 1 ELSE 0 END) AS bucket_0_30,
            SUM(CASE WHEN DATEDIFF(p.fecha_programada, ?) BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS bucket_31_60,
            SUM(CASE WHEN DATEDIFF(p.fecha_programada, ?) >= 61 THEN 1 ELSE 0 END) AS bucket_61_plus
        FROM planes p
        LEFT JOIN instrumentos i ON i.id = p.instrumento_id AND i.empresa_id = p.empresa_id
        WHERE p.empresa_id = ?
          AND p.fecha_programada IS NOT NULL
          AND p.fecha_programada BETWEEN ? AND ?
        GROUP BY month_key
        ORDER BY month_key
    SQL;

    $planStmt = $conn->prepare($planSql);
    if (!$planStmt) {
        throw new RuntimeException('No se pudo preparar la consulta de vencimientos.');
    }

    $planStmt->bind_param('ssssiss', $today, $today, $today, $today, $empresaId, $startDate, $endDate);
    $planStmt->execute();
    $planResult = $planStmt->get_result();

    $planBuckets = [];
    while ($planResult && ($row = $planResult->fetch_assoc())) {
        $monthKey = $row['month_key'];
        if (!array_key_exists($monthKey, $planBuckets)) {
            $planBuckets[$monthKey] = [
                'overdue' => 0,
                '0_30' => 0,
                '31_60' => 0,
                '61_plus' => 0,
            ];
        }
        $planBuckets[$monthKey]['overdue'] = (int) ($row['bucket_overdue'] ?? 0);
        $planBuckets[$monthKey]['0_30'] = (int) ($row['bucket_0_30'] ?? 0);
        $planBuckets[$monthKey]['31_60'] = (int) ($row['bucket_31_60'] ?? 0);
        $planBuckets[$monthKey]['61_plus'] = (int) ($row['bucket_61_plus'] ?? 0);
    }

    $planStmt->close();

    $series = [];
    $dueSeries = [];
    $lastMonthSummary = null;

    foreach ($months as $monthKey) {
        $calibrationRow = $monthKeys[$monthKey] ?? [
            'total' => 0,
            'approved' => 0,
            'rejected' => 0,
            'avg_interval_days' => null,
            'avg_remaining_days' => null,
            'delayed' => 0,
        ];

        $total = $calibrationRow['total'] ?? 0;
        $approved = $calibrationRow['approved'] ?? 0;
        $rejected = $calibrationRow['rejected'] ?? 0;

        $complianceRate = $total > 0 ? round(($approved / $total) * 100, 2) : null;
        $rejectionRate = $total > 0 ? round(($rejected / $total) * 100, 2) : null;

        $series[] = [
            'month' => $monthKey,
            'total' => $total,
            'approved' => $approved,
            'rejected' => $rejected,
            'compliance_rate' => $complianceRate,
            'rejection_rate' => $rejectionRate,
            'avg_interval_days' => $calibrationRow['avg_interval_days'],
            'avg_remaining_days' => $calibrationRow['avg_remaining_days'],
            'delayed' => $calibrationRow['delayed'] ?? 0,
        ];

        $bucketRow = $planBuckets[$monthKey] ?? [
            'overdue' => 0,
            '0_30' => 0,
            '31_60' => 0,
            '61_plus' => 0,
        ];

        $dueTotal = array_sum($bucketRow);
        $dueSeries[] = [
            'month' => $monthKey,
            'overdue' => $bucketRow['overdue'],
            'bucket_0_30' => $bucketRow['0_30'],
            'bucket_31_60' => $bucketRow['31_60'],
            'bucket_61_plus' => $bucketRow['61_plus'],
            'total' => $dueTotal,
        ];

        $lastMonthSummary = [
            'month' => $monthKey,
            'compliance_rate' => $complianceRate,
            'rejection_rate' => $rejectionRate,
            'avg_remaining_days' => $calibrationRow['avg_remaining_days'],
            'due_total' => $dueTotal,
        ];
    }

    $response = [
        'generated_at' => date(DATE_ATOM),
        'filters' => [
            'start' => $startDate,
            'end' => $endDate,
            'company_id' => $empresaId,
        ],
        'months' => $months,
        'series' => [
            'calibrations' => $series,
        ],
        'due_buckets' => $dueSeries,
        'summaries' => [
            'latest' => $lastMonthSummary,
        ],
    ];

    echo json_encode($response);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['error' => $e->getMessage()]);
} finally {
    if (isset($conn) && $conn instanceof mysqli) {
        $conn->close();
    }
}