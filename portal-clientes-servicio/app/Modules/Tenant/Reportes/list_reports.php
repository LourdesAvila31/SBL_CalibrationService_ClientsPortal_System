<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
header('Content-Type: application/json');

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once __DIR__ . '/custom_report_definitions.php';

$reportes = [];
$empresaId = obtenerEmpresaId();

$sql = "SELECT rp.id, rp.nombre, rp.fecha_creacion, rp.formato_preferido, rp.configuracion, rp.filtros,
               u.usuario AS creado_por
        FROM reportes_personalizados rp
        LEFT JOIN usuarios u ON rp.usuario_id = u.id
        WHERE rp.empresa_id = ?
        ORDER BY rp.id DESC";

$definitions = custom_report_definitions();
$allowedFormats = custom_report_allowed_formats();

if ($stmt = $conn->prepare($sql)) {
    $stmt->bind_param('i', $empresaId);
    $stmt->execute();
    $res = $stmt->get_result();
    while ($res && ($row = $res->fetch_assoc())) {
        $configData = [];
        $decodedConfig = json_decode($row['configuracion'] ?? '', true);
        if (is_array($decodedConfig)) {
            $configData = $decodedConfig;
        }

        $datasetKey = (string) ($configData['dataset'] ?? '');
        $datasetDefinition = $definitions[$datasetKey] ?? null;
        $datasetLabel = $datasetDefinition['label'] ?? ($datasetKey !== '' ? ucfirst($datasetKey) : 'Sin dataset');

        $columns = $configData['columnas'] ?? [];
        if (!is_array($columns)) {
            $columns = [];
        }
        if (empty($columns) && $datasetDefinition) {
            $columns = array_values($datasetDefinition['default_columns'] ?? []);
        }

        $columnLabels = [];
        foreach ($columns as $colKey) {
            $label = $datasetDefinition['columns'][$colKey]['label'] ?? (string) $colKey;
            $columnLabels[] = $label;
        }

        $columnSummary = 'Sin columnas seleccionadas';
        if (!empty($columnLabels)) {
            $display = array_slice($columnLabels, 0, 6);
            if (count($columnLabels) > 6) {
                $display[] = '…';
            }
            $columnSummary = implode(', ', $display);
        }

        $filters = [];
        $decodedFilters = json_decode($row['filtros'] ?? '', true);
        if (is_array($decodedFilters)) {
            foreach ($decodedFilters as $key => $value) {
                if (is_scalar($value)) {
                    $filters[(string) $key] = (string) $value;
                }
            }
        }

        $filterParts = [];
        $fechaDesde = $filters['fecha_desde'] ?? '';
        $fechaHasta = $filters['fecha_hasta'] ?? '';
        if ($fechaDesde !== '' || $fechaHasta !== '') {
            if ($fechaDesde !== '' && $fechaHasta !== '') {
                $filterParts[] = 'Fecha: ' . $fechaDesde . ' a ' . $fechaHasta;
            } elseif ($fechaDesde !== '') {
                $filterParts[] = 'Desde: ' . $fechaDesde;
            } else {
                $filterParts[] = 'Hasta: ' . $fechaHasta;
            }
        }
        if (!empty($filters['estado'])) {
            $filterParts[] = 'Estado: ' . $filters['estado'];
        }

        $filtersSummary = $filterParts ? implode(' • ', $filterParts) : 'Sin filtros';

        $formatKey = strtolower((string) ($row['formato_preferido'] ?? ''));
        if (!in_array($formatKey, $allowedFormats, true)) {
            $formatKey = $allowedFormats[0];
        }

        $reportes[] = [
            'id' => (int) $row['id'],
            'nombre' => $row['nombre'],
            'dataset' => $datasetKey,
            'dataset_label' => $datasetLabel,
            'columnas' => $columns,
            'columnas_resumen' => $columnSummary,
            'filtros' => $filters,
            'filtros_resumen' => $filtersSummary,
            'formato_preferido' => $formatKey,
            'formato_label' => strtoupper($formatKey),
            'fecha_creacion' => $row['fecha_creacion'],
            'creado_por' => $row['creado_por'] ?? '',
        ];
    }
    $stmt->close();
}

echo json_encode($reportes);
?>