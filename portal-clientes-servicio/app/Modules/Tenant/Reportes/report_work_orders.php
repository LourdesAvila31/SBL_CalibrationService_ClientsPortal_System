<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    exit;
}

require_once __DIR__ . '/report_helpers.php';

global $conn;
$empresaId = require_empresa_id();
$format = strtolower((string) request_input('format', 'pdf'));

function normalizar_estado_orden(?string $estado): string
{
    $texto = trim((string) $estado);
    if ($texto === '') {
        return 'Programada';
    }

    $texto = strtr($texto, [
        'Á' => 'a', 'á' => 'a',
        'É' => 'e', 'é' => 'e',
        'Í' => 'i', 'í' => 'i',
        'Ó' => 'o', 'ó' => 'o',
        'Ú' => 'u', 'ú' => 'u',
        'Ü' => 'u', 'ü' => 'u',
    ]);
    $texto = mb_strtolower($texto, 'UTF-8');
    $compacto = preg_replace('/[^a-z]+/u', '_', $texto);
    if (!is_string($compacto) || $compacto === '') {
        return 'Programada';
    }
    $compacto = trim($compacto, '_');
    if ($compacto === '') {
        return 'Programada';
    }

    $mapa = [
        'en_curso' => 'En curso',
        'encurso' => 'En curso',
        'en_ejecucion' => 'En curso',
        'enejecucion' => 'En curso',
        'ejecucion' => 'En curso',
        'en_proceso' => 'En curso',
        'enproceso' => 'En curso',
        'proceso' => 'En curso',
        'curso' => 'En curso',
        'completada' => 'Completada',
        'completado' => 'Completada',
        'finalizada' => 'Completada',
        'finalizado' => 'Completada',
        'terminada' => 'Completada',
        'terminado' => 'Completada',
        'cerrada' => 'Completada',
        'cerrado' => 'Completada',
        'concluida' => 'Completada',
        'concluido' => 'Completada',
        'ejecutada' => 'Completada',
        'ejecutado' => 'Completada',
        'cancelada' => 'Cancelada',
        'cancelado' => 'Cancelada',
        'anulada' => 'Cancelada',
        'anulado' => 'Cancelada',
        'suspendida' => 'Cancelada',
        'suspendido' => 'Cancelada',
        'detenida' => 'Cancelada',
        'detenido' => 'Cancelada',
        'sin_fecha' => 'Sin fecha',
        'sinfecha' => 'Sin fecha',
        'sin fecha' => 'Sin fecha',
        'programada' => 'Programada',
        'programado' => 'Programada',
        'pendiente' => 'Programada',
        'planificada' => 'Programada',
        'planificado' => 'Programada',
    ];

    return $mapa[$compacto] ?? 'Programada';
}

$sql = <<<SQL
SELECT
    p.id,
    COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) AS instrumento,
    i.codigo,
    DATE_FORMAT(p.fecha_programada, '%Y-%m-%d') AS fecha_programada,
    COALESCE(oc.estado_ejecucion, p.estado, 'Programada') AS estado_orden,
    COALESCE(oc.tecnico_id, p.responsable_id) AS tecnico_id,
    DATE_FORMAT(oc.fecha_inicio, '%Y-%m-%d') AS fecha_inicio,
    DATE_FORMAT(oc.fecha_cierre, '%Y-%m-%d') AS fecha_cierre,
    COALESCE(oc.observaciones, '') AS observaciones,
    COALESCE(
        CONCAT(t.nombre, ' ', t.apellidos),
        t.usuario,
        CONCAT('Usuario #', oc.tecnico_id),
        CONCAT('Usuario #', p.responsable_id)
    ) AS tecnico
FROM planes p
JOIN instrumentos i ON i.id = p.instrumento_id AND i.empresa_id = $empresaId
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN ordenes_calibracion oc ON oc.plan_id = p.id AND oc.empresa_id = p.empresa_id
LEFT JOIN usuarios t ON t.id = COALESCE(oc.tecnico_id, p.responsable_id) AND t.empresa_id = p.empresa_id
WHERE p.empresa_id = $empresaId
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
ORDER BY p.fecha_programada ASC, instrumento ASC
SQL;

$result = $conn->query($sql);
if (!$result) {
    respond_database_error($conn);
}

$rows = [];
while ($row = $result->fetch_assoc()) {
    $estadoOrden = normalizar_estado_orden($row['estado_orden'] ?? '');
    $tecnico = trim((string) ($row['tecnico'] ?? ''));
    if ($tecnico === '') {
        $tecnico = 'Sin asignar';
    }
    $rows[] = [
        $row['id'],
        $row['instrumento'],
        $row['codigo'],
        $row['fecha_programada'] ?? '-',
        $tecnico,
        $estadoOrden,
        $row['fecha_inicio'] ?? '-',
        $row['fecha_cierre'] ?? '-',
        $row['observaciones'] ?? '',
    ];
}

$headers = ['ID', 'Instrumento', 'Código', 'Fecha Programada', 'Técnico', 'Estado Orden', 'Inicio Orden', 'Cierre Orden', 'Observaciones'];

switch ($format) {
    case 'excel':
        stream_excel_report('calibration_work_orders.xls', $headers, $rows);
        break;
    case 'pdf':
        stream_pdf_report('calibration_work_orders.pdf', 'Órdenes de trabajo de calibración', $headers, $rows);
        break;
    case 'csv':
        stream_csv_report('calibration_work_orders.csv', $headers, $rows);
        break;
    default:
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo 'Formato no soportado';
        exit;
}
