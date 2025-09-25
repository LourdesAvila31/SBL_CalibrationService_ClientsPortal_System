<?php
session_start();

header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
ensure_portal_access('service');
if (!check_permission('clientes_gestionar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso no autorizado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_schedule.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_status.php';

$empresaId = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$instrumentoId = filter_input(INPUT_GET, 'instrumento_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);

try {
    $sql = "SELECT cal.id, cal.instrumento_id, cal.empresa_id, cal.fecha_calibracion, cal.fecha_proxima,
                   cal.resultado, cal.observaciones, cal.tipo, cal.duracion_horas, cal.costo_total,
                   cal.estado_ejecucion, cal.motivo_reprogramacion, cal.fecha_reprogramada, cal.dias_atraso,
                   i.codigo AS instrumento_codigo,
                   COALESCE(ci.nombre, '') AS instrumento_nombre,
                   e.nombre AS empresa_nombre
            FROM calibraciones cal
            LEFT JOIN instrumentos i ON cal.instrumento_id = i.id
            LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
            LEFT JOIN empresas e ON cal.empresa_id = e.id
            WHERE 1 = 1";

    $types = '';
    $params = [];

    if ($empresaId) {
        $sql .= ' AND cal.empresa_id = ?';
        $types .= 'i';
        $params[] = $empresaId;
    }

    if ($instrumentoId) {
        $sql .= ' AND cal.instrumento_id = ?';
        $types .= 'i';
        $params[] = $instrumentoId;
    }

    $sql .= ' ORDER BY cal.fecha_calibracion DESC, cal.id DESC';

    if ($types) {
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No fue posible obtener las calibraciones.');
        }
        $stmt->bind_param($types, ...$params);
        $stmt->execute();
        $result = $stmt->get_result();
    } else {
        $result = $conn->query($sql);
    }

    $calibraciones = [];
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $id = (int) ($row['id'] ?? 0);
            $diasRegistrados = isset($row['dias_atraso']) ? (int) $row['dias_atraso'] : null;
            $diasCalculados = calibration_compute_delay(null, $row['fecha_calibracion'] ?? null, $row['fecha_reprogramada'] ?? null);
            $diasFinal = $diasRegistrados ?? $diasCalculados;

            $estadoEjecucion = $row['estado_ejecucion'] ?? '';
            if ($estadoEjecucion === '') {
                $estadoEjecucion = ($row['resultado'] ?? '') !== '' ? 'Completada' : 'Programada';
            } else {
                $estadoEjecucion = calibration_normalize_status($estadoEjecucion);
            }

            $motivo = trim((string) ($row['motivo_reprogramacion'] ?? ''));
            $requiereJustificacion = calibration_requires_justification($estadoEjecucion, $diasFinal);
            $justificacionPendiente = $requiereJustificacion && $motivo === '';

            $status = calcularEstadoCalibracion($row['fecha_proxima'] ?? null);

            $calibraciones[$id] = [
                'id' => $id,
                'empresa_id' => (int) ($row['empresa_id'] ?? 0),
                'instrumento_id' => isset($row['instrumento_id']) ? (int) $row['instrumento_id'] : null,
                'fecha_calibracion' => $row['fecha_calibracion'] ?? null,
                'fecha_proxima' => $row['fecha_proxima'] ?? null,
                'fecha_reprogramada' => $row['fecha_reprogramada'] ?? null,
                'resultado' => $row['resultado'] ?? '',
                'observaciones' => $row['observaciones'] ?? '',
                'tipo' => $row['tipo'] ?? '',
                'duracion_horas' => $row['duracion_horas'],
                'costo_total' => $row['costo_total'],
                'estado_ejecucion' => $estadoEjecucion,
                'motivo_reprogramacion' => $motivo,
                'dias_atraso' => $diasFinal,
                'requiere_justificacion' => $requiereJustificacion,
                'justificacion_pendiente' => $justificacionPendiente,
                // El servicio expone el estado calculado y los días restantes para
                // alinear integraciones externas con el listado público.
                'estado_calibracion' => $status['estado_calibracion'],
                'dias_restantes' => $status['dias_restantes'],
                'instrumento_codigo' => $row['instrumento_codigo'] ?? '',
                'instrumento_nombre' => $row['instrumento_nombre'] ?? '',
                'empresa_nombre' => $row['empresa_nombre'] ?? '',
                'certificados' => [],
            ];
        }
    }

    if ($calibraciones) {
        $placeholders = implode(',', array_fill(0, count($calibraciones), '?'));
        $typesCert = str_repeat('i', count($calibraciones));
        $ids = array_keys($calibraciones);

        $certSql = "SELECT id, calibracion_id, archivo, fecha_subida FROM certificados WHERE calibracion_id IN ($placeholders)";
        $stmtCert = $conn->prepare($certSql);
        if ($stmtCert) {
            $stmtCert->bind_param($typesCert, ...$ids);
            $stmtCert->execute();
            $resCert = $stmtCert->get_result();
            if ($resCert) {
                while ($row = $resCert->fetch_assoc()) {
                    $calId = (int) ($row['calibracion_id'] ?? 0);
                    if (!isset($calibraciones[$calId])) {
                        continue;
                    }
                    $calibraciones[$calId]['certificados'][] = [
                        'id' => (int) ($row['id'] ?? 0),
                        'archivo' => $row['archivo'] ?? '',
                        'fecha_subida' => $row['fecha_subida'] ?? null,
                    ];
                }
            }
            $stmtCert->close();
        }
    }

    echo json_encode(['calibraciones' => array_values($calibraciones)]);
} catch (Throwable $e) {
    error_log('Error al listar calibraciones para servicio: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible obtener las calibraciones.']);
}
