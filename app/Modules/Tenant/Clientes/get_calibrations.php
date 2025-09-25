<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_status.php';

header('Content-Type: application/json');

ensure_portal_access('tenant');

$roleAlias = session_role_alias() ?? '';
if ($roleAlias !== 'cliente') {
    http_response_code(403);
    echo json_encode(['calibrations' => [], 'message' => 'Acceso denegado.']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['calibrations' => [], 'message' => 'No se pudo determinar la empresa asociada.']);
    exit;
}

$sql = "SELECT cal.id, cal.instrumento_id, cal.fecha_calibracion, cal.fecha_proxima, cal.resultado, cal.observaciones, cal.tipo, cal.duracion_horas, cal.costo_total,\n       cal.liberado_por, cal.fecha_liberacion,\n       i.codigo, COALESCE(ci.nombre, '') AS instrumento\n        FROM calibraciones cal\n        JOIN instrumentos i ON cal.instrumento_id = i.id\n        LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id\n       WHERE cal.empresa_id = ?\n  ORDER BY cal.fecha_calibracion DESC, cal.id DESC";
$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['calibrations' => [], 'message' => 'No se pudo recuperar la información.']);
    exit;
}
$stmt->bind_param('i', $empresaId);
$stmt->execute();
$calibrations = [];
if (method_exists($stmt, 'get_result')) {
    $res = $stmt->get_result();
    while ($row = $res->fetch_assoc()) {
        $row['id'] = (int)$row['id'];
        $row['instrumento_id'] = (int)$row['instrumento_id'];
        // Se agregan "estado_calibracion" y "dias_restantes" para que los
        // consumidores externos puedan reutilizar la misma semántica que el
        // tablero principal.
        $status = calcularEstadoCalibracion($row['fecha_proxima'] ?? null);
        $row['estado_calibracion'] = $status['estado_calibracion'];
        $row['dias_restantes'] = $status['dias_restantes'];
        $row['certificates'] = [];
        $row['liberado_por'] = $row['liberado_por'] !== null ? (int) $row['liberado_por'] : null;
        $row['fecha_liberacion'] = $row['fecha_liberacion'] ?? null;
        $calibrations[$row['id']] = $row;
    }
} else {
    $stmt->bind_result($id, $instrumentoId, $fechaCal, $fechaProx, $resultado, $observaciones, $tipo, $duracionHoras, $costoTotal, $liberadoPor, $fechaLiberacion, $codigo, $instrumento);
    while ($stmt->fetch()) {
        $status = calcularEstadoCalibracion($fechaProx);
        $calibrations[(int)$id] = [
            'id' => (int)$id,
            'instrumento_id' => (int)$instrumentoId,
            'fecha_calibracion' => $fechaCal,
            'fecha_proxima' => $fechaProx,
            // Se incluyen los campos derivados del estatus de calibración para que
            // el portal de clientes mantenga el mismo contrato que el backend.
            'estado_calibracion' => $status['estado_calibracion'],
            'dias_restantes' => $status['dias_restantes'],
            'resultado' => $resultado,
            'observaciones' => $observaciones,
            'tipo' => $tipo,
            'duracion_horas' => $duracionHoras,
            'costo_total' => $costoTotal,
            'codigo' => $codigo,
            'instrumento' => $instrumento,
            'certificates' => [],
            'liberado_por' => $liberadoPor !== null ? (int)$liberadoPor : null,
            'fecha_liberacion' => $fechaLiberacion
        ];
    }
}
$stmt->close();

$ids = array_keys($calibrations);
if ($ids) {
    $certSql = "SELECT c.id, c.calibracion_id, c.archivo, c.fecha_subida\n                FROM certificados c\n                JOIN calibraciones cal ON cal.id = c.calibracion_id\n                WHERE cal.empresa_id = ?\n                  AND cal.liberado_por IS NOT NULL\n                  AND cal.fecha_liberacion IS NOT NULL";
    $certStmt = $conn->prepare($certSql);
    if ($certStmt) {
        $certStmt->bind_param('i', $empresaId);
        $certStmt->execute();
        if (method_exists($certStmt, 'get_result')) {
            $res = $certStmt->get_result();
            while ($row = $res->fetch_assoc()) {
                $calId = (int)$row['calibracion_id'];
                if (isset($calibrations[$calId])) {
                    $calibrations[$calId]['certificates'][] = [
                        'id' => (int)$row['id'],
                        'file' => $row['archivo'],
                        'fecha_subida' => $row['fecha_subida']
                    ];
                }
            }
        } else {
            $certStmt->bind_result($certId, $calId, $archivo, $fechaSubida);
            while ($certStmt->fetch()) {
                $calId = (int)$calId;
                if (isset($calibrations[$calId])) {
                    $calibrations[$calId]['certificates'][] = [
                        'id' => (int)$certId,
                        'file' => $archivo,
                        'fecha_subida' => $fechaSubida
                    ];
                }
            }
        }
        $certStmt->close();
    }
}

$conn->close();

echo json_encode(['calibrations' => array_values($calibrations)]);
