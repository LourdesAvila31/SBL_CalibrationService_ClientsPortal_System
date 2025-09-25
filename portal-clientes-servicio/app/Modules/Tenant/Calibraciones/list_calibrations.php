<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}
$puedeAprobar = check_permission('calibraciones_aprobar');
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_schedule.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_status.php';
$empresaId = obtenerEmpresaId();
header('Content-Type: application/json');
$filtro = filter_input(INPUT_GET, 'instrumento_id', FILTER_VALIDATE_INT);
$filtroTecnico = filter_input(INPUT_GET, 'tecnico_id', FILTER_VALIDATE_INT);
$filtroTecnicoRaw = $_GET['tecnico_id'] ?? null;
$filtrarTecnicoSinAsignar = $filtroTecnicoRaw === 'sin_asignar';

if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$zonaHoraria = obtenerZonaHorariaEmpresa($conn, $empresaId);

$sql = "SELECT
            p.id AS plan_id,
            e.id AS instrumento_id,
            e.catalogo_id AS catalogo_id,
            ci.nombre AS instrumento,
            e.codigo,
            p.fecha_programada,
            p.estado AS estado_plan,
            COALESCE(cal_rel.id, cal_fb.id) AS calibracion_id,
            COALESCE(cal_rel.fecha_calibracion, cal_fb.fecha_calibracion) AS fecha_calibracion,
            COALESCE(cal_rel.fecha_proxima, cal_fb.fecha_proxima) AS fecha_proxima,
            COALESCE(cal_rel.estado, cal_fb.estado, 'Pendiente') AS estado,
            COALESCE(cal_rel.resultado_preliminar, cal_fb.resultado_preliminar) AS resultado_preliminar,
            COALESCE(cal_rel.resultado, cal_fb.resultado) AS resultado_liberado,
            COALESCE(cal_rel.periodo, cal_fb.periodo) AS periodo,
            COALESCE(cal_rel.tipo, cal_fb.tipo) AS tipo,
            COALESCE(cal_rel.patron_id, cal_fb.patron_id) AS patron_id,
            COALESCE(cal_rel.patron_certificado, cal_fb.patron_certificado, pat_rel.certificado_numero, pat_fb.certificado_numero) AS patron_certificado,
            COALESCE(pat_rel.nombre, pat_fb.nombre) AS patron_nombre,
            COALESCE(pat_rel.certificado_archivo, pat_fb.certificado_archivo) AS patron_certificado_archivo,
            COALESCE(pat_rel.fecha_vencimiento, pat_fb.fecha_vencimiento) AS patron_fecha_vencimiento,
            COALESCE(cal_rel.duracion_horas, cal_fb.duracion_horas) AS duracion_horas,
            COALESCE(cal_rel.costo_total, cal_fb.costo_total) AS costo_total,
            COALESCE(cal_rel.u_value, cal_fb.u_value) AS u_value,
            COALESCE(cal_rel.u_method, cal_fb.u_method) AS u_method,
            COALESCE(cal_rel.u_k, cal_fb.u_k) AS u_k,
            COALESCE(cal_rel.u_coverage, cal_fb.u_coverage) AS u_coverage,
            COALESCE(cal_rel.estado_ejecucion, cal_fb.estado_ejecucion) AS estado_ejecucion,
            COALESCE(cal_rel.motivo_reprogramacion, cal_fb.motivo_reprogramacion) AS motivo_reprogramacion,
            COALESCE(cal_rel.motivo_rechazo, cal_fb.motivo_rechazo) AS motivo_rechazo,
            COALESCE(cal_rel.fecha_reprogramada, cal_fb.fecha_reprogramada) AS fecha_reprogramada,
            COALESCE(cal_rel.dias_atraso, cal_fb.dias_atraso) AS dias_atraso_registrado,
            COALESCE(cal_rel.observaciones, cal_fb.observaciones) AS observaciones,
            COALESCE(cal_rel.usuario_id, cal_fb.usuario_id) AS tecnico_id,
            CONCAT_WS(' ', tech.nombre, tech.apellidos) AS tecnico_nombre
            
        FROM planes p
        JOIN instrumentos e ON e.id = p.instrumento_id
        LEFT JOIN catalogo_instrumentos ci ON e.catalogo_id = ci.id
        LEFT JOIN calibraciones_planes cp ON cp.plan_id = p.id
        LEFT JOIN calibraciones cal_rel ON cal_rel.id = cp.calibracion_id AND cal_rel.empresa_id = ?
        LEFT JOIN calibraciones cal_fb ON cp.calibracion_id IS NULL
            AND cal_fb.instrumento_id = e.id
            AND cal_fb.empresa_id = ?
            AND cal_fb.id = (
                SELECT c2.id
                FROM calibraciones c2
                WHERE c2.instrumento_id = e.id
                  AND c2.empresa_id = ?
                  AND MONTH(c2.fecha_calibracion) = MONTH(p.fecha_programada)
                  AND YEAR(c2.fecha_calibracion) = YEAR(p.fecha_programada)
                ORDER BY c2.fecha_calibracion DESC, c2.id DESC
                LIMIT 1
            )

        LEFT JOIN patrones pat_rel ON pat_rel.id = cal_rel.patron_id AND pat_rel.empresa_id = e.empresa_id
        LEFT JOIN patrones pat_fb ON pat_fb.id = cal_fb.patron_id AND pat_fb.empresa_id = e.empresa_id
        LEFT JOIN usuarios tech ON tech.id = COALESCE(cal_rel.usuario_id, cal_fb.usuario_id)
        WHERE e.empresa_id = ?";

$params = [$empresaId, $empresaId, $empresaId, $empresaId];
$types = 'iiii';

if ($filtro) {
    $sql .= ' AND p.instrumento_id = ?';
    $params[] = $filtro;
    $types .= 'i';
}

if ($filtrarTecnicoSinAsignar) {
    $sql .= ' AND COALESCE(cal_rel.usuario_id, cal_fb.usuario_id) IS NULL';
} elseif ($filtroTecnico) {
    $sql .= ' AND COALESCE(cal_rel.usuario_id, cal_fb.usuario_id) = ?';
    $params[] = $filtroTecnico;
    $types .= 'i';
}

$sql .= ' ORDER BY p.fecha_programada';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta.']);
    $conn->close();
    exit;
}

$bindParams = [];
foreach ($params as $index => $value) {
    $bindParams[$index] = &$params[$index];
}
array_unshift($bindParams, $types);
call_user_func_array([$stmt, 'bind_param'], $bindParams);

    if (!$stmt->execute()) {
        http_response_code(500);
        echo json_encode(['error' => 'No se pudo ejecutar la consulta de calibraciones.']);
        $stmt->close();
        $conn->close();
        exit;
    }

    $res = $stmt->get_result();
    $rows = [];
    $downloadBase = '/backend/patrones/download_certificate.php?id=';
    $calibrationIds = [];
    while ($row = $res->fetch_assoc()) {
    $fechaProgramada = $row['fecha_programada'] ?? null;
    $fechaCalibracion = $row['fecha_calibracion'] ?? null;
    $fechaReprogramada = $row['fecha_reprogramada'] ?? null;
    $diasRegistrados = $row['dias_atraso_registrado'];

    $diasCalculados = calibration_compute_delay($fechaProgramada, $fechaCalibracion, $fechaReprogramada);
    $diasFinal = $diasRegistrados !== null ? (int) $diasRegistrados : ($diasCalculados ?? null);

    $estadoEjecucion = $row['estado_ejecucion'] ?? null;
    if (!$estadoEjecucion) {
        $estadoEjecucion = $row['calibracion_id'] ? 'Completada' : 'Programada';
    }

    $decision = calibration_prepare_decision_output(
        $row['estado'] ?? null,
        $row['resultado_preliminar'] ?? null,
        $row['resultado_liberado'] ?? null
    );

    $requiereJustificacion = calibration_requires_justification($estadoEjecucion, $diasFinal);
    $motivo = trim((string) ($row['motivo_reprogramacion'] ?? ''));

    $row['estado_ejecucion'] = $estadoEjecucion;
    $row['dias_atraso'] = $diasFinal;
    $row['requiere_justificacion'] = $requiereJustificacion;
    $row['justificacion_pendiente'] = $requiereJustificacion && $motivo === '';
    $row['estado_resultado'] = $row['calibracion_id'] ? 'Actualizado' : 'Pendiente';
    $row['tecnico_id'] = isset($row['tecnico_id']) ? (int) $row['tecnico_id'] : null;

    $patronNombre = trim((string) ($row['patron_nombre'] ?? ''));
    if ($patronNombre === '' && !empty($row['patron_id'])) {
        $patronNombre = 'Patrón #' . (int) $row['patron_id'];
    }
    $row['patron'] = $patronNombre !== '' ? $patronNombre : null;

    $patronCertificado = trim((string) ($row['patron_certificado'] ?? ''));
    $row['patron_certificado'] = $patronCertificado !== '' ? $patronCertificado : null;

    if (!empty($row['patron_id']) && !empty($row['patron_certificado_archivo'])) {
        $row['patron_certificado_url'] = $downloadBase . (int) $row['patron_id'];
    } else {
        $row['patron_certificado_url'] = null;
    }

    if (!empty($row['calibracion_id'])) {
        $calibrationIds[] = (int) $row['calibracion_id'];
    }

    // Calcula el estado/días restantes respetando la zona horaria configurada por el tenant.
    if ($zonaHoraria instanceof DateTimeZone) {
        $estado = calcularEstadoCalibracion($row['fecha_proxima'] ?? null, $zonaHoraria);
    } else {
        $estado = calcularEstadoCalibracion($row['fecha_proxima'] ?? null);
    }
    $row['estado_calibracion'] = $estado['estado_calibracion'];
    $row['dias_restantes'] = $estado['dias_restantes'];

    $rows[] = $row;

}
    $stmt->close();
    $referencesByCalibration = calibration_references_fetch($conn, $calibrationIds);
    foreach ($rows as &$row) {
        $calibrationId = isset($row['calibracion_id']) ? (int) $row['calibracion_id'] : 0;
        $row['referencias'] = ($calibrationId && isset($referencesByCalibration[$calibrationId]))
            ? $referencesByCalibration[$calibrationId]
            : [];
    }
    unset($row);

$json = json_encode($rows, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
if ($json === false) {
    http_response_code(500);
    $mensaje = json_last_error_msg() ?: 'Error al listar las calibraciones.';
    echo json_encode(['error' => $mensaje], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
} else {
    echo $json;
}
$conn->close();
?>
