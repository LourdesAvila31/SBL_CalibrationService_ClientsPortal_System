<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');
header('Content-Type: application/json');



if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/calibration_status.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';
require_once __DIR__ . '/../attachments/InstrumentAttachmentService.php';

const GAGE_DETAILS_MISSING_CERTIFICATE_MESSAGE = 'No se ha añadido su primer certificado';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$id = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
if (!$id) {
    echo json_encode(['error' => 'ID inválido']);
    exit;
}

$zonaHoraria = obtenerZonaHorariaEmpresa($conn, $empresaId);

$stmt = $conn->prepare(
    "SELECT i.id, ci.nombre AS nombre, m.nombre AS marca, mo.nombre AS modelo, i.serie, i.codigo, d.nombre AS departamento, i.ubicacion, i.fecha_alta, i.fecha_baja, i.proxima_calibracion, i.estado, i.programado
     FROM instrumentos i
     LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
     LEFT JOIN marcas m ON i.marca_id = m.id
     LEFT JOIN modelos mo ON i.modelo_id = mo.id
     LEFT JOIN departamentos d ON i.departamento_id = d.id
     WHERE i.id = ? AND i.empresa_id = ?"
);
$stmt->bind_param('ii', $id, $empresaId);
$stmt->execute();
$res = $stmt->get_result();
if (!$res || $res->num_rows === 0) {
    echo json_encode(['error' => 'Instrumento no encontrado']);
    exit;
}
// La respuesta incluye "estado_calibracion" y "dias_restantes" calculados a partir de
// la próxima calibración para mantener alineados a los consumidores externos con el
// listado principal de instrumentos.
$registroInstrumento = $res->fetch_assoc();
if ($zonaHoraria instanceof DateTimeZone) {
    $instrumento = anexarEstadoCalibracion($registroInstrumento, $zonaHoraria);
} else {
    $instrumento = anexarEstadoCalibracion($registroInstrumento);
}
$instrumento['proxima_calibracion_mensaje'] = null;
$estadoInfo = instrumento_estado_operativo_info($instrumento['estado'] ?? null);
$instrumento['estado_operativo'] = $estadoInfo['estado_operativo'];
$instrumento['estado_operativo_label'] = $estadoInfo['estado_operativo_label'];
$instrumento['estado_operativo_historial'] = [];
$stmt->close();

// Historial de departamento responsable
$deptoHist = [];
$stmt = $conn->prepare(
    "SELECT d.nombre AS departamento, hd.fecha
     FROM historial_departamentos hd
     JOIN departamentos d ON hd.departamento_id = d.id
     WHERE hd.instrumento_id = ? AND hd.empresa_id = ?
     ORDER BY hd.fecha DESC"
);
$stmt->bind_param('ii', $id, $empresaId);
$stmt->execute();
if ($resDep = $stmt->get_result()) {
    while ($row = $resDep->fetch_assoc()) {
        $deptoHist[] = $row;
    }
}
$stmt->close();
$instrumento['departamento_historial'] = $deptoHist;

// Historial de tipo de instrumento
$estadoHist = [];
$stmt = $conn->prepare(
    "SELECT estado, fecha FROM historial_tipos_instrumento
     WHERE instrumento_id = ? AND empresa_id = ?
     ORDER BY fecha DESC"
);
$stmt->bind_param('ii', $id, $empresaId);
$stmt->execute();
if ($resEstado = $stmt->get_result()) {
    while ($row = $resEstado->fetch_assoc()) {
        $estadoNormalizado = instrumento_normalizar_estado_operativo($row['estado'] ?? null);
        $row['estado_operativo'] = $estadoNormalizado;
        $row['estado_operativo_label'] = instrumento_estado_operativo_label($row['estado'] ?? null, $estadoNormalizado);
        $estadoHist[] = $row;
    }
}
$stmt->close();
$instrumento['estado_historial'] = $estadoHist;
$instrumento['estado_operativo_historial'] = $estadoHist;

// Requerimientos de calibración (editable)
$stmt = $conn->prepare(
    "SELECT requerimiento FROM requerimientos_calibracion WHERE instrumento_id = ? AND empresa_id = ? LIMIT 1"
);
$stmt->bind_param('ii', $id, $empresaId);
$stmt->execute();
$instrumento['requerimientos_calibracion'] = '';
if ($resReq = $stmt->get_result()) {
    if ($reqRow = $resReq->fetch_assoc()) {
        $instrumento['requerimientos_calibracion'] = $reqRow['requerimiento'];
    }
}
$stmt->close();

// Plan basado en riesgos
$planColumns = [
    'requerimiento',
    'impacto_falla',
    'consideraciones_falla',
    'clase_riesgo',
    'capacidad_deteccion',
    'frecuencia',
    'observaciones',
    'tipo_calibracion',
    'fecha_actualizacion'
];

$hasEspecificaciones = false;
try {
    $colResult = $conn->query("SHOW COLUMNS FROM plan_riesgos LIKE 'especificaciones'");
    if ($colResult) {
        $hasEspecificaciones = $colResult->num_rows > 0;
        $colResult->close();
    }
} catch (Throwable $e) {
    // Ignoramos errores y asumimos que la columna no existe.
    $hasEspecificaciones = false;
}

if ($hasEspecificaciones) {
    array_splice($planColumns, count($planColumns) - 1, 0, ['especificaciones']);
}

$planSelect = implode(', ', $planColumns);
$stmt = $conn->prepare(
    "SELECT $planSelect FROM plan_riesgos WHERE instrumento_id = ? AND empresa_id = ? LIMIT 1"
);
if ($stmt) {
    $stmt->bind_param('ii', $id, $empresaId);

    $stmt->execute();
}
$instrumento['plan_riesgos'] = [
    'requerimiento' => '',
    'impacto_falla' => '',
    'consideraciones_falla' => '',
    'clase_riesgo' => '',
    'capacidad_deteccion' => '',
    'frecuencia' => '',
    'observaciones' => '',
    'tipo_calibracion' => '',
    'especificaciones' => '',
    'fecha_actualizacion' => '',
    'descripcion' => ''
];
if ($stmt) {
    if ($resPlan = $stmt->get_result()) {
        if ($planRow = $resPlan->fetch_assoc()) {
            if (!$hasEspecificaciones) {
                $planRow['especificaciones'] = '';
            }
            $clase = strtolower(trim((string)($planRow['clase_riesgo'] ?? '')));
            if ($clase !== '') {
                switch ($clase) {
                    case 'alto':
                        $planRow['descripcion'] = 'Alta';
                        break;
                    case 'medio':
                        $planRow['descripcion'] = 'Media';
                        break;
                    case 'bajo':
                        $planRow['descripcion'] = 'Baja';
                        break;
                    default:
                        $planRow['descripcion'] = $planRow['clase_riesgo'];
                        break;
                }
            } else {
                $planRow['descripcion'] = '';
            }

            $instrumento['plan_riesgos'] = $planRow;
            $observacionPlan = trim((string)($planRow['observaciones'] ?? ''));
            if ($observacionPlan === GAGE_DETAILS_MISSING_CERTIFICATE_MESSAGE) {
                $instrumento['proxima_calibracion_mensaje'] = GAGE_DETAILS_MISSING_CERTIFICATE_MESSAGE;
                $instrumento['proxima_calibracion'] = null;
            }
        }
    }
    $stmt->close();
}

// Historial de calibraciones con certificados adjuntos
$historial = [];
$stmt = $conn->prepare(
    "SELECT c.id, c.fecha_calibracion, c.fecha_proxima, c.resultado, c.tipo, c.duracion_horas, c.costo_total,
            c.liberado_por, c.fecha_liberacion,
            CONCAT(TRIM(COALESCE(u.nombre, '')), ' ', TRIM(COALESCE(u.apellidos, ''))) AS liberado_por_nombre
     FROM calibraciones c
     LEFT JOIN usuarios u ON u.id = c.liberado_por
     WHERE c.instrumento_id = ? AND c.empresa_id = ?
     ORDER BY c.fecha_calibracion DESC"
);
$stmt->bind_param('ii', $id, $empresaId);
$stmt->execute();
if ($resCal = $stmt->get_result()) {
    while ($row = $resCal->fetch_assoc()) {
        $certificados = [];
        $calId = intval($row['id']);
        $liberadoPor = $row['liberado_por'] ?? null;
        $fechaLiberacion = $row['fecha_liberacion'] ?? null;
        $aprobado = $liberadoPor !== null && $fechaLiberacion !== null;
        $certificadoRegistrado = false;

        $stmtCert = $conn->prepare(
            "SELECT archivo FROM certificados c
             JOIN calibraciones cal ON c.calibracion_id = cal.id
             WHERE c.calibracion_id = ? AND cal.empresa_id = ?"
        );
        if ($stmtCert) {
            $stmtCert->bind_param('ii', $calId, $empresaId);
            $stmtCert->execute();
            $resCert = $stmtCert->get_result();
            while ($cert = $resCert->fetch_assoc()) {
                $certificadoRegistrado = true;
                if ($aprobado) {
                    $certificados[] = $cert['archivo'];
                }
            }
            $stmtCert->close();
        }

        $row['certificados'] = $certificados;
        $row['liberado_por'] = $liberadoPor !== null ? (int) $liberadoPor : null;
        $row['liberado_por_nombre'] = $row['liberado_por_nombre'] !== null ? trim((string) $row['liberado_por_nombre']) : null;
        $row['fecha_liberacion'] = $fechaLiberacion;
        if ($aprobado) {
            $row['certificado_estado'] = 'liberado';
        } elseif ($certificadoRegistrado) {
            $row['certificado_estado'] = 'pendiente';
        } else {
            $row['certificado_estado'] = 'no_registrado';
        }
        $historial[] = $row;
    }
}
$stmt->close();

$instrumento['calibraciones'] = $historial;

try {
    $adjuntos = InstrumentAttachmentService::listAttachments($conn, $id, $empresaId);
    $instrumento['adjuntos'] = $adjuntos;
    $instrumento['adjuntos_total'] = count($adjuntos);
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[instrumento_adjuntos] Error en detalles: ' . $e->getMessage() . PHP_EOL);
    $instrumento['adjuntos'] = [];
    $instrumento['adjuntos_total'] = 0;
}
// El JSON expone los campos calculados de estado para permitir que integraciones
// externas repliquen la lógica de semáforos sin consultas adicionales.
echo json_encode($instrumento);
$conn->close();
?>
