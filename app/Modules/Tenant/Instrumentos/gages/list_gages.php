<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

header('Content-Type: application/json');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    echo json_encode(["error" => "Acceso denegado"]);
    exit;
}

/**
 * Determina si el usuario en sesión puede gestionar múltiples empresas.
 */
function usuarioPuedeGestionarMultiempresa(): bool
{
    static $cache = null;

    if ($cache !== null) {
        return $cache;
    }

    if (session_is_superadmin()) {
        return $cache = true;
    }

    $permisosMultiempresa = [
        'usuarios_view',
        'usuarios_add',
        'usuarios_edit',
        'usuarios_delete',
        'clientes_gestionar',
    ];

    foreach ($permisosMultiempresa as $permiso) {
        if (check_permission($permiso)) {
            return $cache = true;
        }
    }

    return $cache = false;
}

try {
    require_once dirname(__DIR__, 4) . '/Core/db.php';
    require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
    require_once dirname(__DIR__, 4) . '/Core/helpers/calibration_status.php';
    require_once dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';
} catch (Throwable $e) {
    file_put_contents('php://stderr', 'Error de conexión a la base de datos: ' . $e->getMessage() . PHP_EOL);
    http_response_code(500);
    echo json_encode(['error' => 'Error de conexión a la base de datos']);
    exit;
}

const GAGES_MISSING_CERTIFICATE_MESSAGE = 'No se ha añadido su primer certificado';

$empresaId = obtenerEmpresaId();
$empresaContexto = null;
if (!$empresaId && usuarioPuedeGestionarMultiempresa()) {
    $empresaInfo   = null;
    $empresaSolicitud = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT);
    if ($empresaSolicitud !== false && $empresaSolicitud !== null && $empresaSolicitud > 0) {
        $stmtEmpresa = $conn->prepare('SELECT id, nombre FROM empresas WHERE id = ? LIMIT 1');
        if ($stmtEmpresa) {
            $stmtEmpresa->bind_param('i', $empresaSolicitud);
            if ($stmtEmpresa->execute()) {
                $resultadoEmpresa = $stmtEmpresa->get_result();
                if ($resultadoEmpresa) {
                    $empresaInfo = $resultadoEmpresa->fetch_assoc() ?: null;
                    $resultadoEmpresa->free();
                }
            }
            $stmtEmpresa->close();
        }

        if ($empresaInfo) {
            $empresaId = (int) $empresaInfo['id'];
            $_SESSION['empresa_id'] = $empresaId;
            $empresaInfo['motivo'] = 'explicit';
        }
    }

    if (!$empresaId) {
        $resultado = $conn->query('SELECT id, nombre FROM empresas ORDER BY id LIMIT 1');
        if ($resultado instanceof mysqli_result) {
            $fila = $resultado->fetch_assoc();
            if ($fila) {
                $empresaId = (int) $fila['id'];
                $_SESSION['empresa_id'] = $empresaId;
                $empresaInfo = $fila;
                $empresaInfo['motivo'] = $empresaSolicitud ? 'fallback' : 'automatic';
            }
            $resultado->free();
        }
    }

    if (!empty($empresaInfo) && isset($empresaInfo['motivo'])) {
        $nombreEmpresa = trim((string) ($empresaInfo['nombre'] ?? ''));
        if ($nombreEmpresa === '') {
            $nombreEmpresa = 'empresa #' . $empresaId;
        }

        if ($empresaInfo['motivo'] === 'fallback') {
            $mensaje = 'Empresa solicitada no disponible. Mostrando inventario de ' . $nombreEmpresa . '.';
        } else {
            $mensaje = 'Mostrando inventario de ' . $nombreEmpresa . '.';
        }

        $mensaje = str_replace(["\r", "\n"], ' ', $mensaje);
        $empresaContexto = [
            'company_id'   => $empresaId,
            'company_name' => $nombreEmpresa,
            'mode'         => $empresaInfo['motivo'],
            'message'      => $mensaje,
        ];

        header('X-App-Company-Context: ' . $mensaje);
        header('X-App-Company-Id: ' . $empresaId);
        header('X-App-Company-Mode: ' . $empresaInfo['motivo']);
    }
}

if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$zonaHoraria = obtenerZonaHorariaEmpresa($conn, $empresaId);

// Parámetros de filtrado y ordenamiento
$filtro = filter_input(INPUT_GET, 'filtro') ?: '';
$departamento = filter_input(INPUT_GET, 'departamento') ?: '';
$orden = filter_input(INPUT_GET, 'orden') ?: 'id';
$estadoOperativoFiltro = filter_input(INPUT_GET, 'estado_operativo');
if ($estadoOperativoFiltro !== null && $estadoOperativoFiltro !== false) {
    $estadoOperativoFiltro = instrumento_validar_estado_operativo($estadoOperativoFiltro);
} else {
    $estadoOperativoFiltro = null;
}

$whereClauses = ['i.empresa_id = ?'];
$estadoNoBajaClause = "(i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')";
$params = [$empresaId];
$types  = 'i';

// Filtro por estado/programación
if ($filtro === 'activo') {
    $whereClauses[] = "TRIM(LOWER(i.estado)) = ?";
    $params[] = 'activo';
    $types   .= 's';
} elseif ($filtro === 'stock') {
    $whereClauses[] = "TRIM(LOWER(i.estado)) IN ('stock','en stock')";
} elseif ($filtro === 'inactivo') {
    $whereClauses[] = "TRIM(LOWER(i.estado)) = ?";
    $params[] = 'inactivo';
    $types   .= 's';
} elseif ($filtro === 'baja') {
    $whereClauses[] = "TRIM(LOWER(i.estado)) = ?";
    $params[] = 'baja';
    $types   .= 's';
} elseif ($filtro === 'programado') {
    $whereClauses[] = "i.programado = 1";
} elseif ($filtro === 'no_programado') {
    $whereClauses[] = "i.programado = 0";
}

if (($filtro === '' || $filtro === 'all') && $estadoOperativoFiltro !== 'baja') {
    $whereClauses[] = $estadoNoBajaClause;
}

// Filtro por departamento
if ($departamento !== '') {
    $whereClauses[] = "TRIM(LOWER(d.nombre)) = ?";
    $params[] = strtolower($departamento);
    $types   .= 's';
}

$where = $whereClauses ? 'WHERE ' . implode(' AND ', $whereClauses) : '';

// Ordenamiento
switch ($orden) {
    case 'departamento':
        $orderBy = 'd.nombre ASC, i.id ASC';
        break;
    case 'alfabetico':
        $orderBy = 'nombre ASC, i.id ASC';
        break;
    case 'proxima':
        $orderBy = 'COALESCE(i.proxima_calibracion, pr.fecha_actualizacion) ASC, i.id ASC';
        break;
    default:
        $orderBy = 'i.id ASC';
}

$sql = <<<SQL
SELECT i.id,
       ci.nombre AS nombre,
       m.nombre  AS marca,
       mo.nombre AS modelo,
       i.serie,
       i.codigo,
       d.nombre  AS departamento,
       i.ubicacion,
       i.fecha_alta,
       i.fecha_baja,
       COALESCE(i.proxima_calibracion, pr.fecha_actualizacion) AS proxima_calibracion,
       pr.frecuencia AS plan_frecuencia,
       pr.fecha_actualizacion AS plan_fecha_programada,
       pr.observaciones AS plan_observaciones,
       i.estado,
       i.programado,
       (
           SELECT COUNT(*)
           FROM instrumento_adjuntos ia
           WHERE ia.instrumento_id = i.id
             AND ia.empresa_id = i.empresa_id
       ) AS adjuntos_total
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN marcas m ON i.marca_id = m.id
LEFT JOIN modelos mo ON i.modelo_id = mo.id
LEFT JOIN departamentos d ON i.departamento_id = d.id
LEFT JOIN plan_riesgos pr ON pr.instrumento_id = i.id AND pr.empresa_id = i.empresa_id
$where
ORDER BY $orderBy
SQL;

$stmt = $conn->prepare($sql);
if ($params) {
     $refs = [];
    foreach ($params as $key => $value) {
        $refs[$key] = &$params[$key];
    }
    $stmt->bind_param($types, ...$refs);
}
try {
    $stmt->execute();
    $res = $stmt->get_result();
} catch (mysqli_sql_exception $e) {
    file_put_contents('php://stderr', 'Error al consultar instrumentos: ' . $e->getMessage() . PHP_EOL);
    echo json_encode(['error' => $e->getMessage()]);
    $conn->close();
    exit;
}

$instrumentos = [];
while ($row = $res->fetch_assoc()) {
    // Cada instrumento incluye "estado_calibracion" y "dias_restantes" para que el
    // consumidor de la API pueda representar la proximidad de la calibración sin
    // cálculos adicionales.
    if ($zonaHoraria instanceof DateTimeZone) {
        $instrumento = anexarEstadoCalibracion($row, $zonaHoraria);
    } else {
        $instrumento = anexarEstadoCalibracion($row);
    }

    $observacionPlan = trim((string)($instrumento['plan_observaciones'] ?? ''));
    if ($observacionPlan === GAGES_MISSING_CERTIFICATE_MESSAGE) {
        $instrumento['proxima_calibracion_mensaje'] = GAGES_MISSING_CERTIFICATE_MESSAGE;
        $instrumento['proxima_calibracion'] = null;
    } else {
        $instrumento['proxima_calibracion_mensaje'] = null;
    }
    unset($instrumento['plan_observaciones']);

    $estadoInfo = instrumento_estado_operativo_info($instrumento['estado'] ?? null);
    $instrumento['estado_operativo'] = $estadoInfo['estado_operativo'];
    $instrumento['estado_operativo_label'] = $estadoInfo['estado_operativo_label'];

    if ($estadoOperativoFiltro !== null && $instrumento['estado_operativo'] !== $estadoOperativoFiltro) {
        continue;
    }
    $instrumento['estado_operativo_historial'] = [];
    $instrumentos[] = $instrumento;
}
$stmt->close();
$payload = $instrumentos;
if ($empresaContexto !== null) {
    $payload = [
        'instrumentos' => $instrumentos,
        'meta'         => [
            'company_context' => $empresaContexto,
        ],
    ];
}

$instrumentoIds = array_column($instrumentos, 'id');
if (!empty($instrumentoIds)) {
    $placeholders = implode(',', array_fill(0, count($instrumentoIds), '?'));
    $sqlHistorial = "SELECT instrumento_id, estado, fecha, `timestamp` FROM historial_tipos_instrumento WHERE empresa_id = ? AND instrumento_id IN ($placeholders) ORDER BY fecha DESC, id DESC";
    $stmtHistorial = $conn->prepare($sqlHistorial);
    if ($stmtHistorial instanceof mysqli_stmt) {
        $histParams = array_merge([$empresaId], $instrumentoIds);
        $typesHist = 'i' . str_repeat('i', count($instrumentoIds));
        $refsHist = [];
        foreach ($histParams as $key => $value) {
            $refsHist[$key] = &$histParams[$key];
        }
        $stmtHistorial->bind_param($typesHist, ...$refsHist);
        if ($stmtHistorial->execute()) {
            $resultadoHistorial = $stmtHistorial->get_result();
            $historialMap = [];
            while ($hist = $resultadoHistorial->fetch_assoc()) {
                $instrumentoId = (int) ($hist['instrumento_id'] ?? 0);
                if ($instrumentoId <= 0) {
                    continue;
                }

                $estadoRaw = $hist['estado'] ?? '';
                $estadoNormalizado = instrumento_normalizar_estado_operativo($estadoRaw);
                $historialMap[$instrumentoId][] = [
                    'estado' => $estadoRaw,
                    'fecha' => $hist['fecha'] ?? null,
                    'timestamp' => $hist['timestamp'] ?? null,
                    'estado_operativo' => $estadoNormalizado,
                    'estado_operativo_label' => instrumento_estado_operativo_label($estadoRaw, $estadoNormalizado),
                ];
            }
            $resultadoHistorial->free();

            foreach ($instrumentos as $idx => $instrumento) {
                $idInstrumento = (int) ($instrumento['id'] ?? 0);
                $instrumentos[$idx]['estado_operativo_historial'] = $historialMap[$idInstrumento] ?? [];
            }
        }
        $stmtHistorial->close();
    }
}

$json = json_encode($instrumentos, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
if ($json === false) {
    http_response_code(500);
    $errorMessage = json_last_error_msg() ?: 'Error al procesar los instrumentos.';
    echo json_encode(['error' => $errorMessage], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
} else {
    echo $json;
}

$conn->close();
