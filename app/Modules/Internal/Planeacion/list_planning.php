<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('planeacion_leer')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

header('Content-Type: application/json');

$roleName = $_SESSION['role_id'] ?? '';
if (!in_array($roleName, ['Superadministrador', 'Administrador', 'Supervisor'], true)) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once __DIR__ . '/planeacion_view_helper.php';

const PLAN_MISSING_CERTIFICATE_MESSAGE = 'No se ha añadido su primer certificado';

if (!ensurePlaneacionView($conn)) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la vista de planeación']);
    $conn->close();
    exit;
}

/**
 * Normaliza una cadena de estado a la clave estándar.
 */
function normalizarEstadoClave(?string $valor): string
{
    $texto = trim((string) $valor);
    if ($texto === '') {
        return 'programada';
    }

    $sinDiacriticos = strtr($texto, [
        'Á' => 'a', 'á' => 'a',
        'É' => 'e', 'é' => 'e',
        'Í' => 'i', 'í' => 'i',
        'Ó' => 'o', 'ó' => 'o',
        'Ú' => 'u', 'ú' => 'u',
        'Ü' => 'u', 'ü' => 'u',
    ]);
    $sinDiacriticos = strtolower($sinDiacriticos);
    $compacto = preg_replace('/[^a-z]+/u', '_', $sinDiacriticos);
    if (!is_string($compacto) || $compacto === '') {
        return 'programada';
    }
    $compacto = trim($compacto, '_');
    if ($compacto === '') {
        return 'programada';
    }

    $mapa = [
        'encurso' => 'en_curso',
        'en_curso' => 'en_curso',
        'en_ejecucion' => 'en_curso',
        'enejecucion' => 'en_curso',
        'ejecucion' => 'en_curso',
        'en_proceso' => 'en_curso',
        'enproceso' => 'en_curso',
        'proceso' => 'en_curso',
        'curso' => 'en_curso',
        'completada' => 'completada',
        'completado' => 'completada',
        'finalizada' => 'completada',
        'finalizado' => 'completada',
        'terminada' => 'completada',
        'terminado' => 'completada',
        'cerrada' => 'completada',
        'cerrado' => 'completada',
        'concluida' => 'completada',
        'concluido' => 'completada',
        'ejecutada' => 'completada',
        'ejecutado' => 'completada',
        'cancelada' => 'cancelada',
        'cancelado' => 'cancelada',
        'anulada' => 'cancelada',
        'anulado' => 'cancelada',
        'suspendida' => 'cancelada',
        'suspendido' => 'cancelada',
        'detenida' => 'cancelada',
        'detenido' => 'cancelada',
        'sin_fecha' => 'sin_fecha',
        'sinfecha' => 'sin_fecha',
        'programada' => 'programada',
        'programado' => 'programada',
        'pendiente' => 'programada',
        'planificada' => 'programada',
        'planificado' => 'programada',
    ];

    return $mapa[$compacto] ?? 'programada';
}

function obtenerEtiquetaEstado(string $clave): string
{
    switch ($clave) {
        case 'sin_fecha':
            return 'Sin fecha';
        case 'en_curso':
            return 'En curso';
        case 'completada':
            return 'Completada';
        case 'cancelada':
            return 'Cancelada';
        default:
            return 'Programada';
    }
}

function prepararEstadoRespuesta(?string $etiqueta, ?string $clave = null): array
{
    $claveNormalizada = normalizarEstadoClave($clave ?? $etiqueta ?? '');
    $etiquetaFinal = obtenerEtiquetaEstado($claveNormalizada);
    return [$etiquetaFinal, $claveNormalizada];
}

function normalizarFiltroEstado(?string $valor): string
{
    $texto = trim((string) $valor);
    if ($texto === '') {
        return 'all';
    }

    $sinDiacriticos = strtr($texto, [
        'Á' => 'a', 'á' => 'a',
        'É' => 'e', 'é' => 'e',
        'Í' => 'i', 'í' => 'i',
        'Ó' => 'o', 'ó' => 'o',
        'Ú' => 'u', 'ú' => 'u',
        'Ü' => 'u', 'ü' => 'u',
    ]);
    $sinDiacriticos = strtolower($sinDiacriticos);
    $compacto = preg_replace('/[^a-z]+/u', '_', $sinDiacriticos);
    if (!is_string($compacto) || $compacto === '') {
        return 'all';
    }
    $compacto = trim($compacto, '_');
    if ($compacto === '' || in_array($compacto, ['all', 'todo', 'todos'], true)) {
        return 'all';
    }
    if (in_array($compacto, ['stock', 'en_stock'], true)) {
        return 'stock';
    }

    return normalizarEstadoClave($texto);
}

$estado = normalizarFiltroEstado(filter_input(INPUT_GET, 'estado', FILTER_SANITIZE_FULL_SPECIAL_CHARS));
$estadosPermitidos = ['all', 'sin_fecha', 'programada', 'en_curso', 'completada', 'cancelada', 'stock'];
if (!in_array($estado, $estadosPermitidos, true)) {
    $estado = 'all';
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$condicion = '';
$estadoInstrumento = "v.estado_instrumento = 'activo'";

switch ($estado) {
    case 'programada':
    case 'en_curso':
    case 'completada':
    case 'cancelada':
        $condicion = " AND v.estado_clave = '" . $conn->real_escape_string($estado) . "'";
        break;
    case 'sin_fecha':
        $condicion = " AND (v.tiene_plan = 0 OR v.fecha_programada IS NULL OR v.estado_clave = 'sin_fecha')";
        break;
    case 'stock':
        $estadoInstrumento = "v.estado_instrumento IN ('stock','en stock')";
        break;
}

$rangoInferior = 30;
$rangoSuperior = 60;

$sql = "SELECT
            v.instrumento_id AS id,
            v.instrumento,
            v.codigo,
            v.marca,
            v.modelo,
            v.serie,
            v.departamento,
            v.ubicacion,
            v.fecha_proxima,
            v.plan_observaciones,
            v.fecha_programada,
            v.responsable_id,
            v.responsable,
            v.tiene_plan,
            v.estado_plan,
            v.estado_clave,
            v.dias_restantes,
            v.instrucciones_cliente
        FROM vista_planeacion_instrumentos v
        WHERE v.empresa_id = ?
          AND $estadoInstrumento
          AND v.fecha_proxima BETWEEN DATE_ADD(CURDATE(), INTERVAL ? DAY) AND DATE_ADD(CURDATE(), INTERVAL ? DAY)
            ORDER BY
                CASE WHEN v.fecha_proxima IS NULL THEN 1 ELSE 0 END,
                v.fecha_proxima ASC,
                v.instrumento ASC";

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta']);
    $conn->close();
    exit;
}

$stmt->bind_param('iii', $empresaId, $rangoInferior, $rangoSuperior);
$stmt->execute();
$res = $stmt->get_result();
$planes = [];
while ($row = $res->fetch_assoc()) {
    if (!isset($row['instrucciones_cliente']) || $row['instrucciones_cliente'] === null) {
        $row['instrucciones_cliente'] = null;
    }
    $observacionPlan = trim((string)($row['plan_observaciones'] ?? ''));
    if ($observacionPlan === PLAN_MISSING_CERTIFICATE_MESSAGE) {
        $row['fecha_proxima_mensaje'] = PLAN_MISSING_CERTIFICATE_MESSAGE;
        $row['fecha_proxima'] = null;
    } else {
        $row['fecha_proxima_mensaje'] = null;
    }
    $tienePlan = (int)($row['tiene_plan'] ?? 0) === 1;
    if (!$tienePlan || empty($row['fecha_programada'])) {
        $row['estado'] = 'Sin fecha';
        $row['estado_plan'] = 'Sin fecha';
        $row['estado_clave'] = 'sin_fecha';
    } else {
        [$etiqueta, $clave] = prepararEstadoRespuesta($row['estado_plan'] ?? null, $row['estado_clave'] ?? null);
        $row['estado'] = $etiqueta;
        $row['estado_clave'] = $clave;
    }
    unset($row['plan_observaciones']);
    $planes[] = $row;
}
$stmt->close();
echo json_encode($planes);
$conn->close();
?>