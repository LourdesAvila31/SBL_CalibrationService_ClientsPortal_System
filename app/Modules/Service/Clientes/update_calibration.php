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
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_uncertainty.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if ($empresaId <= 0) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada.']);
    exit;
}

$input = file_get_contents('php://input');
$data = [];
if ($input) {
    $decoded = json_decode($input, true);
    if (json_last_error() === JSON_ERROR_NONE) {
        $data = $decoded;
    }
}

$data = array_merge($_POST, $data);

$calibracionId = isset($data['calibracion_id']) ? (int) $data['calibracion_id'] : 0;
if ($calibracionId <= 0) {
    http_response_code(422);
    echo json_encode(['error' => 'Identificador de calibración no válido.']);
    exit;
}

$consulta = $conn->prepare('SELECT fecha_calibracion, fecha_proxima, resultado, tipo, duracion_horas, costo_total, estado_ejecucion, motivo_reprogramacion, fecha_reprogramada, dias_atraso, u_value, u_method, u_k, u_coverage FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$consulta) {
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible obtener la calibración.']);
    exit;
}
$consulta->bind_param('ii', $calibracionId, $empresaId);
$consulta->execute();
$resultado = $consulta->get_result();
$calActual = $resultado ? $resultado->fetch_assoc() : null;
$consulta->close();

if (!$calActual) {
    http_response_code(404);
    echo json_encode(['error' => 'Calibración no encontrada.']);
    exit;
}

$sanitized = [];

$uncertaintyValueFinal = isset($calActual['u_value']) && $calActual['u_value'] !== null
    ? (float) $calActual['u_value']
    : null;
$uncertaintyMethodFinal = isset($calActual['u_method']) ? trim((string) $calActual['u_method']) : '';
$uncertaintyKFinal = isset($calActual['u_k']) && $calActual['u_k'] !== null
    ? (float) $calActual['u_k']
    : null;
$uncertaintyCoverageFinal = isset($calActual['u_coverage']) ? trim((string) $calActual['u_coverage']) : '';

if (array_key_exists('fecha_calibracion', $data)) {
    $value = trim((string) ($data['fecha_calibracion'] ?? ''));
    if ($value === '') {
        $sanitized['fecha_calibracion'] = null;
    } else {
        $dt = calibration_create_date($value);
        if (!$dt) {
            http_response_code(422);
            echo json_encode(['error' => 'Formato de fecha de calibración no válido.']);
            exit;
        }
        $sanitized['fecha_calibracion'] = $dt->format('Y-m-d');
    }
}

if (array_key_exists('fecha_proxima', $data)) {
    $value = trim((string) ($data['fecha_proxima'] ?? ''));
    if ($value === '') {
        $sanitized['fecha_proxima'] = null;
    } else {
        $dt = calibration_create_date($value);
        if (!$dt) {
            http_response_code(422);
            echo json_encode(['error' => 'Formato de fecha próxima no válido.']);
            exit;
        }
        $sanitized['fecha_proxima'] = $dt->format('Y-m-d');
    }
}

if (array_key_exists('fecha_reprogramada', $data)) {
    $value = trim((string) ($data['fecha_reprogramada'] ?? ''));
    if ($value === '') {
        $sanitized['fecha_reprogramada'] = null;
    } else {
        $dt = calibration_create_date($value);
        if (!$dt) {
            http_response_code(422);
            echo json_encode(['error' => 'Formato de fecha reprogramada no válido.']);
            exit;
        }
        $sanitized['fecha_reprogramada'] = $dt->format('Y-m-d');
    }
}

if (array_key_exists('resultado', $data)) {
    $sanitized['resultado'] = trim((string) $data['resultado']);
}

if (array_key_exists('observaciones', $data)) {
    $sanitized['observaciones'] = trim((string) $data['observaciones']);
}

if (array_key_exists('tipo', $data)) {
    $tipo = trim((string) $data['tipo']);
    $sanitized['tipo'] = $tipo === '' ? null : $tipo;
}

if (array_key_exists('duracion_horas', $data)) {
    $raw = $data['duracion_horas'];
    if ($raw === '' || $raw === null) {
        $sanitized['duracion_horas'] = null;
    } else {
        $duration = filter_var($raw, FILTER_VALIDATE_FLOAT);
        if ($duration === false || $duration < 0) {
            http_response_code(422);
            echo json_encode(['error' => 'Duración no válida.']);
            exit;
        }
        $sanitized['duracion_horas'] = number_format($duration, 1, '.', '');
    }
}

if (array_key_exists('costo_total', $data)) {
    $raw = $data['costo_total'];
    if ($raw === '' || $raw === null) {
        $sanitized['costo_total'] = null;
    } else {
        $cost = filter_var($raw, FILTER_VALIDATE_FLOAT);
        if ($cost === false || $cost < 0) {
            http_response_code(422);
            echo json_encode(['error' => 'Costo no válido.']);
            exit;
        }
        $sanitized['costo_total'] = number_format($cost, 2, '.', '');
    }
}

$estadoFinal = $calActual['estado_ejecucion'] ?? '';
if (($estadoFinal === '' || $estadoFinal === null) && ($calActual['resultado'] ?? '') !== '') {
    $estadoFinal = 'Completada';
} elseif ($estadoFinal === '' || $estadoFinal === null) {
    $estadoFinal = 'Programada';
}

if (array_key_exists('estado_ejecucion', $data)) {
    $estadoRaw = trim((string) $data['estado_ejecucion']);
    $normalizado = calibration_normalize_status($estadoRaw);
    if ($estadoRaw !== '' && strcasecmp($estadoRaw, $normalizado) !== 0) {
        http_response_code(422);
        echo json_encode(['error' => 'Estado de ejecución inválido.']);
        exit;
    }
    $sanitized['estado_ejecucion'] = $normalizado;
    $estadoFinal = $normalizado;
}

if (array_key_exists('u_value', $data)) {
    $raw = $data['u_value'];
    if ($raw === '' || $raw === null) {
        $sanitized['u_value'] = null;
        $uncertaintyValueFinal = null;
    } else {
        $value = filter_var($raw, FILTER_VALIDATE_FLOAT);
        if ($value === false || $value < 0) {
            http_response_code(422);
            echo json_encode(['error' => 'La incertidumbre debe ser un número mayor o igual a cero.']);
            exit;
        }
        $uncertaintyValueFinal = $value;
        $sanitized['u_value'] = number_format($value, 6, '.', '');
    }
}

if (array_key_exists('u_method', $data)) {
    $methodRaw = is_string($data['u_method']) ? trim($data['u_method']) : '';
    if ($methodRaw === '') {
        $sanitized['u_method'] = null;
        $uncertaintyMethodFinal = '';
    } else {
        $uncertaintyMethodFinal = $methodRaw;
        $sanitized['u_method'] = $methodRaw;
    }
}

if (array_key_exists('u_k', $data)) {
    $raw = $data['u_k'];
    if ($raw === '' || $raw === null) {
        $sanitized['u_k'] = null;
        $uncertaintyKFinal = null;
    } else {
        $value = filter_var($raw, FILTER_VALIDATE_FLOAT);
        if ($value === false || $value <= 0) {
            http_response_code(422);
            echo json_encode(['error' => 'El factor de cobertura k debe ser mayor a cero.']);
            exit;
        }
        $uncertaintyKFinal = $value;
        $sanitized['u_k'] = number_format($value, 4, '.', '');
    }
}

if (array_key_exists('u_coverage', $data)) {
    $coverageRaw = is_string($data['u_coverage']) ? trim($data['u_coverage']) : '';
    if ($coverageRaw === '') {
        $sanitized['u_coverage'] = null;
        $uncertaintyCoverageFinal = '';
    } else {
        $uncertaintyCoverageFinal = $coverageRaw;
        $sanitized['u_coverage'] = $coverageRaw;
    }
}

$resultadoFinal = array_key_exists('resultado', $sanitized)
    ? $sanitized['resultado']
    : trim((string) ($calActual['resultado'] ?? ''));

$uncertaintyRequired = calibration_requires_uncertainty($estadoFinal, $resultadoFinal);
if ($uncertaintyRequired) {
    if ($uncertaintyValueFinal === null || $uncertaintyMethodFinal === '' || $uncertaintyKFinal === null || $uncertaintyCoverageFinal === '') {
        http_response_code(422);
        echo json_encode(['error' => 'Antes de cerrar la calibración registra la incertidumbre, método, k y cobertura.']);
        exit;
    }
}

$motivoFinal = trim((string) ($calActual['motivo_reprogramacion'] ?? ''));
if (array_key_exists('motivo_reprogramacion', $data)) {
    $motivoFinal = trim((string) $data['motivo_reprogramacion']);
    $sanitized['motivo_reprogramacion'] = $motivoFinal === '' ? null : $motivoFinal;
}

$fechaProgramada = null;
if (array_key_exists('fecha_programada', $data)) {
    $fechaProgramadaRaw = trim((string) $data['fecha_programada']);
    if ($fechaProgramadaRaw !== '') {
        $fechaPlan = calibration_create_date($fechaProgramadaRaw);
        if (!$fechaPlan) {
            http_response_code(422);
            echo json_encode(['error' => 'Formato de fecha programada no válido.']);
            exit;
        }
        $fechaProgramada = $fechaPlan->format('Y-m-d');
    }
}

$fechaCalibracionFinal = $sanitized['fecha_calibracion'] ?? ($calActual['fecha_calibracion'] ?? null);
$fechaReprogramadaFinal = array_key_exists('fecha_reprogramada', $sanitized)
    ? $sanitized['fecha_reprogramada']
    : ($calActual['fecha_reprogramada'] ?? null);

if ($estadoFinal === 'Reprogramada' && !$fechaReprogramadaFinal) {
    http_response_code(422);
    echo json_encode(['error' => 'Debe registrar la nueva fecha cuando la calibración se reprograma.']);
    exit;
}

if ($fechaReprogramadaFinal && $fechaProgramada) {
    $planDate = calibration_create_date($fechaProgramada);
    $reprogDate = calibration_create_date($fechaReprogramadaFinal);
    if ($planDate && $reprogDate) {
        $diff = (int) $planDate->diff($reprogDate)->format('%r%a');
        if ($diff < 0) {
            http_response_code(422);
            echo json_encode(['error' => 'La fecha reprogramada debe ser igual o posterior a la programada.']);
            exit;
        }
    }
}

$diasAtrasoFinal = null;
if (array_key_exists('dias_atraso', $data)) {
    $raw = $data['dias_atraso'];
    if ($raw === '' || $raw === null) {
        $diasAtrasoFinal = null;
    } else {
        $intVal = filter_var($raw, FILTER_VALIDATE_INT);
        if ($intVal === false || $intVal < 0) {
            http_response_code(422);
            echo json_encode(['error' => 'Los días de atraso deben ser un número entero mayor o igual que cero.']);
            exit;
        }
        $diasAtrasoFinal = $intVal;
    }
}

if ($diasAtrasoFinal === null) {
    $diasCalculados = calibration_compute_delay($fechaProgramada, $fechaCalibracionFinal, $fechaReprogramadaFinal);
    if ($diasCalculados !== null) {
        $diasAtrasoFinal = $diasCalculados;
    } elseif (isset($calActual['dias_atraso'])) {
        $diasAtrasoFinal = (int) $calActual['dias_atraso'];
    }
}

if ($diasAtrasoFinal !== null) {
    $sanitized['dias_atraso'] = (string) max(0, (int) $diasAtrasoFinal);
}

$requiereJustificacion = calibration_requires_justification($estadoFinal, $diasAtrasoFinal);
if ($requiereJustificacion && $motivoFinal === '') {
    http_response_code(422);
    echo json_encode(['error' => 'Es necesario capturar la justificación del atraso o reprogramación.']);
    exit;
}

if ($requiereJustificacion && !array_key_exists('motivo_reprogramacion', $sanitized)) {
    $sanitized['motivo_reprogramacion'] = $motivoFinal === '' ? null : $motivoFinal;
}

if ($estadoFinal !== null && !array_key_exists('estado_ejecucion', $sanitized)) {
    $sanitized['estado_ejecucion'] = $estadoFinal;
}

if (!array_key_exists('fecha_reprogramada', $sanitized) && $fechaReprogramadaFinal !== null) {
    $sanitized['fecha_reprogramada'] = $fechaReprogramadaFinal;
}

if (empty($sanitized)) {
    echo json_encode(['success' => false, 'message' => 'No se proporcionaron cambios.']);
    exit;
}

$typesMap = [
    'fecha_calibracion' => 's',
    'fecha_proxima' => 's',
    'resultado' => 's',
    'observaciones' => 's',
    'tipo' => 's',
    'duracion_horas' => 's',
    'costo_total' => 's',
    'estado_ejecucion' => 's',
    'motivo_reprogramacion' => 's',
    'fecha_reprogramada' => 's',
    'dias_atraso' => 's',
    'u_value' => 's',
    'u_method' => 's',
    'u_k' => 's',
    'u_coverage' => 's',
];

$setParts = [];
$types = '';
$params = [];
foreach ($sanitized as $field => $value) {
    $setParts[] = "$field = ?";
    $types .= $typesMap[$field] ?? 's';
    $params[] = $value;
}

$types .= 'ii';
$params[] = $calibracionId;
$params[] = $empresaId;

try {
    $sql = 'UPDATE calibraciones SET ' . implode(', ', $setParts) . ' WHERE id = ? AND empresa_id = ?';
    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        throw new RuntimeException('No fue posible preparar la actualización.');
    }
    $stmt->bind_param($types, ...$params);
    $stmt->execute();
    $filas = $stmt->affected_rows;
    $stmt->close();

    echo json_encode([
        'success' => true,
        'updated' => $filas,
    ]);
} catch (Throwable $e) {
    error_log('Error al actualizar calibración: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible actualizar la calibración.']);
}
