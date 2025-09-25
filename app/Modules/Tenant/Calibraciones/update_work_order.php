<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (
    !check_permission('planeacion_actualizar') &&
    !check_permission('planeacion_crear') &&
    !check_permission('calibraciones_actualizar') &&
    !check_permission('calibraciones_crear')
) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

$roleAlias = session_role_alias();
$allowedRoles = ['superadministrador', 'administrador', 'supervisor', 'operador'];
if ($roleAlias === null || !in_array($roleAlias, $allowedRoles, true)) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Modules/Internal/Auditoria/audit.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$payload = json_decode(file_get_contents('php://input'), true) ?? [];
$planId = (int)($payload['plan_id'] ?? 0);
$estadoEntrada = isset($payload['estado_orden']) ? (string)$payload['estado_orden'] : (string)($payload['estado'] ?? '');
$estadoEntrada = trim($estadoEntrada);
$tecnicoEntrada = $payload['tecnico_id'] ?? null;
$tecnicoSolicitado = $tecnicoEntrada !== null ? (int)$tecnicoEntrada : 0;
$fechaInicioRaw = array_key_exists('fecha_inicio', $payload) ? trim((string)$payload['fecha_inicio']) : null;
$fechaCierreRaw = array_key_exists('fecha_cierre', $payload) ? trim((string)$payload['fecha_cierre']) : null;
$observacionesRaw = array_key_exists('observaciones', $payload) ? trim((string)$payload['observaciones']) : null;

if ($planId <= 0) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Identificador de plan inválido']);
    $conn->close();
    exit;
}

$fechaInicio = null;
if ($fechaInicioRaw !== null && $fechaInicioRaw !== '') {
    $dtInicio = DateTime::createFromFormat('Y-m-d', $fechaInicioRaw);
    if (!$dtInicio || $dtInicio->format('Y-m-d') !== $fechaInicioRaw) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Fecha de inicio inválida']);
        $conn->close();
        exit;
    }
    $fechaInicio = $dtInicio->format('Y-m-d');
}

$fechaCierre = null;
if ($fechaCierreRaw !== null && $fechaCierreRaw !== '') {
    $dtCierre = DateTime::createFromFormat('Y-m-d', $fechaCierreRaw);
    if (!$dtCierre || $dtCierre->format('Y-m-d') !== $fechaCierreRaw) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Fecha de cierre inválida']);
        $conn->close();
        exit;
    }
    $fechaCierre = $dtCierre->format('Y-m-d');
}

if ($fechaInicio && $fechaCierre && $fechaCierre < $fechaInicio) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'La fecha de cierre no puede ser anterior al inicio']);
    $conn->close();
    exit;
}

$planStmt = $conn->prepare('SELECT p.id, p.instrumento_id, p.estado, p.responsable_id, oc.id AS orden_id, oc.tecnico_id, oc.estado_ejecucion, oc.fecha_inicio, oc.fecha_cierre, oc.observaciones,
    ut.nombre AS tecnico_nombre, ut.apellidos AS tecnico_apellidos, ut.usuario AS tecnico_usuario,
    ur.nombre AS responsable_nombre, ur.apellidos AS responsable_apellidos, ur.usuario AS responsable_usuario
    FROM planes p
    LEFT JOIN ordenes_calibracion oc ON oc.plan_id = p.id AND oc.empresa_id = p.empresa_id
    LEFT JOIN usuarios ut ON ut.id = oc.tecnico_id
    LEFT JOIN usuarios ur ON ur.id = p.responsable_id
    WHERE p.id = ? AND p.empresa_id = ?
    LIMIT 1');
if (!$planStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo recuperar el plan']);
    $conn->close();
    exit;
}
$planStmt->bind_param('ii', $planId, $empresaId);
$planStmt->execute();
$result = $planStmt->get_result();
$planData = $result ? $result->fetch_assoc() : null;
$planStmt->close();

if (!$planData) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Plan no encontrado']);
    $conn->close();
    exit;
}

$tecnicoActual = (int)($planData['tecnico_id'] ?? 0);
$planResponsable = (int)($planData['responsable_id'] ?? 0);
$tecnicoFinal = $tecnicoSolicitado > 0 ? $tecnicoSolicitado : ($tecnicoActual > 0 ? $tecnicoActual : $planResponsable);

if ($tecnicoFinal <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Debe asignarse un técnico válido']);
    $conn->close();
    exit;
}

$tecnicoNombreFinal = null;
if ($tecnicoFinal !== $tecnicoActual) {
    $validaTecnico = $conn->prepare('SELECT nombre, apellidos, usuario FROM usuarios WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$validaTecnico) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo validar al técnico']);
        $conn->close();
        exit;
    }
    $validaTecnico->bind_param('ii', $tecnicoFinal, $empresaId);
    $validaTecnico->execute();
    $validaTecnico->bind_result($tecNombre, $tecApellidos, $tecUsuario);
    if (!$validaTecnico->fetch()) {
        $validaTecnico->close();
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Técnico no encontrado en la empresa']);
        $conn->close();
        exit;
    }
    $validaTecnico->close();
    $tecnicoNombreFinal = trim($tecNombre . ' ' . $tecApellidos);
    if ($tecnicoNombreFinal === '') {
        $tecnicoNombreFinal = $tecUsuario;
    }
} else {
    $tecnicoNombreFinal = trim(($planData['tecnico_nombre'] ?? '') . ' ' . ($planData['tecnico_apellidos'] ?? ''));
    if ($tecnicoNombreFinal === '') {
        $tecnicoNombreFinal = $planData['tecnico_usuario'] ?? '';
    }
}

if ($tecnicoNombreFinal === '') {
    $tecnicoNombreFinal = trim(($planData['responsable_nombre'] ?? '') . ' ' . ($planData['responsable_apellidos'] ?? ''));
    if ($tecnicoNombreFinal === '') {
        $tecnicoNombreFinal = $planData['responsable_usuario'] ?? 'Sin asignar';
    }
}

$estadoOrdenFinal = $estadoEntrada !== '' ? $estadoEntrada : ($planData['estado_ejecucion'] ?? ($planData['estado'] ?? 'Programada'));
$observacionesFinal = $observacionesRaw !== null ? $observacionesRaw : ($planData['observaciones'] ?? null);
if ($observacionesFinal !== null && mb_strlen($observacionesFinal) > 2000) {
    $observacionesFinal = mb_substr($observacionesFinal, 0, 2000);
}
if ($observacionesFinal !== null && $observacionesFinal === '') {
    $observacionesFinal = null;
}

$before = [
    'orden_id' => $planData['orden_id'] ?? null,
    'tecnico_id' => $planData['tecnico_id'] ?? null,
    'estado_ejecucion' => $planData['estado_ejecucion'] ?? ($planData['estado'] ?? null),
    'fecha_inicio' => $planData['fecha_inicio'] ?? null,
    'fecha_cierre' => $planData['fecha_cierre'] ?? null,
    'observaciones' => $planData['observaciones'] ?? null,
];

$conn->begin_transaction();

$ordenId = (int)($planData['orden_id'] ?? 0);
$instrumentoId = (int)$planData['instrumento_id'];

if ($ordenId > 0) {
    $update = $conn->prepare('UPDATE ordenes_calibracion SET instrumento_id = ?, tecnico_id = ?, estado_ejecucion = ?, fecha_inicio = ?, fecha_cierre = ?, observaciones = ? WHERE id = ?');
    if (!$update) {
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo actualizar la orden']);
        $conn->close();
        exit;
    }
    $update->bind_param('iissssi', $instrumentoId, $tecnicoFinal, $estadoOrdenFinal, $fechaInicio, $fechaCierre, $observacionesFinal, $ordenId);
    if (!$update->execute()) {
        $update->close();
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo actualizar la orden']);
        $conn->close();
        exit;
    }
    $update->close();
} else {
    $insert = $conn->prepare('INSERT INTO ordenes_calibracion (plan_id, instrumento_id, empresa_id, tecnico_id, estado_ejecucion, fecha_inicio, fecha_cierre, observaciones) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    if (!$insert) {
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo crear la orden']);
        $conn->close();
        exit;
    }
    $insert->bind_param('iiiissss', $planId, $instrumentoId, $empresaId, $tecnicoFinal, $estadoOrdenFinal, $fechaInicio, $fechaCierre, $observacionesFinal);
    if (!$insert->execute()) {
        $insert->close();
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo crear la orden']);
        $conn->close();
        exit;
    }
    $ordenId = (int)$conn->insert_id;
    $insert->close();
}

$planUpdates = [];
$planParams = [];
$planTypes = '';

if ($tecnicoFinal !== $planResponsable) {
    $planUpdates[] = 'responsable_id = ?';
    $planTypes .= 'i';
    $planParams[] = $tecnicoFinal;
}

if ($estadoEntrada !== '' && $estadoEntrada !== ($planData['estado'] ?? '')) {
    $planUpdates[] = 'estado = ?';
    $planTypes .= 's';
    $planParams[] = $estadoOrdenFinal;
}

if (!empty($planUpdates)) {
    $planSql = 'UPDATE planes SET ' . implode(', ', $planUpdates) . ' WHERE id = ?';
    $planTypes .= 'i';
    $planParams[] = $planId;
    $planStmtUpd = $conn->prepare($planSql);
    if (!$planStmtUpd) {
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo actualizar el plan']);
        $conn->close();
        exit;
    }
    $bindParams = [];
    foreach ($planParams as $idx => $value) {
        $bindParams[$idx] = &$planParams[$idx];
    }
    array_unshift($bindParams, $planTypes);
    call_user_func_array([$planStmtUpd, 'bind_param'], $bindParams);
    if (!$planStmtUpd->execute()) {
        $planStmtUpd->close();
        $conn->rollback();
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo actualizar el plan']);
        $conn->close();
        exit;
    }
    $planStmtUpd->close();
}

$conn->commit();

$after = [
    'orden_id' => $ordenId,
    'tecnico_id' => $tecnicoFinal,
    'estado_ejecucion' => $estadoOrdenFinal,
    'fecha_inicio' => $fechaInicio,
    'fecha_cierre' => $fechaCierre,
    'observaciones' => $observacionesFinal,
];

$nombreSesion = trim((string)(($_SESSION['nombre'] ?? '') . ' ' . ($_SESSION['apellidos'] ?? '')));
if ($nombreSesion === '') {
    $nombreSesion = $_SESSION['usuario'] ?? 'Usuario del sistema';
}

log_activity($nombreSesion, [
    'seccion' => 'Órdenes de Calibración',
    'rango_referencia' => 'Plan #' . $planId,
    'instrumento_id' => $instrumentoId,
    'valor_anterior' => json_encode($before, JSON_UNESCAPED_UNICODE),
    'valor_nuevo' => json_encode($after, JSON_UNESCAPED_UNICODE),
]);

echo json_encode([
    'success' => true,
    'plan_id' => $planId,
    'orden_id' => $ordenId,
    'estado_orden' => $estadoOrdenFinal,
    'tecnico_id' => $tecnicoFinal,
    'tecnico' => $tecnicoNombreFinal,
    'fecha_inicio' => $fechaInicio,
    'fecha_cierre' => $fechaCierre,
    'observaciones' => $observacionesFinal,
]);

$conn->close();
