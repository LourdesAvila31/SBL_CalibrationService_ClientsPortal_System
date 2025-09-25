<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('planeacion_crear')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

function asegurarOrdenCalibracion(mysqli $conn, int $planId, int $instrumentoId, int $empresaId, int $tecnicoId, string $estado): bool
{
    if ($planId <= 0 || $instrumentoId <= 0 || $empresaId <= 0 || $tecnicoId <= 0) {
        return false;
    }

    $estadoOrden = trim($estado) !== '' ? trim($estado) : 'Programada';

    $buscarOrden = $conn->prepare('SELECT id FROM ordenes_calibracion WHERE plan_id = ? AND empresa_id = ? LIMIT 1');
    if (!$buscarOrden) {
        return false;
    }
    $buscarOrden->bind_param('ii', $planId, $empresaId);
    $buscarOrden->execute();
    $buscarOrden->bind_result($ordenId);
    $existe = $buscarOrden->fetch();
    $buscarOrden->close();

    if ($existe) {
        $actualizar = $conn->prepare('UPDATE ordenes_calibracion SET instrumento_id = ?, tecnico_id = ?, estado_ejecucion = ? WHERE id = ?');
        if (!$actualizar) {
            return false;
        }
        $actualizar->bind_param('iisi', $instrumentoId, $tecnicoId, $estadoOrden, $ordenId);
        $ok = $actualizar->execute();
        $actualizar->close();
        return $ok;
    }

    $insertar = $conn->prepare('INSERT INTO ordenes_calibracion (plan_id, instrumento_id, empresa_id, tecnico_id, estado_ejecucion) VALUES (?, ?, ?, ?, ?)');
    if (!$insertar) {
        return false;
    }
    $insertar->bind_param('iiiis', $planId, $instrumentoId, $empresaId, $tecnicoId, $estadoOrden);
    $ok = $insertar->execute();
    $insertar->close();
    return $ok;
}

$roleName = $_SESSION['role_id'] ?? '';
$usuarioId = intval($_SESSION['usuario_id'] ?? 0);
if (!in_array($roleName, ['Superadministrador', 'Administrador', 'Supervisor'], true) || !$usuarioId) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    $conn->close();
    exit;
}

$data = json_decode(file_get_contents('php://input'), true) ?? [];
$instrumentoId = intval($data['instrumento_id'] ?? 0);
$fecha = trim((string)($data['fecha_programada'] ?? ''));
$responsableId = intval($data['responsable_id'] ?? 0);
$estado = trim((string)($data['estado'] ?? ''));
$instruccionesCliente = null;
$hasInstructionsField = array_key_exists('instrucciones_cliente', $data);
if ($hasInstructionsField) {
    $instruccionesCliente = trim((string)($data['instrucciones_cliente'] ?? ''));
    if ($instruccionesCliente === '') {
        $instruccionesCliente = null;
    } elseif (mb_strlen($instruccionesCliente) > 2000) {
        echo json_encode(['success' => false, 'message' => 'Las instrucciones especiales no pueden superar los 2000 caracteres.']);
        $conn->close();
        exit;
    }
}

if (!$instrumentoId || !$fecha || !$estado || !$responsableId) {
    echo json_encode(['success' => false, 'message' => 'Datos incompletos']);
    $conn->close();
    exit;
}

$fechaValida = DateTime::createFromFormat('Y-m-d', $fecha);
if (!$fechaValida || $fechaValida->format('Y-m-d') !== $fecha) {
    echo json_encode(['success' => false, 'message' => 'Fecha programada inválida']);
    $conn->close();
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

// Verifica que el instrumento pertenezca a la empresa y no esté en stock
$checkInstrumento = $conn->prepare('SELECT estado FROM instrumentos WHERE id = ? AND empresa_id = ?');
if (!$checkInstrumento) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo verificar el instrumento']);
    $conn->close();
    exit;
}
$checkInstrumento->bind_param('ii', $instrumentoId, $empresaId);
$checkInstrumento->execute();
$checkInstrumento->bind_result($estadoInstrumento);
if (!$checkInstrumento->fetch()) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Instrumento no encontrado']);
    $checkInstrumento->close();
    $conn->close();
    exit;
}
$checkInstrumento->close();

if (in_array(strtolower((string)$estadoInstrumento), ['stock', 'en stock'], true)) {
    echo json_encode(['success' => false, 'message' => 'Instrumento en stock no requiere planeación']);
    $conn->close();
    exit;
}

// Verifica que el responsable pertenezca a la empresa
$checkResponsable = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$checkResponsable) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo verificar al responsable']);
    $conn->close();
    exit;
}
$checkResponsable->bind_param('ii', $responsableId, $empresaId);
$checkResponsable->execute();
$checkResponsable->store_result();
if ($checkResponsable->num_rows === 0) {
    $checkResponsable->close();
    echo json_encode(['success' => false, 'message' => 'Responsable no válido para la empresa']);
    $conn->close();
    exit;
}
$checkResponsable->close();

$conn->begin_transaction();

$planExistente = null;
$buscarPlan = $conn->prepare('SELECT id FROM planes WHERE instrumento_id = ? AND empresa_id = ? LIMIT 1');
if ($buscarPlan) {
    $buscarPlan->bind_param('ii', $instrumentoId, $empresaId);
    $buscarPlan->execute();
    $buscarPlan->bind_result($planId);
    if ($buscarPlan->fetch()) {
        $planExistente = $planId;
    }
    $buscarPlan->close();
}

if ($planExistente !== null) {
    if ($hasInstructionsField) {
        $stmt = $conn->prepare('UPDATE planes SET fecha_programada = ?, responsable_id = ?, estado = ?, instrucciones_cliente = ? WHERE id = ?');
        if (!$stmt) {
            $conn->rollback();
            $conn->close();
            http_response_code(500);
            echo json_encode(['success' => false, 'message' => 'No se pudo actualizar la planeación']);
            exit;
        }
        $stmt->bind_param('sissi', $fecha, $responsableId, $estado, $instruccionesCliente, $planExistente);
    } else {
        $stmt = $conn->prepare('UPDATE planes SET fecha_programada = ?, responsable_id = ?, estado = ? WHERE id = ?');
        if (!$stmt) {
            $conn->rollback();
            $conn->close();
            http_response_code(500);
            echo json_encode(['success' => false, 'message' => 'No se pudo actualizar la planeación']);
            exit;
        }
        $stmt->bind_param('sisi', $fecha, $responsableId, $estado, $planExistente);
    }
} else {
    if ($hasInstructionsField) {
        $stmt = $conn->prepare('INSERT INTO planes (instrumento_id, fecha_programada, responsable_id, estado, empresa_id, instrucciones_cliente) VALUES (?, ?, ?, ?, ?, ?)');
        if (!$stmt) {
            $conn->rollback();
            $conn->close();
            http_response_code(500);
            echo json_encode(['success' => false, 'message' => 'No se pudo registrar la planeación']);
            exit;
        }
        $stmt->bind_param('isisis', $instrumentoId, $fecha, $responsableId, $estado, $empresaId, $instruccionesCliente);
    } else {
        $stmt = $conn->prepare('INSERT INTO planes (instrumento_id, fecha_programada, responsable_id, estado, empresa_id) VALUES (?, ?, ?, ?, ?)');
        if (!$stmt) {
            $conn->rollback();
            $conn->close();
            http_response_code(500);
            echo json_encode(['success' => false, 'message' => 'No se pudo registrar la planeación']);
            exit;
        }
        $stmt->bind_param('isisi', $instrumentoId, $fecha, $responsableId, $estado, $empresaId);
    }
}

$ok = $stmt->execute();
$stmt->close();

if ($ok) {
    $planIdFinal = $planExistente !== null ? (int)$planExistente : (int)$conn->insert_id;
    if ($planIdFinal <= 0 || !asegurarOrdenCalibracion($conn, $planIdFinal, $instrumentoId, $empresaId, $responsableId, $estado)) {
        $conn->rollback();
        echo json_encode(['success' => false, 'message' => 'No se pudo sincronizar la orden de calibración']);
        $conn->close();
        exit;
    }

    $up = $conn->prepare('UPDATE instrumentos SET programado = 1 WHERE id = ? AND empresa_id = ?');
    if ($up) {
        $up->bind_param('ii', $instrumentoId, $empresaId);
        $up->execute();
        $up->close();
    }
    $conn->commit();
    echo json_encode(['success' => true]);
} else {
    $conn->rollback();
    echo json_encode(['success' => false, 'message' => 'Error al registrar planeación']);
}

$conn->close();
?>
