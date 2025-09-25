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

function asegurarOrdenCalibracionBulk(mysqli $conn, int $planId, int $instrumentoId, int $empresaId, int $tecnicoId, string $estado): bool
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
$ids = $data['instrumentos'] ?? [];
$fecha = trim((string)($data['fecha_programada'] ?? ''));
$responsableId = intval($data['responsable_id'] ?? 0);
$estado = trim((string)($data['estado'] ?? ''));

if (!is_array($ids)) {
    $ids = [];
}

$instrumentos = array_values(array_unique(array_map('intval', $ids)));
$instrumentos = array_filter($instrumentos, static function ($id) {
    return $id > 0;
});

if (empty($instrumentos) || !$fecha || !$estado || !$responsableId) {
    echo json_encode(['success' => false, 'message' => 'Datos incompletos']);
    $conn->close();
    exit;
}

if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$fechaValida = DateTime::createFromFormat('Y-m-d', $fecha);
if (!$fechaValida || $fechaValida->format('Y-m-d') !== $fecha) {
    echo json_encode(['success' => false, 'message' => 'Fecha programada inválida']);
    $conn->close();
    exit;
}

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

$selectInstrumento = $conn->prepare('SELECT estado FROM instrumentos WHERE id = ? AND empresa_id = ?');
$selectPlan = $conn->prepare('SELECT id FROM planes WHERE instrumento_id = ? AND empresa_id = ? LIMIT 1');
$insertPlan = $conn->prepare('INSERT INTO planes (instrumento_id, fecha_programada, responsable_id, estado, empresa_id) VALUES (?, ?, ?, ?, ?)');
$updatePlan = $conn->prepare('UPDATE planes SET fecha_programada = ?, responsable_id = ?, estado = ? WHERE id = ?');
$marcarProgramado = $conn->prepare('UPDATE instrumentos SET programado = 1 WHERE id = ? AND empresa_id = ?');

if (!$selectInstrumento || !$selectPlan || !$insertPlan || !$updatePlan || !$marcarProgramado) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudieron preparar las consultas']);
    $conn->close();
    exit;
}

$conn->begin_transaction();

$procesados = 0;
$error = false;

foreach ($instrumentos as $instrumentoId) {
    $selectInstrumento->bind_param('ii', $instrumentoId, $empresaId);
    if (!$selectInstrumento->execute()) {
        $error = true;
        break;
    }
    $selectInstrumento->bind_result($estadoInstrumento);
    if (!$selectInstrumento->fetch()) {
        $selectInstrumento->free_result();
        $selectInstrumento->reset();
        continue;
    }
    $selectInstrumento->free_result();
    $selectInstrumento->reset();

    if (in_array(strtolower((string)$estadoInstrumento), ['stock', 'en stock'], true)) {
        continue;
    }

    $planExistente = null;
    $selectPlan->bind_param('ii', $instrumentoId, $empresaId);
    if (!$selectPlan->execute()) {
        $error = true;
        break;
    }
    $selectPlan->bind_result($planId);
    if ($selectPlan->fetch()) {
        $planExistente = $planId;
    }
    $selectPlan->free_result();
    $selectPlan->reset();

    if ($planExistente !== null) {
        $updatePlan->bind_param('sisi', $fecha, $responsableId, $estado, $planExistente);
        if (!$updatePlan->execute()) {
            $error = true;
            break;
        }
        if (!asegurarOrdenCalibracionBulk($conn, (int)$planExistente, $instrumentoId, $empresaId, $responsableId, $estado)) {
            $error = true;
            break;
        }
    } else {
        $insertPlan->bind_param('isisi', $instrumentoId, $fecha, $responsableId, $estado, $empresaId);
        if (!$insertPlan->execute()) {
            $error = true;
            break;
        }
        $nuevoPlanId = (int)$conn->insert_id;
        if ($nuevoPlanId <= 0 || !asegurarOrdenCalibracionBulk($conn, $nuevoPlanId, $instrumentoId, $empresaId, $responsableId, $estado)) {
            $error = true;
            break;
        }
    }

    $marcarProgramado->bind_param('ii', $instrumentoId, $empresaId);
    if (!$marcarProgramado->execute()) {
        $error = true;
        break;
    }

    $procesados++;
}

if ($error || $procesados === 0) {
    $conn->rollback();
    if ($procesados === 0 && !$error) {
        echo json_encode(['success' => false, 'message' => 'No se registraron planeaciones válidas']);
    } else {
        echo json_encode(['success' => false, 'message' => 'Error al registrar la planeación masiva']);
    }
} else {
    $conn->commit();
    echo json_encode(['success' => true, 'procesados' => $procesados]);
}

$selectInstrumento->close();
$selectPlan->close();
$insertPlan->close();
$updatePlan->close();
$marcarProgramado->close();
$conn->close();
