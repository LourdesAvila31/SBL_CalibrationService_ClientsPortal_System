<?php
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_results.php';
require_once dirname(__DIR__, 2) . '/Auditoria/audit.php';

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

ensure_portal_access('service');

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

if (!check_permission('calibraciones_aprobar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada']);
    exit;
}

$id = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
if (!$id) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Identificador inválido']);
    exit;
}

$motivo = isset($_POST['motivo']) ? trim((string) $_POST['motivo']) : '';
if ($motivo === '') {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Debes capturar el motivo de rechazo.']);
    exit;
}

$usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : 0;
if ($usuarioId <= 0) {
    http_response_code(401);
    echo json_encode(['success' => false, 'message' => 'Sesión inválida']);
    exit;
}

$consulta = $conn->prepare('SELECT id, estado, instrumento_id FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$consulta) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la consulta']);
    exit;
}
$consulta->bind_param('ii', $id, $empresaId);
$consulta->execute();
$resultadoConsulta = $consulta->get_result();
$calibracion = $resultadoConsulta ? $resultadoConsulta->fetch_assoc() : null;
$consulta->close();

if (!$calibracion) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Calibración no encontrada']);
    exit;
}

$estadoActual = calibration_normalize_decision_state($calibracion['estado'] ?? null);
if ($estadoActual === 'Rechazado') {
    echo json_encode(['success' => true, 'message' => 'La calibración ya se encuentra rechazada.', 'estado' => 'Rechazado']);
    exit;
}

$ahora = date('Y-m-d H:i:s');
$estadoRechazado = 'Rechazado';

$update = $conn->prepare('UPDATE calibraciones SET estado = ?, motivo_rechazo = ?, resultado = NULL, liberado_por = NULL, fecha_liberacion = NULL, aprobado_por = ?, fecha_aprobacion = ? WHERE id = ? AND empresa_id = ?');
if (!$update) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la actualización']);
    exit;
}
$update->bind_param(
    'ssissi',
    $estadoRechazado,
    $motivo,
    $usuarioId,
    $ahora,
    $id,
    $empresaId
);

if (!$update->execute()) {
    $update->close();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo rechazar la calibración']);
    exit;
}
$update->close();

$nombre = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
if ($nombre === '' && $usuarioId > 0) {
    $nombreStmt = $conn->prepare('SELECT nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
    if ($nombreStmt) {
        $nombreStmt->bind_param('i', $usuarioId);
        if ($nombreStmt->execute()) {
            $nombreStmt->bind_result($nombreDb, $apellidosDb);
            if ($nombreStmt->fetch()) {
                $nombre = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
            }
        }
        $nombreStmt->close();
    }
}
if ($nombre === '') {
    $nombre = 'Desconocido';
}
$correo = $_SESSION['usuario'] ?? null;

$instrumentoId = $calibracion['instrumento_id'] ?? null;
$detalle = sprintf('Rechazó calibración #%d', $id);
log_activity($nombre, [
    'valor_nuevo' => $detalle,
    'valor_anterior' => $motivo,
    'seccion' => 'calibraciones',
    'instrumento_id' => $instrumentoId,
    'usuario_id' => $usuarioId,
    'usuario_correo' => $correo,
]);

echo json_encode([
    'success' => true,
    'message' => 'Calibración rechazada.',
    'estado' => $estadoRechazado,
]);
