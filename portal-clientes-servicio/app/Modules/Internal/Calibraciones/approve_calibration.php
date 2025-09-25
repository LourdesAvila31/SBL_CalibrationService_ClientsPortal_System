<?php
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_results.php';
require_once dirname(__DIR__, 2) . '/Auditoria/audit.php';

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

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

$usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : 0;
if ($usuarioId <= 0) {
    http_response_code(401);
    echo json_encode(['success' => false, 'message' => 'Sesión inválida']);
    exit;
}

$resultadoOverride = isset($_POST['resultado']) ? trim((string) $_POST['resultado']) : null;

$consulta = $conn->prepare('SELECT id, estado, resultado_preliminar, resultado, instrumento_id, usuario_id AS ejecutor_id, aprobado_por FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
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

$ejecutorId = isset($calibracion['ejecutor_id']) ? (int) $calibracion['ejecutor_id'] : null;
if ($ejecutorId !== null && $ejecutorId === $usuarioId) {
    http_response_code(409);
    echo json_encode([
        'success' => false,
        'message' => 'Otro miembro del equipo debe revisar y aprobar esta calibración para cumplir la doble validación.',
    ]);
    exit;
}

$aprobadoPor = isset($calibracion['aprobado_por']) ? (int) $calibracion['aprobado_por'] : null;
if ($aprobadoPor && $aprobadoPor === $usuarioId) {
    http_response_code(409);
    echo json_encode([
        'success' => false,
        'message' => 'Ya registraste la aprobación de esta calibración.',
    ]);
    exit;
}

if ($aprobadoPor && $aprobadoPor !== $usuarioId) {
    http_response_code(409);
    echo json_encode([
        'success' => false,
        'message' => 'Esta calibración ya fue aprobada por otro responsable y espera liberación.',
    ]);
    exit;
}

$estadoActual = calibration_normalize_decision_state($calibracion['estado'] ?? null);
$preliminar = $calibracion['resultado_preliminar'] ?? null;
$liberadoActual = $calibracion['resultado'] ?? null;

if ($resultadoOverride !== null && $resultadoOverride !== '') {
    $resultadoFinal = $resultadoOverride;
    $preliminar = $resultadoOverride;
} else {
    $resultadoFinal = $liberadoActual ?: $preliminar;
}

if ($resultadoFinal === null || trim($resultadoFinal) === '') {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'No existe un resultado para liberar.']);
    exit;
}

$ahora = date('Y-m-d H:i:s');

$update = $conn->prepare('UPDATE calibraciones SET estado = ?, resultado = ?, resultado_preliminar = ?, liberado_por = NULL, fecha_liberacion = NULL, aprobado_por = ?, fecha_aprobacion = ?, motivo_rechazo = NULL WHERE id = ? AND empresa_id = ?');
if (!$update) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la actualización']);
    exit;
}

$estadoAprobado = 'Aprobado';
$update->bind_param(
    'sssisii',
    $estadoAprobado,
    $resultadoFinal,
    $preliminar,
    $usuarioId,
    $ahora,
    $id,
    $empresaId
);

if (!$update->execute()) {
    $update->close();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo aprobar la calibración']);
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
$detalle = sprintf('Aprobó calibración #%d', $id);
log_activity($nombre, [
    'valor_nuevo' => $detalle,
    'seccion' => 'calibraciones',
    'instrumento_id' => $instrumentoId,
    'usuario_id' => $usuarioId,
    'usuario_correo' => $correo,
]);

echo json_encode([
    'success' => true,
    'message' => 'Calibración aprobada. Ahora queda pendiente la liberación por un segundo responsable.',
    'estado' => $estadoAprobado,
]);
