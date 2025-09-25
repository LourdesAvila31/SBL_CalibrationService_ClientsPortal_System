<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';

header('Content-Type: application/json');

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Auditoria/audit.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no identificada']);
    exit;
}

$payload = json_decode(file_get_contents('php://input'), true);
if (!is_array($payload)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Solicitud inválida']);
    exit;
}

$capacitacionId = isset($payload['capacitacion_id']) ? (int)$payload['capacitacion_id'] : 0;
$participantes = $payload['participantes'] ?? [];
$fechaEjecucion = isset($payload['fecha_ejecucion']) ? trim((string)$payload['fecha_ejecucion']) : '';

if ($capacitacionId <= 0 || !is_array($participantes) || count($participantes) === 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Se requiere la capacitación y la lista de participantes']);
    exit;
}

$fechaEjecucionValida = null;
if ($fechaEjecucion !== '') {
    $fechaEj = DateTime::createFromFormat('Y-m-d', $fechaEjecucion) ?: DateTime::createFromFormat('Y-m-d H:i:s', $fechaEjecucion);
    if (!$fechaEj) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Fecha de ejecución inválida']);
        exit;
    }
    $fechaEjecucionValida = $fechaEj->format('Y-m-d');
}

$conn->begin_transaction();

$validarCapacitacion = $conn->prepare('SELECT estado FROM calidad_capacitaciones WHERE id = ? AND empresa_id = ? FOR UPDATE');
if (!$validarCapacitacion) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar la capacitación']);
    exit;
}

$validarCapacitacion->bind_param('ii', $capacitacionId, $empresaId);
$validarCapacitacion->execute();
$validarCapacitacion->bind_result($estadoActual);
if (!$validarCapacitacion->fetch()) {
    $validarCapacitacion->close();
    $conn->rollback();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Capacitación no encontrada']);
    exit;
}
$validarCapacitacion->close();

$verificarUsuario = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$verificarUsuario) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar los participantes']);
    exit;
}

$guardarAsistencia = $conn->prepare(
    'INSERT INTO calidad_capacitaciones_participantes (capacitacion_id, participante_id, asistencia, calificacion, comentarios, registrado_en)
     VALUES (?, ?, ?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE asistencia = VALUES(asistencia), calificacion = VALUES(calificacion), comentarios = VALUES(comentarios), registrado_en = VALUES(registrado_en)'
);
if (!$guardarAsistencia) {
    $verificarUsuario->close();
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible preparar el registro de asistencia']);
    exit;
}

$totalActualizados = 0;
foreach ($participantes as $participante) {
    $usuarioId = isset($participante['usuario_id']) ? (int)$participante['usuario_id'] : 0;
    $asistio = isset($participante['asistio']) && $participante['asistio'] ? 1 : 0;
    $comentarios = trim((string)($participante['observaciones'] ?? $participante['comentarios'] ?? ''));
    $calificacion = isset($participante['calificacion']) ? (float)$participante['calificacion'] : null;

    if ($usuarioId <= 0) {
        continue;
    }

    $verificarUsuario->bind_param('ii', $usuarioId, $empresaId);
    $verificarUsuario->execute();
    $verificarUsuario->store_result();
    $usuarioValido = $verificarUsuario->num_rows > 0;
    $verificarUsuario->free_result();

    if (!$usuarioValido) {
        continue;
    }

    $comentariosParam = $comentarios !== '' ? $comentarios : null;
    $calificacionParam = $calificacion !== null ? (string)$calificacion : null;

    $guardarAsistencia->bind_param(
        'iiiss',
        $capacitacionId,
        $usuarioId,
        $asistio,
        $calificacionParam,
        $comentariosParam
    );

    if ($guardarAsistencia->execute()) {
        $totalActualizados++;
    }
}

$verificarUsuario->close();
$guardarAsistencia->close();

$estadoFinal = 'publicado';
$fechaEjecucionGuardar = $fechaEjecucionValida ?? date('Y-m-d');

$actualizarCapacitacion = $conn->prepare('UPDATE calidad_capacitaciones SET estado = ?, fecha_ejecucion = ?, actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
if ($actualizarCapacitacion) {
    $actualizarCapacitacion->bind_param('ssii', $estadoFinal, $fechaEjecucionGuardar, $capacitacionId, $empresaId);
    $actualizarCapacitacion->execute();
    $actualizarCapacitacion->close();
}

$conn->commit();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? ''));

log_activity($nombreAud, [
    'seccion' => 'calidad_capacitaciones',
    'valor_nuevo' => sprintf('Asistencia registrada en capacitación #%d (%d participantes)', $capacitacionId, $totalActualizados),
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'Asistencias actualizadas',
    'data' => [
        'capacitacion_id' => $capacitacionId,
        'participantes_actualizados' => $totalActualizados,
        'estado' => $estadoFinal,
        'fecha_ejecucion' => $fechaEjecucionGuardar,
    ],
]);

$conn->close();
