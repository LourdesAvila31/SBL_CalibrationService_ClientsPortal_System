<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('usuarios_edit')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Internal/Auditoria/audit.php';

header('Content-Type: application/json; charset=UTF-8');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada']);
    exit;
}

$competenciaId = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
if (!$competenciaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Identificador inválido']);
    exit;
}

$sql = 'SELECT id, usuario_id, archivo FROM usuario_competencias WHERE id = ? AND empresa_id = ? LIMIT 1';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo completar la operación']);
    exit;
}
$stmt->bind_param('ii', $competenciaId, $empresaId);
$stmt->execute();
$result = $stmt->get_result();
$record = $result->fetch_assoc();
$stmt->close();

if (!$record) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Evidencia no encontrada']);
    exit;
}

$usuarioId = (int) ($record['usuario_id'] ?? 0);
$archivo = $record['archivo'] ?? null;

$delete = $conn->prepare('DELETE FROM usuario_competencias WHERE id = ? AND empresa_id = ?');
if (!$delete) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo eliminar la evidencia']);
    exit;
}
$delete->bind_param('ii', $competenciaId, $empresaId);
if (!$delete->execute()) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo eliminar la evidencia']);
    $delete->close();
    exit;
}
$delete->close();

if ($archivo) {
    $path = dirname(__DIR__, 4) . '/storage/usuarios/competencias/' . $archivo;
    if (is_file($path)) {
        @unlink($path);
    }
}

$nombreAud = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = $_SESSION['usuario'] ?? null;
log_activity($nombreAud, "Eliminó evidencia de competencia $competenciaId del usuario $usuarioId", 'usuarios', $correoAud);

echo json_encode(['success' => true, 'message' => 'Evidencia eliminada correctamente']);
$conn->close();
