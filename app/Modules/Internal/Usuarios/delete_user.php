<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_delete')) {
    http_response_code(403);
    echo json_encode(["success" => false, "msg" => "Acceso denegado"]);
    exit;
}
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';

$id = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
$empresaSesion = ensure_session_empresa_id();
$esSuper = session_is_superadmin();

if ($id) {
    if (!$esSuper) {
        if ($empresaSesion === null) {
            echo json_encode(["success" => false, "msg" => "Empresa no autorizada"]);
            exit;
        }
        $stmtEmpresa = $conn->prepare('SELECT empresa_id FROM usuarios WHERE id=?');
        $stmtEmpresa->bind_param('i', $id);
        $stmtEmpresa->execute();
        $stmtEmpresa->bind_result($empresaUsuario);
        if (!$stmtEmpresa->fetch() || (int) $empresaUsuario !== $empresaSesion) {
            $stmtEmpresa->close();
            echo json_encode(["success" => false, "msg" => "No puedes eliminar usuarios de otra empresa"]);
            exit;
        }
        $stmtEmpresa->close();
    }
    $stmt = $conn->prepare('DELETE FROM usuarios WHERE id=?');
    $stmt->bind_param('i', $id);
    $ok = $stmt->execute();
    $stmt->close();
    if ($ok) {
        $nombreAud = $_SESSION['nombre'] ?? '';
        $correoAud = $_SESSION['usuario'] ?? null;
        log_activity($nombreAud, "Eliminó usuario $id", null, $correoAud);
    }
    echo json_encode(["success" => $ok, "msg" => $ok ? "Usuario eliminado" : "Error al eliminar usuario"]);
} else {
    echo json_encode(["success" => false, "msg" => "Falta el ID"]);
}
?>
