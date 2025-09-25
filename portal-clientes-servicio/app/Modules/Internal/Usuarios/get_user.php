<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';
require_once __DIR__ . '/tenant_roles_helpers.php';
$id = intval($_GET['id'] ?? 0);
$sql = "SELECT u.*, r.nombre AS role_name, e.nombre AS empresa_nombre FROM usuarios u "
     . "LEFT JOIN roles r ON u.role_id = r.id "
     . "LEFT JOIN empresas e ON u.empresa_id = e.id "
     . "WHERE u.id=?";
$stmt = $conn->prepare($sql);
$stmt->bind_param("i", $id);
$stmt->execute();
$res = $stmt->get_result();
$u = $res->fetch_assoc();
if($u){
    $empresaSesion = ensure_session_empresa_id();
    if (!session_is_superadmin() && $empresaSesion !== null) {
        $empresaUsuario = isset($u['empresa_id']) ? (int) $u['empresa_id'] : null;
        if ($empresaUsuario !== null && $empresaUsuario !== $empresaSesion) {
            echo json_encode([]);
            exit;
        }
    }

  //restrict-user-permission-checks
    $u['role_id'] = $u['role_id'] ?? $u['rol'] ?? null;
    unset($u['rol']);

    $u['role_id'] = $u['role_name'] ?? $u['role_id'] ?? null;
    $u['puesto'] = $u['puesto'] ?? null;
    $u['telefono'] = $u['telefono'] ?? null;
    $u['empresa_id'] = isset($u['empresa_id']) ? (int) $u['empresa_id'] : null;
    $u['empresa_nombre'] = $u['empresa_nombre'] ?? null;

    $u['tenant_role_ids'] = [];
    $u['tenant_roles']    = [];
    if ($u['empresa_id'] !== null && tenant_roles_table_exists($conn, 'tenant_user_roles') && tenant_roles_table_exists($conn, 'tenant_roles')) {
        $stmtRoles = $conn->prepare('SELECT tur.tenant_role_id, tr.nombre FROM tenant_user_roles tur INNER JOIN tenant_roles tr ON tr.id = tur.tenant_role_id WHERE tur.usuario_id = ? ORDER BY tr.nombre');
        if ($stmtRoles) {
            $stmtRoles->bind_param('i', $id);
            if ($stmtRoles->execute()) {
                $resultRoles = $stmtRoles->get_result();
                while ($rowRole = $resultRoles->fetch_assoc()) {
                    $roleIdTenant = (int) $rowRole['tenant_role_id'];
                    $u['tenant_role_ids'][] = $roleIdTenant;
                    $u['tenant_roles'][]    = [
                        'id'     => $roleIdTenant,
                        'nombre' => $rowRole['nombre'],
                    ];
                }
            }
            $stmtRoles->close();
        }
    }
} else {
    $u = [];
}
echo json_encode($u ?: []);
$nombreAud = $_SESSION['nombre'] ?? '';
$correoAud = $_SESSION['usuario'] ?? null;
log_activity($nombreAud, "Consultó usuario $id", null, $correoAud);
?>
