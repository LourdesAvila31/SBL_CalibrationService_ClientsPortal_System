<?php
session_start();

$accion = $_POST['accion'] ?? '';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if ($accion === 'eliminar') {
    if (!check_permission('usuarios_delete')) { exit('Acceso denegado'); }
} else {
    if (!check_permission('usuarios_edit')) { exit('Acceso denegado'); }
}
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/delegated_roles.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';

$id    = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
$valor = $_POST['valor'] ?? '';
$empresaSesion = ensure_session_empresa_id();

if (!$id) {
    echo "ID inválido";
    exit;
}

$stmtUsuario = $conn->prepare('SELECT empresa_id, role_id, nombre, apellidos, usuario FROM usuarios WHERE id=? LIMIT 1');
$stmtUsuario->bind_param('i', $id);
$stmtUsuario->execute();
$usuarioRow = $stmtUsuario->get_result()->fetch_assoc();
$stmtUsuario->close();

if (!$usuarioRow) {
    echo "Usuario no encontrado";
    exit;
}

$empresaUsuario = isset($usuarioRow['empresa_id']) ? (int) $usuarioRow['empresa_id'] : null;
$rolAnterior = isset($usuarioRow['role_id']) ? (int) $usuarioRow['role_id'] : null;
$usuarioLabel = trim(($usuarioRow['nombre'] ?? '') . ' ' . ($usuarioRow['apellidos'] ?? ''));
if ($usuarioLabel === '') {
    $usuarioLabel = $usuarioRow['usuario'] ?? ('ID ' . $id);
}

if (!session_is_superadmin()) {
    if ($empresaSesion === null) {
        echo "Empresa no autorizada";
        exit;
    }
    if ($empresaUsuario === null || $empresaUsuario !== $empresaSesion) {
        echo "No puedes modificar usuarios de otra empresa";
        exit;
    }
}

if ($accion == 'activo') {
    $stmt = $conn->prepare("UPDATE usuarios SET activo=? WHERE id=?");
    $stmt->bind_param("ii", $valor, $id);
    $stmt->execute();
    echo "Estado actualizado";
    $nombreAud = $_SESSION['nombre'] ?? '';
    $correoAud = $_SESSION['usuario'] ?? null;
    log_activity($nombreAud, "Actualizó estado de usuario $id", null, $correoAud);
} elseif ($accion == 'role_id') {
    $roleId = resolve_role_id($valor, $empresaUsuario);
    if ($roleId === null) {
        echo "Rol inválido";
        exit;
    }
    $roleId = (int) $roleId;

    $stmtRoleInfo = $conn->prepare('SELECT nombre, delegated FROM roles WHERE id=? LIMIT 1');
    $stmtRoleInfo->bind_param('i', $roleId);
    $stmtRoleInfo->execute();
    $roleInfo = $stmtRoleInfo->get_result()->fetch_assoc();
    $stmtRoleInfo->close();

    if (!$roleInfo) {
        echo "Rol inválido";
        exit;
    }

    $roleName = $roleInfo['nombre'] ?? 'Desconocido';
    $isDelegated = (int) ($roleInfo['delegated'] ?? 0) === 1;
    $nombreAud = $_SESSION['nombre'] ?? '';
    $correoAud = $_SESSION['usuario'] ?? null;
    $actorId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;

    if ($isDelegated) {
        if ($actorId === null) {
            echo "Sesión inválida";
            exit;
        }
        try {
            assign_delegated_role_to_user($roleId, $id, $actorId);
            echo "Rol delegado asignado";
        } catch (Throwable $e) {
            echo $e->getMessage();
        }
    } else {
        $stmt = $conn->prepare("UPDATE usuarios SET role_id=? WHERE id=?");
        $stmt->bind_param("ii", $roleId, $id);
        $stmt->execute();
        echo "Rol actualizado";

        $rolAnteriorNombre = null;
        if ($rolAnterior !== null && $rolAnterior !== $roleId) {
            $stmtOld = $conn->prepare('SELECT nombre FROM roles WHERE id=? LIMIT 1');
            if ($stmtOld) {
                $stmtOld->bind_param('i', $rolAnterior);
                $stmtOld->execute();
                $dataOld = $stmtOld->get_result()->fetch_assoc();
                $stmtOld->close();
                if ($dataOld) {
                    $rolAnteriorNombre = $dataOld['nombre'] ?? null;
                }
            }
        }

        log_activity($nombreAud, [
            'seccion' => 'usuarios',
            'valor_anterior' => $rolAnteriorNombre ? "Rol anterior: {$rolAnteriorNombre}" : null,
            'valor_nuevo' => sprintf("Asignó rol '%s' al usuario %s (ID %d)", $roleName, $usuarioLabel, $id),
            'usuario_id' => $actorId,
            'usuario_correo' => $correoAud,
        ]);
    }
} elseif ($accion == 'eliminar') {
    if (!check_permission('usuarios_delete')) exit('Acceso denegado');
    $stmt = $conn->prepare("DELETE FROM usuarios WHERE id=?");
    $stmt->bind_param("i", $id);
    $stmt->execute();
    echo "Usuario eliminado";
    $nombreAud = $_SESSION['nombre'] ?? '';
    $correoAud = $_SESSION['usuario'] ?? null;
    log_activity($nombreAud, "Eliminó usuario $id", null, $correoAud);
} else {
    echo "Acción no válida";
}
?>
