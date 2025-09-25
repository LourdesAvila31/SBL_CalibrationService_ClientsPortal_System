<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_add')) {
    http_response_code(403);
    exit('Acceso denegado');
}
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__) . '/Auditoria/audit.php';

$usuario      = trim($_POST['usuario'] ?? '');
$correo       = filter_input(INPUT_POST, 'correo', FILTER_VALIDATE_EMAIL);
$nombre       = trim($_POST['nombre'] ?? '');
$passwordRaw  = $_POST['contrasena'] ?? '';
$telefono     = trim($_POST['telefono'] ?? '');
$empresaId    = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT);
$roleRequest  = trim((string) ($_POST['role_id'] ?? 'Operador'));

if (mb_strlen($telefono) > 50) {
    http_response_code(400);
    exit('El teléfono no puede exceder 50 caracteres.');
}

$empresaSesion = ensure_session_empresa_id();
if (!session_is_superadmin()) {
    if ($empresaId === null || $empresaId <= 0 || $empresaSesion === null || $empresaSesion !== $empresaId) {
        http_response_code(403);
        exit('No tienes permiso para asignar usuarios a otra empresa.');
    }
}

if ($empresaId === null || $empresaId <= 0) {
    $empresaId = $empresaSesion ?? 1;
}

$roleId = null;
if ($roleRequest !== '') {
    $resolved = resolve_role_id($roleRequest, $empresaId);
    if ($resolved !== null) {
        $roleId = $resolved;
    }
}

if ($roleId === null) {
    $roleId = resolve_role_id('Operador', $empresaId);
}

if (!$roleId) {
    http_response_code(400);
    exit('Rol inválido.');
}

if ($usuario && $correo && $nombre && $passwordRaw) {
    $hash = password_hash($passwordRaw, PASSWORD_DEFAULT);
    $stmt = $conn->prepare('INSERT INTO usuarios (usuario, correo, nombre, contrasena, role_id, empresa_id, telefono) VALUES (?,?,?,?,?,?,?)');
    $stmt->bind_param('ssssiis', $usuario, $correo, $nombre, $hash, $roleId, $empresaId, $telefono);
    if ($stmt->execute()) {
        echo 'Usuario registrado correctamente.';
        $nombreAud = $_SESSION['nombre'] ?? '';
        $correoAud = $_SESSION['usuario'] ?? null;
        log_activity($nombreAud, "Alta de usuario $usuario", null, $correoAud);
    } else {
        echo 'Error: ' . $conn->error;
    }
    $stmt->close();
} else {
    echo 'Faltan datos para registrar el usuario.';
}

$conn->close();
?>
