<?php
header('Content-Type: application/json');

try {
    require_once dirname(__DIR__, 3) . '/Core/db.php';
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'message' => 'No se pudo conectar a la base de datos.',
    ]);
    exit;
}

require_once dirname(__DIR__) . '/Auditoria/audit.php';

function json_response(int $status, array $payload): void
{
    http_response_code($status);
    echo json_encode($payload);
    exit;
}

$rawInput = file_get_contents('php://input');
$data = [];
if ($rawInput !== false && $rawInput !== '' && strpos($_SERVER['CONTENT_TYPE'] ?? '', 'application/json') !== false) {
    $decoded = json_decode($rawInput, true);
    if (is_array($decoded)) {
        $data = $decoded;
    }
}

$token = $data['token'] ?? ($_POST['token'] ?? '');
$token = is_string($token) ? trim($token) : '';

$nuevaContrasena = $data['contrasena'] ?? ($_POST['contrasena'] ?? '');
$nuevaContrasena = is_string($nuevaContrasena) ? trim($nuevaContrasena) : '';

if ($token === '' || $nuevaContrasena === '') {
    json_response(400, [
        'success' => false,
        'message' => 'Debe proporcionar el token y la nueva contraseña.',
    ]);
}

if (strlen($nuevaContrasena) < 8) {
    json_response(400, [
        'success' => false,
        'message' => 'La nueva contraseña debe tener al menos 8 caracteres.',
    ]);
}

$tokenHash = hash('sha256', $token);

$sql = 'SELECT pr.id, pr.usuario_id, pr.expira_en, u.nombre, u.correo '
     . 'FROM password_resets pr '
     . 'INNER JOIN usuarios u ON u.id = pr.usuario_id '
     . 'WHERE pr.token_hash = ? AND pr.expira_en >= NOW() '
     . 'LIMIT 1';

try {
    $stmt = $conn->prepare($sql);
    $stmt->bind_param('s', $tokenHash);
    $stmt->execute();
    $resultado = $stmt->get_result();
    $registro = $resultado->fetch_assoc();
    $stmt->close();
} catch (Throwable $e) {
    error_log('Error al validar token de restablecimiento: ' . $e->getMessage());
    json_response(500, [
        'success' => false,
        'message' => 'No se pudo validar el token proporcionado.',
    ]);
}

if (!$registro) {
    json_response(400, [
        'success' => false,
        'message' => 'El enlace de restablecimiento no es válido o ha expirado.',
    ]);
}

try {
    $hash = password_hash($nuevaContrasena, PASSWORD_DEFAULT);

    $stmtUpdate = $conn->prepare('UPDATE usuarios SET contrasena = ? WHERE id = ?');
    $stmtUpdate->bind_param('si', $hash, $registro['usuario_id']);
    $stmtUpdate->execute();
    $stmtUpdate->close();

    $stmtDelete = $conn->prepare('DELETE FROM password_resets WHERE id = ?');
    $stmtDelete->bind_param('i', $registro['id']);
    $stmtDelete->execute();
    $stmtDelete->close();

    $nombre = $registro['nombre'] ?? '';
    $correo = $registro['correo'] ?? null;
    log_activity($nombre, 'Restablecimiento de contraseña', 'media', $correo);
} catch (Throwable $e) {
    error_log('Error al restablecer contraseña: ' . $e->getMessage());
    json_response(500, [
        'success' => false,
        'message' => 'No fue posible actualizar la contraseña en este momento.',
    ]);
}

json_response(200, [
    'success' => true,
    'message' => 'Contraseña actualizada correctamente. Ya puedes iniciar sesión con tus nuevos datos.',
]);
