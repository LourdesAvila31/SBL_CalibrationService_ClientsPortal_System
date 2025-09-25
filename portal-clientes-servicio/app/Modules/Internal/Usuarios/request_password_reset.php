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

require_once dirname(__DIR__, 3) . '/Core/helpers/mail_helper.php';

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

$correo = $data['correo'] ?? null;
if ($correo === null) {
    $correo = $_POST['correo'] ?? null;
}
$correo = filter_var($correo, FILTER_VALIDATE_EMAIL);

if (!$correo) {
    json_response(400, [
        'success' => false,
        'message' => 'Debe proporcionar un correo electrónico válido.',
    ]);
}

$sql = 'SELECT id, nombre, correo FROM usuarios WHERE correo = ? AND activo = 1 LIMIT 1';
try {
    $stmt = $conn->prepare($sql);
    $stmt->bind_param('s', $correo);
    $stmt->execute();
    $resultado = $stmt->get_result();
    $usuario = $resultado->fetch_assoc();
    $stmt->close();
} catch (Throwable $e) {
    error_log('Error al buscar usuario para restablecimiento: ' . $e->getMessage());
    json_response(500, [
        'success' => false,
        'message' => 'Error interno al procesar la solicitud.',
    ]);
}

if ($usuario) {
    try {
        $token = bin2hex(random_bytes(32));
        $tokenHash = hash('sha256', $token);
        $expiraEn = (new DateTime('+1 hour'))->format('Y-m-d H:i:s');

        // Limpia tokens expirados y previos del usuario
        $conn->query('DELETE FROM password_resets WHERE expira_en < NOW()');

        $stmtDelete = $conn->prepare('DELETE FROM password_resets WHERE usuario_id = ?');
        $stmtDelete->bind_param('i', $usuario['id']);
        $stmtDelete->execute();
        $stmtDelete->close();

        $stmtInsert = $conn->prepare('INSERT INTO password_resets (usuario_id, token_hash, expira_en) VALUES (?, ?, ?)');
        $stmtInsert->bind_param('iss', $usuario['id'], $tokenHash, $expiraEn);
        $stmtInsert->execute();
        $stmtInsert->close();

        send_password_reset_email($usuario['correo'], $usuario['nombre'], $token);
    } catch (Throwable $e) {
        error_log('No se pudo generar token de restablecimiento: ' . $e->getMessage());
        json_response(500, [
            'success' => false,
            'message' => 'No se pudo completar la solicitud de restablecimiento.',
        ]);
    }
}

// Respuesta genérica para evitar enumeración de usuarios
json_response(200, [
    'success' => true,
    'message' => 'Si la cuenta existe, recibirás un correo con instrucciones en los próximos minutos.',
]);
