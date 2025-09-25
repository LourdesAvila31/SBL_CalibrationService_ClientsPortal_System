<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('calibraciones_actualizar')) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode(['error' => 'No tienes permisos para actualizar alertas de calibración.']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_alerts.php';

header('Content-Type: application/json');

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

try {
    $rawBody = file_get_contents('php://input');
    $payload = json_decode($rawBody ?: '[]', true);

    if (!is_array($payload)) {
        throw new InvalidArgumentException('El cuerpo de la petición debe ser un objeto JSON.');
    }

    $notificationId = isset($payload['id']) ? (int) $payload['id'] : 0;
    $action = isset($payload['action']) ? (string) $payload['action'] : '';

    if ($notificationId <= 0 || $action === '') {
        http_response_code(400);
        echo json_encode(['error' => 'Identificador de alerta o acción inválidos.']);
        $conn->close();
        exit;
    }

    $empresaId = obtenerEmpresaId();
    if ($empresaId <= 0) {
        http_response_code(400);
        echo json_encode(['error' => 'Empresa no especificada.']);
        $conn->close();
        exit;
    }

    $usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : 0;
    if ($usuarioId <= 0) {
        http_response_code(401);
        echo json_encode(['error' => 'Sesión inválida.']);
        $conn->close();
        exit;
    }

    $stmt = $conn->prepare('SELECT nombre, apellidos, correo FROM usuarios WHERE id = ? LIMIT 1');
    if (!$stmt) {
        throw new RuntimeException('No se pudo preparar la consulta del usuario.');
    }

    $stmt->bind_param('i', $usuarioId);
    if (!$stmt->execute()) {
        $stmt->close();
        throw new RuntimeException('No se pudo obtener la información del usuario.');
    }

    $result = $stmt->get_result();
    $userRow = $result ? $result->fetch_assoc() : null;
    if ($result instanceof mysqli_result) {
        $result->close();
    }
    $stmt->close();

    if (!$userRow) {
        http_response_code(401);
        echo json_encode(['error' => 'No se pudo validar al usuario que atiende la alerta.']);
        $conn->close();
        exit;
    }

    $usuarioNombre = trim(((string) ($userRow['nombre'] ?? '')) . ' ' . ((string) ($userRow['apellidos'] ?? '')));
    if ($usuarioNombre === '') {
        $usuarioNombre = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
    }
    if ($usuarioNombre === '') {
        $usuarioNombre = $_SESSION['usuario'] ?? 'Usuario del sistema';
    }

    $usuarioCorreo = (string) ($userRow['correo'] ?? ($_SESSION['usuario'] ?? 'desconocido@sistema.local'));
    if ($usuarioCorreo === '') {
        $usuarioCorreo = 'desconocido@sistema.local';
    }

    $options = [];
    if (array_key_exists('notes', $payload)) {
        $options['notes'] = $payload['notes'];
    }

    $updated = calibration_alerts_attend_notification(
        $conn,
        $notificationId,
        $empresaId,
        $action,
        $usuarioId,
        $usuarioNombre,
        $usuarioCorreo,
        $options
    );

    echo json_encode([
        'success' => true,
        'data' => $updated,
    ]);
} catch (InvalidArgumentException $invalid) {
    http_response_code(400);
    echo json_encode(['error' => $invalid->getMessage()]);
} catch (Throwable $exception) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo actualizar la alerta.', 'details' => $exception->getMessage()]);
}

$conn->close();
