<?php
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 2) . '/Internal/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

session_start();

ensure_portal_access('service');

header('Content-Type: application/json');

if (!check_permission('calibraciones_aprobar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado.']);
    exit;
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido.']);
    exit;
}

$calibrationId = filter_input(INPUT_POST, 'calibration_id', FILTER_VALIDATE_INT);
if ($calibrationId === false || $calibrationId === null) {
    $calibrationId = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
}

if (!$calibrationId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Identificador de calibración inválido.']);
    exit;
}

$password = trim($_POST['password'] ?? '');
if ($password === '') {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Debes ingresar tu contraseña para firmar la liberación.']);
    exit;
}

$twoFactorCode = trim($_POST['two_factor_code'] ?? '');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada.']);
    exit;
}

$userId = $_SESSION['usuario_id'] ?? null;
if (!$userId) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Sesión no válida.']);
    exit;
}

$calibrationStmt = $conn->prepare(
    "SELECT cal.id, cal.instrumento_id, cal.liberado_por, cal.fecha_liberacion, cal.resultado, cal.periodo,\n            cal.fecha_calibracion, cal.fecha_proxima, cal.u_value, cal.u_method, cal.u_k, cal.u_coverage,\n            cal.estado AS estado, cal.aprobado_por AS aprobado_por, cal.fecha_aprobacion AS fecha_aprobacion,\n            cert.archivo AS certificado\n     FROM calibraciones cal\n     LEFT JOIN (\n         SELECT c1.*\n         FROM certificados c1\n         JOIN (\n             SELECT calibracion_id, MAX(id) AS max_id\n             FROM certificados\n             GROUP BY calibracion_id\n         ) latest ON latest.calibracion_id = c1.calibracion_id AND latest.max_id = c1.id\n     ) cert ON cert.calibracion_id = cal.id\n     WHERE cal.id = ? AND cal.empresa_id = ?\n     LIMIT 1"
);

if (!$calibrationStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar la consulta de calibración.']);
    exit;
}

$calibrationStmt->bind_param('ii', $calibrationId, $empresaId);
$calibrationStmt->execute();
$result = $calibrationStmt->get_result();
$calibration = $result ? $result->fetch_assoc() : null;
$calibrationStmt->close();

if (!$calibration) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Calibración no encontrada.']);
    exit;
}

if (!empty($calibration['liberado_por']) && !empty($calibration['fecha_liberacion'])) {
    echo json_encode(['success' => false, 'message' => 'La calibración ya fue liberada anteriormente.']);
    exit;
}

$estado = isset($calibration['estado']) ? trim((string) $calibration['estado']) : '';
if ($estado !== 'Aprobado') {
    echo json_encode(['success' => false, 'message' => 'Primero debes contar con la aprobación registrada en el portal interno.']);
    exit;
}

$aprobadoPor = isset($calibration['aprobado_por']) ? (int) $calibration['aprobado_por'] : 0;
$fechaAprobacion = isset($calibration['fecha_aprobacion']) ? trim((string) $calibration['fecha_aprobacion']) : '';
if (!$aprobadoPor || $fechaAprobacion === '') {
    echo json_encode(['success' => false, 'message' => 'Esta calibración aún no ha sido aprobada por otro responsable.']);
    exit;
}

if ($aprobadoPor === (int) $userId) {
    echo json_encode(['success' => false, 'message' => 'La liberación debe ser realizada por una persona distinta a quien aprobó la calibración.']);
    exit;
}

if (empty($calibration['certificado'])) {
    echo json_encode(['success' => false, 'message' => 'Adjunta un certificado antes de solicitar la liberación.']);
    exit;
}

$uncertaintyMissing = (
    $calibration['u_value'] === null
    || $calibration['u_method'] === null || trim((string) $calibration['u_method']) === ''
    || $calibration['u_k'] === null
    || $calibration['u_coverage'] === null || trim((string) $calibration['u_coverage']) === ''
);

if ($uncertaintyMissing) {
    echo json_encode([
        'success' => false,
        'message' => 'Antes de liberar la calibración registra la incertidumbre, método, factor k y cobertura.'
    ]);
    exit;
}

$userStmt = $conn->prepare('SELECT contrasena, nombre, apellidos, correo FROM usuarios WHERE id = ? LIMIT 1');
if (!$userStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo validar al usuario firmante.']);
    exit;
}
$userStmt->bind_param('i', $userId);
$userStmt->execute();
$userStmt->bind_result($hash, $nombreDb, $apellidosDb, $correoDb);
if (!$userStmt->fetch()) {
    $userStmt->close();
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Usuario no válido para firmar.']);
    exit;
}
$userStmt->close();

if (!password_verify($password, $hash)) {
    echo json_encode(['success' => false, 'message' => 'La contraseña ingresada es incorrecta.']);
    exit;
}

if (!tenant_calibration_validate_two_factor($conn, $userId, $twoFactorCode)) {
    echo json_encode(['success' => false, 'message' => 'El código de verificación de dos factores no es válido.']);
    exit;
}

$fechaLiberacion = date('Y-m-d H:i:s');
$updateStmt = $conn->prepare('UPDATE calibraciones SET liberado_por = ?, fecha_liberacion = ? WHERE id = ? AND empresa_id = ?');
if (!$updateStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la liberación.']);
    exit;
}
$updateStmt->bind_param('isii', $userId, $fechaLiberacion, $calibrationId, $empresaId);
if (!$updateStmt->execute()) {
    $updateStmt->close();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'Error al actualizar la calibración.']);
    exit;
}
$updateStmt->close();

$nombreOperador = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
if ($nombreOperador === '') {
    $nombreOperador = 'Desconocido';
}
log_activity($nombreOperador, "Liberación de calibración {$calibrationId}", 'calibraciones', $correoDb ?: null);

echo json_encode([
    'success' => true,
    'message' => 'Liberación registrada correctamente tras la aprobación previa.',
    'fecha_liberacion' => $fechaLiberacion,
]);
exit;

function tenant_calibration_validate_two_factor(mysqli $conn, int $userId, string $code): bool
{
    $secret = tenant_calibration_two_factor_secret($conn, $userId);
    if ($secret === null || $secret === '') {
        return $code === '';
    }
    return tenant_calibration_verify_totp($secret, $code);
}

function tenant_calibration_two_factor_secret(mysqli $conn, int $userId): ?string
{
    static $columnChecked = null;
    static $columnExists = false;

    if ($columnChecked === null) {
        $columnChecked = true;
        $result = $conn->query("SHOW COLUMNS FROM usuarios LIKE 'two_factor_secret'");
        if ($result instanceof mysqli_result) {
            $columnExists = $result->num_rows > 0;
            $result->free();
        }
    }

    if (!$columnExists) {
        return null;
    }

    $stmt = $conn->prepare('SELECT two_factor_secret FROM usuarios WHERE id = ? LIMIT 1');
    if (!$stmt) {
        return null;
    }
    $stmt->bind_param('i', $userId);
    $stmt->execute();
    $stmt->bind_result($secret);
    $value = null;
    if ($stmt->fetch()) {
        $value = $secret;
    }
    $stmt->close();
    return $value !== null ? trim((string) $value) : null;
}

function tenant_calibration_verify_totp(string $secret, string $code, int $window = 1, int $period = 30): bool
{
    $code = preg_replace('/\s+/', '', $code);
    if ($code === '' || !ctype_digit($code)) {
        return false;
    }

    $digits = tenant_calibration_totp_digits();
    if (strlen($code) !== $digits) {
        return false;
    }

    $binaryKey = tenant_calibration_base32_decode($secret);
    if ($binaryKey === '') {
        return false;
    }

    $timestamp = time();

    for ($i = -$window; $i <= $window; $i++) {
        $timeSlice = $timestamp + ($i * $period);
        $generated = tenant_calibration_generate_totp($binaryKey, $timeSlice, $period, $digits);
        if (hash_equals($generated, $code)) {

            return true;
        }
    }

    return false;
}

function tenant_calibration_totp_digits(): int
{
    static $digits = null;

    if ($digits !== null) {
        return $digits;
    }

    $candidates = [
        getenv('TENANT_CALIBRATION_TOTP_DIGITS'),
        $_ENV['TENANT_CALIBRATION_TOTP_DIGITS'] ?? null,
        $_SERVER['TENANT_CALIBRATION_TOTP_DIGITS'] ?? null,
        getenv('TOTP_DIGITS'),
        $_ENV['TOTP_DIGITS'] ?? null,
        $_SERVER['TOTP_DIGITS'] ?? null,
    ];

    foreach ($candidates as $value) {
        if ($value === null || $value === false) {
            continue;
        }

        $trimmed = trim((string) $value);
        if ($trimmed === '' || !ctype_digit($trimmed)) {
            continue;
        }

        $numeric = (int) $trimmed;
        if ($numeric < 4 || $numeric > 10) {
            continue;
        }

        $digits = $numeric;
        return $digits;
    }

    $digits = 6;
    return $digits;
}

function tenant_calibration_generate_totp(string $key, int $time, int $period, int $digits): string
{
    $counter = (int) floor($time / $period);
    $binaryCounter = pack('N*', 0, $counter);
    $hash = hash_hmac('sha1', $binaryCounter, $key, true);
    $offset = ord(substr($hash, -1)) & 0x0F;
    $truncated = ((ord($hash[$offset]) & 0x7F) << 24)
        | ((ord($hash[$offset + 1]) & 0xFF) << 16)
        | ((ord($hash[$offset + 2]) & 0xFF) << 8)
        | (ord($hash[$offset + 3]) & 0xFF);
    $code = $truncated % (10 ** $digits);
    return str_pad((string) $code, $digits, '0', STR_PAD_LEFT);
}

function tenant_calibration_base32_decode(string $secret): string
{
    $alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
    $secret = strtoupper(preg_replace('/[^A-Z2-7]/', '', $secret));
    if ($secret === '') {
        return '';
    }

    $buffer = 0;
    $bitsLeft = 0;
    $output = '';

    $length = strlen($secret);
    for ($i = 0; $i < $length; $i++) {
        $char = $secret[$i];
        $pos = strpos($alphabet, $char);
        if ($pos === false) {
            return '';
        }

        $buffer = ($buffer << 5) | $pos;
        $bitsLeft += 5;

        if ($bitsLeft >= 8) {
            $bitsLeft -= 8;
            $output .= chr(($buffer >> $bitsLeft) & 0xFF);
        }
    }

    return $output;
}
