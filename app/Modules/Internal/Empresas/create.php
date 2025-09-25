<?php
session_start();
header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('configuracion_actualizar')) {
    http_response_code(403);
    echo json_encode(["success" => false, "msg" => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company_provisioning.php';

function empresas_respond_error(string $message, int $status = 400): void
{
    http_response_code($status);
    echo json_encode(['success' => false, 'msg' => $message]);
    exit;
}

$nombre = trim((string) ($_POST['nombre'] ?? ''));
$contacto = trim((string) ($_POST['contacto'] ?? ''));
$direccion = trim((string) ($_POST['direccion'] ?? ''));
$telefono = trim((string) ($_POST['telefono'] ?? ''));
$emailRaw = $_POST['email'] ?? null;
$email = null;
if ($emailRaw !== null && $emailRaw !== '') {
    $email = filter_var($emailRaw, FILTER_VALIDATE_EMAIL);
    if ($email === false) {
        empresas_respond_error('El correo electrónico proporcionado no es válido.');
    }
}

if ($nombre === '') {
    empresas_respond_error('El nombre de la empresa es obligatorio.');
}

if (mb_strlen($nombre) > 150) {
    empresas_respond_error('El nombre de la empresa excede el límite permitido.');
}

if (mb_strlen($telefono) > 50) {
    empresas_respond_error('El teléfono excede el límite permitido.');
}

try {
    $conn->begin_transaction();

    $stmt = $conn->prepare('INSERT INTO empresas (nombre, contacto, direccion, telefono, email, activo) VALUES (?, ?, ?, ?, ?, 1)');
    if (!$stmt) {
        throw new RuntimeException('No fue posible preparar el registro de la empresa.');
    }

    $contactoParam = $contacto !== '' ? $contacto : null;
    $direccionParam = $direccion !== '' ? $direccion : null;
    $telefonoParam = $telefono !== '' ? $telefono : null;

    $stmt->bind_param('sssss', $nombre, $contactoParam, $direccionParam, $telefonoParam, $email);
    if (!$stmt->execute()) {
        $stmt->close();
        throw new RuntimeException('No fue posible registrar la empresa.');
    }

    $empresaId = (int) $conn->insert_id;
    $stmt->close();

    if ($empresaId <= 0) {
        throw new RuntimeException('No se obtuvo el identificador de la empresa registrada.');
    }

    company_provision_roles($conn, $empresaId);
    if ($conn->errno !== 0) {
        throw new RuntimeException('No fue posible configurar los roles iniciales de la empresa.');
    }

    $conn->commit();

    echo json_encode([
        'success' => true,
        'empresa_id' => $empresaId,
        'msg' => 'Empresa registrada correctamente.',
    ]);
} catch (Throwable $e) {
    if ($conn->errno && $conn->error) {
        error_log('Fallo al registrar empresa: ' . $conn->error);
    }
    if ($conn->in_transaction) {
        $conn->rollback();
    }
    $status = $e instanceof RuntimeException ? 400 : 500;
    empresas_respond_error($e->getMessage(), $status);
}
