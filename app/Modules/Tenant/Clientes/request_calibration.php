<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

header('Content-Type: application/json');

ensure_portal_access('tenant');

$roleAlias = session_role_alias() ?? '';
if ($roleAlias !== 'cliente') {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso permitido únicamente para clientes.']);
    exit;
}

$empresaId = obtenerEmpresaId();
$usuarioId = $_SESSION['usuario_id'] ?? null;
if (!$empresaId || !$usuarioId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Sesión incompleta.']);
    exit;
}

$instrumentoId = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT);
$tipo          = trim($_POST['tipo'] ?? '');
$fechaDeseada  = trim($_POST['fecha_deseada'] ?? '');
$comentarios   = trim($_POST['comentarios'] ?? '');
$instrucciones = null;
if (array_key_exists('instrucciones_cliente', $_POST)) {
    $instrucciones = trim((string) ($_POST['instrucciones_cliente'] ?? ''));
    if ($instrucciones === '') {
        $instrucciones = null;
    } elseif (mb_strlen($instrucciones) > 2000) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Las instrucciones especiales no pueden superar los 2000 caracteres.']);
        exit;
    }
}

if (!$instrumentoId) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Debe seleccionar un instrumento válido.']);
    exit;
}

$tipoNormalizado = strtolower($tipo);
if ($tipoNormalizado === 'interna') {
    $tipo = 'Interna';
} elseif ($tipoNormalizado === 'externa') {
    $tipo = 'Externa';
} else {
    $tipo = 'Solicitud';
}

if ($fechaDeseada !== '') {
    $fecha = DateTime::createFromFormat('Y-m-d', $fechaDeseada);
    if (!$fecha) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'La fecha deseada no tiene el formato correcto.']);
        exit;
    }
    $fechaDeseada = $fecha->format('Y-m-d');
} else {
    $fechaDeseada = null;
}

$check = $conn->prepare('SELECT 1 FROM instrumentos WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$check) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo validar el instrumento.']);
    exit;
}
$check->bind_param('ii', $instrumentoId, $empresaId);
$check->execute();
$exists = false;
if (method_exists($check, 'store_result')) {
    $check->store_result();
    $exists = $check->num_rows > 0;
} elseif (method_exists($check, 'get_result')) {
    $result = $check->get_result();
    $exists = $result && $result->num_rows > 0;
}
$check->close();

if (!$exists) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'El instrumento seleccionado no pertenece a su empresa.']);
    exit;
}

$estado = 'Pendiente';
$sql = 'INSERT INTO solicitudes_calibracion (empresa_id, instrumento_id, usuario_id, tipo, fecha_deseada, comentarios, instrucciones_cliente, estado)'
     . ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la solicitud.']);
    exit;
}
$stmt->bind_param(
    'iiisssss',
    $empresaId,
    $instrumentoId,
    $usuarioId,
    $tipo,
    $fechaDeseada,
    $comentarios,
    $instrucciones,
    $estado
);

if ($stmt->execute()) {
    echo json_encode(['success' => true, 'message' => 'Solicitud de calibración registrada.']);
} else {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible guardar la solicitud.']);
}

$stmt->close();
$conn->close();
