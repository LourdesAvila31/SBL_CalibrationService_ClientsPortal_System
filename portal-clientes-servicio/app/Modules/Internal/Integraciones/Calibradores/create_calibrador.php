<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

if (!check_permission('integraciones_calibradores_configurar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

header('Content-Type: application/json');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no determinada']);
    exit;
}

$nombre = trim($_POST['nombre'] ?? '');
$numeroSerie = trim($_POST['numero_serie'] ?? '');
$fabricante = trim($_POST['fabricante'] ?? '');
$modelo = trim($_POST['modelo'] ?? '');
$tipo = trim($_POST['tipo'] ?? '');
$descripcion = trim($_POST['descripcion'] ?? '');
$instrumentoIdRaw = $_POST['instrumento_id_default'] ?? null;
$tokenManual = trim($_POST['token_firma'] ?? '');
$activo = isset($_POST['activo']) ? (int) (($_POST['activo'] === '0' || $_POST['activo'] === 0) ? 0 : 1) : 1;

if ($nombre === '') {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'El nombre es obligatorio']);
    exit;
}

$instrumentoId = null;
if ($instrumentoIdRaw !== null && $instrumentoIdRaw !== '') {
    $instrumentoId = filter_var($instrumentoIdRaw, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($instrumentoId === false) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Instrumento inválido']);
        exit;
    }

    $instrumentoStmt = $conn->prepare('SELECT id FROM instrumentos WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$instrumentoStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo validar el instrumento']);
        exit;
    }
    $instrumentoStmt->bind_param('ii', $instrumentoId, $empresaId);
    $instrumentoStmt->execute();
    $instrumentoStmt->store_result();
    if ($instrumentoStmt->num_rows === 0) {
        $instrumentoStmt->close();
        http_response_code(404);
        echo json_encode(['success' => false, 'message' => 'Instrumento no encontrado']);
        exit;
    }
    $instrumentoStmt->close();
}

if ($tokenManual !== '') {
    $token = $tokenManual;
} else {
    try {
        $token = bin2hex(random_bytes(20));
    } catch (Exception $e) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo generar un token seguro']);
        exit;
    }
}

$stmt = $conn->prepare('INSERT INTO calibradores (empresa_id, nombre, numero_serie, fabricante, modelo, tipo, descripcion, instrumento_id_default, token_firma, activo)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo preparar el registro']);
    exit;
}

$stmt->bind_param(
    'issssssisi',
    $empresaId,
    $nombre,
    $numeroSerie,
    $fabricante,
    $modelo,
    $tipo,
    $descripcion,
    $instrumentoId,
    $token,
    $activo
);

if (!$stmt->execute()) {
    $errorCode = $stmt->errno;
    $stmt->close();
    if ($errorCode === 1062) {
        http_response_code(409);
        echo json_encode(['success' => false, 'message' => 'Ya existe un calibrador con ese nombre en la empresa']);
        exit;
    }
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo crear el calibrador']);
    exit;
}

$calibradorId = $stmt->insert_id;
$stmt->close();

echo json_encode([
    'success' => true,
    'data' => [
        'id' => $calibradorId,
        'nombre' => $nombre,
        'numero_serie' => $numeroSerie,
        'fabricante' => $fabricante,
        'modelo' => $modelo,
        'tipo' => $tipo,
        'descripcion' => $descripcion,
        'instrumento_id_default' => $instrumentoId,
        'token_firma' => $token,
        'activo' => $activo,
    ],
]);
