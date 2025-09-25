<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

if (!check_permission('integraciones_calibradores_vincular')) {
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

$calibradorId = filter_input(INPUT_POST, 'calibrador_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if ($calibradorId === false || $calibradorId === null) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Calibrador inválido']);
    exit;
}

$instrumentoIdRaw = $_POST['instrumento_id'] ?? null;
$instrumentoId = null;
if ($instrumentoIdRaw !== null && $instrumentoIdRaw !== '') {
    $instrumentoId = filter_var($instrumentoIdRaw, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($instrumentoId === false) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'Instrumento inválido']);
        exit;
    }
}

global $conn;
$checkStmt = $conn->prepare('SELECT id FROM calibradores WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$checkStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo validar el calibrador']);
    exit;
}
$checkStmt->bind_param('ii', $calibradorId, $empresaId);
$checkStmt->execute();
$checkStmt->store_result();
if ($checkStmt->num_rows === 0) {
    $checkStmt->close();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Calibrador no encontrado']);
    exit;
}
$checkStmt->close();

if ($instrumentoId !== null) {
    $instrStmt = $conn->prepare('SELECT id FROM instrumentos WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$instrStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo validar el instrumento']);
        exit;
    }
    $instrStmt->bind_param('ii', $instrumentoId, $empresaId);
    $instrStmt->execute();
    $instrStmt->store_result();
    if ($instrStmt->num_rows === 0) {
        $instrStmt->close();
        http_response_code(404);
        echo json_encode(['success' => false, 'message' => 'Instrumento no encontrado']);
        exit;
    }
    $instrStmt->close();
}

$updateStmt = $conn->prepare('UPDATE calibradores SET instrumento_id_default = ?, actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
if (!$updateStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo actualizar el calibrador']);
    exit;
}
$updateStmt->bind_param('iii', $instrumentoId, $calibradorId, $empresaId);
$success = $updateStmt->execute();
$updateStmt->close();

if (!$success) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo vincular el calibrador']);
    exit;
}

echo json_encode(['success' => true]);
