<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('usuarios_edit')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Internal/Auditoria/audit.php';

header('Content-Type: application/json; charset=UTF-8');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no especificada']);
    exit;
}

$usuarioId = filter_input(INPUT_POST, 'usuario_id', FILTER_VALIDATE_INT);
if (!$usuarioId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Usuario inválido']);
    exit;
}

$descripcion = trim($_POST['descripcion'] ?? '');
$fechaEvidencia = trim($_POST['evidencia_fecha'] ?? '');
if ($fechaEvidencia !== '') {
    $date = DateTime::createFromFormat('Y-m-d', $fechaEvidencia);
    if (!$date || $date->format('Y-m-d') !== $fechaEvidencia) {
        http_response_code(400);
        echo json_encode(['success' => false, 'message' => 'Fecha de evidencia inválida']);
        exit;
    }
}

$catalogoId = filter_input(INPUT_POST, 'catalogo_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if ($catalogoId === false) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Selecciona un tipo de instrumento válido.']);
    exit;
}

$vigenteDesdeRaw = trim($_POST['vigente_desde'] ?? '');
$vigenteHastaRaw = trim($_POST['vigente_hasta'] ?? '');

$vigenteDesde = null;
if ($vigenteDesdeRaw !== '') {
    $desde = DateTime::createFromFormat('Y-m-d', $vigenteDesdeRaw);
    if (!$desde || $desde->format('Y-m-d') !== $vigenteDesdeRaw) {
        http_response_code(400);
        echo json_encode(['success' => false, 'message' => 'La fecha de inicio de vigencia es inválida.']);
        exit;
    }
    $vigenteDesde = $desde->format('Y-m-d');
}

$vigenteHasta = null;
if ($vigenteHastaRaw !== '') {
    $hasta = DateTime::createFromFormat('Y-m-d', $vigenteHastaRaw);
    if (!$hasta || $hasta->format('Y-m-d') !== $vigenteHastaRaw) {
        http_response_code(400);
        echo json_encode(['success' => false, 'message' => 'La fecha de fin de vigencia es inválida.']);
        exit;
    }
    $vigenteHasta = $hasta->format('Y-m-d');
}

if ($vigenteDesde !== null && $vigenteHasta !== null && $vigenteDesde > $vigenteHasta) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'La fecha de fin debe ser posterior a la fecha de inicio.']);
    exit;
}

if ($catalogoId !== null) {
    $catalogoStmt = $conn->prepare('SELECT id FROM catalogo_instrumentos WHERE id = ? LIMIT 1');
    if (!$catalogoStmt) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo validar la familia del instrumento.']);
        exit;
    }
    $catalogoStmt->bind_param('i', $catalogoId);
    $catalogoStmt->execute();
    $catalogoStmt->store_result();
    if ($catalogoStmt->num_rows === 0) {
        $catalogoStmt->close();
        http_response_code(400);
        echo json_encode(['success' => false, 'message' => 'La familia del instrumento no existe.']);
        exit;
    }
    $catalogoStmt->close();
}

$check = $conn->prepare('SELECT empresa_id FROM usuarios WHERE id = ? LIMIT 1');
if (!$check) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo verificar el usuario']);
    exit;
}
$check->bind_param('i', $usuarioId);
$check->execute();
$check->bind_result($usuarioEmpresaId);
if (!$check->fetch()) {
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'Usuario no encontrado']);
    $check->close();
    exit;
}
$check->close();

if ((int) $usuarioEmpresaId !== (int) $empresaId) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'No puedes actualizar usuarios de otra empresa']);
    exit;
}

if (!isset($_FILES['archivo']) || $_FILES['archivo']['error'] !== UPLOAD_ERR_OK) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Debes adjuntar un archivo de evidencia']);
    exit;
}

$archivo = $_FILES['archivo'];
$allowedExtensions = ['pdf', 'jpg', 'jpeg', 'png'];
$originalName = $archivo['name'];
$extension = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));
if (!in_array($extension, $allowedExtensions, true)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Tipo de archivo no permitido']);
    exit;
}

$finfo = new finfo(FILEINFO_MIME_TYPE);
$mimeType = $finfo->file($archivo['tmp_name']);
$allowedMime = [
    'pdf' => 'application/pdf',
    'jpg' => 'image/jpeg',
    'jpeg' => 'image/jpeg',
    'png' => 'image/png',
];
if (!isset($allowedMime[$extension]) || $allowedMime[$extension] !== $mimeType) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'El archivo no coincide con el tipo permitido']);
    exit;
}

$destDir = dirname(__DIR__, 4) . '/storage/usuarios/competencias/';
if (!is_dir($destDir)) {
    if (!mkdir($destDir, 0755, true) && !is_dir($destDir)) {
        http_response_code(500);
        echo json_encode(['success' => false, 'message' => 'No se pudo crear el directorio de almacenamiento']);
        exit;
    }
}

try {
    $safeName = bin2hex(random_bytes(16));
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo generar el nombre del archivo']);
    exit;
}

$newFileName = $safeName . '.' . $extension;
$destination = $destDir . $newFileName;
if (!move_uploaded_file($archivo['tmp_name'], $destination)) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo guardar el archivo de evidencia']);
    exit;
}

$stmt = $conn->prepare('INSERT INTO usuario_competencias (usuario_id, empresa_id, descripcion, archivo, evidencia_fecha, catalogo_id, vigente_desde, vigente_hasta) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
if (!$stmt) {
    @unlink($destination);
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la evidencia']);
    exit;
}
$fechaParam = $fechaEvidencia !== '' ? $fechaEvidencia : null;
$descripcionParam = $descripcion !== '' ? $descripcion : null;
$catalogoParam = $catalogoId;
$vigenteDesdeParam = $vigenteDesde;
$vigenteHastaParam = $vigenteHasta;
$stmt->bind_param('iisssiss', $usuarioId, $empresaId, $descripcionParam, $newFileName, $fechaParam, $catalogoParam, $vigenteDesdeParam, $vigenteHastaParam);
if (!$stmt->execute()) {
    @unlink($destination);
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo registrar la evidencia']);
    $stmt->close();
    exit;
}
$stmt->close();

$nombreAud = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = $_SESSION['usuario'] ?? null;
log_activity($nombreAud, "Adjuntó evidencia de competencia al usuario $usuarioId", 'usuarios', $correoAud);

echo json_encode(['success' => true, 'message' => 'Evidencia registrada correctamente']);
$conn->close();
