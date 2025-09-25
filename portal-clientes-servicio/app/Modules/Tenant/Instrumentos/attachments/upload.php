<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once __DIR__ . '/InstrumentAttachmentService.php';

header('Content-Type: application/json');

if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$instrumentoId = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);

if (!$instrumentoId) {
    http_response_code(422);
    echo json_encode(['error' => 'Instrumento no especificado']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

if (!isset($_FILES['archivo'])) {
    http_response_code(422);
    echo json_encode(['error' => 'Debe adjuntar un archivo']);
    $conn->close();
    exit;
}

$archivo = $_FILES['archivo'];
if (is_array($archivo['error'])) {
    http_response_code(422);
    echo json_encode(['error' => 'Solo se admite la carga de un archivo por solicitud']);
    $conn->close();
    exit;
}

$error = (int) ($archivo['error'] ?? UPLOAD_ERR_NO_FILE);
if ($error !== UPLOAD_ERR_OK) {
    $messages = [
        UPLOAD_ERR_INI_SIZE => 'El archivo excede el tamaño máximo permitido.',
        UPLOAD_ERR_FORM_SIZE => 'El archivo excede el límite del formulario.',
        UPLOAD_ERR_PARTIAL => 'El archivo se cargó de forma incompleta.',
        UPLOAD_ERR_NO_FILE => 'No se seleccionó archivo.',
    ];
    $message = $messages[$error] ?? 'No se pudo cargar el archivo.';
    http_response_code(422);
    echo json_encode(['error' => $message]);
    $conn->close();
    exit;
}

$fileInfo = [
    'tmp_path' => $archivo['tmp_name'] ?? '',
    'original_name' => $archivo['name'] ?? '',
    'size' => $archivo['size'] ?? 0,
    'mime' => $archivo['type'] ?? null,
];

$tipo = isset($_POST['tipo']) ? (string) $_POST['tipo'] : null;
$descripcion = isset($_POST['descripcion']) ? (string) $_POST['descripcion'] : null;
$usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;

try {
    $attachment = InstrumentAttachmentService::createAttachment(
        $conn,
        $instrumentoId,
        $empresaId,
        $fileInfo,
        $tipo,
        $descripcion,
        $usuarioId
    );

    echo json_encode([
        'success' => true,
        'attachment' => $attachment,
        'total' => InstrumentAttachmentService::countByInstrument($conn, $instrumentoId, $empresaId),
    ], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
} catch (Throwable $e) {
    file_put_contents('php://stderr', '[instrumento_adjuntos] Error al subir: ' . $e->getMessage() . PHP_EOL);
    http_response_code(500);
    echo json_encode(['error' => $e->getMessage()]);
}

$conn->close();

