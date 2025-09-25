<?php
session_start();

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';

header('Content-Type: application/json');

if (!isset($_SESSION['usuario_id'])) {
    http_response_code(401);
    echo json_encode(['error' => 'Sesión no válida']);
    exit;
}

if (!check_permission('feedback_crear')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

$usuarioId = (int) $_SESSION['usuario_id'];
$allowedOrigen = ['internal', 'tenant'];
$allowedTipos = ['error', 'sugerencia'];
$allowedEstados = ['abierto', 'en_progreso', 'cerrado'];

$origenInput = isset($_POST['origen']) ? strtolower(trim((string) $_POST['origen'])) : '';
$origen = in_array($origenInput, $allowedOrigen, true) ? $origenInput : null;

if ($origen === null) {
    $alias = session_role_alias() ?? '';
    $origen = $alias === 'cliente' ? 'tenant' : 'internal';
}

$tipoInput = isset($_POST['tipo']) ? strtolower(trim((string) $_POST['tipo'])) : '';
if (!in_array($tipoInput, $allowedTipos, true)) {
    http_response_code(422);
    echo json_encode(['error' => 'Tipo de feedback no válido']);
    exit;
}

$resumen = trim((string) ($_POST['resumen'] ?? ''));
if ($resumen === '') {
    http_response_code(422);
    echo json_encode(['error' => 'El resumen es obligatorio']);
    exit;
}

$descripcion = trim((string) ($_POST['descripcion'] ?? ''));
$allowedPrioridades = ['baja', 'media', 'alta', 'critica'];
$prioridadInput = isset($_POST['prioridad']) ? strtolower(trim((string) $_POST['prioridad'])) : '';
if (!in_array($prioridadInput, $allowedPrioridades, true)) {
    $prioridadInput = 'media';
}
$prioridad = $prioridadInput;

$estadoInput = isset($_POST['estado']) ? strtolower(trim((string) $_POST['estado'])) : 'abierto';
$estado = in_array($estadoInput, $allowedEstados, true) ? $estadoInput : 'abierto';

$empresaId = null;
if (session_is_superadmin()) {
    $empresaIdFilter = filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT, [
        'options' => ['min_range' => 1],
    ]);
    if ($empresaIdFilter !== false && $empresaIdFilter !== null) {
        $empresaId = (int) $empresaIdFilter;
    }
} else {
    $empresaId = ensure_session_empresa_id();
}

if ($origen === 'tenant' && $empresaId === null) {
    $empresaId = ensure_session_empresa_id();
}

$pendingAttachments = [];
$maxAttachments = 5;
$maxAttachmentSize = 10 * 1024 * 1024; // 10 MB por archivo
$allowedExtensions = [
    'pdf'  => ['application/pdf'],
    'jpg'  => ['image/jpeg'],
    'jpeg' => ['image/jpeg'],
    'png'  => ['image/png'],
    'gif'  => ['image/gif'],
    'bmp'  => ['image/bmp'],
    'tif'  => ['image/tiff', 'image/x-tiff'],
    'tiff' => ['image/tiff', 'image/x-tiff'],
    'webp' => ['image/webp'],
    'doc'  => ['application/msword', 'application/octet-stream'],
    'docx' => ['application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'application/zip'],
    'xls'  => ['application/vnd.ms-excel', 'application/octet-stream'],
    'xlsx' => ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/zip'],
    'txt'  => ['text/plain'],
    'csv'  => ['text/csv', 'text/plain'],
    'zip'  => ['application/zip', 'application/x-zip-compressed', 'multipart/x-zip'],
    'rar'  => ['application/x-rar', 'application/x-rar-compressed', 'application/vnd.rar'],
    '7z'   => ['application/x-7z-compressed'],
];

if (isset($_FILES['evidencias']) && is_array($_FILES['evidencias']['name'])) {
    $names = $_FILES['evidencias']['name'];
    $tmpNames = $_FILES['evidencias']['tmp_name'];
    $errors = $_FILES['evidencias']['error'];
    $sizes = $_FILES['evidencias']['size'];

    $fileCount = is_array($names) ? count($names) : 0;
    $selectedFiles = 0;

    $uploadDir = __DIR__ . '/uploads/';
    $finfo = class_exists('finfo') ? new finfo(FILEINFO_MIME_TYPE) : null;

    for ($i = 0; $i < $fileCount; $i++) {
        $error = $errors[$i] ?? UPLOAD_ERR_NO_FILE;
        if ($error === UPLOAD_ERR_NO_FILE) {
            continue;
        }

        $selectedFiles++;
        if ($selectedFiles > $maxAttachments) {
            http_response_code(422);
            echo json_encode(['error' => 'Se permiten máximo ' . $maxAttachments . ' archivos adjuntos.']);
            exit;
        }

        if ($error !== UPLOAD_ERR_OK) {
            http_response_code(422);
            echo json_encode(['error' => 'Ocurrió un error al cargar uno de los archivos.']);
            exit;
        }

        $tmpName = $tmpNames[$i] ?? '';
        $originalName = (string) ($names[$i] ?? '');
        $size = isset($sizes[$i]) ? (int) $sizes[$i] : 0;

        if ($size > $maxAttachmentSize) {
            http_response_code(422);
            echo json_encode(['error' => 'Cada archivo debe ser menor a 10 MB.']);
            exit;
        }

        $extension = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));
        if ($extension === '' || !array_key_exists($extension, $allowedExtensions)) {
            http_response_code(422);
            echo json_encode(['error' => 'Tipo de archivo no permitido.']);
            exit;
        }

        if (!is_uploaded_file($tmpName)) {
            http_response_code(400);
            echo json_encode(['error' => 'Archivo no válido.']);
            exit;
        }

        $mimeType = null;
        if ($finfo instanceof finfo) {
            $detected = $finfo->file($tmpName);
            if ($detected === false) {
                http_response_code(422);
                echo json_encode(['error' => 'No se pudo validar el archivo.']);
                exit;
            }
            $mimeType = $detected;
            if (!in_array($detected, $allowedExtensions[$extension], true)) {
                http_response_code(422);
                echo json_encode(['error' => 'El archivo adjunto no coincide con un tipo permitido.']);
                exit;
            }
        } elseif (function_exists('mime_content_type')) {
            $detected = @mime_content_type($tmpName);
            if ($detected !== false) {
                $mimeType = $detected;
            }
        }

        if ($mimeType !== null && !in_array($mimeType, $allowedExtensions[$extension], true)) {
            $mimeType = $allowedExtensions[$extension][0] ?? 'application/octet-stream';
        } elseif ($mimeType === null) {
            $mimeType = $allowedExtensions[$extension][0] ?? 'application/octet-stream';
        }

        if (!is_dir($uploadDir)) {
            if (!mkdir($uploadDir, 0755, true) && !is_dir($uploadDir)) {
                http_response_code(500);
                echo json_encode(['error' => 'No se pudo preparar el directorio de carga.']);
                exit;
            }
        }

        try {
            $safeName = bin2hex(random_bytes(16));
        } catch (Exception $exception) {
            http_response_code(500);
            echo json_encode(['error' => 'No se pudo generar un nombre seguro para el archivo.']);
            exit;
        }

        $storedName = $safeName . '.' . $extension;
        $targetPath = $uploadDir . $storedName;

        if (!move_uploaded_file($tmpName, $targetPath)) {
            http_response_code(500);
            echo json_encode(['error' => 'No se pudo guardar uno de los archivos adjuntos.']);
            exit;
        }

        $cleanOriginal = preg_replace('/[\r\n\t]+/', ' ', $originalName);
        $pendingAttachments[] = [
            'original_name' => $cleanOriginal !== null && $cleanOriginal !== '' ? $cleanOriginal : $storedName,
            'stored_name' => $storedName,
            'mime_type' => $mimeType,
            'size' => $size,
            'path' => $targetPath,
        ];
    }
}

$asignadoId = null;
if (isset($_POST['asignado_a']) && $_POST['asignado_a'] !== '') {
    $asignadoFilter = filter_input(INPUT_POST, 'asignado_a', FILTER_VALIDATE_INT, [
        'options' => ['min_range' => 1],
    ]);
    if ($asignadoFilter === false) {
        http_response_code(422);
        echo json_encode(['error' => 'Asignado no válido']);
        exit;
    }
    $asignadoId = (int) $asignadoFilter;
}

$sql = 'INSERT INTO feedback_reports (empresa_id, reporter_id, origen, tipo, resumen, descripcion, estado, prioridad, asignado_a)'
        . ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta']);
    exit;
}

if (!$conn->begin_transaction()) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo iniciar la transacción']);
    $stmt->close();
    exit;
}

$stmt->bind_param(
    'iissssssi',
    $empresaId,
    $usuarioId,
    $origen,
    $tipoInput,
    $resumen,
    $descripcion,
    $estado,
    $prioridad,
    $asignadoId
);

$attachmentSummaries = [];
$reportId = null;
$attachmentStmt = null;

try {
    if (!$stmt->execute()) {
        throw new RuntimeException('No se pudo registrar el feedback');
    }

    $reportId = (int) $conn->insert_id;

    if (!empty($pendingAttachments)) {
        $attachmentStmt = $conn->prepare('INSERT INTO feedback_attachments (feedback_id, nombre_original, archivo, mime_type, tamano) VALUES (?, ?, ?, ?, ?)');
        if (!$attachmentStmt) {
            throw new RuntimeException('No se pudo preparar la consulta de adjuntos');
        }

        $feedbackIdParam = $reportId;
        $originalNameParam = null;
        $storedNameParam = null;
        $mimeTypeParam = null;
        $sizeParam = null;

        $attachmentStmt->bind_param('isssi', $feedbackIdParam, $originalNameParam, $storedNameParam, $mimeTypeParam, $sizeParam);

        foreach ($pendingAttachments as $attachment) {
            $originalNameParam = substr($attachment['original_name'], 0, 255);
            $storedNameParam = $attachment['stored_name'];
            $mimeTypeParam = $attachment['mime_type'] ?: null;
            $sizeParam = $attachment['size'] !== null ? (int) $attachment['size'] : null;

            if (!$attachmentStmt->execute()) {
                throw new RuntimeException('No se pudo guardar uno de los archivos adjuntos');
            }

            $attachmentId = (int) $attachmentStmt->insert_id;
            $attachmentSummaries[] = [
                'id' => $attachmentId,
                'nombre' => $originalNameParam,
                'descarga_url' => '/backend/feedback/download_attachment.php?id=' . $attachmentId,
            ];
        }

        $attachmentStmt->close();
        $attachmentStmt = null;
    }

    $conn->commit();
} catch (Throwable $throwable) {
    $conn->rollback();
    if ($attachmentStmt instanceof mysqli_stmt) {
        $attachmentStmt->close();
    }

    foreach ($pendingAttachments as $attachment) {
        if (isset($attachment['path']) && is_file($attachment['path'])) {
            @unlink($attachment['path']);
        }
    }

    http_response_code(500);
    echo json_encode(['error' => 'No se pudo registrar el feedback']);
    $stmt->close();
    exit;
}

$stmt->close();

echo json_encode([
    'success' => true,
    'report_id' => $reportId,
    'attachments' => $attachmentSummaries,
]);
