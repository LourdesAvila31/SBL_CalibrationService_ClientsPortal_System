<?php
session_start();

header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
ensure_portal_access('service');
if (!check_permission('clientes_gestionar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso no autorizado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';

$calibracionId = filter_input(INPUT_POST, 'calibracion_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
if (!$calibracionId) {
    http_response_code(422);
    echo json_encode(['error' => 'Identificador de calibración no válido.']);
    exit;
}

try {
    $calStmt = $conn->prepare('SELECT id FROM calibraciones WHERE id = ? LIMIT 1');
    if (!$calStmt) {
        throw new RuntimeException('No fue posible validar la calibración.');
    }
    $calStmt->bind_param('i', $calibracionId);
    $calStmt->execute();
    $calibrationExists = false;
    if (method_exists($calStmt, 'store_result')) {
        $calStmt->store_result();
        $calibrationExists = $calStmt->num_rows > 0;
    } else {
        $res = $calStmt->get_result();
        $calibrationExists = $res && $res->num_rows > 0;
    }
    $calStmt->close();

    if (!$calibrationExists) {
        http_response_code(404);
        echo json_encode(['error' => 'La calibración indicada no existe.']);
        exit;
    }

    $uploadKey = null;
    foreach (['archivo', 'certificado', 'file'] as $candidate) {
        if (!empty($_FILES[$candidate]['tmp_name'])) {
            $uploadKey = $candidate;
            break;
        }
    }

    if (!$uploadKey) {
        http_response_code(400);
        echo json_encode(['error' => 'Debes adjuntar un archivo de certificado.']);
        exit;
    }

    $file = $_FILES[$uploadKey];
    if ($file['error'] !== UPLOAD_ERR_OK) {
        http_response_code(400);
        echo json_encode(['error' => 'No se pudo subir el archivo.']);
        exit;
    }

    $originalName = $file['name'] ?? 'certificado.pdf';
    $extension = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));
    if (!in_array($extension, ['pdf', 'png', 'jpg', 'jpeg'], true)) {
        http_response_code(415);
        echo json_encode(['error' => 'Formato de archivo no permitido. Usa PDF o imagen.']);
        exit;
    }

    $destinationDir = realpath(dirname(__DIR__, 3) . '/public/backend/calibraciones/certificates');
    if (!$destinationDir) {
        $destinationDir = dirname(__DIR__, 3) . '/public/backend/calibraciones/certificates';
        if (!is_dir($destinationDir) && !mkdir($destinationDir, 0775, true)) {
            throw new RuntimeException('No se pudo crear el directorio de certificados.');
        }
    }

    $safeName = sprintf('cal_%d_%s.%s', $calibracionId, bin2hex(random_bytes(6)), $extension);
    $targetPath = rtrim($destinationDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $safeName;

    if (!move_uploaded_file($file['tmp_name'], $targetPath)) {
        throw new RuntimeException('No se pudo almacenar el certificado.');
    }

    $relativePath = $safeName;

    $insert = $conn->prepare('INSERT INTO certificados (calibracion_id, archivo) VALUES (?, ?)');
    if (!$insert) {
        throw new RuntimeException('No fue posible registrar el certificado.');
    }
    $insert->bind_param('is', $calibracionId, $relativePath);
    $insert->execute();
    $certId = $insert->insert_id;
    $insert->close();

    echo json_encode([
        'success' => true,
        'certificate' => [
            'id' => (int) $certId,
            'file' => $relativePath,
        ],
    ]);
} catch (Throwable $e) {
    error_log('Error al adjuntar certificado: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible adjuntar el certificado.']);
}
