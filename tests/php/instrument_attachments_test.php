<?php
try {
    require_once dirname(__DIR__) . '/../app/Core/db.php';
} catch (Throwable $connectionError) {
    fwrite(STDOUT, "Prueba de adjuntos omitida: " . $connectionError->getMessage() . PHP_EOL);
    exit(0);
}
require_once dirname(__DIR__) . '/../app/Modules/Tenant/Instrumentos/attachments/InstrumentAttachmentService.php';

session_start();

function fail_and_exit(string $message, mysqli $conn, ?int $instrumentId = null): void {
    if ($instrumentId !== null) {
        $cleanup = $conn->prepare('DELETE FROM instrumentos WHERE id = ?');
        if ($cleanup) {
            $cleanup->bind_param('i', $instrumentId);
            $cleanup->execute();
            $cleanup->close();
        }
    }
    fwrite(STDERR, $message . PHP_EOL);
    $conn->close();
    exit(1);
}

$empresaId = 1;
$instrumentId = null;

try {
    $usuarioId = null;
    if ($result = $conn->query('SELECT id FROM usuarios ORDER BY id ASC LIMIT 1')) {
        if ($row = $result->fetch_assoc()) {
            $usuarioId = (int) $row['id'];
        }
        $result->close();
    }

    $serie = 'TEST-' . bin2hex(random_bytes(4));
    $codigo = 'DOC-' . bin2hex(random_bytes(3));
    $fechaAlta = date('Y-m-d');
    $proxima = date('Y-m-d', strtotime('+30 days'));

    $stmt = $conn->prepare('INSERT INTO instrumentos (catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id, ubicacion, fecha_alta, estado, proxima_calibracion, programado, empresa_id) VALUES (NULL, NULL, NULL, ?, ?, NULL, \'Área de prueba\', ?, \'activo\', ?, 0, ?)');
    if (!$stmt) {
        fail_and_exit('No se pudo preparar el registro del instrumento de prueba.', $conn, null);
    }
    $stmt->bind_param('ssssi', $serie, $codigo, $fechaAlta, $proxima, $empresaId);
    if (!$stmt->execute()) {
        $stmt->close();
        fail_and_exit('No se pudo registrar el instrumento de prueba.', $conn, null);
    }
    $stmt->close();
    $instrumentId = (int) $conn->insert_id;

    $tmpFile = tempnam(sys_get_temp_dir(), 'att');
    if ($tmpFile === false) {
        fail_and_exit('No se pudo crear archivo temporal.', $conn, $instrumentId);
    }
    file_put_contents($tmpFile, 'Documento de prueba ' . date('c'));
    $fileSize = filesize($tmpFile);

    $attachment = InstrumentAttachmentService::createAttachment(
        $conn,
        $instrumentId,
        $empresaId,
        [
            'tmp_path' => $tmpFile,
            'original_name' => 'manual_prueba.pdf',
            'size' => $fileSize,
            'mime' => 'application/pdf',
        ],
        'Manual',
        'Archivo de prueba automatizado',
        $usuarioId
    );

    if (!isset($attachment['id'])) {
        fail_and_exit('La creación del adjunto no devolvió un identificador.', $conn, $instrumentId);
    }

    $attachments = InstrumentAttachmentService::listAttachments($conn, $instrumentId, $empresaId);
    if (count($attachments) === 0) {
        fail_and_exit('La lista de adjuntos está vacía tras la inserción.', $conn, $instrumentId);
    }

    $found = false;
    foreach ($attachments as $item) {
        if ((int) $item['id'] === (int) $attachment['id']) {
            $found = true;
            break;
        }
    }
    if (!$found) {
        fail_and_exit('El adjunto recién creado no se encontró en la lista.', $conn, $instrumentId);
    }

    InstrumentAttachmentService::deleteAttachment($conn, (int) $attachment['id'], $instrumentId, $empresaId);
    $remaining = InstrumentAttachmentService::countByInstrument($conn, $instrumentId, $empresaId);
    if ($remaining !== 0) {
        fail_and_exit('El adjunto no se eliminó correctamente.', $conn, $instrumentId);
    }

    $cleanup = $conn->prepare('DELETE FROM instrumentos WHERE id = ?');
    if ($cleanup) {
        $cleanup->bind_param('i', $instrumentId);
        $cleanup->execute();
        $cleanup->close();
    }

    echo "Prueba de adjuntos completada correctamente." . PHP_EOL;
    $conn->close();
    exit(0);
} catch (Throwable $e) {
    fail_and_exit('Error en la prueba de adjuntos: ' . $e->getMessage(), $conn, $instrumentId);
}

