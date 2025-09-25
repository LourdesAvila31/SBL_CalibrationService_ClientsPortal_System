<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_documentos_crear')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);
$usuarioId = calidadUsuarioId();

$data = calidadJsonBody();

$documentId = isset($data['id']) ? (int) $data['id'] : 0;
$titulo = trim((string) ($data['titulo'] ?? ''));
$descripcion = trim((string) ($data['descripcion'] ?? ''));
$contenido = trim((string) ($data['contenido'] ?? ''));
$responsable = trim((string) ($data['responsable'] ?? '')) ?: null;

if ($titulo === '') {
    calidadRespond(400, 'error', 'El título del documento es obligatorio');
}

if ($documentId > 0) {
    $existsStmt = $conn->prepare('SELECT estado FROM calidad_documentos WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$existsStmt) {
        calidadRespond(500, 'error', 'No se pudo verificar el documento solicitado');
    }
    $existsStmt->bind_param('ii', $documentId, $empresaId);
    $existsStmt->execute();
    $existsStmt->bind_result($estadoActual);
    if (!$existsStmt->fetch()) {
        $existsStmt->close();
        calidadRespond(404, 'error', 'Documento no encontrado');
    }
    $existsStmt->close();

    $update = $conn->prepare('UPDATE calidad_documentos SET titulo = ?, descripcion = ?, contenido = ?, responsable = ?, actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
    if (!$update) {
        calidadRespond(500, 'error', 'No se pudo preparar la actualización del documento');
    }
    $update->bind_param('ssssii', $titulo, $descripcion, $contenido, $responsable, $documentId, $empresaId);
    if (!$update->execute()) {
        $update->close();
        calidadRespond(500, 'error', 'No se pudo actualizar el documento');
    }
    $update->close();

    calidadRespond(200, 'success', 'Documento actualizado correctamente', [
        'id' => $documentId,
        'estado' => $estadoActual,
    ]);
}

$insert = $conn->prepare('INSERT INTO calidad_documentos (empresa_id, titulo, descripcion, contenido, estado, creado_por, responsable) VALUES (?, ?, ?, ?, "borrador", ?, ?)');
if (!$insert) {
    calidadRespond(500, 'error', 'No se pudo preparar el guardado del documento');
}
$insert->bind_param('isssis', $empresaId, $titulo, $descripcion, $contenido, $usuarioId, $responsable);

if (!$insert->execute()) {
    $insert->close();
    calidadRespond(500, 'error', 'No se pudo guardar el documento');
}

$nuevoId = (int) $insert->insert_id;
$insert->close();

calidadRespond(201, 'success', 'Documento creado correctamente', [
    'id' => $nuevoId,
    'estado' => 'borrador',
]);
