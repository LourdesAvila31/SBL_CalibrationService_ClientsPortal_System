<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_documentos_publicar')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);
$usuarioId = calidadUsuarioId();

$data = calidadJsonBody();
$documentId = isset($data['id']) ? (int) $data['id'] : 0;

if ($documentId <= 0) {
    calidadRespond(400, 'error', 'Identificador de documento inválido');
}

$check = $conn->prepare('SELECT estado FROM calidad_documentos WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$check) {
    calidadRespond(500, 'error', 'No se pudo preparar la verificación del documento');
}
$check->bind_param('ii', $documentId, $empresaId);
$check->execute();
$check->bind_result($estadoActual);
if (!$check->fetch()) {
    $check->close();
    calidadRespond(404, 'error', 'Documento no encontrado');
}
$check->close();

if ($estadoActual === 'publicado') {
    calidadRespond(200, 'success', 'El documento ya estaba publicado', [
        'id' => $documentId,
        'estado' => $estadoActual,
    ]);
}

$publish = $conn->prepare('UPDATE calidad_documentos SET estado = "publicado", publicado_en = NOW(), publicado_por = ?, actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
if (!$publish) {
    calidadRespond(500, 'error', 'No se pudo preparar la publicación del documento');
}
$publish->bind_param('iii', $usuarioId, $documentId, $empresaId);
if (!$publish->execute()) {
    $publish->close();
    calidadRespond(500, 'error', 'No se pudo publicar el documento');
}
$publish->close();

calidadRespond(200, 'success', 'Documento publicado correctamente', [
    'id' => $documentId,
    'estado' => 'publicado',
]);
