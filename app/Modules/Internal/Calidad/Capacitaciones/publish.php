<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_capacitaciones_publicar')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);
$usuarioId = calidadUsuarioId();

$data = calidadJsonBody();
$capacitacionId = isset($data['id']) ? (int) $data['id'] : 0;

if ($capacitacionId <= 0) {
    calidadRespond(400, 'error', 'Identificador de capacitación inválido');
}

$check = $conn->prepare('SELECT estado FROM calidad_capacitaciones WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$check) {
    calidadRespond(500, 'error', 'No se pudo preparar la verificación de la capacitación');
}
$check->bind_param('ii', $capacitacionId, $empresaId);
$check->execute();
$check->bind_result($estadoActual);
if (!$check->fetch()) {
    $check->close();
    calidadRespond(404, 'error', 'Capacitación no encontrada');
}
$check->close();

if ($estadoActual === 'publicado') {
    calidadRespond(200, 'success', 'La capacitación ya estaba publicada', [
        'id' => $capacitacionId,
        'estado' => $estadoActual,
    ]);
}

$publish = $conn->prepare('UPDATE calidad_capacitaciones SET estado = "publicado", publicado_en = NOW(), publicado_por = ?, actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
if (!$publish) {
    calidadRespond(500, 'error', 'No se pudo preparar la publicación de la capacitación');
}
$publish->bind_param('iii', $usuarioId, $capacitacionId, $empresaId);
if (!$publish->execute()) {
    $publish->close();
    calidadRespond(500, 'error', 'No se pudo publicar la capacitación');
}
$publish->close();

calidadRespond(200, 'success', 'Capacitación publicada correctamente', [
    'id' => $capacitacionId,
    'estado' => 'publicado',
]);
