<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_capacitaciones_crear')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);
$usuarioId = calidadUsuarioId();

$data = calidadJsonBody();

$capacitacionId = isset($data['id']) ? (int) $data['id'] : 0;
$tema = trim((string) ($data['tema'] ?? ''));
$descripcion = trim((string) ($data['descripcion'] ?? ''));
$fechaProgramada = trim((string) ($data['fecha_programada'] ?? ''));
$responsable = trim((string) ($data['responsable'] ?? '')) ?: null;

if ($tema === '') {
    calidadRespond(400, 'error', 'El tema de la capacitación es obligatorio');
}

if ($fechaProgramada !== '') {
    $fecha = DateTime::createFromFormat('Y-m-d', $fechaProgramada);
    if (!$fecha || $fecha->format('Y-m-d') !== $fechaProgramada) {
        calidadRespond(400, 'error', 'La fecha programada debe tener el formato YYYY-MM-DD');
    }
} else {
    $fechaProgramada = null;
}

if ($capacitacionId > 0) {
    $existsStmt = $conn->prepare('SELECT estado FROM calidad_capacitaciones WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$existsStmt) {
        calidadRespond(500, 'error', 'No se pudo verificar la capacitación solicitada');
    }
    $existsStmt->bind_param('ii', $capacitacionId, $empresaId);
    $existsStmt->execute();
    $existsStmt->bind_result($estadoActual);
    if (!$existsStmt->fetch()) {
        $existsStmt->close();
        calidadRespond(404, 'error', 'Capacitación no encontrada');
    }
    $existsStmt->close();

    $update = $conn->prepare('UPDATE calidad_capacitaciones SET tema = ?, descripcion = ?, fecha_programada = ?, responsable = ?, actualizado_en = NOW() WHERE id = ? AND empresa_id = ?');
    if (!$update) {
        calidadRespond(500, 'error', 'No se pudo preparar la actualización de la capacitación');
    }
    $update->bind_param('ssssii', $tema, $descripcion, $fechaProgramada, $responsable, $capacitacionId, $empresaId);
    if (!$update->execute()) {
        $update->close();
        calidadRespond(500, 'error', 'No se pudo actualizar la capacitación');
    }
    $update->close();

    calidadRespond(200, 'success', 'Capacitación actualizada correctamente', [
        'id' => $capacitacionId,
        'estado' => $estadoActual,
    ]);
}

$insert = $conn->prepare('INSERT INTO calidad_capacitaciones (empresa_id, tema, descripcion, fecha_programada, responsable, estado, creado_por) VALUES (?, ?, ?, ?, ?, "borrador", ?)');
if (!$insert) {
    calidadRespond(500, 'error', 'No se pudo preparar el guardado de la capacitación');
}
$insert->bind_param('issssi', $empresaId, $tema, $descripcion, $fechaProgramada, $responsable, $usuarioId);

if (!$insert->execute()) {
    $insert->close();
    calidadRespond(500, 'error', 'No se pudo guardar la capacitación');
}

$nuevoId = (int) $insert->insert_id;
$insert->close();

calidadRespond(201, 'success', 'Capacitación creada correctamente', [
    'id' => $nuevoId,
    'estado' => 'borrador',
]);
