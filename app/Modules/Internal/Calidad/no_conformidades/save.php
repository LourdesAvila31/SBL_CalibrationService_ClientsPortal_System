<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_no_conformidades_crear')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);
$usuarioId = calidadUsuarioId();

$data = calidadJsonBody();

$ncId = isset($data['id']) ? (int) $data['id'] : 0;
$titulo = trim((string) ($data['titulo'] ?? ''));
$descripcion = trim((string) ($data['descripcion'] ?? ''));
$causaRaiz = trim((string) ($data['causa_raiz'] ?? ''));
$acciones = trim((string) ($data['acciones'] ?? ''));
$responsable = trim((string) ($data['responsable'] ?? '')) ?: null;
$estado = trim((string) ($data['estado'] ?? ''));

if ($titulo === '') {
    calidadRespond(400, 'error', 'El título de la no conformidad es obligatorio');
}

$estadoValido = null;
if ($estado !== '') {
    if (!in_array($estado, ['abierta', 'en_proceso'], true)) {
        calidadRespond(400, 'error', 'El estado proporcionado no es válido para esta operación');
    }
    $estadoValido = $estado;
}

if ($ncId > 0) {
    $existsStmt = $conn->prepare('SELECT estado FROM calidad_no_conformidades WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$existsStmt) {
        calidadRespond(500, 'error', 'No se pudo verificar la no conformidad solicitada');
    }
    $existsStmt->bind_param('ii', $ncId, $empresaId);
    $existsStmt->execute();
    $existsStmt->bind_result($estadoActual);
    if (!$existsStmt->fetch()) {
        $existsStmt->close();
        calidadRespond(404, 'error', 'No conformidad no encontrada');
    }
    $existsStmt->close();

    if ($estadoActual === 'cerrada') {
        calidadRespond(400, 'error', 'No es posible modificar una no conformidad cerrada');
    }

    $sql = 'UPDATE calidad_no_conformidades SET titulo = ?, descripcion = ?, causa_raiz = ?, acciones = ?, responsable = ?, actualizado_en = NOW()';
    if ($estadoValido !== null) {
        $sql .= ', estado = ?';
    }
    $sql .= ' WHERE id = ? AND empresa_id = ?';

    $update = $conn->prepare($sql);
    if (!$update) {
        calidadRespond(500, 'error', 'No se pudo preparar la actualización de la no conformidad');
    }

    if ($estadoValido !== null) {
        $update->bind_param('ssssssii', $titulo, $descripcion, $causaRaiz, $acciones, $responsable, $estadoValido, $ncId, $empresaId);
    } else {
        $update->bind_param('sssssii', $titulo, $descripcion, $causaRaiz, $acciones, $responsable, $ncId, $empresaId);
    }

    if (!$update->execute()) {
        $update->close();
        calidadRespond(500, 'error', 'No se pudo actualizar la no conformidad');
    }
    $update->close();

    calidadRespond(200, 'success', 'No conformidad actualizada correctamente', [
        'id' => $ncId,
        'estado' => $estadoValido ?? $estadoActual,
    ]);
}

$estadoInicial = $estadoValido ?? 'abierta';
$insert = $conn->prepare('INSERT INTO calidad_no_conformidades (empresa_id, titulo, descripcion, causa_raiz, acciones, responsable, estado, creado_por) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
if (!$insert) {
    calidadRespond(500, 'error', 'No se pudo preparar el guardado de la no conformidad');
}
$insert->bind_param('issssssi', $empresaId, $titulo, $descripcion, $causaRaiz, $acciones, $responsable, $estadoInicial, $usuarioId);

if (!$insert->execute()) {
    $insert->close();
    calidadRespond(500, 'error', 'No se pudo guardar la no conformidad');
}

$nuevoId = (int) $insert->insert_id;
$insert->close();

calidadRespond(201, 'success', 'No conformidad registrada correctamente', [
    'id' => $nuevoId,
    'estado' => $estadoInicial,
]);
