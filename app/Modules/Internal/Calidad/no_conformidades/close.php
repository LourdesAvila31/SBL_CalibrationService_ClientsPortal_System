<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_no_conformidades_cerrar')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);
$usuarioId = calidadUsuarioId();

$data = calidadJsonBody();
$ncId = isset($data['id']) ? (int) $data['id'] : 0;
$acciones = isset($data['acciones']) ? trim((string) $data['acciones']) : null;
$causaRaiz = isset($data['causa_raiz']) ? trim((string) $data['causa_raiz']) : null;
$responsable = isset($data['responsable']) ? trim((string) $data['responsable']) : null;

if ($ncId <= 0) {
    calidadRespond(400, 'error', 'Identificador de no conformidad inválido');
}

$check = $conn->prepare('SELECT estado FROM calidad_no_conformidades WHERE id = ? AND empresa_id = ? LIMIT 1');
if (!$check) {
    calidadRespond(500, 'error', 'No se pudo preparar la verificación de la no conformidad');
}
$check->bind_param('ii', $ncId, $empresaId);
$check->execute();
$check->bind_result($estadoActual);
if (!$check->fetch()) {
    $check->close();
    calidadRespond(404, 'error', 'No conformidad no encontrada');
}
$check->close();

if ($estadoActual === 'cerrada') {
    calidadRespond(200, 'success', 'La no conformidad ya estaba cerrada', [
        'id' => $ncId,
        'estado' => $estadoActual,
    ]);
}

$sql = 'UPDATE calidad_no_conformidades SET estado = "cerrada", cerrado_en = NOW(), cerrado_por = ?, actualizado_en = NOW()';
$params = [];
$types = 'i';
$params[] = &$usuarioId;

if ($acciones !== null && $acciones !== '') {
    $sql .= ', acciones = ?';
    $types .= 's';
    $params[] = &$acciones;
}
if ($causaRaiz !== null && $causaRaiz !== '') {
    $sql .= ', causa_raiz = ?';
    $types .= 's';
    $params[] = &$causaRaiz;
}
if ($responsable !== null && $responsable !== '') {
    $sql .= ', responsable = ?';
    $types .= 's';
    $params[] = &$responsable;
}

$sql .= ' WHERE id = ? AND empresa_id = ?';
$types .= 'ii';
$params[] = &$ncId;
$params[] = &$empresaId;

$close = $conn->prepare($sql);
if (!$close) {
    calidadRespond(500, 'error', 'No se pudo preparar el cierre de la no conformidad');
}

array_unshift($params, $types);
$bindResult = $close->bind_param(...$params);
if ($bindResult === false || !$close->execute()) {
    $close->close();
    calidadRespond(500, 'error', 'No se pudo cerrar la no conformidad');
}
$close->close();

calidadRespond(200, 'success', 'No conformidad cerrada correctamente', [
    'id' => $ncId,
    'estado' => 'cerrada',
]);
