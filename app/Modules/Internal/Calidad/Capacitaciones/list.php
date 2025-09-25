<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_capacitaciones_leer')) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;
$empresaId = calidadEmpresaId($conn);

$estado = filter_input(INPUT_GET, 'estado', FILTER_SANITIZE_FULL_SPECIAL_CHARS) ?: null;
$permitidos = ['borrador', 'publicado'];
$condicionEstado = '';
if ($estado && in_array($estado, $permitidos, true)) {
    $condicionEstado = ' AND estado = ?';
}

$sql = 'SELECT id, tema, descripcion, fecha_programada, responsable, estado, publicado_en, creado_en, actualizado_en '
    . 'FROM calidad_capacitaciones WHERE empresa_id = ?' . $condicionEstado . ' ORDER BY fecha_programada IS NULL, fecha_programada DESC, actualizado_en DESC';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    calidadRespond(500, 'error', 'No se pudo preparar la consulta de capacitaciones');
}

if ($condicionEstado) {
    $stmt->bind_param('is', $empresaId, $estado);
} else {
    $stmt->bind_param('i', $empresaId);
}

if (!$stmt->execute()) {
    $stmt->close();
    calidadRespond(500, 'error', 'No se pudo ejecutar la consulta de capacitaciones');
}

$result = $stmt->get_result();
$capacitaciones = [];
while ($row = $result->fetch_assoc()) {
    $capacitaciones[] = $row;
}
$stmt->close();

calidadRespond(200, 'success', 'Capacitaciones recuperadas correctamente', $capacitaciones);
