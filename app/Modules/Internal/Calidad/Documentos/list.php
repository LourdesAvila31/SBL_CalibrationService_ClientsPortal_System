<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_documentos_leer')) {
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

$sql = 'SELECT id, titulo, descripcion, estado, publicado_en, creado_en, actualizado_en, responsable '
    . 'FROM calidad_documentos WHERE empresa_id = ?' . $condicionEstado . ' ORDER BY actualizado_en DESC, id DESC';
$stmt = $conn->prepare($sql);
if (!$stmt) {
    calidadRespond(500, 'error', 'No se pudo preparar la consulta de documentos');
}

if ($condicionEstado) {
    $stmt->bind_param('is', $empresaId, $estado);
} else {
    $stmt->bind_param('i', $empresaId);
}

if (!$stmt->execute()) {
    $stmt->close();
    calidadRespond(500, 'error', 'No se pudo ejecutar la consulta de documentos');
}

$result = $stmt->get_result();
$documentos = [];
while ($row = $result->fetch_assoc()) {
    $documentos[] = $row;
}
$stmt->close();

calidadRespond(200, 'success', 'Documentos recuperados correctamente', $documentos);
