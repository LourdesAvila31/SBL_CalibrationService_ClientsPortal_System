<?php
session_start();

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';

header('Content-Type: application/json');

if (!session_is_superadmin()) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso restringido']);
    exit;
}

$filters = [];
$params = [];
$types = '';

$empresaFilter = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);
if ($empresaFilter !== false && $empresaFilter !== null) {
    $filters[] = 'fr.empresa_id = ?';
    $params[] = (int) $empresaFilter;
    $types .= 'i';
}

$estadoFilter = isset($_GET['estado']) ? strtolower(trim((string) $_GET['estado'])) : '';
if ($estadoFilter !== '' && in_array($estadoFilter, ['abierto', 'en_progreso', 'cerrado'], true)) {
    $filters[] = 'fr.estado = ?';
    $params[] = $estadoFilter;
    $types .= 's';
}

$origenFilter = isset($_GET['origen']) ? strtolower(trim((string) $_GET['origen'])) : '';
if ($origenFilter !== '' && in_array($origenFilter, ['interno', 'externo'], true)) {
    $filters[] = 'fr.origen = ?';
    $params[] = $origenFilter;
    $types .= 's';
}

$sql = "SELECT
            fr.id,
            fr.empresa_id,
            fr.reporter_id,
            fr.origen,
            fr.tipo,
            fr.resumen,
            fr.descripcion,
            fr.estado,
            fr.prioridad,
            fr.asignado_a,
            fr.creado_en,
            fr.actualizado_en,
            e.nombre AS empresa_nombre,
            CONCAT(r.nombre, ' ', r.apellidos) AS reportero_nombre,
            r.usuario AS reportero_usuario,
            ar.usuario AS asignado_usuario,
            CONCAT(ar.nombre, ' ', ar.apellidos) AS asignado_nombre
        FROM feedback_reports fr
        LEFT JOIN empresas e ON fr.empresa_id = e.id
        LEFT JOIN usuarios r ON fr.reporter_id = r.id
        LEFT JOIN usuarios ar ON fr.asignado_a = ar.id";

if ($filters) {
    $sql .= ' WHERE ' . implode(' AND ', $filters);
}

$sql .= ' ORDER BY fr.creado_en DESC';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la consulta']);
    exit;
}

if ($types !== '') {
    $stmt->bind_param($types, ...$params);
}

if (!$stmt->execute()) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo obtener la información']);
    $stmt->close();
    exit;
}

$result = $stmt->get_result();
$items = [];
$indexById = [];
while ($row = $result->fetch_assoc()) {
    $itemId = (int) $row['id'];
    $indexById[$itemId] = count($items);
    $items[] = [
        'id' => $itemId,
        'empresa_id' => $row['empresa_id'] !== null ? (int) $row['empresa_id'] : null,
        'empresa_nombre' => $row['empresa_nombre'],
        'reporter_id' => (int) $row['reporter_id'],
        'reporter' => [
            'nombre' => trim((string) $row['reportero_nombre']) ?: $row['reportero_usuario'],
            'usuario' => $row['reportero_usuario'],
        ],
        'origen' => $row['origen'],
        'tipo' => $row['tipo'],
        'resumen' => $row['resumen'],
        'descripcion' => $row['descripcion'],
        'estado' => $row['estado'],
        'prioridad' => $row['prioridad'],
        'asignado_a' => $row['asignado_a'] !== null ? (int) $row['asignado_a'] : null,
        'asignado' => $row['asignado_a'] !== null ? [
            'nombre' => trim((string) $row['asignado_nombre']) ?: $row['asignado_usuario'],
            'usuario' => $row['asignado_usuario'],
        ] : null,
        'creado_en' => $row['creado_en'],
        'actualizado_en' => $row['actualizado_en'],
        'adjuntos' => [],
    ];
}

$stmt->close();

if (!empty($items)) {
    $attachmentIds = array_map('intval', array_keys($indexById));
    $placeholders = implode(',', array_fill(0, count($attachmentIds), '?'));
    $attachmentSql = 'SELECT id, feedback_id, nombre_original FROM feedback_attachments WHERE feedback_id IN (' . $placeholders . ') ORDER BY id';
    $attachmentStmt = $conn->prepare($attachmentSql);
    if (!$attachmentStmt) {
        http_response_code(500);
        echo json_encode(['error' => 'No se pudieron obtener los adjuntos']);
        exit;
    }

    $types = str_repeat('i', count($attachmentIds));
    $attachmentStmt->bind_param($types, ...$attachmentIds);

    if (!$attachmentStmt->execute()) {
        http_response_code(500);
        echo json_encode(['error' => 'No se pudieron obtener los adjuntos']);
        $attachmentStmt->close();
        exit;
    }

    $attachmentResult = $attachmentStmt->get_result();
    while ($attachmentRow = $attachmentResult->fetch_assoc()) {
        $feedbackId = (int) $attachmentRow['feedback_id'];
        if (!array_key_exists($feedbackId, $indexById)) {
            continue;
        }

        $items[$indexById[$feedbackId]]['adjuntos'][] = [
            'id' => (int) $attachmentRow['id'],
            'nombre' => $attachmentRow['nombre_original'],
            'descarga_url' => '/backend/feedback/download_attachment.php?id=' . (int) $attachmentRow['id'],
        ];
    }

    $attachmentStmt->close();
}

echo json_encode(['items' => $items]);
