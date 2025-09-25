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

$idFilter = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1],
]);
if ($idFilter === false || $idFilter === null) {
    http_response_code(422);
    echo json_encode(['error' => 'Identificador inválido']);
    exit;
}
$recordId = (int) $idFilter;

$allowedEstados = ['abierto', 'en_progreso', 'cerrado'];
$estado = null;
if (isset($_POST['estado'])) {
    $estadoInput = strtolower(trim((string) $_POST['estado']));
    if (!in_array($estadoInput, $allowedEstados, true)) {
        http_response_code(422);
        echo json_encode(['error' => 'Estado no válido']);
        exit;
    }
    $estado = $estadoInput;
}

$sqlCurrent = 'SELECT estado, reporter_id, resumen FROM feedback_reports WHERE id = ? LIMIT 1';
$stmtCurrent = $conn->prepare($sqlCurrent);
if (!$stmtCurrent) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo consultar el estado actual']);
    exit;
}

$stmtCurrent->bind_param('i', $recordId);
if (!$stmtCurrent->execute()) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo obtener el registro']);
    $stmtCurrent->close();
    exit;
}

$currentResult = $stmtCurrent->get_result();
$currentData = $currentResult ? $currentResult->fetch_assoc() : null;
$stmtCurrent->close();

if (!$currentData) {
    http_response_code(404);
    echo json_encode(['error' => 'Registro no encontrado']);
    exit;
}

$estadoAnterior = isset($currentData['estado']) ? (string) $currentData['estado'] : null;
$reporterId = isset($currentData['reporter_id']) ? (int) $currentData['reporter_id'] : 0;
$resumenReporte = isset($currentData['resumen']) ? (string) $currentData['resumen'] : '';

$prioridad = null;
if (isset($_POST['prioridad'])) {
    $prioridadOptions = ['baja', 'media', 'alta', 'critica'];
    $prioridadInput = strtolower(trim((string) $_POST['prioridad']));
    if ($prioridadInput === '') {
        $prioridad = 'media';
    } elseif (!in_array($prioridadInput, $prioridadOptions, true)) {
        http_response_code(422);
        echo json_encode(['error' => 'Prioridad no válida']);
        exit;
    } else {
        $prioridad = $prioridadInput;
    }
}

$asignado = null;
$asignadoEsNulo = false;
if (array_key_exists('asignado_a', $_POST)) {
    $raw = trim((string) $_POST['asignado_a']);
    if ($raw === '' || strtolower($raw) === 'null') {
        $asignadoEsNulo = true;
    } else {
        $asignadoFilter = filter_var($raw, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
        if ($asignadoFilter === false) {
            http_response_code(422);
            echo json_encode(['error' => 'Asignado no válido']);
            exit;
        }
        $asignado = (int) $asignadoFilter;
    }
}

$fields = [];
$params = [];
$types = '';
$estadoCambio = $estado !== null && $estadoAnterior !== null && $estado !== $estadoAnterior;
$notificacionGenerada = false;

if ($estado !== null) {
    $fields[] = 'estado = ?';
    $params[] = $estado;
    $types .= 's';
}

if ($prioridad !== null) {
    $fields[] = 'prioridad = ?';
    $params[] = $prioridad;
    $types .= 's';
}

if ($asignadoEsNulo) {
    $fields[] = 'asignado_a = NULL';
} elseif ($asignado !== null) {
    $fields[] = 'asignado_a = ?';
    $params[] = $asignado;
    $types .= 'i';
}

if (!$fields) {
    http_response_code(422);
    echo json_encode(['error' => 'No hay cambios para aplicar']);
    exit;
}

$fields[] = 'actualizado_en = CURRENT_TIMESTAMP';

$sql = 'UPDATE feedback_reports SET ' . implode(', ', $fields) . ' WHERE id = ?';
$params[] = $recordId;
$types .= 'i';

$stmt = $conn->prepare($sql);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la actualización']);
    exit;
}

$stmt->bind_param($types, ...$params);

if (!$stmt->execute()) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo actualizar el registro']);
    $stmt->close();
    exit;
}

if ($stmt->affected_rows > 0 && $estadoCambio && $reporterId > 0) {
    $estadoLabels = [
        'abierto' => 'Abierto',
        'en_progreso' => 'En progreso',
        'cerrado' => 'Cerrado',
    ];
    $estadoAmigable = $estadoLabels[$estado] ?? ucfirst(str_replace('_', ' ', (string) $estado));
    $tituloNotificacion = 'Estado de reporte actualizado';
    $resumenSeguro = htmlspecialchars($resumenReporte, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
    $mensajeNotificacion = sprintf(
        'Tu reporte <b>%s</b> cambió al estado <b>%s</b>.',
        $resumenSeguro !== '' ? $resumenSeguro : 'sin título',
        htmlspecialchars($estadoAmigable, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8')
    );

    $sqlInsertNotificacion = 'INSERT INTO user_notifications (user_id, titulo, mensaje, tipo) VALUES (?, ?, ?, ?)';
    $stmtNotificacion = $conn->prepare($sqlInsertNotificacion);
    if ($stmtNotificacion) {
        $tipoNotificacion = 'feedback_estado';
        $stmtNotificacion->bind_param('isss', $reporterId, $tituloNotificacion, $mensajeNotificacion, $tipoNotificacion);
        if ($stmtNotificacion->execute()) {
            $notificacionGenerada = true;
        }
        $stmtNotificacion->close();
    }
}

if ($stmt->affected_rows === 0) {
    echo json_encode(['success' => true, 'updated' => 0, 'notificacion_generada' => $notificacionGenerada]);
} else {
    echo json_encode(['success' => true, 'updated' => $stmt->affected_rows, 'notificacion_generada' => $notificacionGenerada]);
}

$stmt->close();
