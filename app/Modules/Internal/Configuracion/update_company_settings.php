<?php

declare(strict_types=1);

session_start();

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('configuracion_actualizar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Internal/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    echo json_encode(['success' => false, 'message' => 'Empresa no determinada']);
    exit;
}

$responsableIdRaw = $_POST['responsable_calidad_id'] ?? '';
$responsableId = null;
if ($responsableIdRaw !== '' && $responsableIdRaw !== null) {
    $responsableId = filter_var($responsableIdRaw, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($responsableId === false) {
        echo json_encode(['success' => false, 'message' => 'Responsable inválido']);
        exit;
    }
}

if (!company_set_quality_responsible($conn, $empresaId, $responsableId)) {
    echo json_encode(['success' => false, 'message' => 'No se pudo actualizar el responsable de calidad']);
    exit;
}

$nombreOperador = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
if ($nombreOperador === '' && !empty($_SESSION['usuario_id'])) {
    $usuarioStmt = $conn->prepare('SELECT nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
    if ($usuarioStmt) {
        $usuarioStmt->bind_param('i', $_SESSION['usuario_id']);
        if ($usuarioStmt->execute()) {
            $usuarioStmt->bind_result($nombreDb, $apellidosDb);
            if ($usuarioStmt->fetch()) {
                $nombreOperador = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
            }
        }
        $usuarioStmt->close();
    }
}
if ($nombreOperador === '') {
    $nombreOperador = 'Desconocido';
}
$correoOperador = $_SESSION['usuario'] ?? null;

log_activity($nombreOperador, 'Actualizó responsable de calidad de la empresa', 'configuracion', $correoOperador);

echo json_encode(['success' => true]);
