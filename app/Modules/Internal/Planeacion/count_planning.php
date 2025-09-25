<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('planeacion_leer')) {
    http_response_code(403);
    echo json_encode(['programado' => 0, 'no_programado' => 0]);
    exit;
}

header('Content-Type: application/json');

$roleName = $_SESSION['role_id'] ?? '';
if (!in_array($roleName, ['Superadministrador', 'Administrador', 'Supervisor'], true)) {
    http_response_code(403);
    echo json_encode(['programado' => 0, 'no_programado' => 0]);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once __DIR__ . '/planeacion_view_helper.php';

if (!ensurePlaneacionView($conn)) {
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo preparar la vista de planeación']);
    $conn->close();
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$sql = "SELECT
            SUM(CASE WHEN v.tiene_plan = 1 THEN 1 ELSE 0 END) AS programado,
            SUM(CASE WHEN v.tiene_plan = 0 THEN 1 ELSE 0 END) AS no_programado
        FROM vista_planeacion_instrumentos v
        WHERE v.empresa_id = ?
          AND v.estado_instrumento = 'activo'
          AND v.fecha_proxima BETWEEN DATE_ADD(CURDATE(), INTERVAL ? DAY) AND DATE_ADD(CURDATE(), INTERVAL ? DAY)";

$stmt = $conn->prepare($sql);
$rangoInferior = 30;
$rangoSuperior = 60;
$stmt->bind_param('iii', $empresaId, $rangoInferior, $rangoSuperior);
$stmt->execute();
$stmt->bind_result($programados, $noProgramados);
$stmt->fetch();
$stmt->close();

echo json_encode([
    'programado' => (int) $programados,
    'no_programado' => (int) $noProgramados
]);

$conn->close();
?>
