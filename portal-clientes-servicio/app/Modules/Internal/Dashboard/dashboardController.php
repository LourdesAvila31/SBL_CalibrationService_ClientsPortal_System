<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
header('Content-Type: application/json');

// Usa la conexión global del proyecto
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

// Si la conexión falla, db.php terminará la ejecución

// Fecha actual para cálculos de rangos
$hoy = date('Y-m-d');
$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$hoyMas30 = date('Y-m-d', strtotime($hoy . ' +30 days'));
$hoyMas60 = date('Y-m-d', strtotime($hoy . ' +60 days'));
$hoyMas90 = date('Y-m-d', strtotime($hoy . ' +90 days'));

function countBySql(mysqli $conn, string $sql, string $types = '', ...$params): int
{
    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return 0;
    }
    if ($types !== '') {
        $stmt->bind_param($types, ...$params);
    }
    $stmt->execute();
    $stmt->bind_result($count);
    $stmt->fetch();
    $stmt->close();
    return (int) $count;
}

// Instrumentos
$activos = countBySql($conn, "SELECT COUNT(*) FROM instrumentos WHERE estado = 'activo' AND empresa_id = ?", 'i', $empresaId);
$inactivos = countBySql($conn, "SELECT COUNT(*) FROM instrumentos WHERE estado = 'inactivo' AND empresa_id = ?", 'i', $empresaId);
$programado = countBySql($conn, "SELECT COUNT(*) FROM instrumentos WHERE programado = 1 AND empresa_id = ?", 'i', $empresaId);
$no_programado = countBySql(
    $conn,
    "SELECT COUNT(*) FROM instrumentos WHERE programado = 0 AND estado <> 'stock' AND empresa_id = ?
      AND proxima_calibracion BETWEEN ? AND ?",
    'iss',
    $empresaId,
    $hoy,
    $hoyMas60
);

// Calibraciones próximas
$vencido = countBySql($conn, "SELECT COUNT(*) FROM instrumentos WHERE empresa_id = ? AND proxima_calibracion < ?", 'is', $empresaId, $hoy);
$vence_30 = countBySql(
    $conn,
    "SELECT COUNT(*) FROM instrumentos WHERE empresa_id = ? AND proxima_calibracion >= ? AND proxima_calibracion < ?",
    'iss',
    $empresaId,
    $hoy,
    $hoyMas30
);
$vence_60 = countBySql(
    $conn,
    "SELECT COUNT(*) FROM instrumentos WHERE empresa_id = ? AND proxima_calibracion >= ? AND proxima_calibracion < ?",
    'iss',
    $empresaId,
    $hoyMas30,
    $hoyMas60
);
$vence_90 = countBySql(
    $conn,
    "SELECT COUNT(*) FROM instrumentos WHERE empresa_id = ? AND proxima_calibracion >= ? AND proxima_calibracion < ?",
    'iss',
    $empresaId,
    $hoyMas60,
    $hoyMas90
);

$empresaId = (int)$empresaId;

// Instrumentos
$activos = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE estado = 'activo' AND empresa_id = $empresaId")->fetch_row()[0];
$inactivos = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE estado = 'inactivo' AND empresa_id = $empresaId")->fetch_row()[0];
$programado = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE programado = 1 AND empresa_id = $empresaId")->fetch_row()[0];
$no_programado = $conn->query(
    "SELECT COUNT(*) FROM instrumentos WHERE empresa_id = $empresaId AND programado = 0 AND estado <> 'stock' " .
    "AND proxima_calibracion BETWEEN '$hoy' AND DATE_ADD('$hoy', INTERVAL 60 DAY)"
)->fetch_row()[0];

// Calibraciones próximas
// Instrumentos vencidos = sin registro de planeación y con calibración caducada
$vencidoQuery = <<<SQL
SELECT COUNT(*)
FROM instrumentos i
WHERE i.proxima_calibracion < '$hoy'
  AND NOT EXISTS (
      SELECT 1
      FROM planes p
      WHERE p.instrumento_id = i.id
  )
SQL;
$vencido = $conn->query($vencidoQuery)->fetch_row()[0];
$vence_30 = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE proxima_calibracion >= '$hoy' AND proxima_calibracion < DATE_ADD('$hoy', INTERVAL 30 DAY)")->fetch_row()[0];
$vence_60 = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE proxima_calibracion >= DATE_ADD('$hoy', INTERVAL 30 DAY) AND proxima_calibracion < DATE_ADD('$hoy', INTERVAL 60 DAY)")->fetch_row()[0];
$vence_90 = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE proxima_calibracion >= DATE_ADD('$hoy', INTERVAL 60 DAY) AND proxima_calibracion < DATE_ADD('$hoy', INTERVAL 90 DAY)")->fetch_row()[0];


$vencido = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE empresa_id = $empresaId AND proxima_calibracion < '$hoy'")->fetch_row()[0];
$vence_30 = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE empresa_id = $empresaId AND proxima_calibracion >= '$hoy' AND proxima_calibracion < DATE_ADD('$hoy', INTERVAL 30 DAY)")->fetch_row()[0];
$vence_60 = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE empresa_id = $empresaId AND proxima_calibracion >= DATE_ADD('$hoy', INTERVAL 30 DAY) AND proxima_calibracion < DATE_ADD('$hoy', INTERVAL 60 DAY)")->fetch_row()[0];
$vence_90 = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE empresa_id = $empresaId AND proxima_calibracion >= DATE_ADD('$hoy', INTERVAL 60 DAY) AND proxima_calibracion < DATE_ADD('$hoy', INTERVAL 90 DAY)")->fetch_row()[0];


// Cuenta
$usuarios_cuenta = countBySql($conn, "SELECT COUNT(*) FROM usuarios WHERE empresa_id = ?", 'i', $empresaId);
$proveedores_registrados = 0;
if ($stmt = $conn->prepare('SELECT COUNT(*) FROM proveedores WHERE empresa_id = ?')) {
    $stmt->bind_param('i', $empresaId);
    $stmt->execute();
    $stmt->bind_result($count);
    if ($stmt->fetch()) {
        $proveedores_registrados = (int) $count;
    }
    $stmt->close();
} else {
    $proveedoresRes = $conn->query('SELECT COUNT(*) FROM proveedores');
    if ($proveedoresRes) {
        $proveedoresRow = $proveedoresRes->fetch_row();
        if ($proveedoresRow) {
            $proveedores_registrados = (int) $proveedoresRow[0];
        }
    }
}
$usuarios_max = 5; // valor fijo, ajusta si tienes límite
$almacenamiento_usado = 0.3; // simulado, ajusta si quieres calcular
$almacenamiento_max = 5;
$almacenamiento_porc = round(($almacenamiento_usado/$almacenamiento_max)*100,2) . "%";

$instrumentos_usados = countBySql($conn, "SELECT COUNT(*) FROM instrumentos WHERE empresa_id = ?", 'i', $empresaId);
$instrumentos_max = 25;
$registros_usados = countBySql(
    $conn,
    "SELECT COUNT(*) FROM calibraciones c JOIN instrumentos i ON i.id = c.instrumento_id WHERE i.empresa_id = ?",
    'i',
    $empresaId
);

$instrumentos_usados = $conn->query("SELECT COUNT(*) FROM instrumentos WHERE empresa_id = $empresaId")->fetch_row()[0];
$instrumentos_max = 25;
$registros_usados = $conn->query("SELECT COUNT(*) FROM calibraciones WHERE empresa_id = $empresaId")->fetch_row()[0];


echo json_encode([
    "activos" => $activos,
    "inactivos" => $inactivos,
    "vencido" => $vencido,
    "vence_30" => $vence_30,
    "vence_60" => $vence_60,
    "vence_90" => $vence_90,
    "usuarios_cuenta" => $usuarios_cuenta,
    "usuarios_max" => $usuarios_max,
    "almacenamiento_usado" => $almacenamiento_usado,
    "almacenamiento_max" => $almacenamiento_max,
    "almacenamiento_porc" => $almacenamiento_porc,
    "proveedores_registrados" => $proveedores_registrados,
    "instrumentos_usados" => $instrumentos_usados,
    "instrumentos_max" => $instrumentos_max,
    "registros_usados" => $registros_usados
]);

$conn->close();
?>