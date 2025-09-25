<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

if (!check_permission('instrumentos_leer')) {
    http_response_code(403);
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo "Error: empresa no especificada";
    $conn->close();
    exit;
}

header('Content-Type: text/csv');
header('Content-Disposition: attachment; filename="gages_due.csv"');

echo "Instrumento,Fecha de Vencimiento,Departamento\n";

$sql = "SELECT ci.nombre AS instrumento, i.proxima_calibracion AS vence, d.nombre AS departamento\n        FROM instrumentos i\n        LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id\n        LEFT JOIN departamentos d ON i.departamento_id = d.id\n        WHERE i.empresa_id = ?\n          AND i.proxima_calibracion <= DATE_ADD(CURDATE(), INTERVAL 60 DAY)\n          AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')\n        ORDER BY i.proxima_calibracion ASC, instrumento ASC";

$stmt = $conn->prepare($sql);
if ($stmt) {
    $stmt->bind_param('i', $empresaId);
    if ($stmt->execute()) {
        $res = $stmt->get_result();
        if ($res) {
            while ($row = $res->fetch_assoc()) {
                $instrumento = str_replace('"', '""', $row['instrumento'] ?? '');
                $vence = $row['vence'] ?? '';
                $departamento = str_replace('"', '""', $row['departamento'] ?? '');
                echo "\"{$instrumento}\",\"{$vence}\",\"{$departamento}\"\n";
            }
        }
    }
    $stmt->close();
}

$conn->close();
