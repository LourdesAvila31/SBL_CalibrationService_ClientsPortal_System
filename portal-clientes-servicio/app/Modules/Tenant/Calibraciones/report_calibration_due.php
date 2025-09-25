<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_alerts.php';

$empresaId = obtenerEmpresaId();

header('Content-Type: application/vnd.ms-excel');
header('Content-Disposition: attachment; filename="calibration_due.xls"');

echo "<table border='1'><tr>",
    "    <th>Instrumento</th>",
    "    <th>Patrón</th>",
    "    <th>Certificado de patrón</th>",
    "    <th>Fecha de Calibración</th>",
    "    <th>Fecha Próxima Calibración</th>",
    "    <th>Resultado</th>",
    "    <th>Técnico</th>",
    "    <th>Estado logístico</th>",
    "    <th>Fecha envío</th>",
    "    <th>Fecha retorno</th>",
"</tr>";

if (!$empresaId) {
    http_response_code(400);
    echo "<tr><td colspan='7'>Empresa no especificada</td></tr></table>";
    $conn->close();
    exit;
}

try {
    $rows = calibration_alerts_fetch_upcoming($conn, 60, $empresaId);
} catch (RuntimeException $e) {
    echo "<tr><td colspan='7'>" . htmlspecialchars($e->getMessage(), ENT_QUOTES, 'UTF-8') . "</td></tr></table>";
    $conn->close();
    exit;
}

foreach ($rows as $row) {
    $instrumento = htmlspecialchars((string) ($row['instrumento_nombre'] ?? ''), ENT_QUOTES, 'UTF-8');
    $fechaCalibracion = (string) ($row['fecha_calibracion'] ?? '');
    $fechaProxima = (string) ($row['fecha_proxima'] ?? '');
    $resultado = (string) ($row['resultado'] ?? '');
    $tecnico = trim((string) ($row['tecnico_nombre'] ?? ''));
    $estadoLogistica = (string) (($row['logistica']['estado'] ?? $row['logistica_estado'] ?? 'Pendiente'));
    $fechaEnvio = (string) (($row['logistica']['fecha_envio'] ?? $row['logistica_fecha_envio'] ?? ''));
    $fechaRetorno = (string) (($row['logistica']['fecha_retorno'] ?? $row['logistica_fecha_retorno'] ?? ''));
    $patronNombre = trim((string) ($row['patron_nombre'] ?? ''));
    $patronCertNumero = trim((string) ($row['patron_certificado_numero'] ?? ''));
    $patronCertUrl = trim((string) ($row['patron_certificado_url'] ?? ''));

    $certificadoTexto = $patronCertNumero !== '' ? $patronCertNumero : '-';
    if ($patronCertUrl !== '') {
        $basePath = '/SISTEMA-COMPUTARIZADO-ISO-17025/public';
        $fullUrl = $patronCertUrl;
        if (strpos($patronCertUrl, 'http') !== 0) {
            $fullUrl = $basePath . ((isset($patronCertUrl[0]) && $patronCertUrl[0] === '/') ? $patronCertUrl : '/' . $patronCertUrl);
        }
        $enlace = htmlspecialchars($fullUrl, ENT_QUOTES, 'UTF-8');
        $certificadoTexto = '<a href="' . $enlace . '" target="_blank" rel="noopener">' . htmlspecialchars($certificadoTexto, ENT_QUOTES, 'UTF-8') . '</a>';
    } else {
        $certificadoTexto = htmlspecialchars($certificadoTexto, ENT_QUOTES, 'UTF-8');
    }

    echo '<tr>' .
        '<td>' . $instrumento . '</td>' .
        '<td>' . htmlspecialchars($patronNombre !== '' ? $patronNombre : '-', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . $certificadoTexto . '</td>' .
        '<td>' . htmlspecialchars($fechaCalibracion !== '' ? $fechaCalibracion : '-', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . htmlspecialchars($fechaProxima !== '' ? $fechaProxima : '-', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . htmlspecialchars($resultado !== '' ? $resultado : '-', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . htmlspecialchars($tecnico !== '' ? $tecnico : 'Sin asignar', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . htmlspecialchars($estadoLogistica !== '' ? $estadoLogistica : 'Pendiente', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . htmlspecialchars($fechaEnvio !== '' ? $fechaEnvio : '-', ENT_QUOTES, 'UTF-8') . '</td>' .
        '<td>' . htmlspecialchars($fechaRetorno !== '' ? $fechaRetorno : '-', ENT_QUOTES, 'UTF-8') . '</td>' .
    '</tr>';
}

echo '</table>';
$conn->close();
