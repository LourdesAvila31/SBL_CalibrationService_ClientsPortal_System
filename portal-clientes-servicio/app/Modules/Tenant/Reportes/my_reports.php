<?php
declare(strict_types=1);

require_once dirname(__DIR__, 3) . '/Core/auth.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';

ensure_portal_access('internal');

if (!check_permission('reportes_leer')) {
    http_response_code(403);
    exit('Acceso denegado');
}

$rol = $_SESSION['rol'] ?? '';
$empresaId = obtenerEmpresaId();
$usuarioId = (int) ($_SESSION['usuario_id'] ?? 0);
$roleAlias = session_role_alias() ?? '';
$isSuperAdmin = session_is_superadmin();

$reports = [];
$errorCarga = null;
$basePath = '/SISTEMA-COMPUTARIZADO-ISO-17025/public';
$activeSection = 'reportes';

global $conn;
if ($conn instanceof mysqli) {
    try {
        $reports = fetch_generated_reports($conn, $isSuperAdmin, $roleAlias, $empresaId, $usuarioId);
    } catch (Throwable $e) {
        $errorCarga = $e->getMessage();
    }
} else {
    $errorCarga = 'No hay conexión con la base de datos.';
}

function fetch_generated_reports(mysqli $conn, bool $isSuperAdmin, string $roleAlias, ?int $empresaId, int $usuarioId): array
{
    $sql = "SELECT e.id, e.template_key, e.year_value, e.revision_number, e.firmas_json, e.parametros_json, e.drive_file_url, e.drive_file_id, e.created_at,"
         . " u.usuario AS generado_por, t.descripcion, t.title_pattern"
         . " FROM report_template_exports e"
         . " LEFT JOIN report_templates t ON t.id = e.template_id"
         . " LEFT JOIN usuarios u ON u.id = e.usuario_id"
         . " WHERE 1=1";

    $params = [];
    $types  = '';

    $restrictToUser = in_array($roleAlias, ['cliente', 'operador'], true);
    if (!$isSuperAdmin) {
        if ($empresaId !== null) {
            $sql .= ' AND (e.empresa_id = ? OR (e.empresa_id IS NULL AND t.empresa_id = ?))';
            $types  .= 'ii';
            $params[] = $empresaId;
            $params[] = $empresaId;
        } else {
            $restrictToUser = true;
        }
    }

    if ($restrictToUser) {
        $sql .= ' AND e.usuario_id = ?';
        $types  .= 'i';
        $params[] = $usuarioId;
    }

    $sql .= ' ORDER BY e.created_at DESC, e.id DESC LIMIT 200';

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        throw new RuntimeException('No se pudo preparar la consulta de reportes generados.');
    }

    if ($types !== '') {
        $stmt->bind_param($types, ...$params);
    }
    $stmt->execute();

    $results = [];
    if (method_exists($stmt, 'get_result')) {
        $res = $stmt->get_result();
        while ($res && ($row = $res->fetch_assoc())) {
            $results[] = normalizar_fila_reporte($row);
        }
    } else {
        $stmt->bind_result(
            $id,
            $templateKey,
            $yearValue,
            $revisionNumber,
            $firmasJson,
            $parametrosJson,
            $driveUrl,
            $driveId,
            $createdAt,
            $usuarioNombre,
            $descripcion,
            $titlePattern
        );
        while ($stmt->fetch()) {
            $results[] = normalizar_fila_reporte([
                'id'             => $id,
                'template_key'   => $templateKey,
                'year_value'     => $yearValue,
                'revision_number'=> $revisionNumber,
                'firmas_json'    => $firmasJson,
                'parametros_json'=> $parametrosJson,
                'drive_file_url' => $driveUrl,
                'drive_file_id'  => $driveId,
                'created_at'     => $createdAt,
                'generado_por'   => $usuarioNombre,
                'descripcion'    => $descripcion,
                'title_pattern'  => $titlePattern,
            ]);
        }
    }

    $stmt->close();
    return $results;
}

/**
 * @param array<string,mixed> $row
 * @return array<string,mixed>
 */
function normalizar_fila_reporte(array $row): array
{
    $firmas = [];
    if (!empty($row['firmas_json'])) {
        $decoded = json_decode((string) $row['firmas_json'], true);
        if (is_array($decoded)) {
            foreach ($decoded as $firma) {
                $firmas[] = format_signature_text($firma);
            }
        }
    }

    return [
        'id'             => (int) ($row['id'] ?? 0),
        'template_key'   => (string) ($row['template_key'] ?? ''),
        'descripcion'    => (string) ($row['descripcion'] ?? ''),
        'title_pattern'  => (string) ($row['title_pattern'] ?? ''),
        'year'           => (int) ($row['year_value'] ?? 0),
        'revision'       => (int) ($row['revision_number'] ?? 0),
        'firmas'         => array_filter($firmas, static fn($value) => $value !== ''),
        'drive_url'      => (string) ($row['drive_file_url'] ?? ''),
        'drive_id'       => (string) ($row['drive_file_id'] ?? ''),
        'fecha'          => (string) ($row['created_at'] ?? ''),
        'generado_por'   => (string) ($row['generado_por'] ?? ''),
    ];
}

function format_signature_text($signature): string
{
    if (is_array($signature)) {
        $parts = [];
        foreach ($signature as $value) {
            if (is_scalar($value) && trim((string) $value) !== '') {
                $parts[] = trim((string) $value);
            }
        }
        if (!empty($parts)) {
            return implode(' – ', $parts);
        }
    }
    if (is_scalar($signature)) {
        return trim((string) $signature);
    }
    return '';
}

function titulo_template(array $reporte): string
{
    if ($reporte['descripcion'] !== '') {
        return $reporte['descripcion'];
    }
    if ($reporte['title_pattern'] !== '') {
        return $reporte['title_pattern'];
    }
    return $reporte['template_key'];
}
?>
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Mis Reportes | SBL Pharma</title>
    <link rel="stylesheet" href="<?= htmlspecialchars($basePath) ?>/assets/styles/tenant.css">
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.2/css/all.min.css"/>
    <style>
        .reports-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1.5rem;
        }
        .reports-table th,
        .reports-table td {
            border: 1px solid #d9d9d9;
            padding: 0.75rem 1rem;
            text-align: left;
            vertical-align: top;
        }
        .reports-table th {
            background-color: #f0f4ff;
            font-weight: 600;
            color: #1a2a4a;
        }
        .reports-table tbody tr:nth-child(even) {
            background-color: #f9fbff;
        }
        .reports-table tbody tr:hover {
            background-color: #eef3ff;
        }
        .reports-table .signature-list {
            margin: 0;
            padding-left: 1.25rem;
        }
        .reports-table .signature-list li {
            margin-bottom: 0.25rem;
        }
        .reports-table .empty {
            color: #6f7c8a;
            font-style: italic;
        }
        .reports-table .download-link {
            color: #1a57c4;
            font-weight: 600;
            text-decoration: none;
        }
        .reports-table .download-link:hover {
            text-decoration: underline;
        }
        .reports-table .small-text {
            color: #6f7c8a;
            font-size: 0.85rem;
            margin-top: 0.25rem;
        }
        .alert {
            background: #fff4e5;
            border: 1px solid #f0c36d;
            color: #6f3d00;
            padding: 1rem;
            border-radius: 6px;
            margin-top: 1.5rem;
        }
    </style>
</head>
<body>
   <aside id="sidebar" class="sidebar">
        <button class="sidebar-close" id="sidebarToggle"><i class="fa fa-times"></i></button>
        <div class="sidebar-logo"><img src="<?= htmlspecialchars($basePath) ?>/assets/images/sbl-logo.png" alt="SBL Pharma"></div>
        <nav class="sidebar-nav">
            <a class="nav-link<?= $activeSection === 'dashboard' ? ' active' : '' ?>" href="<?= htmlspecialchars($basePath) ?>/backend/dashboard/dashboard.php">
                <i class="fa fa-tachometer-alt"></i>
                <span>Dashboard</span>
            </a>
            <a class="nav-link<?= $activeSection === 'usuarios' ? ' active' : '' ?>" href="<?= htmlspecialchars($basePath) ?>/backend/usuarios/users.php">
                <i class="fa fa-users"></i>
                <span>Usuarios</span>
            </a>
            <a class="nav-link<?= $activeSection === 'calibraciones' ? ' active' : '' ?>" href="<?= htmlspecialchars($basePath) ?>/backend/calibraciones/calibrations.php">
                <i class="fa fa-balance-scale"></i>
                <span>Calibraciones</span>
            </a>
            <a class="nav-link<?= $activeSection === 'reportes' ? ' active' : '' ?>" href="<?= htmlspecialchars($basePath) ?>/backend/reportes/reports.php">
                <i class="fa fa-file-alt"></i>
                <span>Reportes</span>
            </a>
            <a class="nav-link<?= $activeSection === 'clientes' ? ' active' : '' ?>" href="<?= htmlspecialchars($basePath) ?>/backend/clientes/clients.php">
                <i class="fa fa-handshake"></i>
                <span>Clientes</span>
            </a>
        </nav>
    </aside>
    <div class="main-content">
        <div class="topbar">
            <div class="topbar-left">
                <i class="fa fa-bars menu-toggle" id="sidebarOpen"></i>
                <span class="topbar-title">Mis Reportes</span>
            </div>
        </div>
        <section class="dashboard-section">
            <h2>
                <?php
                if ($rol === 'Operador') {
                    echo 'Reportes generados recientemente';
                } elseif ($rol === 'Cliente') {
                    echo 'Historial de reportes disponibles';
                } elseif ($rol === 'Superadministrador') {
                    echo 'Historial global de reportes';
                } else {
                    echo 'Historial de reportes';
                }
                ?>
            </h2>

            <?php if ($errorCarga !== null): ?>
                <div class="alert">
                    <?php echo htmlspecialchars($errorCarga, ENT_QUOTES, 'UTF-8'); ?>
                </div>
            <?php elseif (empty($reports)): ?>
                <div class="alert">
                    Aún no se han generado reportes mediante las plantillas configuradas.
                </div>
            <?php else: ?>
                <div class="table-responsive">
                    <table class="reports-table">
                        <thead>
                            <tr>
                                <th>Plantilla</th>
                                <th>Año</th>
                                <th>Revisión</th>
                                <th>Firmas</th>
                                <th>Generado por</th>
                                <th>Fecha</th>
                                <th>Archivo</th>
                            </tr>
                        </thead>
                        <tbody>
                        <?php foreach ($reports as $reporte): ?>
                            <tr>
                                <td>
                                    <strong><?php echo htmlspecialchars(titulo_template($reporte), ENT_QUOTES, 'UTF-8'); ?></strong>
                                    <div class="small-text">Clave: <?php echo htmlspecialchars($reporte['template_key'], ENT_QUOTES, 'UTF-8'); ?></div>
                                </td>
                                <td><?php echo htmlspecialchars((string) $reporte['year'], ENT_QUOTES, 'UTF-8'); ?></td>
                                <td><?php echo htmlspecialchars((string) $reporte['revision'], ENT_QUOTES, 'UTF-8'); ?></td>
                                <td>
                                    <?php if (empty($reporte['firmas'])): ?>
                                        <span class="empty">Sin firmas registradas</span>
                                    <?php else: ?>
                                        <ul class="signature-list">
                                            <?php foreach ($reporte['firmas'] as $firma): ?>
                                                <li><?php echo nl2br(htmlspecialchars($firma, ENT_QUOTES, 'UTF-8')); ?></li>
                                            <?php endforeach; ?>
                                        </ul>
                                    <?php endif; ?>
                                </td>
                                <td><?php echo htmlspecialchars($reporte['generado_por'] !== '' ? $reporte['generado_por'] : 'Desconocido', ENT_QUOTES, 'UTF-8'); ?></td>
                                <td><?php echo htmlspecialchars($reporte['fecha'], ENT_QUOTES, 'UTF-8'); ?></td>
                                <td>
                                    <?php if ($reporte['drive_url'] !== ''): ?>
                                        <a class="download-link" href="<?php echo htmlspecialchars($reporte['drive_url'], ENT_QUOTES, 'UTF-8'); ?>" target="_blank" rel="noopener">
                                            Ver en Drive
                                        </a>
                                        <div class="small-text">ID: <?php echo htmlspecialchars($reporte['drive_id'], ENT_QUOTES, 'UTF-8'); ?></div>
                                    <?php else: ?>
                                        <span class="empty">No disponible</span>
                                    <?php endif; ?>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            <?php endif; ?>
        </section>
    </div>
    <script type="module" src="<?= htmlspecialchars($basePath) ?>/assets/scripts/scripts-tenant.js"></script>
</body>
</html>
