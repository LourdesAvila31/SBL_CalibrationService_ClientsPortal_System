<?php
require_once dirname(__DIR__, 3) . '/Core/auth.php';
ensure_portal_access('internal');

$rol        = $_SESSION['rol'] ?? '';
$roleAlias  = session_role_alias() ?? '';
$allowedRol = ['superadministrador', 'administrador', 'supervisor', 'operador', 'lector'];
$allowed    = ['Superadministrador', 'Administrador', 'Supervisor', 'Operador', 'Lector'];
$basePath = '/SISTEMA-COMPUTARIZADO-ISO-17025/public';
$activeSection = 'reportes';

if (!in_array($roleAlias, $allowedRol, true)) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}
?>
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reportes | SBL Pharma</title>
    <link rel="stylesheet" href="<?= htmlspecialchars($basePath) ?>/assets/styles/tenant.css">
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.2/css/all.min.css"/>
</head>
<body>
  <aside id="sidebar" class="sidebar">
        <button class="sidebar-close" id="sidebarToggle"><i class="fa fa-times"></i></button>
        <div class="sidebar-logo">
            <img src="<?= htmlspecialchars($basePath) ?>/assets/images/sbl-logo.png" alt="SBL Pharma">
        </div>
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
                <span class="topbar-title">Reportes</span>
            </div>
        </div>
        <section class="dashboard-section">
            <h2><?php echo in_array($rol, ['Supervisor','Administrador','Superadministrador'], true) ? 'Ver y editar reportes' : 'Consultar reportes'; ?></h2>
            <!-- Aquí puedes agregar la tabla o formulario de reportes según el rol -->
        </section>
    </div>
    <script type="module" src="<?= htmlspecialchars($basePath) ?>/assets/scripts/scripts-tenant.js"></script>
</body>
</html>