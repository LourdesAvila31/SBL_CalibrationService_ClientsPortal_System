<?php
require_once dirname(__DIR__, 3) . '/Core/auth.php';

$rol        = $_SESSION['rol'] ?? '';
$roleAlias  = session_role_alias() ?? '';
$allowedRol = ['superadministrador', 'administrador', 'supervisor', 'operador', 'lector', 'cliente'];
$portalScopes = $roleAlias === 'cliente' ? ['tenant'] : ['internal'];

ensure_portal_access($portalScopes);

if (!in_array($roleAlias, $allowedRol, true)) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}

$nombre = $_SESSION['nombre'];

$basePath     = '/SBL_SISTEMA_INTERNO/public';
$addUserPage  = $basePath . '/apps/internal/usuarios/add_user.html';
$links        = [];

switch ($rol) {
    case 'Superadministrador':
    case 'Administrador':
        $links[] = ['Gestión de usuarios', $basePath . '/apps/internal/usuarios/list_users.html', 'full'];
        $links[] = ['Calibraciones', $basePath . '/apps/internal/calibraciones/list_calibrations.html', 'full'];
        $links[] = ['Reportes', $basePath . '/apps/internal/reportes/reports.html', 'full'];
        $links[] = ['Gestión de clientes', $basePath . '/index.php?app=service', 'full'];
        $links[] = ['Crear cuenta de Personal', $addUserPage . '?tipo=personal', 'full'];
        $links[] = ['Crear cuenta de Cliente', $addUserPage . '?tipo=cliente', 'full'];
        break;
    case 'Supervisor':
        $links[] = ['Gestión de usuarios', '#', 'none'];
        $links[] = ['Calibraciones', $basePath . '/backend/calibraciones/calibrations.php', 'full'];
        $links[] = ['Reportes', $basePath . '/backend/reportes/reports.php', 'full'];
        $links[] = ['Gestión de clientes', '#', 'none'];
        $links[] = ['Crear cuenta de Personal', '#', 'none'];
        $links[] = ['Crear cuenta de Cliente', '#', 'none'];
        break;
    case 'Operador':
        $links[] = ['Gestión de usuarios', '#', 'none'];
        $links[] = ['Calibraciones', $basePath . '/backend/calibraciones/my_calibrations.php', 'full'];
        $links[] = ['Reportes', $basePath . '/backend/reportes/my_reports.php', 'view'];
        $links[] = ['Gestión de clientes', '#', 'none'];
        $links[] = ['Crear cuenta de Personal', '#', 'none'];
        $links[] = ['Crear cuenta de Cliente', '#', 'none'];
        break;
    case 'Lector':
        $links[] = ['Gestión de usuarios', '#', 'none'];
        $links[] = ['Calibraciones', $basePath . '/backend/calibraciones/calibrations.php', 'view'];
        $links[] = ['Reportes', $basePath . '/backend/reportes/reports.php', 'view'];
        $links[] = ['Gestión de clientes', '#', 'none'];
        $links[] = ['Crear cuenta de Personal', '#', 'none'];
        $links[] = ['Crear cuenta de Cliente', '#', 'none'];
        break;
    case 'Cliente':
        $links[] = ['Gestión de usuarios', '#', 'none'];
        $links[] = ['Calibraciones', $basePath . '/backend/calibraciones/my_calibrations.php', 'view'];
        $links[] = ['Reportes', $basePath . '/backend/reportes/my_reports.php', 'view'];
        $links[] = ['Gestión de clientes', '#', 'none'];
        $links[] = ['Crear cuenta de Personal', '#', 'none'];
        $links[] = ['Crear cuenta de Cliente', '#', 'none'];
        break;
}

$links[] = ['Ir a inicio', $basePath . '/apps/internal/index.html', 'full'];

?>
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Panel principal</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&display=swap" rel="stylesheet">
    <style>
        body {
            background: linear-gradient(135deg, #e6f6fb 0%, #a3defd 55%, #0d575a 100%);
            font-family: 'Montserrat', Arial, sans-serif;
            margin: 0;
        }
        .dashboard-box {
            background: #fff;
            box-shadow: 0 8px 32px rgba(60, 100, 180, 0.13);
            border-radius: 18px;
            padding: 42px 36px 32px 36px;
            max-width: 440px;
            width: 100%;
            margin: 46px auto 0 auto;
            text-align: center;
        }
        h2 {
            color: #0d575a;
            margin-bottom: 8px;
            font-size: 1.5rem;
            font-weight: 700;
        }
        .rol-info {
            color: #217b9b;
            margin-bottom: 26px;
            font-size: 1.07rem;
        }
        .dash-btn {
            position: relative;
            display: block;
            margin: 10px 0;
            background: #217b9b;
            color: #fff;
            border: none;
            border-radius: 10px;
            padding: 12px 0;
            width: 100%;
            box-shadow: 0 3px 9px #217b9b22;
            cursor: pointer;
            transition: background .18s;
            font-weight: 600;
            font-size: 1.08rem;
        }
        .dash-btn:hover {
            background: #0d575a;
        }
        .dash-btn.disabled {
            background: #ccc;
            color: #666;
            cursor: not-allowed;
            box-shadow: none;
        }
        .dash-btn.icon-lock::after,
        .dash-btn.icon-eye::after {
            position: absolute;
            bottom: 4px;
            right: 8px;
            font-size: 1rem;
        }
        .dash-btn.icon-lock::after {
            content: '\1F512';
        }
        .dash-btn.icon-eye::after {
            content: '\1F441';
        }
        .logout-link {
            margin-top: 22px;
            color: #0d575a;
            font-weight: 700;
            display: inline-block;
        }
    </style>
</head>
<body>
    <div class="dashboard-box">
        <h2>¡Bienvenido, <?= htmlspecialchars($nombre) ?>!</h2>
        <div class="rol-info">Tu rol: <strong><?= htmlspecialchars($rol) ?></strong></div>

        <?php foreach ($links as [$label, $url, $access]):
            $class = 'dash-btn';
            $onclick = '';
            if ($access === 'none') {
                $class .= ' disabled icon-lock';
            } elseif ($access === 'view') {
                $class .= ' icon-eye';
                $onclick = " onclick=\"location.href='$url'\"";
            } else {
                $onclick = " onclick=\"location.href='$url'\"";
            }
        ?>
            <button class="<?= $class ?>"<?= $onclick ?>><?= htmlspecialchars($label) ?></button>
        <?php endforeach; ?>

        <a class="logout-link" href="<?= htmlspecialchars($basePath) ?>/backend/usuarios/logout.php">Cerrar sesión</a>
    </div>
</body>
</html>

