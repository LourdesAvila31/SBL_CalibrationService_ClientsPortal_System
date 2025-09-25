<?php
session_start();

require_once dirname(__DIR__, 3) . '/Core/login_paths.php';
require_once __DIR__ . '/FirmaInternaService.php';

$portalHint = detect_portal_context('internal');
$destino = login_path_for_context($portalHint);
$error = null;

do {
    // 1. Conectar a la base de datos
    try {
        require_once dirname(__DIR__, 3) . '/Core/db.php';
        require_once dirname(__DIR__, 3) . '/Core/portal_domains.php';
    } catch (Throwable $e) {
        error_log("Error de conexión a BD en login: " . $e->getMessage());
        $error = 'Error al conectar con la base de datos';
        $destino = login_path_for_context($portalHint) . '?error=' . urlencode($error);
        break;
    }

    // 2. Recoger datos del formulario
    // Permite que el input se llame "login" o "correo"
    $login = trim($_POST['login'] ?? $_POST['correo'] ?? '');
    $contrasena = trim($_POST['contrasena'] ?? '');

    // Validar que los campos no estén vacíos
    if (empty($login) || empty($contrasena)) {
        $error = 'Por favor ingrese correo/usuario y contraseña';
        $destino = login_path_for_context($portalHint) . '?error=' . urlencode($error);
        break;
    }

    // Permitir acceso usando correo o nombre de usuario
    $sql = "SELECT u.*,"
         . " u.portal_id AS user_portal_id,"
         . " r.nombre AS role_name,"
         . " r.portal_id AS role_portal_id,"
         . " rp.slug AS role_portal_slug,"
         . " rp.nombre AS role_portal_nombre,"
         . " up.slug AS user_portal_slug,"
         . " up.nombre AS user_portal_nombre"
         . " FROM usuarios u"
         . " LEFT JOIN roles r ON u.role_id = r.id"
         . " LEFT JOIN portals rp ON rp.id = r.portal_id"
         . " LEFT JOIN portals up ON up.id = u.portal_id"
         . " WHERE (u.correo = ? OR u.usuario = ?) AND u.activo = 1";

    try {
        $stmt = $conn->prepare($sql);
        $stmt->bind_param("ss", $login, $login);
        $stmt->execute();
        $res = $stmt->get_result();
        $row = $res->fetch_assoc();
    } catch (Exception $e) {
        error_log("Error en query de login: " . $e->getMessage());
        $error = 'Error interno del sistema';
        $destino = login_path_for_context($portalHint) . '?error=' . urlencode($error);
        break;
    }

    $requestedPortal = trim((string) ($_POST['portal'] ?? $_POST['portal_scope'] ?? $_POST['portal_slug'] ?? ''));

    if ($row && password_verify($contrasena, $row['contrasena'])) {
        $_SESSION['usuario']    = $row['correo'];
        $_SESSION['nombre']     = $row['nombre'];
        $_SESSION['role_id']    = $row['role_name']; // usa el nombre del rol para las verificaciones
        $_SESSION['role_name']  = $row['role_name'];
        $_SESSION['role_num']   = $row['role_id']; // conserva el id numérico por si se requiere
        $_SESSION['rol']        = $row['role_name'];
        $_SESSION['usuario_id'] = $row['id'];
        $_SESSION['usuario_login'] = $row['usuario'];

        $empresaId = null;
        if (!empty($row['empresa_id'])) {
            $empresaId = (int) $row['empresa_id'];
        } elseif (!empty($row['cliente_id'])) {
            // Compatibilidad hacia atrás: algunos registros antiguos aún usan cliente_id.
            $empresaId = (int) $row['cliente_id'];
        }

        if ($empresaId !== null && $empresaId > 0) {
            $_SESSION['empresa_id'] = $empresaId;
        } else {
            unset($_SESSION['empresa_id']);
        }

        unset($_SESSION['cliente_id']);

        firmas_internas_sincronizar_sesion($conn, $row);

        $domainPortal = null;
        if (strpos($login, '@') !== false) {
            $domainPortal = resolve_portal_by_email($login);
        }
        if ($domainPortal === null && $requestedPortal !== '' && strpos($requestedPortal, '.') !== false) {
            $domainPortal = resolve_portal_by_domain($requestedPortal);
        }

        if (is_array($domainPortal)) {
            $requestedPortal = $domainPortal['portal_slug'] ?? $requestedPortal;
        }

        $rol = $_SESSION['rol'] ?? '';
        $rolNormalizado = strtolower($rol);

        require_once dirname(__DIR__, 3) . '/Core/permissions.php';
        $canAccessService = check_permission('clientes_gestionar');

        $requestedContext = null;
        $normalizedRequested = normalize_portal_slug($requestedPortal);
        if ($normalizedRequested !== null) {
            $requestedContext = $normalizedRequested;
        } elseif (!empty($portalHint)) {
            $requestedContext = $portalHint;
        } elseif (!empty($_SESSION['app_context'])) {
            $requestedContext = strtolower((string) $_SESSION['app_context']);
        }

        $allowedContexts = [];
        if ($rolNormalizado === 'cliente') {
            $allowedContexts[] = 'tenant';
        } else {
            $allowedContexts[] = 'internal';
            if ($canAccessService) {
                $allowedContexts[] = 'service';
            }
        }

        $finalContext = $allowedContexts[0];
        if ($requestedContext !== null && in_array($requestedContext, $allowedContexts, true)) {
            $finalContext = $requestedContext;
        }

        $_SESSION['app_context'] = $finalContext;
        $_SESSION['portal_slug'] = $finalContext;

        unset($_SESSION['portal_id']);

        if (is_array($domainPortal)
            && ($domainPortal['portal_slug'] ?? null) === $finalContext
            && !empty($domainPortal['portal_id'])
        ) {
            $_SESSION['portal_id'] = (int) $domainPortal['portal_id'];
        } elseif (!empty($row['user_portal_id'])) {
            $_SESSION['portal_id'] = (int) $row['user_portal_id'];
        } elseif (!empty($row['role_portal_id'])) {
            $_SESSION['portal_id'] = (int) $row['role_portal_id'];
        }
        $baseUrl = Paths::baseUrl();
        $backendDashboard = $baseUrl === '/'
            ? '/backend/dashboard/dashboard.php'
            : rtrim($baseUrl, '/') . '/backend/dashboard/dashboard.php';
        $redirects = [
            'internal' => $backendDashboard,
            'tenant'   => Paths::app('tenant/index.html'),
            'service'  => Paths::app('service/index.html'),
        ];

        $destino = $redirects[$finalContext] ?? $redirects['internal'];

        break;
    }

    if ($row) {
        $error = 'Contraseña incorrecta';
    } else {
        // Verificar si el usuario existe pero está inactivo
        $sql_check = "SELECT u.* FROM usuarios u WHERE u.correo = ? OR u.usuario = ?";
        $stmt_check = $conn->prepare($sql_check);
        $stmt_check->bind_param("ss", $login, $login);
        $stmt_check->execute();
        $res_check = $stmt_check->get_result();
        $row_check = $res_check->fetch_assoc();

        if ($row_check && !$row_check['activo']) {
            $error = 'Usuario inactivo. Contacte al administrador';
        } else {
            $error = 'Usuario no encontrado';
        }
    }

    $destino = login_path_for_context($portalHint) . '?error=' . urlencode($error);
} while (false);

header('Location: ' . $destino);
exit;
?>