<?php
require_once __DIR__ . '/../../../app/Core/permissions.php';

$portalSlug = session_portal_slug();
$modulesDir = __DIR__ . '/../../../app/Modules';
$serviceFile = $modulesDir . '/Service/Calibraciones/approve_calibration.php';
$tenantFile = $modulesDir . '/Tenant/Calibraciones/approve_calibration.php';

if ($portalSlug === 'service' && file_exists($serviceFile)) {
    require_once $serviceFile;
} else {
    require_once $tenantFile;
}

