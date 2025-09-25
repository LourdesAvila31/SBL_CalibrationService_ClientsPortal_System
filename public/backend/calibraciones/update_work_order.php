<?php
require_once __DIR__ . '/../../../app/Core/permissions.php';

$portalSlug = session_portal_slug();

$modulesDir = __DIR__ . '/../../../app/Modules';
$serviceFile = $modulesDir . '/Service/Calibraciones/update_work_order.php';
$tenantFile = $modulesDir . '/Tenant/Calibraciones/update_work_order.php';

if ($portalSlug === 'service' && file_exists($serviceFile)) {
    require_once $serviceFile;
} else {
    require_once $tenantFile;
}
