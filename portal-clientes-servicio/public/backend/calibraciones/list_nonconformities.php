<?php
require_once __DIR__ . '/../../../app/Core/permissions.php';

$portalSlug = session_portal_slug();
$modulesDir = __DIR__ . '/../../../app/Modules';
$serviceFile = $modulesDir . '/Service/Calibraciones/list_nonconformities.php';
$tenantFile = $modulesDir . '/Tenant/Calibraciones/list_nonconformities.php';

if ($portalSlug === 'service' && file_exists($serviceFile)) {
    require_once $serviceFile;
} else {
    require_once $tenantFile;
}
