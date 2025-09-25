<?php
require_once __DIR__ . '/../../../app/Core/permissions.php';

$portalSlug = session_portal_slug();
$modulesDir = __DIR__ . '/../../../app/Modules';
$serviceFile = $modulesDir . '/Service/Calibraciones/reject_calibration.php';
$internalFile = $modulesDir . '/Internal/Calibraciones/reject_calibration.php';

if ($portalSlug === 'service' && file_exists($serviceFile)) {
    require_once $serviceFile;
} else {
    require_once $internalFile;
}
