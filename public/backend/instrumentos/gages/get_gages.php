<?php
require_once __DIR__ . '/../../../../app/Core/permissions.php';

$portalSlug = session_portal_slug();
$modulesBase = __DIR__ . '/../../../../app/Modules/';

if ($portalSlug === 'service') {
    require_once $modulesBase . 'Service/Instrumentos/gages/get_gages.php';
} else {
    require_once $modulesBase . 'Tenant/Instrumentos/gages/get_gages.php';
}
