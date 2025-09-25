<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}

$filename = 'plantilla_inventario_instrumentos.csv';

header('Content-Type: text/csv; charset=utf-8');
header('Content-Disposition: attachment; filename="' . $filename . '"');

echo "Instrumento,Marca,Modelo,Serie,Código,Departamento responsable,Ubicación,Fecha de alta,Fecha de baja,Próxima calibración,Estado,Programado\n";
echo "Termómetro,Acme,TX-100,12345,TMP-001,Control de Calidad,Laboratorio Central,2024-01-15,,2024-07-15,Activo,1\n";
