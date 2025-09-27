<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}
$q = mb_strtolower(trim($_GET['q']??''));
$res = [];
// Simula búsqueda (conecta a tu base real)
if(strpos('balanza XA105', $q)!==false)
    $res[] = [ 'texto'=>'Instrumento: Balanza XA105', 'url'=>'/SBL_SISTEMA_INTERNO/public/apps/internal/instrumentos/list_gages.html?instrumento=XA105' ];
if(strpos('lourdes', $q)!==false)
    $res[] = [ 'texto'=>'Usuario: Lourdes Avila', 'url'=>'/SBL_SISTEMA_INTERNO/public/apps/internal/usuarios/list_users.html?id=1' ];
if(strpos('calibración', $q)!==false)
    $res[] = [ 'texto'=>'Calibración 2025-08', 'url'=>'/SBL_SISTEMA_INTERNO/public/apps/internal/calibraciones/list_calibrations.html?id=202508' ];
echo json_encode($res);
?>