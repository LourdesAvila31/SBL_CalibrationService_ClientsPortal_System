<?php

declare(strict_types=1);

session_start();

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('configuracion_leer')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso denegado']);
    exit;
}

header('Content-Type: application/json');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

$empresaId = obtenerEmpresaId();

$response = [
    'empresa' => [
        'id' => $empresaId,
        'nombre' => null,
        'responsable_calidad_id' => null,
        'responsable_nombre' => null,
        'responsable_correo' => null,
    ],
    'candidatos' => [],
];

$stmt = $conn->prepare('SELECT e.nombre, e.responsable_calidad_id, u.nombre AS nombre_responsable, u.apellidos, u.correo FROM empresas e LEFT JOIN usuarios u ON u.id = e.responsable_calidad_id WHERE e.id = ? LIMIT 1');
if ($stmt) {
    $stmt->bind_param('i', $empresaId);
    if ($stmt->execute()) {
        $stmt->bind_result($nombreEmpresa, $responsableId, $nombreResp, $apellidosResp, $correoResp);
        if ($stmt->fetch()) {
            $response['empresa']['nombre'] = $nombreEmpresa;
            if ($responsableId) {
                $response['empresa']['responsable_calidad_id'] = (int) $responsableId;
                $nombreCompleto = trim(((string) $nombreResp) . ' ' . ((string) $apellidosResp));
                $response['empresa']['responsable_nombre'] = $nombreCompleto !== '' ? $nombreCompleto : $correoResp;
                $response['empresa']['responsable_correo'] = $correoResp;
            }
        }
    }
    $stmt->close();
}

$candidatosStmt = $conn->prepare('SELECT id, nombre, apellidos, correo FROM usuarios WHERE empresa_id = ? AND activo = 1 ORDER BY nombre, apellidos');
if ($candidatosStmt) {
    $candidatosStmt->bind_param('i', $empresaId);
    if ($candidatosStmt->execute()) {
        $candidatosStmt->bind_result($candId, $candNombre, $candApellidos, $candCorreo);
        while ($candidatosStmt->fetch()) {
            $nombreCompleto = trim(((string) $candNombre) . ' ' . ((string) $candApellidos));
            $response['candidatos'][] = [
                'id' => (int) $candId,
                'nombre' => $nombreCompleto !== '' ? $nombreCompleto : $candCorreo,
                'correo' => $candCorreo,
            ];
        }
    }
    $candidatosStmt->close();
}

echo json_encode($response);
