<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';

if (!check_permission('calibraciones_leer')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/user_competency.php';

header('Content-Type: application/json; charset=UTF-8');

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['error' => 'Empresa no especificada']);
    exit;
}

$catalogoId = null;
$catalogoIdRaw = filter_input(INPUT_GET, 'catalogo_id', FILTER_DEFAULT);
if ($catalogoIdRaw !== null && $catalogoIdRaw !== '') {
    $catalogoIdValidado = filter_var($catalogoIdRaw, FILTER_VALIDATE_INT);
    if ($catalogoIdValidado === false) {
        http_response_code(400);
        echo json_encode(['error' => 'Tipo de instrumento inválido']);
        exit;
    }

    if ($catalogoIdValidado > 0) {
        $catalogoId = $catalogoIdValidado;
    }
}

$usuarios = fetch_competent_users($conn, $empresaId, $catalogoId);
$response = array_map(static function (array $user): array {
    $nombre = trim(((string) ($user['nombre'] ?? '')) . ' ' . ((string) ($user['apellidos'] ?? '')));
    return [
        'id' => $user['id'],
        'nombre' => $nombre !== '' ? $nombre : 'Usuario #' . $user['id'],
        'puesto' => $user['puesto'] ?? '',
        'correo' => $user['correo'] ?? '',
        'evidencias' => $user['evidencias'] ?? 0,
        'catalogo_id' => $user['catalogo_id'] ?? null,
        'vigente_desde' => $user['vigente_desde'] ?? null,
        'vigente_hasta' => $user['vigente_hasta'] ?? null,
        'vigencia_activa' => isset($user['vigencia_activa']) ? (bool) $user['vigencia_activa'] : true,
        'tiene_vigencia_especifica' => isset($user['tiene_vigencia_especifica']) ? (bool) $user['tiene_vigencia_especifica'] : false,
    ];
}, $usuarios);

echo json_encode(['technicians' => $response]);
$conn->close();
