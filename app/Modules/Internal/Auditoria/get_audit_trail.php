<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
header('Content-Type: application/json');
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

function normalizarSegmentoConsulta(?string $segmento): string
{
    $valor = strtolower(trim((string) $segmento));
    if ($valor === '') {
        return 'Administradores';
    }

    if (strpos($valor, 'sistema') !== false) {
        return 'Sistemas';
    }

    if (strpos($valor, 'dev') !== false) {
        return 'Developer';
    }

    if (strpos($valor, 'admin') !== false) {
        return 'Administradores';
    }

    return 'Administradores';
}

$usuario = trim($_GET['usuario'] ?? '') ?: null;
$seccion = trim($_GET['seccion'] ?? '') ?: null;
if (!$seccion && isset($_GET['relevancia'])) {
    $seccion = trim($_GET['relevancia']);
    if ($seccion === '') {
        $seccion = null;
    }
}
$segmentoSolicitado = normalizarSegmentoConsulta($_GET['segmento'] ?? null);

$empresaId = obtenerEmpresaId();

$roleSession = null;
if (isset($_SESSION['role_id']) && is_string($_SESSION['role_id'])) {
    $roleSession = $_SESSION['role_id'];
} elseif (isset($_SESSION['role_name']) && is_string($_SESSION['role_name'])) {
    $roleSession = $_SESSION['role_name'];
} elseif (isset($_SESSION['rol']) && is_string($_SESSION['rol'])) {
    $roleSession = $_SESSION['rol'];
}

if ($roleSession !== null && strtolower($roleSession) === 'developer') {
    $seccion = 'Developer';
}

$sql = <<<SQL
    SELECT
        aud.id,
        aud.fecha_evento,
        aud.segmento_actor,
        aud.seccion,
        aud.rango_referencia,
        aud.valor_anterior,
        aud.valor_nuevo,
        aud.usuario_correo,
        aud.usuario_nombre,
        aud.usuario_firma_interna,
        COALESCE(usr.puesto, 'Sin especificar') AS usuario_puesto,
        CASE
            WHEN (aud.valor_anterior IS NULL OR TRIM(aud.valor_anterior) = '')
                AND (aud.valor_nuevo IS NULL OR TRIM(aud.valor_nuevo) = '') THEN 'Registro sin cambios'
            WHEN aud.valor_anterior IS NULL OR TRIM(aud.valor_anterior) = '' THEN CONCAT('Alta: ', LEFT(COALESCE(TRIM(aud.valor_nuevo), ''), 120))
            WHEN aud.valor_nuevo IS NULL OR TRIM(aud.valor_nuevo) = '' THEN CONCAT('Baja de: ', LEFT(COALESCE(TRIM(aud.valor_anterior), ''), 120))
            WHEN TRIM(COALESCE(aud.valor_anterior, '')) = TRIM(COALESCE(aud.valor_nuevo, '')) THEN CONCAT('Sin cambios (', LEFT(COALESCE(TRIM(aud.valor_nuevo), ''), 120), ')')
            ELSE CONCAT('Actualizó de ', LEFT(COALESCE(TRIM(aud.valor_anterior), ''), 60), ' a ', LEFT(COALESCE(TRIM(aud.valor_nuevo), ''), 60))
        END AS descripcion_cambio
    FROM audit_trail aud
    LEFT JOIN usuarios usr
        ON usr.empresa_id = ?
       AND aud.usuario_correo IS NOT NULL
       AND (
           LOWER(usr.correo) = LOWER(aud.usuario_correo)
           OR LOWER(usr.usuario) = LOWER(aud.usuario_correo)
       )
SQL;
$conds = [];
$params = [$empresaId];
$types = 'i';

$conds[] = 'aud.empresa_id = ?';
$params[] = $empresaId;
$types .= 'i';

$conds[] = 'aud.segmento_actor = ?';
$params[] = $segmentoSolicitado;
$types .= 's';

if ($usuario) {
    $conds[] = '('
        . ' LOWER(aud.usuario_correo) = LOWER(?)'
        . " OR LOWER(aud.usuario_nombre) LIKE CONCAT('%', LOWER(?), '%')"
        . " OR LOWER(COALESCE(aud.usuario_firma_interna, '')) LIKE CONCAT('%', LOWER(?), '%')"
        . ' OR ('
        . '     usr.id IS NOT NULL'
        . '     AND ('
        . '         LOWER(usr.correo) = LOWER(?)'
        . '         OR LOWER(usr.usuario) = LOWER(?)'
        . '     )'
        . ' )'
        . ')';
    $params[] = $usuario;
    $params[] = $usuario;
    $params[] = $usuario;
    $params[] = $usuario;
    $params[] = $usuario;
    $types .= 'sssss';
}
if ($seccion) {
    $conds[] = 'LOWER(aud.seccion) = LOWER(?)';
    $params[] = $seccion;
    $types .= 's';
}

if ($conds) {
    $sql .= ' WHERE ' . implode(' AND ', $conds);
}

$sql .= ' ORDER BY COALESCE(aud.fecha_evento, aud.id) DESC, aud.id DESC';

$stmt = $conn->prepare($sql);
if ($params) {
    $refs = [];
    foreach ($params as $key => $value) {
        $refs[$key] = &$params[$key];
    }
    $stmt->bind_param($types, ...$refs);
}
$stmt->execute();
$result = $stmt->get_result();

$registros = [];
if ($result) {
    while ($row = $result->fetch_assoc()) {
        $fechaEvento = $row['fecha_evento'] ?? null;
        $registros[] = array_merge($row, [
            'fecha' => $fechaEvento ? substr($fechaEvento, 0, 10) : null,
            'hora' => $fechaEvento ? substr($fechaEvento, 11, 8) : null,
            'actividad' => $row['descripcion_cambio'],
            'correo_usuario' => $row['usuario_correo'],
            'nombre_usuario' => $row['usuario_nombre'],
            'usuario_firma_interna' => $row['usuario_firma_interna'],
            'descripcion_cambio' => $row['descripcion_cambio'],
            'puesto_usuario' => $row['usuario_puesto'],
            'segmento_actor' => $row['segmento_actor'],
        ]);
    }
}

echo json_encode($registros);

$stmt->close();
$conn->close();
?>