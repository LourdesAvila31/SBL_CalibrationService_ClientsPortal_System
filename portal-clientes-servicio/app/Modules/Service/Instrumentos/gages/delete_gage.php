<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

header('Content-Type: application/json');

if (!check_permission('instrumentos_eliminar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Modules/Internal/Auditoria/audit.php';

function build_bind_refs(array &$params): array
{
    $refs = [];
    foreach ($params as $key => &$value) {
        $refs[$key] = &$value;
    }

    return $refs;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'error' => 'Empresa no especificada']);
    $conn->close();
    exit;
}

$input = json_decode(file_get_contents('php://input'), true);
$ids = isset($input['ids']) && is_array($input['ids']) ? array_map('intval', $input['ids']) : [];
$ids = array_values(array_filter($ids, static fn($v) => $v > 0));

if (!$ids) {
    echo json_encode(['success' => false, 'error' => 'No se proporcionaron instrumentos válidos.']);
    $conn->close();
    exit;
}

$placeholders = implode(',', array_fill(0, count($ids), '?'));
$params = array_merge([$empresaId], $ids);
$types = str_repeat('i', count($params));

$calibrationSql = "SELECT instrumento_id FROM calibraciones WHERE empresa_id = ? AND instrumento_id IN ($placeholders) GROUP BY instrumento_id";
$calibrationStmt = $conn->prepare($calibrationSql);
if (!$calibrationStmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'error' => 'No se pudo validar las calibraciones asociadas.']);
    $conn->close();
    exit;
}

$calibrationRefs = build_bind_refs($params);
$calibrationStmt->bind_param($types, ...$calibrationRefs);
$calibrationStmt->execute();
$calibrationResult = $calibrationStmt->get_result();
$blocked = [];
if ($calibrationResult) {
    while ($row = $calibrationResult->fetch_assoc()) {
        $blocked[] = (int) $row['instrumento_id'];
    }
}
$calibrationStmt->close();

if (!empty($blocked)) {
    http_response_code(409);
    echo json_encode([
        'success' => false,
        'error' => 'No es posible dar de baja los instrumentos seleccionados porque cuentan con calibraciones registradas. Solicite autorización superior para reactivarlos en caso necesario.',
        'blocked_ids' => $blocked,
        'requires_authorization' => true,
    ]);
    $conn->close();
    exit;
}

try {
    $conn->begin_transaction();

    $selectSql = "SELECT id, estado, fecha_baja FROM instrumentos WHERE empresa_id = ? AND id IN ($placeholders) FOR UPDATE";
    $selectStmt = $conn->prepare($selectSql);
    if (!$selectStmt) {
        throw new RuntimeException('No se pudo preparar la consulta de instrumentos.');
    }

    $selectParams = array_merge([$empresaId], $ids);
    $selectTypes = str_repeat('i', count($selectParams));
    $selectRefs = build_bind_refs($selectParams);
    $selectStmt->bind_param($selectTypes, ...$selectRefs);
    $selectStmt->execute();
    $result = $selectStmt->get_result();

    $instrumentos = [];
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $instrumentos[] = $row;
        }
    }
    $selectStmt->close();

    if (empty($instrumentos)) {
        $conn->rollback();
        echo json_encode(['success' => false, 'error' => 'No se encontraron instrumentos para actualizar.']);
        $conn->close();
        exit;
    }

    $updateStmt = $conn->prepare('UPDATE instrumentos SET estado = ?, fecha_baja = ?, programado = 0 WHERE id = ? AND empresa_id = ?');
    $historialEstadoStmt = $conn->prepare('INSERT INTO historial_tipos_instrumento (instrumento_id, estado, empresa_id) VALUES (?, ?, ?)');
    $historialBajaStmt = $conn->prepare('INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id) VALUES (?, ?, ?)');

    if (!$updateStmt || !$historialEstadoStmt || !$historialBajaStmt) {
        throw new RuntimeException('No se pudieron preparar las operaciones de actualización.');
    }

    $usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;
    $usuarioNombre = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
    if ($usuarioNombre === '' && $usuarioId) {
        $usuarioStmt = $conn->prepare('SELECT nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
        if ($usuarioStmt) {
            $usuarioStmt->bind_param('i', $usuarioId);
            if ($usuarioStmt->execute()) {
                $usuarioStmt->bind_result($nombreDb, $apellidosDb);
                if ($usuarioStmt->fetch()) {
                    $usuarioNombre = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
                }
            }
            $usuarioStmt->close();
        }
    }
    if ($usuarioNombre === '') {
        $usuarioNombre = 'Desconocido';
    }
    $usuarioCorreo = $_SESSION['usuario'] ?? null;

    $processed = [];
    $estadoBaja = 'baja';
    $today = date('Y-m-d');

    foreach ($instrumentos as $instrumento) {
        $instrumentoId = (int) ($instrumento['id'] ?? 0);
        if ($instrumentoId <= 0) {
            continue;
        }

        $estadoAnterior = trim((string) ($instrumento['estado'] ?? ''));
        $fechaBajaAnterior = $instrumento['fecha_baja'] ?? null;
        $nuevaFechaBaja = $fechaBajaAnterior;
        if ($nuevaFechaBaja === null || $nuevaFechaBaja === '' || $nuevaFechaBaja === '0000-00-00') {
            $nuevaFechaBaja = $today;
        }

        $updateStmt->bind_param('ssii', $estadoBaja, $nuevaFechaBaja, $instrumentoId, $empresaId);
        $updateStmt->execute();

        $historialEstadoStmt->bind_param('isi', $instrumentoId, $estadoBaja, $empresaId);
        $historialEstadoStmt->execute();

        if ($fechaBajaAnterior !== $nuevaFechaBaja) {
            $historialBajaStmt->bind_param('isi', $instrumentoId, $nuevaFechaBaja, $empresaId);
            $historialBajaStmt->execute();
        }

        log_activity($usuarioNombre, [
            'seccion' => 'instrumentos',
            'instrumento_id' => $instrumentoId,
            'valor_anterior' => $estadoAnterior !== '' ? $estadoAnterior : null,
            'valor_nuevo' => $estadoBaja,
            'rango_referencia' => 'estado',
            'usuario_correo' => $usuarioCorreo,
        ]);

        $processed[] = $instrumentoId;
    }

    $updateStmt->close();
    $historialEstadoStmt->close();
    $historialBajaStmt->close();

    $conn->commit();

    echo json_encode([
        'success' => true,
        'updated_ids' => $processed,
        'message' => 'Instrumentos dados de baja correctamente.',
    ]);
} catch (Throwable $e) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'error' => 'No se pudo dar de baja los instrumentos: ' . $e->getMessage()]);
}

$conn->close();
