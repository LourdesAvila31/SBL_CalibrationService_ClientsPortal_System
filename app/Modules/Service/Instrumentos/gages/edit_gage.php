<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

if (!check_permission('instrumentos_actualizar')) {
    http_response_code(403);
    echo json_encode(['success' => false, 'error' => 'Acceso denegado']);
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';

header('Content-Type: application/json');

$empresaId = obtenerEmpresaId();

// Lee y valida el payload
$input = json_decode(file_get_contents('php://input'), true);
$id = intval($input['id'] ?? 0);
if ($id <= 0) {
    echo json_encode(['success' => false, 'error' => 'ID inválido']);
    $conn->close();
    exit;
}

// Función auxiliar para obtener el ID a partir del nombre
function getIdByName(mysqli $conn, string $table, ?string $name): ?int {
    if ($name === null || $name === '') {
        return null;
    }
    $sql = "SELECT id FROM {$table} WHERE nombre = ? LIMIT 1";
    $stmt = $conn->prepare($sql);
    $stmt->bind_param('s', $name);
    $stmt->execute();
    $res = $stmt->get_result();
    $id = ($res && $res->num_rows) ? intval($res->fetch_assoc()['id']) : null;
    $stmt->close();
    return $id;
}

$nombre       = trim($input['nombre'] ?? '');
$marca        = trim($input['marca'] ?? '');
$modelo       = trim($input['modelo'] ?? '');
$serie        = trim($input['serie'] ?? '');
$codigo       = trim($input['codigo'] ?? '');
$departamento = trim($input['departamento'] ?? '');
$ubicacion    = trim($input['ubicacion'] ?? '');
$fechaAlta    = trim($input['fechaAlta'] ?? '');
$fechaBaja    = trim($input['fechaBaja'] ?? '');

// IDs correspondientes
$catalogoId     = getIdByName($conn, 'catalogo_instrumentos', $nombre);
$marcaId        = getIdByName($conn, 'marcas', $marca);
$modeloId       = getIdByName($conn, 'modelos', $modelo);
$departamentoId = getIdByName($conn, 'departamentos', $departamento);

// Datos anteriores para comparar cambios
$stmt = $conn->prepare('SELECT catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id, ubicacion, estado, fecha_alta, fecha_baja FROM instrumentos WHERE id = ? AND empresa_id = ?');
$stmt->bind_param('ii', $id, $empresaId);
$stmt->execute();
$old = $stmt->get_result()->fetch_assoc();
$stmt->close();

if (!$old) {
    echo json_encode(['success' => false, 'error' => 'Instrumento no encontrado']);
    $conn->close();
    exit;
}

if ($catalogoId === null) {
    $catalogoId = $old['catalogo_id'];
}
if ($marcaId === null) {
    $marcaId = $old['marca_id'];
}
if ($modeloId === null) {
    $modeloId = $old['modelo_id'];
}
if ($departamentoId === null) {
    $departamentoId = $old['departamento_id'];
}
if ($serie === '') {
    $serie = $old['serie'];
}
if ($codigo === '') {
    $codigo = $old['codigo'];
}
if ($ubicacion === '') {
    $ubicacion = $old['ubicacion'];
}
if ($fechaAlta === '') {
    $fechaAlta = $old['fecha_alta'];
}
if ($fechaBaja === '') {
    $fechaBaja = $old['fecha_baja'];
}

$catalogoId = (int) $catalogoId;
$marcaId = (int) $marcaId;
$modeloId = $modeloId !== null ? (int) $modeloId : null;
$departamentoId = $departamentoId !== null ? (int) $departamentoId : null;

$ultimoResultado = null;
$ultimasObservaciones = null;
$stmt = $conn->prepare('SELECT resultado, observaciones FROM calibraciones WHERE instrumento_id = ? AND empresa_id = ? ORDER BY fecha_calibracion DESC, id DESC LIMIT 1');
if ($stmt) {
    $stmt->bind_param('ii', $id, $empresaId);
    if ($stmt->execute()) {
        $stmt->bind_result($ultimoResultado, $ultimasObservaciones);
        $stmt->fetch();
    }
    $stmt->close();
}

$estadoDerivado = derivarEstadoInstrumento([
    'estadoActual'   => $old['estado'] ?? '',
    'fechaBaja'      => $fechaBaja,
    'resultado'      => $ultimoResultado,
    'observaciones'  => $ultimasObservaciones,
    'departamentoId' => $departamentoId,
]);

$conn->begin_transaction();
try {
    $stmt = $conn->prepare('UPDATE instrumentos SET catalogo_id=?, marca_id=?, modelo_id=?, serie=?, codigo=?, departamento_id=?, ubicacion=?, fecha_alta=?, fecha_baja=?, estado=? WHERE id=? AND empresa_id=?');
    $stmt->bind_param('iiississssii', $catalogoId, $marcaId, $modeloId, $serie, $codigo, $departamentoId, $ubicacion, $fechaAlta, $fechaBaja, $estadoDerivado, $id, $empresaId);
    $stmt->execute();
    $stmt->close();

    // Registro de historial cuando cambian los valores
    if ($departamentoId != $old['departamento_id']) {
        $stmt = $conn->prepare('INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id) VALUES (?, ?, ?)');
        $stmt->bind_param('iii', $id, $departamentoId, $empresaId);
        $stmt->execute();
        $stmt->close();
    }
    if ($ubicacion !== $old['ubicacion']) {
        $stmt = $conn->prepare('INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id) VALUES (?, ?, ?)');
        $stmt->bind_param('isi', $id, $ubicacion, $empresaId);
        $stmt->execute();
        $stmt->close();
    }
    if ($estadoDerivado !== $old['estado']) {
        $stmt = $conn->prepare('INSERT INTO historial_tipos_instrumento (instrumento_id, estado, empresa_id) VALUES (?, ?, ?)');
        $stmt->bind_param('isi', $id, $estadoDerivado, $empresaId);
        $stmt->execute();
        $stmt->close();
    }
    if ($fechaAlta !== $old['fecha_alta']) {
        $stmt = $conn->prepare('INSERT INTO historial_fecha_alta (instrumento_id, fecha, empresa_id) VALUES (?, ?, ?)');
        $stmt->bind_param('isi', $id, $fechaAlta, $empresaId);
        $stmt->execute();
        $stmt->close();
    }
    if ($fechaBaja !== $old['fecha_baja']) {
        $stmt = $conn->prepare('INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id) VALUES (?, ?, ?)');
        $stmt->bind_param('isi', $id, $fechaBaja, $empresaId);
        $stmt->execute();
        $stmt->close();
    }

    $conn->commit();
    echo json_encode(['success' => true]);
} catch (Throwable $e) {
    $conn->rollback();
    echo json_encode(['success' => false, 'error' => $e->getMessage()]);
}

$conn->close();
?>
