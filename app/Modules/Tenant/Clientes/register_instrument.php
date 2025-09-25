<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/instrument_status.php';

header('Content-Type: application/json');

ensure_portal_access('tenant');

$roleAlias = session_role_alias() ?? '';
if ($roleAlias !== 'cliente') {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Acceso permitido únicamente para clientes.']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'No se pudo determinar la empresa asociada.']);
    exit;
}

$nombre       = trim($_POST['nombre'] ?? '');
$marca        = trim($_POST['marca'] ?? '');
$modelo       = trim($_POST['modelo'] ?? '');
$serie        = trim($_POST['serie'] ?? '');
$codigo       = trim($_POST['codigo'] ?? '');
$ubicacion    = trim($_POST['ubicacion'] ?? '');
$departamento = trim($_POST['departamento'] ?? '');
$fechaAlta    = trim($_POST['fecha_alta'] ?? '');

if ($nombre === '' || $serie === '' || $codigo === '' || $ubicacion === '') {
    http_response_code(422);
    echo json_encode([
        'success' => false,
        'message' => 'Los campos nombre, número de serie, código y ubicación son obligatorios.'
    ]);
    exit;
}

if ($fechaAlta !== '') {
    $fecha = DateTime::createFromFormat('Y-m-d', $fechaAlta);
    if (!$fecha) {
        http_response_code(422);
        echo json_encode(['success' => false, 'message' => 'La fecha de alta no es válida.']);
        exit;
    }
    $fechaAlta = $fecha->format('Y-m-d');
} else {
    $fechaAlta = date('Y-m-d');
}

function fetchId(mysqli_stmt $stmt): ?int {
    $id = null;
    if (method_exists($stmt, 'get_result')) {
        $res = $stmt->get_result();
        if ($res && ($row = $res->fetch_assoc())) {
            $id = isset($row['id']) ? (int)$row['id'] : null;
        }
    } else {
        $stmt->bind_result($tmpId);
        if ($stmt->fetch()) {
            $id = (int)$tmpId;
        }
    }
    return $id;
}

function ensureNamedRecord(mysqli $conn, string $table, string $name): ?int {
    if ($name === '') {
        return null;
    }
    $sql = "SELECT id FROM {$table} WHERE nombre = ? LIMIT 1";
    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return null;
    }
    $stmt->bind_param('s', $name);
    $stmt->execute();
    $id = fetchId($stmt);
    $stmt->close();
    if ($id !== null) {
        return $id;
    }
    $insert = $conn->prepare("INSERT INTO {$table} (nombre) VALUES (?)");
    if (!$insert) {
        return null;
    }
    $insert->bind_param('s', $name);
    if ($insert->execute()) {
        $id = $insert->insert_id ?: $conn->insert_id;
        if (!$id && method_exists($conn, 'lastInsertId')) {
            $id = (int)$conn->lastInsertId();
        }
        $insert->close();
        return $id ?: null;
    }
    $insert->close();
    return null;
}

function ensureModel(mysqli $conn, string $modelo, ?int $marcaId): ?int {
    if ($modelo === '') {
        return null;
    }
    $sql = 'SELECT id FROM modelos WHERE nombre = ? LIMIT 1';
    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return null;
    }
    $stmt->bind_param('s', $modelo);
    $stmt->execute();
    $id = fetchId($stmt);
    $stmt->close();
    if ($id !== null) {
        return $id;
    }
    if ($marcaId !== null) {
        $insert = $conn->prepare('INSERT INTO modelos (nombre, marca_id) VALUES (?, ?)');
        if (!$insert) {
            return null;
        }
        $insert->bind_param('si', $modelo, $marcaId);
    } else {
        $insert = $conn->prepare('INSERT INTO modelos (nombre) VALUES (?)');
        if (!$insert) {
            return null;
        }
        $insert->bind_param('s', $modelo);
    }
    if ($insert->execute()) {
        $id = $insert->insert_id ?: $conn->insert_id;
        if (!$id && method_exists($conn, 'lastInsertId')) {
            $id = (int)$conn->lastInsertId();
        }
        $insert->close();
        return $id ?: null;
    }
    $insert->close();
    return null;
}

$conn->begin_transaction();
try {
    $catalogoId = ensureNamedRecord($conn, 'catalogo_instrumentos', $nombre);
    $marcaId    = ensureNamedRecord($conn, 'marcas', $marca);
    $modeloId   = ensureModel($conn, $modelo, $marcaId);
    $deptoId    = ensureNamedRecord($conn, 'departamentos', $departamento);

    $estado = derivarEstadoInstrumento([
        'departamentoId' => $deptoId,
        'enStock'        => $deptoId === null,
    ]);

    $stmt = $conn->prepare(
        'INSERT INTO instrumentos (catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id, ubicacion, fecha_alta, estado, empresa_id)'
        . ' VALUES (?,?,?,?,?,?,?,?,?,?)'
    );
    if (!$stmt) {
        throw new RuntimeException('No fue posible preparar la inserción del instrumento.');
    }
    $stmt->bind_param(
        'iiississsi',
        $catalogoId,
        $marcaId,
        $modeloId,
        $serie,
        $codigo,
        $deptoId,
        $ubicacion,
        $fechaAlta,
        $estado,
        $empresaId
    );
    if (!$stmt->execute()) {
        throw new RuntimeException('Error al guardar el instrumento: ' . $stmt->error);
    }
    $stmt->close();
    $conn->commit();
    echo json_encode(['success' => true, 'message' => 'Instrumento registrado correctamente.']);
} catch (Throwable $e) {
    $conn->rollback();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => $e->getMessage()]);
}
$conn->close();
