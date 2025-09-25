<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    echo 'Acceso denegado';
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';

$empresaId = obtenerEmpresaId();

$catalogoId     = filter_input(INPUT_POST, 'catalogo_id', FILTER_VALIDATE_INT);
$marcaId        = filter_input(INPUT_POST, 'marca_id', FILTER_VALIDATE_INT);
$modeloId       = filter_input(INPUT_POST, 'modelo_id', FILTER_VALIDATE_INT);
$serie          = trim($_POST['serie'] ?? '');
$codigo         = trim($_POST['codigo'] ?? '');
$departamentoId = filter_input(INPUT_POST, 'departamento_id', FILTER_VALIDATE_INT);
$ubicacion      = trim($_POST['ubicacion'] ?? '');
$fechaAlta      = trim($_POST['fecha_de_alta'] ?? '');

$estadoFormulario = trim($_POST['estado_instrumento'] ?? '');
$departamentoIdValido = $departamentoId !== false && $departamentoId !== null;

if ($departamentoIdValido) {
    $departamentoId = (int) $departamentoId;
} else {
    $departamentoId = null;
}

$estado = derivarEstadoInstrumento([
    'estadoActual'   => $estadoFormulario,
    'departamentoId' => $departamentoId,
    'enStock'        => in_array(mb_strtolower($estadoFormulario), ['stock', 'en stock'], true),
]);

if ($catalogoId && $marcaId && $modeloId && $serie !== '' && $codigo !== '' &&
    $departamentoId !== null && $ubicacion !== '' && $fechaAlta !== '') {
    $stmt = $conn->prepare(
        'INSERT INTO instrumentos (
            catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id, ubicacion, fecha_alta, estado, empresa_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?)'
    );
    $stmt->bind_param(
        'iiississsi',
        $catalogoId,
        $marcaId,
        $modeloId,
        $serie,
        $codigo,
        $departamentoId,
        $ubicacion,
        $fechaAlta,
        $estado,
        $empresaId
    );
    if ($stmt->execute()) {
        echo 'Instrumento registrado correctamente.';
    } else {
        echo 'Error: ' . $conn->error;
    }
    $stmt->close();
} else {
    echo 'Faltan datos para registrar el instrumento.';
}

$conn->close();
?>