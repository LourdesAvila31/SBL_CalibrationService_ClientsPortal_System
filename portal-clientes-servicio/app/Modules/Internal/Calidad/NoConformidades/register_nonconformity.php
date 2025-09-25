<?php
require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';

header('Content-Type: application/json');

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Auditoria/audit.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Empresa no identificada']);
    exit;
}

$payload = json_decode(file_get_contents('php://input'), true);
if (!is_array($payload)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Solicitud inválida']);
    exit;
}

$titulo = trim((string)($payload['titulo'] ?? ''));
$descripcion = trim((string)($payload['descripcion'] ?? ''));
$detectadaPorId = isset($payload['detectada_por_id']) ? (int)$payload['detectada_por_id'] : 0;
$fechaDeteccion = trim((string)($payload['fecha_detectada'] ?? date('Y-m-d')));
$codigo = trim((string)($payload['codigo'] ?? ''));
$clasificacionId = isset($payload['clasificacion_id']) ? (int)$payload['clasificacion_id'] : null;
$causaRaiz = trim((string)($payload['causa_raiz'] ?? (string)($payload['tipo'] ?? '')));
$accionesPropuestas = trim((string)($payload['acciones'] ?? ''));
$responsableId = isset($payload['responsable_id']) ? (int)$payload['responsable_id'] : $detectadaPorId;
$proceso = trim((string)($payload['proceso'] ?? ''));
$prioridad = trim((string)($payload['prioridad'] ?? ''));

if ($titulo === '' || $descripcion === '' || $detectadaPorId <= 0) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Título, descripción y responsable de detección son obligatorios']);
    exit;
}

$fechaValida = DateTime::createFromFormat('Y-m-d', $fechaDeteccion);
if (!$fechaValida) {
    http_response_code(422);
    echo json_encode(['success' => false, 'message' => 'Fecha de detección inválida']);
    exit;
}
$fechaRegistrada = $fechaValida->format('Y-m-d');

$verificarReporta = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ?');
if (!$verificarReporta) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible validar al usuario que reporta']);
    exit;
}
$verificarReporta->bind_param('ii', $detectadaPorId, $empresaId);
$verificarReporta->execute();
$verificarReporta->store_result();
if ($verificarReporta->num_rows === 0) {
    $verificarReporta->close();
    http_response_code(404);
    echo json_encode(['success' => false, 'message' => 'El usuario que reporta no pertenece a la empresa']);
    exit;
}
$verificarReporta->free_result();

$responsableNombre = null;
if ($responsableId > 0) {
    $verificarReporta->bind_param('ii', $responsableId, $empresaId);
    $verificarReporta->execute();
    $verificarReporta->store_result();
    if ($verificarReporta->num_rows === 0) {
        $verificarReporta->close();
        http_response_code(404);
        echo json_encode(['success' => false, 'message' => 'El responsable indicado no pertenece a la empresa']);
        exit;
    }
    $verificarReporta->free_result();

    $nombreResponsable = $conn->prepare('SELECT TRIM(CONCAT(COALESCE(nombre, ""), " ", COALESCE(apellidos, ""))) FROM usuarios WHERE id = ?');
    if ($nombreResponsable) {
        $nombreResponsable->bind_param('i', $responsableId);
        $nombreResponsable->execute();
        $nombreResponsable->bind_result($responsableNombreTmp);
        if ($nombreResponsable->fetch()) {
            $responsableNombre = trim((string)$responsableNombreTmp) ?: null;
        }
        $nombreResponsable->close();
    }
} else {
    $responsableId = null;
}
$verificarReporta->close();

$detallesAdicionales = [];
if ($proceso !== '') {
    $detallesAdicionales[] = 'Proceso: ' . $proceso;
}
if ($prioridad !== '') {
    $detallesAdicionales[] = 'Prioridad: ' . $prioridad;
}

$descripcionAmpliada = trim($descripcion . (count($detallesAdicionales) ? ("\n" . implode("\n", $detallesAdicionales)) : ''));
$accionesTexto = $accionesPropuestas !== '' ? $accionesPropuestas : null;
$causaRaizTexto = $causaRaiz !== '' ? $causaRaiz : null;
$codigoParam = $codigo !== '' ? $codigo : null;
$clasificacionParam = $clasificacionId && $clasificacionId > 0 ? $clasificacionId : null;
$creadoPor = isset($_SESSION['usuario_id']) ? (int)$_SESSION['usuario_id'] : 0;
$creadoPorParam = $creadoPor > 0 ? $creadoPor : null;

$stmt = $conn->prepare(
    'INSERT INTO calidad_no_conformidades (empresa_id, codigo, clasificacion_id, titulo, descripcion, causa_raiz, acciones, responsable_id, responsable, estado, reportado_por, creado_por, detectado_en)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "abierta", ?, ?, ?)'
);
if (!$stmt) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No fue posible registrar la no conformidad']);
    exit;
}

$stmt->bind_param(
    'isissssisiis',
    $empresaId,
    $codigoParam,
    $clasificacionParam,
    $titulo,
    $descripcionAmpliada,
    $causaRaizTexto,
    $accionesTexto,
    $responsableId,
    $responsableNombre,
    $detectadaPorId,
    $creadoPorParam,
    $fechaRegistrada
);

if (!$stmt->execute()) {
    $stmt->close();
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'No se pudo guardar la no conformidad']);
    exit;
}

$noConformidadId = $stmt->insert_id;
$stmt->close();

$nombreAud = trim(((string)($_SESSION['nombre'] ?? '')) . ' ' . ((string)($_SESSION['apellidos'] ?? '')));
if ($nombreAud === '') {
    $nombreAud = 'Sistema';
}
$correoAud = trim((string)($_SESSION['usuario'] ?? ''));

log_activity($nombreAud, [
    'seccion' => 'calidad_no_conformidades',
    'valor_nuevo' => sprintf('Registro no conformidad #%d (%s)', $noConformidadId, $titulo),
    'usuario_correo' => $correoAud,
    'usuario_id' => $_SESSION['usuario_id'] ?? null,
]);

echo json_encode([
    'success' => true,
    'message' => 'No conformidad registrada',
    'data' => [
        'no_conformidad_id' => $noConformidadId,
        'estado' => 'abierta',
    ],
]);

$conn->close();
