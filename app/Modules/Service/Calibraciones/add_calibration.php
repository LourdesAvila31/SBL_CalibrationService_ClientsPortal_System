<?php
// Usar rutas relativas para que la conexión funcione independientemente del directorio base
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 2) . '/Internal/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/instrument_status.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_nonconformity.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_schedule.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_references.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_uncertainty.php';

$empresaId = null;
session_start();
ensure_portal_access('service');
if (!check_permission('calibraciones_crear')) {
    http_response_code(403);
    echo json_encode(['success'=>false,'message'=>'Acceso denegado']);
    exit;
}
header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $empresaId = $empresaId ?? obtenerEmpresaId();
    if (!$empresaId) {
        echo json_encode(['success' => false, 'message' => 'Empresa no determinada.']);
        exit;
    }

    $instrumento_id   = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT);
    $tecnicoIdRaw     = filter_input(INPUT_POST, 'tecnico_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($tecnicoIdRaw === false) {
        echo json_encode([
            'success' => false,
            'message' => 'Identificador de técnico inválido.'
        ]);
        exit;
    }

    $measurementDefaults = [
        'calibration_date' => null,
        'due_date' => null,
        'result' => null,
        'periodo' => null,
        'notes' => null,
        'type' => null,
        'duration_hours' => null,
        'cost_total' => null,
        'estado_ejecucion' => null,
        'motivo_reprogramacion' => null,
        'fecha_reprogramada' => null,
        'dias_atraso' => null,
    ];
    $measurementPayloadJson = null;
    $measurementRecord = null;

    $measurementId = filter_input(INPUT_POST, 'measurement_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($measurementId === false) {
        $measurementId = null;
    }

    $calibradorSeleccionadoRaw = filter_input(INPUT_POST, 'calibrador_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($calibradorSeleccionadoRaw === false) {
        echo json_encode([
            'success' => false,
            'message' => 'Identificador de calibrador inválido.'
        ]);
        exit;
    }
    $calibradorSeleccionado = $calibradorSeleccionadoRaw !== null ? (int) $calibradorSeleccionadoRaw : null;

    $calibration_date = trim($_POST['calibration_date'] ?? '');
    if ($calibration_date === '' && $measurementDefaults['calibration_date']) {
        $calibration_date = $measurementDefaults['calibration_date'];
    }
    $calibrationDateObject = $calibration_date !== '' ? calibration_create_date($calibration_date) : null;
    // La fecha de la siguiente calibración se calculará con base en la
    // frecuencia registrada en plan_riesgos, por lo que el valor recibido
    // desde el formulario se utiliza únicamente como respaldo.
    $due_date         = trim($_POST['next_calibration_date'] ?? '');
    if ($due_date === '' && $measurementDefaults['due_date']) {
        $due_date = (string) $measurementDefaults['due_date'];
    }
    $result           = trim($_POST['result'] ?? '');
    if ($result === '' && $measurementDefaults['result']) {
        $result = (string) $measurementDefaults['result'];
    }
    $periodo          = strtoupper(trim($_POST['periodo'] ?? ''));
    if ($periodo === '' && $measurementDefaults['periodo']) {
        $periodo = strtoupper((string) $measurementDefaults['periodo']);
    }
    $notes            = trim($_POST['notes'] ?? '');
    if ($notes === '' && $measurementDefaults['notes']) {
        $notes = (string) $measurementDefaults['notes'];
    }
    $typeRaw          = trim($_POST['type'] ?? '');
    if ($typeRaw === '' && $measurementDefaults['type']) {
        $typeRaw = (string) $measurementDefaults['type'];
    }
    $durationRaw      = $_POST['duration_hours'] ?? '';
    if (($durationRaw === '' || $durationRaw === null) && $measurementDefaults['duration_hours'] !== null) {
        $durationRaw = (string) $measurementDefaults['duration_hours'];
    }
    $costRaw          = $_POST['cost_total'] ?? '';
    if (($costRaw === '' || $costRaw === null) && $measurementDefaults['cost_total'] !== null) {
        $costRaw = (string) $measurementDefaults['cost_total'];
    }

    $calibradorRaw = $_POST['calibrador_id'] ?? null;
    if ($calibradorRaw === '' || $calibradorRaw === null) {
        $calibradorSeleccionado = null;
    } else {
        $calibradorSeleccionado = filter_var($calibradorRaw, FILTER_VALIDATE_INT);
        if ($calibradorSeleccionado === false) {
            echo json_encode([
                'success' => false,
                'message' => 'Identificador de calibrador inválido.'
            ]);
            exit;
        }
        $calibradorSeleccionado = (int) $calibradorSeleccionado;
    }

    $logisticsProvider = trim($_POST['logistics_provider'] ?? '');
    $logisticsCarrier = trim($_POST['logistics_carrier'] ?? '');
    $logisticsTracking = trim($_POST['logistics_tracking'] ?? '');
    $logisticsOrder = trim($_POST['logistics_order'] ?? '');
    $logisticsComments = trim($_POST['logistics_comments'] ?? '');
    $logisticsStateInput = $_POST['logistics_state'] ?? '';
    $logisticsShipDate = $_POST['logistics_ship_date'] ?? '';
    $logisticsTransitDate = $_POST['logistics_transit_date'] ?? '';
    $logisticsReceivedDate = $_POST['logistics_received_date'] ?? '';
    $logisticsReturnDate = $_POST['logistics_return_date'] ?? '';

    $patronIdRaw = filter_input(INPUT_POST, 'patron_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($patronIdRaw === false) {
        echo json_encode([
            'success' => false,
            'message' => 'Identificador de patrón inválido.'
        ]);
        exit;
    }
    $patronId = $patronIdRaw !== null ? (int) $patronIdRaw : null;
    $patronCertificadoInput = trim($_POST['patron_certificado'] ?? '');
    $patronCertificadoParam = $patronCertificadoInput !== '' ? $patronCertificadoInput : null;
    $patronData = null;

    if ($patronId !== null) {
        $patronStmt = $conn->prepare('SELECT id, nombre, certificado_numero, certificado_archivo, fecha_vencimiento FROM patrones WHERE id = ? AND empresa_id = ? LIMIT 1');
        if (!$patronStmt) {
            echo json_encode([
                'success' => false,
                'message' => 'No se pudo verificar la información del patrón seleccionado.'
            ]);
            exit;
        }
        $patronStmt->bind_param('ii', $patronId, $empresaId);
        $patronStmt->execute();
        $patronResult = $patronStmt->get_result();
        $patronData = $patronResult ? $patronResult->fetch_assoc() : null;
        $patronStmt->close();

        if (!$patronData) {
            echo json_encode([
                'success' => false,
                'message' => 'El patrón seleccionado no existe o pertenece a otra empresa.'
            ]);
            exit;
        }

        $certificadoCatalogo = trim((string) ($patronData['certificado_numero'] ?? ''));
        if ($patronCertificadoParam === null && $certificadoCatalogo !== '') {
            $patronCertificadoParam = $certificadoCatalogo;
        }
    }

    $referencesRaw = $_POST['references_payload'] ?? '';
    $references = calibration_references_parse_payload($referencesRaw);

    $validTypes = ['Interna', 'Externa'];
    $type = $typeRaw !== '' ? $typeRaw : 'Interna';
    if (!in_array($type, $validTypes, true)) {
        $type = 'Interna';
    }
    $typeLower = strtolower($type);
    $type = $typeLower === 'externa' ? 'Externa' : 'Interna';

    if ($type === 'Interna') {
        if ($patronId === null) {
            echo json_encode([
                'success' => false,
                'message' => 'Selecciona el patrón utilizado para la calibración interna.'
            ]);
            exit;
        }
        if ($patronCertificadoParam === null || $patronCertificadoParam === '') {
            echo json_encode([
                'success' => false,
                'message' => 'Registra el certificado vigente del patrón utilizado.'
            ]);
            exit;
        }
    }

    if ($patronData && !empty($patronData['fecha_vencimiento'])) {
        $fechaVencimiento = DateTimeImmutable::createFromFormat('Y-m-d', (string) $patronData['fecha_vencimiento']);
        if ($fechaVencimiento instanceof DateTimeImmutable && $calibrationDateObject instanceof DateTimeImmutable && $fechaVencimiento < $calibrationDateObject) {
            echo json_encode([
                'success' => false,
                'message' => 'El patrón seleccionado está vencido. Renueva su certificación antes de cerrar la calibración.'
            ]);
            exit;
        }
    }

    $durationHours = null;
    if ($durationRaw !== '' && $durationRaw !== null) {
        $durationHours = filter_var($durationRaw, FILTER_VALIDATE_FLOAT);
        if ($durationHours === false) {
            echo json_encode([
                'success' => false,
                'message' => 'Duración inválida.'
            ]);
            exit;
        }
        if ($durationHours < 0) {
            echo json_encode([
                'success' => false,
                'message' => 'La duración no puede ser negativa.'
            ]);
            exit;
        }
    }

    $costTotal = null;
    if ($costRaw !== '' && $costRaw !== null) {
        $costTotal = filter_var($costRaw, FILTER_VALIDATE_FLOAT);
        if ($costTotal === false) {
            echo json_encode([
                'success' => false,
                'message' => 'Costo inválido.'
            ]);
            exit;
        }
        if ($costTotal < 0) {
            echo json_encode([
                'success' => false,
                'message' => 'El costo no puede ser negativo.'
            ]);
            exit;
        }
    }

    $periodosValidos = ['P1', 'P2', 'EXTRA'];

    $usuarioSesionId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;

    $tecnicoAsignado = $tecnicoIdRaw ?: $usuarioSesionId;
    if ($tecnicoAsignado !== null) {
        $tecnicoAsignado = (int) $tecnicoAsignado;
    }
    if (!$tecnicoAsignado) {
        echo json_encode([
            'success' => false,
            'message' => 'Debes seleccionar al técnico responsable.'
        ]);
        exit;
    }

    $stmtUsuario = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$stmtUsuario) {
        echo json_encode(['success' => false, 'message' => 'No se pudo validar al técnico.']);
        exit;
    }
    $stmtUsuario->bind_param('ii', $tecnicoAsignado, $empresaId);
    $stmtUsuario->execute();
    $stmtUsuario->store_result();
    if ($stmtUsuario->num_rows === 0) {
        $stmtUsuario->close();
        echo json_encode(['success' => false, 'message' => 'El técnico seleccionado no pertenece a la empresa.']);
        exit;
    }
    $stmtUsuario->close();

    if (!$instrumento_id || $calibration_date === '' || $result === '') {
        echo json_encode([
            'success' => false,
            'message' => 'Faltan campos obligatorios.'
        ]);
        exit;
    }

    if (!in_array($periodo, $periodosValidos, true)) {
        echo json_encode([
            'success' => false,
            'message' => 'Periodo inválido.'
        ]);
        exit;
    }

    if (!$calibrationDateObject) {
        echo json_encode([
            'success' => false,
            'message' => 'Fecha de calibración inválida.'
        ]);
        exit;
    }

    if ($due_date !== '' && !calibration_create_date($due_date)) {
        echo json_encode([
            'success' => false,
            'message' => 'Fecha próxima inválida.'
        ]);
        exit;

    }

    $planId = filter_input(INPUT_POST, 'plan_id', FILTER_VALIDATE_INT);
    if ($planId === false) {
        echo json_encode([
            'success' => false,
            'message' => 'Identificador de plan inválido.'
        ]);
        exit;
    }

    $estadoRaw = trim($_POST['estado_ejecucion'] ?? '');
    if ($estadoRaw === '' && $measurementDefaults['estado_ejecucion']) {
        $estadoRaw = (string) $measurementDefaults['estado_ejecucion'];
    }
    $estadoEjecucion = calibration_normalize_status($estadoRaw);
    if ($estadoRaw !== '' && strcasecmp($estadoRaw, $estadoEjecucion) !== 0) {
        echo json_encode([
            'success' => false,
            'message' => 'Estado de ejecución inválido.'
        ]);
        exit;
    }

    $uncertaintyValueRaw = $_POST['u_value'] ?? '';
    $uncertaintyMethodRaw = $_POST['u_method'] ?? '';
    $uncertaintyKRaw = $_POST['u_k'] ?? '';
    $uncertaintyCoverageRaw = $_POST['u_coverage'] ?? '';

    $uncertaintyValue = null;
    if ($uncertaintyValueRaw !== '' && $uncertaintyValueRaw !== null) {
        $uncertaintyValue = filter_var($uncertaintyValueRaw, FILTER_VALIDATE_FLOAT);
        if ($uncertaintyValue === false || $uncertaintyValue < 0) {
            echo json_encode([
                'success' => false,
                'message' => 'La incertidumbre debe ser un número igual o mayor a cero.'
            ]);
            exit;
        }
    }

    $uncertaintyK = null;
    if ($uncertaintyKRaw !== '' && $uncertaintyKRaw !== null) {
        $uncertaintyK = filter_var($uncertaintyKRaw, FILTER_VALIDATE_FLOAT);
        if ($uncertaintyK === false || $uncertaintyK <= 0) {
            echo json_encode([
                'success' => false,
                'message' => 'El factor de cobertura k debe ser un número mayor a cero.'
            ]);
            exit;
        }
    }

    $uncertaintyMethod = is_string($uncertaintyMethodRaw) ? trim($uncertaintyMethodRaw) : '';
    $uncertaintyCoverage = is_string($uncertaintyCoverageRaw) ? trim($uncertaintyCoverageRaw) : '';

    $uncertaintyRequired = calibration_requires_uncertainty($estadoEjecucion, $result);

    if ($uncertaintyRequired) {
        if ($uncertaintyValue === null || $uncertaintyMethod === '' || $uncertaintyK === null || $uncertaintyCoverage === '') {
            echo json_encode([
                'success' => false,
                'message' => 'Registra el valor de incertidumbre, método, factor k y cobertura antes de cerrar la calibración.'
            ]);
            exit;
        }
    }

    $motivoReprogramacion = trim($_POST['motivo_reprogramacion'] ?? '');
    if ($motivoReprogramacion === '' && $measurementDefaults['motivo_reprogramacion']) {
        $motivoReprogramacion = (string) $measurementDefaults['motivo_reprogramacion'];
    }
    $fechaReprogramadaRaw = trim($_POST['fecha_reprogramada'] ?? '');
    if ($fechaReprogramadaRaw === '' && $measurementDefaults['fecha_reprogramada']) {
        $fechaReprogramadaRaw = (string) $measurementDefaults['fecha_reprogramada'];
    }
    $fechaReprogramada = $fechaReprogramadaRaw !== '' ? $fechaReprogramadaRaw : null;

    if ($fechaReprogramada !== null && !calibration_create_date($fechaReprogramada)) {
        echo json_encode([
            'success' => false,
            'message' => 'Fecha reprogramada inválida.'
        ]);
        exit;
    }

    $instrumentoCatalogoId = null;
    $check = $conn->prepare('SELECT estado, fecha_baja, departamento_id, proxima_calibracion, catalogo_id FROM instrumentos WHERE id = ? AND empresa_id = ?');
    $check->bind_param('ii', $instrumento_id, $empresaId);
    $check->execute();
    $instrumentData = $check->get_result();
    if (!$instrumentData || $instrumentData->num_rows === 0) {
        echo json_encode([
            'success' => false,
            'message' => 'Instrumento no encontrado'
        ]);
        $check->close();
        exit;
    }
    $instrumentoActual = $instrumentData->fetch_assoc();
    $instrumentoCatalogoId = isset($instrumentoActual['catalogo_id']) ? (int) $instrumentoActual['catalogo_id'] : null;
    if ($instrumentoCatalogoId === 0) {
        $instrumentoCatalogoId = null;
    }
    $check->close();

    if (!user_has_competency($conn, $tecnicoAsignado, $empresaId, $instrumentoCatalogoId)) {
        echo json_encode([
            'success' => false,
            'message' => 'El técnico seleccionado no cuenta con evidencia vigente para este tipo de instrumento.'
        ]);
        exit;
    }

    $instrumentContext = [];
    try {
        $context = tenant_notifications_fetch_instrument_context($conn, $instrumento_id, $empresaId);
        if (is_array($context)) {
            $instrumentContext = $context;
        }
    } catch (Throwable $e) {
        error_log('[nc-calibraciones][WARNING] No se pudo obtener el contexto del instrumento: ' . $e->getMessage());
    }

    $fechaProgramada = null;
    if ($planId !== null) {
        $planStmt = $conn->prepare('SELECT id, fecha_programada, instrumento_id FROM planes WHERE id = ? AND empresa_id = ? LIMIT 1');
        if ($planStmt) {
            $planStmt->bind_param('ii', $planId, $empresaId);
            $planStmt->execute();
            $planResult = $planStmt->get_result();
            $planRow = $planResult ? $planResult->fetch_assoc() : null;
            $planStmt->close();

            if (!$planRow) {
                echo json_encode(['success' => false, 'message' => 'Plan de calibración no encontrado.']);
                exit;
            }

            if ((int) ($planRow['instrumento_id'] ?? 0) !== (int) $instrumento_id) {
                echo json_encode(['success' => false, 'message' => 'El plan seleccionado no corresponde al instrumento.']);
                exit;
            }

            $fechaProgramada = $planRow['fecha_programada'] ?? null;
        } else {
            echo json_encode(['success' => false, 'message' => 'No se pudo verificar el plan asociado.']);
            exit;
        }
    }

    if ($fechaProgramada === null) {
        $autoStmt = $conn->prepare('SELECT id, fecha_programada FROM planes WHERE instrumento_id = ? AND empresa_id = ? ORDER BY ABS(DATEDIFF(fecha_programada, ?)) ASC LIMIT 1');
        if ($autoStmt) {
            $autoStmt->bind_param('iis', $instrumento_id, $empresaId, $calibration_date);
            if ($autoStmt->execute()) {
                $autoResult = $autoStmt->get_result();
                $autoRow = $autoResult ? $autoResult->fetch_assoc() : null;
                if ($autoRow) {
                    $planDate = calibration_create_date($autoRow['fecha_programada'] ?? null);
                    $realDate = calibration_create_date($calibration_date);
                    if ($planDate && $realDate) {
                        $absDiff = abs((int) $planDate->diff($realDate)->format('%r%a'));
                        if ($absDiff <= 90) {
                            $fechaProgramada = $autoRow['fecha_programada'];
                            if ($planId === null) {
                                $planId = (int) $autoRow['id'];
                            }
                        }
                    }
                }
            }
            $autoStmt->close();
        }
    }

    if ($estadoEjecucion === 'Reprogramada' && $fechaReprogramada === null) {
        echo json_encode([
            'success' => false,
            'message' => 'Debes registrar la nueva fecha cuando la calibración se reprograma.'
        ]);
        exit;
    }

    if ($fechaReprogramada !== null && $fechaProgramada !== null) {
        $planDate = calibration_create_date($fechaProgramada);
        $reprogramadaDate = calibration_create_date($fechaReprogramada);
        if ($planDate && $reprogramadaDate) {
            $diffPlan = (int) $planDate->diff($reprogramadaDate)->format('%r%a');
            if ($diffPlan < 0) {
                echo json_encode([
                    'success' => false,
                    'message' => 'La fecha reprogramada debe ser posterior o igual a la programada.'
                ]);
                exit;
            }
        }
    }

    $diasAtraso = calibration_compute_delay($fechaProgramada, $calibration_date, $fechaReprogramada);
    if ($measurementDefaults['dias_atraso'] !== null) {
        $diasAtraso = (int) $measurementDefaults['dias_atraso'];
    }
    $justificacionRequerida = calibration_requires_justification($estadoEjecucion, $diasAtraso);

    if ($justificacionRequerida && $motivoReprogramacion === '') {
        echo json_encode([
            'success' => false,
            'message' => 'Es necesario capturar la justificación del atraso o reprogramación.'
        ]);
        exit;
    }

    $payloadManualRaw = $_POST['payload_json'] ?? '';
    $payloadManualTrimmed = is_string($payloadManualRaw) ? trim($payloadManualRaw) : '';
    $payloadCalibracion = null;
    if ($measurementPayloadJson !== null && $measurementPayloadJson !== '') {
        $payloadCalibracion = $measurementPayloadJson;
    } elseif ($payloadManualTrimmed !== '') {
        $decodedManual = json_decode($payloadManualTrimmed, true);
        if ($decodedManual === null && json_last_error() !== JSON_ERROR_NONE) {
            echo json_encode([
                'success' => false,
                'message' => 'La carga JSON proporcionada no es válida.'
            ]);
            exit;
        }
        $payloadCalibracion = $payloadManualTrimmed;
    }

    $origenDatos = $measurementRecord ? 'integracion' : 'manual';


    $certificatePath = null;
    if (isset($_FILES['certificate'])) {
        if ($_FILES['certificate']['error'] === UPLOAD_ERR_OK) {
            $uploadDir = __DIR__ . '/certificates/';
            if (!is_dir($uploadDir)) {
                if (!mkdir($uploadDir, 0755, true) && !is_dir($uploadDir)) {
                    echo json_encode([
                        'success' => false,
                        'message' => 'No se pudo crear el directorio de carga.'
                    ]);
                    exit;
                }
            }

            $originalName = $_FILES['certificate']['name'];
            $extension = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));
            $allowedMimeTypes = [
                'pdf'  => ['application/pdf'],
                'jpg'  => ['image/jpeg'],
                'jpeg' => ['image/jpeg'],
                'png'  => ['image/png'],
                'gif'  => ['image/gif'],
                'bmp'  => ['image/bmp'],
                'tif'  => ['image/tiff'],
                'tiff' => ['image/tiff'],
                'webp' => ['image/webp']
            ];

            if (!array_key_exists($extension, $allowedMimeTypes)) {
                echo json_encode([
                    'success' => false,
                    'message' => 'Tipo de archivo no permitido.'
                ]);
                exit;
            }

            $finfo = new finfo(FILEINFO_MIME_TYPE);
            $mimeType = $finfo->file($_FILES['certificate']['tmp_name']);
            if ($mimeType === false || !in_array($mimeType, $allowedMimeTypes[$extension], true)) {
                echo json_encode([
                    'success' => false,
                    'message' => 'El archivo cargado no coincide con un tipo permitido.'
                ]);
                exit;
            }

            try {
                $safeName = bin2hex(random_bytes(16));
            } catch (Exception $e) {
                echo json_encode([
                    'success' => false,
                    'message' => 'No se pudo generar un nombre seguro para el archivo.'
                ]);
                exit;
            }

            $newFileName = $safeName . '.' . $extension;
            $target = $uploadDir . $newFileName;

            if (!move_uploaded_file($_FILES['certificate']['tmp_name'], $target)) {
                echo json_encode([
                    'success' => false,
                    'message' => 'No se pudo completar la carga del archivo.'
                ]);
                exit;
            }

            $certificatePath = $newFileName;
        } elseif ($_FILES['certificate']['error'] !== UPLOAD_ERR_NO_FILE) {
            echo json_encode([
                'success' => false,
                'message' => 'Error al cargar el archivo.'
            ]);
            exit;
        }
    }

    $stmt = $conn->prepare(
        'INSERT INTO calibraciones (
            instrumento_id,
            empresa_id,
            patron_id,
            patron_certificado,
            usuario_id,
            fecha_calibracion,
            fecha_proxima,
            resultado_preliminar,
            resultado,
            observaciones,
            periodo,
            tipo,
            duracion_horas,
            costo_total,
            estado_ejecucion,
            motivo_reprogramacion,
            fecha_reprogramada,
            dias_atraso,
            calibrador_id,
            origen_datos,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    );
    if (!$stmt) {
        echo json_encode(['success'=>false,'message'=>'Error en consulta: '.$conn->error]);
        exit;
    }

    $dueDateParam = $due_date !== '' ? $due_date : null;
    $notesParam = $notes !== '' ? $notes : null;

    $durationParam = $durationHours !== null ? number_format($durationHours, 1, '.', '') : null;
    $costParam = $costTotal !== null ? number_format($costTotal, 2, '.', '') : null;
    $motivoParam = $motivoReprogramacion !== '' ? $motivoReprogramacion : null;
    $fechaReprogramadaParam = $fechaReprogramada !== null ? (string) $fechaReprogramada : null;
    $diasAtrasoDb = $diasAtraso !== null ? (int) $diasAtraso : 0;
    $calibradorSeleccionadoParam = $calibradorSeleccionado !== null ? (int) $calibradorSeleccionado : null;

    $estadoRegistro = 'Pendiente';
    $resultadoPreliminar = $result;
    $resultadoLiberado = null;

    $stmt->bind_param(
        'iiisissssssssssssiiss',
        $instrumento_id,
        $empresaId,
        $patronId,
        $patronCertificadoParam,
        $tecnicoAsignado,
        $calibration_date,
        $dueDateParam,
        $resultadoPreliminar,
        $resultadoLiberado,
        $notesParam,
        $periodo,
        $type,
        $durationParam,
        $costParam,
        $estadoEjecucion,
        $motivoParam,
        $fechaReprogramadaParam,
        $diasAtrasoDb,
        $calibradorSeleccionadoParam,
        $origenDatos,
        $payloadCalibracion
    );
    if ($stmt->execute()) {
        $nombreOperador = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
        if ($nombreOperador === '' && !empty($_SESSION['usuario_id'])) {
            $usuarioStmt = $conn->prepare('SELECT nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
            if ($usuarioStmt) {
                $usuarioStmt->bind_param('i', $_SESSION['usuario_id']);
                if ($usuarioStmt->execute()) {
                    $usuarioStmt->bind_result($nombreDb, $apellidosDb);
                    if ($usuarioStmt->fetch()) {
                        $nombreOperador = trim(((string) $nombreDb) . ' ' . ((string) $apellidosDb));
                    }
                }
                $usuarioStmt->close();
            }
        }
        if ($nombreOperador === '') {
            $nombreOperador = 'Desconocido';
        }
        $correoOperador = $_SESSION['usuario'] ?? null;

        log_activity($nombreOperador, "Alta de calibración instrumento $instrumento_id", 'calibraciones', $correoOperador);
        $calibration_id = $stmt->insert_id;

        $uValueParam = $uncertaintyValue !== null ? number_format($uncertaintyValue, 6, '.', '') : null;
        $uMethodParam = $uncertaintyMethod !== '' ? $uncertaintyMethod : null;
        $uKParam = $uncertaintyK !== null ? number_format($uncertaintyK, 4, '.', '') : null;
        $uCoverageParam = $uncertaintyCoverage !== '' ? $uncertaintyCoverage : null;

        $updateUncertainty = $conn->prepare('UPDATE calibraciones SET u_value = ?, u_method = ?, u_k = ?, u_coverage = ?, calibrador_id = IFNULL(?, calibrador_id) WHERE id = ? AND empresa_id = ?');
        if ($updateUncertainty) {
            $calibradorUpdate = $calibradorSeleccionado !== null ? $calibradorSeleccionado : null;
            $updateUncertainty->bind_param('ssssiii', $uValueParam, $uMethodParam, $uKParam, $uCoverageParam, $calibradorUpdate, $calibration_id, $empresaId);
            $updateUncertainty->execute();
            $updateUncertainty->close();
        }

        calibration_references_sync($conn, $calibration_id, $empresaId, $references);

        calibration_references_sync($conn, $calibration_id, $empresaId, $references);

        if ($type === 'Externa') {
            $dates = [
                'fecha_envio' => logistics_date_from_input($logisticsShipDate),
                'fecha_en_transito' => logistics_date_from_input($logisticsTransitDate),
                'fecha_recibido' => logistics_date_from_input($logisticsReceivedDate),
                'fecha_retorno' => logistics_date_from_input($logisticsReturnDate),
            ];
            $estadoLogistica = logistics_infer_state_from_dates($dates, logistics_normalize_state($logisticsStateInput));

            $logStmt = $conn->prepare('INSERT INTO logistica_calibraciones (calibracion_id, estado, proveedor_externo, transportista, numero_guia, orden_servicio, fecha_envio, fecha_en_transito, fecha_recibido, fecha_retorno, comentarios) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
            if ($logStmt) {
                $providerParam = $logisticsProvider !== '' ? $logisticsProvider : null;
                $carrierParam = $logisticsCarrier !== '' ? $logisticsCarrier : null;
                $trackingParam = $logisticsTracking !== '' ? $logisticsTracking : null;
                $orderParam = $logisticsOrder !== '' ? $logisticsOrder : null;
                $commentsParam = $logisticsComments !== '' ? $logisticsComments : null;

                $logStmt->bind_param(
                    'issssssssss',
                    $calibration_id,
                    $estadoLogistica,
                    $providerParam,
                    $carrierParam,
                    $trackingParam,
                    $orderParam,
                    $dates['fecha_envio'],
                    $dates['fecha_en_transito'],
                    $dates['fecha_recibido'],
                    $dates['fecha_retorno'],
                    $commentsParam
                );
                $logStmt->execute();
                $logStmt->close();
            }
        }

        if ($certificatePath !== null) {
            $tipoCertificado = 'calibracion';
            $certStmt = $conn->prepare('INSERT INTO certificados (calibracion_id, archivo, tipo) VALUES (?, ?, ?)');
            if ($certStmt) {
                $certStmt->bind_param('iss', $calibration_id, $certificatePath, $tipoCertificado);
                $certStmt->execute();
                $certStmt->close();
            }

            $certificateNotificationData = [
                'instrumento_id' => $instrumento_id,
                'empresa_id' => $empresaId,
                'empresa_nombre' => $instrumentContext['empresa_nombre'] ?? ('Empresa #' . $empresaId),
                'instrumento_nombre' => $instrumentContext['instrumento_nombre'] ?? ('Instrumento #' . $instrumento_id),
                'instrumento_codigo' => $instrumentContext['instrumento_codigo'] ?? '',
                'certificate_path' => $certificatePath,
                'fecha_calibracion' => $calibration_date,
            ];
        }

        if ($planId !== null) {
            $relStmt = $conn->prepare('INSERT INTO calibraciones_planes (calibracion_id, plan_id) VALUES (?, ?)');
            if ($relStmt) {
                $relStmt->bind_param('ii', $calibration_id, $planId);
                $relStmt->execute();
                $relStmt->close();
            }

            $nuevoEstadoPlan = in_array($estadoEjecucion, ['Reprogramada', 'Atrasada'], true) ? $estadoEjecucion : 'Completada';
            $planUpdate = $conn->prepare('UPDATE planes SET estado = ? WHERE id = ? AND empresa_id = ?');
            if ($planUpdate) {
                $planUpdate->bind_param('sii', $nuevoEstadoPlan, $planId, $empresaId);
                $planUpdate->execute();
                $planUpdate->close();
            }

            if ($estadoEjecucion === 'Reprogramada' && $fechaReprogramadaParam !== null) {
                $planReschedule = $conn->prepare('UPDATE planes SET fecha_programada = ? WHERE id = ? AND empresa_id = ?');
                if ($planReschedule) {
                    $planReschedule->bind_param('sii', $fechaReprogramadaParam, $planId, $empresaId);
                    $planReschedule->execute();
                    $planReschedule->close();
                }
            }
        }

        // Obtener frecuencia del plan basado en riesgos
        $frecuencia = null;
        $freqStmt = $conn->prepare("SELECT frecuencia FROM plan_riesgos WHERE instrumento_id = ?");
        if ($freqStmt) {
            $freqStmt->bind_param('i', $instrumento_id);
            if ($freqStmt->execute()) {
                $freqStmt->bind_result($frecuencia);
                $freqStmt->fetch();
            }
            $freqStmt->close();
        }

        $proxima = null;
        if (is_numeric($frecuencia)) {
            $date = new DateTime($calibration_date);
            $date->modify('+' . intval($frecuencia) . ' months');
            $proxima = $date->format('Y-m-d');

            // Actualiza la fecha calculada en la calibración
            $upCal = $conn->prepare('UPDATE calibraciones SET fecha_proxima = ? WHERE id = ? AND empresa_id = ?');
            if ($upCal) {
                $upCal->bind_param('sii', $proxima, $calibration_id, $empresaId);
                $upCal->execute();
                $upCal->close();
            }
        }

        $siguienteCalibracion = $proxima ?? $dueDateParam ?? null;

        if ($certificateNotificationData !== null) {
            $certificateNotificationData['fecha_proxima'] = $siguienteCalibracion ?? $dueDateParam ?? null;
        }

        $estadoAnterior = $instrumentoActual['estado'] ?? '';
        $departamentoInstrumento = $instrumentoActual['departamento_id'] ?? null;
        if ($departamentoInstrumento !== null) {
            $departamentoInstrumento = (int) $departamentoInstrumento;
        }
        $estadoDerivado = derivarEstadoInstrumento([
            'estadoActual'   => $estadoAnterior,
            'fechaBaja'      => $instrumentoActual['fecha_baja'] ?? '',
            'resultado'      => $result,
            'observaciones'  => $notesParam,
            'departamentoId' => $departamentoInstrumento,
        ]);

        $upInstr = $conn->prepare('UPDATE instrumentos SET proxima_calibracion = ?, programado = 0, estado = ? WHERE id = ? AND empresa_id = ?');
        if ($upInstr) {
            $upInstr->bind_param('ssii', $siguienteCalibracion, $estadoDerivado, $instrumento_id, $empresaId);
            $upInstr->execute();
            $upInstr->close();
        }

        if ($estadoDerivado !== $estadoAnterior) {
            $hist = $conn->prepare('INSERT INTO historial_tipos_instrumento (instrumento_id, estado, empresa_id) VALUES (?, ?, ?)');
            if ($hist) {
                $hist->bind_param('isi', $instrumento_id, $estadoDerivado, $empresaId);
                $hist->execute();
                $hist->close();
            }
        }
      
        try {
            calibration_nonconformities_register(
                $conn,
                $calibration_id,
                $instrumento_id,
                $empresaId,
                $result,
                $notesParam,
                [$motivoReprogramacion, $logisticsComments],
                $calibration_date
            );
        } catch (Throwable $e) {
            error_log('[nc-calibraciones][ERROR] No se pudo procesar la no conformidad: ' . $e->getMessage());
        }

        if ($certificateNotificationData !== null) {
            $notificationLogger = static function (string $level, string $message): void {
                error_log('[certificate-alerts][' . strtoupper($level) . '] ' . $message);
            };

            try {
                tenant_notifications_send_certificate_notification($conn, $certificateNotificationData, $notificationLogger);
            } catch (Throwable $e) {
                error_log('[certificate-alerts][ERROR] No se pudo enviar la notificación de certificado: ' . $e->getMessage());
            }
        }
        echo json_encode([
            'success' => true,
            'message' => 'Calibración guardada correctamente.',
            'data' => [
                'calibracion_id' => $calibration_id,
                'measurement_id' => $measurementId,
                'calibrador_id' => $calibradorSeleccionado,
                'origen_datos' => $origenDatos,
            ],
        ]);
    } else {
        echo json_encode(['success'=>false,'message'=>'Error al guardar calibración: '.$stmt->error]);
    }
    $stmt->close();
} else {
    echo json_encode(['success'=>false,'message'=>'Método no permitido.']);
}
?>