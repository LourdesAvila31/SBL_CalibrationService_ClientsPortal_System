<?php
// Conectar a la base de datos usando ruta relativa
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 2) . '/Internal/Auditoria/audit.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_schedule.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_references.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_uncertainty.php';

session_start();

if (!check_permission('calibraciones_actualizar')) {
    http_response_code(403);
    exit('Acceso denegado');
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    exit('Empresa no especificada');
}

$periodosValidos = ['P1', 'P2', 'EXTRA'];
$validTypes = ['Interna', 'Externa'];
$cal = null;

if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    $id = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
    if ($id) {
        $stmt = $conn->prepare('SELECT c.id, c.instrumento_id, c.usuario_id, c.fecha_calibracion, c.fecha_proxima, c.resultado, c.periodo, c.tipo, c.duracion_horas, c.costo_total, c.estado_ejecucion, c.motivo_reprogramacion, c.fecha_reprogramada, c.dias_atraso, c.observaciones, c.u_value, c.u_method, c.u_k, c.u_coverage, cp.plan_id FROM calibraciones c LEFT JOIN calibraciones_planes cp ON cp.calibracion_id = c.id WHERE c.id = ? AND c.empresa_id = ?');
        $stmt->bind_param('ii', $id, $empresaId);
        $stmt->execute();
        $resultado = $stmt->get_result();
        $cal = $resultado->fetch_assoc();
        $stmt->close();

        if ($cal) {
            $certStmt = $conn->prepare('SELECT archivo FROM certificados WHERE calibracion_id = ? ORDER BY fecha_subida DESC, id DESC LIMIT 1');
            $certStmt->bind_param('i', $id);
            $certStmt->execute();
            $certStmt->bind_result($archivoCert);
            if ($certStmt->fetch()) {
                $cal['certificado'] = $archivoCert;
            }
            $certStmt->close();
            $refMap = calibration_references_fetch($conn, [$id]);
            $cal['referencias'] = $refMap[$id] ?? [];

        }
    }
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $respondJson = (($_POST['as_json'] ?? '') === '1');
    $respond = function (bool $success, string $message = '') use ($respondJson) {
        if ($respondJson) {
            header('Content-Type: application/json');
            $payload = ['success' => $success];
            if ($message !== '') {
                $payload['message'] = $message;
            }
            echo json_encode($payload);
            exit;
        }

        if (!$success) {
            http_response_code(400);
            exit($message);
        }

        echo $message;
        exit;
    };

    $id             = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
    $instrumento_id = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT);
    $calibration_date = trim($_POST['calibration_date'] ?? '');
    $due_date         = trim($_POST['due_date'] ?? '');
    $result           = trim($_POST['result'] ?? '');
    $notes            = trim($_POST['notes'] ?? '');
    $periodo          = strtoupper(trim($_POST['periodo'] ?? ''));
    $certificate      = trim($_POST['certificate'] ?? '');
    $typeRaw          = trim($_POST['type'] ?? '');
    $durationRaw      = $_POST['duration_hours'] ?? '';
    $costRaw          = $_POST['cost_total'] ?? '';
    $planId           = filter_input(INPUT_POST, 'plan_id', FILTER_VALIDATE_INT);
    $estadoRaw        = trim($_POST['estado_ejecucion'] ?? '');
    $tecnicoIdRaw     = filter_input(INPUT_POST, 'tecnico_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
    if ($tecnicoIdRaw === false) {
        $respond(false, 'Identificador de técnico inválido.');
    }
    $motivoReprogramacion = trim($_POST['motivo_reprogramacion'] ?? '');
    $fechaReprogramadaRaw = trim($_POST['fecha_reprogramada'] ?? '');
    $referencesProvided = array_key_exists('references_payload', $_POST);
    $references = $referencesProvided ? calibration_references_parse_payload($_POST['references_payload']) : null;

    if ($planId === false) {
        $respond(false, 'Identificador de plan inválido.');
    }

    $estadoEjecucion = calibration_normalize_status($estadoRaw);
    if ($estadoRaw !== '' && strcasecmp($estadoRaw, $estadoEjecucion) !== 0) {
        $respond(false, 'Estado de ejecución inválido.');
    }

    $fechaReprogramada = $fechaReprogramadaRaw !== '' ? $fechaReprogramadaRaw : null;

    if (!$id || !$instrumento_id || $calibration_date === '' || $result === '') {
        $respond(false, 'Datos incompletos.');
    }

    $usuarioId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;
    if ($usuarioId === null || $usuarioId <= 0) {
        $respond(false, 'Sesión inválida: usuario no identificado.');
    }

    $currentStmt = $conn->prepare('SELECT estado, resultado_preliminar, resultado, motivo_rechazo, liberado_por, fecha_liberacion, aprobado_por, fecha_aprobacion FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$currentStmt) {
        $respond(false, 'No se pudo obtener la calibración.');
    }
    $currentStmt->bind_param('ii', $id, $empresaId);
    $currentStmt->execute();
    $currentResult = $currentStmt->get_result();
    $currentRow = $currentResult ? $currentResult->fetch_assoc() : null;
    $currentStmt->close();

    if (!$currentRow) {
        $respond(false, 'Calibración no encontrada.');
    }

    $estadoActual = $currentRow['estado'] ?? 'Pendiente';
    $puedeAprobar = check_permission('calibraciones_aprobar');
    if ($estadoActual === 'Aprobado' && !$puedeAprobar) {
        $respond(false, 'No cuentas con permisos para editar una calibración aprobada.');
    }

    if (!in_array($periodo, $periodosValidos, true)) {
        $respond(false, 'Periodo inválido.');
    }

    if (!calibration_create_date($calibration_date)) {
        $respond(false, 'Fecha de calibración inválida.');
    }

    if ($due_date !== '' && !calibration_create_date($due_date)) {
        $respond(false, 'Fecha próxima inválida.');
    }

    if ($fechaReprogramada !== null && !calibration_create_date($fechaReprogramada)) {
        $respond(false, 'Fecha reprogramada inválida.');
    }

    $type = $typeRaw !== '' ? $typeRaw : 'Interna';
    if (!in_array($type, $validTypes, true)) {
        $respond(false, 'Tipo de calibración inválido.');
    }
    $type = $type === 'Externa' ? 'Externa' : 'Interna';

    $durationHours = null;
    if ($durationRaw !== '' && $durationRaw !== null) {
        $durationHours = filter_var($durationRaw, FILTER_VALIDATE_FLOAT);
        if ($durationHours === false) {
            $respond(false, 'Duración inválida.');
        }
        if ($durationHours < 0) {
            $respond(false, 'La duración no puede ser negativa.');
        }
    }

    $costTotal = null;
    if ($costRaw !== '' && $costRaw !== null) {
        $costTotal = filter_var($costRaw, FILTER_VALIDATE_FLOAT);
        if ($costTotal === false) {
            $respond(false, 'Costo inválido.');
        }
        if ($costTotal < 0) {
            $respond(false, 'El costo no puede ser negativo.');
        }
    }

    $instrumentoCatalogoId = null;
    $check = $conn->prepare('SELECT catalogo_id FROM instrumentos WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$check) {
        $respond(false, 'No se pudo verificar el instrumento.');
    }
    $check->bind_param('ii', $instrumento_id, $empresaId);
    $check->execute();
    $checkResult = $check->get_result();
    if (!$checkResult || $checkResult->num_rows === 0) {
        $check->close();
        $respond(false, 'Instrumento no encontrado');
    }
    $instrumentoRow = $checkResult->fetch_assoc();
    if ($instrumentoRow && isset($instrumentoRow['catalogo_id'])) {
        $instrumentoCatalogoId = (int) $instrumentoRow['catalogo_id'];
        if ($instrumentoCatalogoId === 0) {
            $instrumentoCatalogoId = null;
        }
    }
    $check->close();

    $instrumentContext = [];
    try {
        $context = tenant_notifications_fetch_instrument_context($conn, $instrumento_id, $empresaId);
        if (is_array($context)) {
            $instrumentContext = $context;
        }
    } catch (Throwable $e) {
        error_log('[nc-calibraciones][WARNING] No se pudo obtener el contexto del instrumento: ' . $e->getMessage());
    }
    $planAnteriorId = null;
    $certificateNotificationData = null;
    $relLookup = $conn->prepare('SELECT plan_id FROM calibraciones_planes WHERE calibracion_id = ? LIMIT 1');
    if ($relLookup) {
        $relLookup->bind_param('i', $id);
        if ($relLookup->execute()) {
            $relLookup->bind_result($planAnteriorIdTemp);
            if ($relLookup->fetch()) {
                $planAnteriorId = $planAnteriorIdTemp !== null ? (int) $planAnteriorIdTemp : null;
            }
        }
        $relLookup->close();
    }

    $fechaProgramada = null;
    $planConsultaId = $planId ?? $planAnteriorId;
    if ($planConsultaId !== null) {
        $planStmt = $conn->prepare('SELECT id, fecha_programada, instrumento_id FROM planes WHERE id = ? AND empresa_id = ? LIMIT 1');
        if ($planStmt) {
            $planStmt->bind_param('ii', $planConsultaId, $empresaId);
            $planStmt->execute();
            $planRes = $planStmt->get_result();
            $planRow = $planRes ? $planRes->fetch_assoc() : null;
            $planStmt->close();
            if ($planRow) {
                if ((int) ($planRow['instrumento_id'] ?? 0) !== (int) $instrumento_id) {
                    if ($planId !== null) {
                        $respond(false, 'El plan seleccionado no corresponde al instrumento.');
                    }
                } else {
                    $fechaProgramada = $planRow['fecha_programada'] ?? null;
                    if ($planId === null && $planAnteriorId === null) {
                        $planAnteriorId = (int) $planRow['id'];
                    }
                }
            } elseif ($planId !== null) {
                $respond(false, 'Plan de calibración no encontrado.');
            }
        } else {
            $respond(false, 'No se pudo verificar el plan asociado.');
        }
    } elseif ($planId !== null) {
        $respond(false, 'Plan de calibración no encontrado.');
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
                        }
                    }
                }
            }
            $autoStmt->close();
        }
    }

    if ($estadoEjecucion === 'Reprogramada' && $fechaReprogramada === null) {
        $respond(false, 'Debes registrar la nueva fecha cuando la calibración se reprograma.');
    }

    if ($fechaReprogramada !== null && $fechaProgramada !== null) {
        $planDate = calibration_create_date($fechaProgramada);
        $reprogramadaDate = calibration_create_date($fechaReprogramada);
        if ($planDate && $reprogramadaDate) {
            $diffPlan = (int) $planDate->diff($reprogramadaDate)->format('%r%a');
            if ($diffPlan < 0) {
                $respond(false, 'La fecha reprogramada debe ser posterior o igual a la programada.');
            }
        }
    }

    $diasAtraso = calibration_compute_delay($fechaProgramada, $calibration_date, $fechaReprogramada);
    $justificacionRequerida = calibration_requires_justification($estadoEjecucion, $diasAtraso);
    if ($justificacionRequerida && $motivoReprogramacion === '') {
        $respond(false, 'Es necesario capturar la justificación del atraso o reprogramación.');
    }

    $dueDateParam = $due_date !== '' ? $due_date : null;
    $durationParam = $durationHours !== null ? number_format($durationHours, 1, '.', '') : null;
    $costParam = $costTotal !== null ? number_format($costTotal, 2, '.', '') : null;

    $uncertaintyValueRaw = $_POST['u_value'] ?? '';
    $uncertaintyMethodRaw = $_POST['u_method'] ?? '';
    $uncertaintyKRaw = $_POST['u_k'] ?? '';
    $uncertaintyCoverageRaw = $_POST['u_coverage'] ?? '';

    $uncertaintyValue = null;
    if ($uncertaintyValueRaw !== '' && $uncertaintyValueRaw !== null) {
        $uncertaintyValue = filter_var($uncertaintyValueRaw, FILTER_VALIDATE_FLOAT);
        if ($uncertaintyValue === false || $uncertaintyValue < 0) {
            $respond(false, 'La incertidumbre debe ser un número igual o mayor a cero.');
        }
    }

    $uncertaintyK = null;
    if ($uncertaintyKRaw !== '' && $uncertaintyKRaw !== null) {
        $uncertaintyK = filter_var($uncertaintyKRaw, FILTER_VALIDATE_FLOAT);
        if ($uncertaintyK === false || $uncertaintyK <= 0) {
            $respond(false, 'El factor de cobertura k debe ser mayor a cero.');
        }
    }

    $uncertaintyMethod = is_string($uncertaintyMethodRaw) ? trim($uncertaintyMethodRaw) : '';
    $uncertaintyCoverage = is_string($uncertaintyCoverageRaw) ? trim($uncertaintyCoverageRaw) : '';

    $uncertaintyRequired = calibration_requires_uncertainty($estadoEjecucion, $result);

    if ($uncertaintyRequired) {
        if ($uncertaintyValue === null || $uncertaintyMethod === '' || $uncertaintyK === null || $uncertaintyCoverage === '') {
            $respond(false, 'Antes de cerrar la calibración registra la incertidumbre, método, k y cobertura.');
        }
    }

    $uValueParam = $uncertaintyValue !== null ? number_format($uncertaintyValue, 6, '.', '') : null;
    $uMethodParam = $uncertaintyMethod !== '' ? $uncertaintyMethod : null;
    $uKParam = $uncertaintyK !== null ? number_format($uncertaintyK, 4, '.', '') : null;
    $uCoverageParam = $uncertaintyCoverage !== '' ? $uncertaintyCoverage : null;
    $motivoParam = $motivoReprogramacion !== '' ? $motivoReprogramacion : null;
    $fechaReprogramadaParam = $fechaReprogramada;
    $diasAtrasoDb = $diasAtraso !== null ? (int) $diasAtraso : 0;

    $techAsignado = $tecnicoIdRaw;
    if ($techAsignado === null) {
        $techLookup = $conn->prepare('SELECT usuario_id FROM calibraciones WHERE id = ? AND empresa_id = ? LIMIT 1');
        if ($techLookup) {
            $techLookup->bind_param('ii', $id, $empresaId);
            if ($techLookup->execute()) {
                $techLookup->bind_result($techPrevio);
                if ($techLookup->fetch()) {
                    $techAsignado = $techPrevio !== null ? (int) $techPrevio : null;
                }
            }
            $techLookup->close();
        }
    }

    $usuarioSesionId = isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : null;
    if ($techAsignado === null) {
        $techAsignado = $usuarioSesionId;
    }

    if (!$techAsignado) {
        $respond(false, 'Debes seleccionar al técnico responsable.');
    }

    $techAsignado = (int) $techAsignado;

    $stmtUsuario = $conn->prepare('SELECT id FROM usuarios WHERE id = ? AND empresa_id = ? LIMIT 1');
    if (!$stmtUsuario) {
        $respond(false, 'No se pudo validar al técnico.');
    }
    $stmtUsuario->bind_param('ii', $techAsignado, $empresaId);
    $stmtUsuario->execute();
    $stmtUsuario->store_result();
    if ($stmtUsuario->num_rows === 0) {
        $stmtUsuario->close();
        $respond(false, 'El técnico seleccionado no pertenece a la empresa.');
    }
    $stmtUsuario->close();

    if (!user_has_competency($conn, (int) $techAsignado, $empresaId, $instrumentoCatalogoId)) {
        $respond(false, 'El técnico seleccionado no cuenta con evidencia vigente para este tipo de instrumento.');
    }

    $stmt = $conn->prepare('UPDATE calibraciones SET instrumento_id = ?, usuario_id = ?, fecha_calibracion = ?, fecha_proxima = ?, resultado = ?, periodo = ?, tipo = ?, duracion_horas = ?, costo_total = ?, estado_ejecucion = ?, motivo_reprogramacion = ?, fecha_reprogramada = ?, dias_atraso = ? WHERE id = ? AND empresa_id = ?');
    if (!$stmt) {
        $respond(false, 'No se pudo preparar la actualización.');
    }

    $patronIdParam = $patronId;
    $patronCertificadoParam = $patronCertificadoInput !== '' ? $patronCertificadoInput : null;

    $stmt->bind_param(
        'iissssssssssiii',
        $instrumento_id,
        $techAsignado,
        $calibration_date,
        $dueDateParam,
        $resultadoPreliminar,
        $resultadoLiberado,
        $periodo,
        $type,
        $durationParam,
        $costParam,
        $estadoEjecucion,
        $motivoParam,
        $fechaReprogramadaParam,
        $diasAtrasoDb,
        $patronIdParam,
        $patronCertificadoParam,
        $id,
        $empresaId
    );

    if ($stmt->execute()) {
        $stmt->close();

        $updateUncertainty = $conn->prepare('UPDATE calibraciones SET u_value = ?, u_method = ?, u_k = ?, u_coverage = ? WHERE id = ? AND empresa_id = ?');
        if ($updateUncertainty) {
            $updateUncertainty->bind_param('ssssii', $uValueParam, $uMethodParam, $uKParam, $uCoverageParam, $id, $empresaId);
            $updateUncertainty->execute();
            $updateUncertainty->close();
        }

        if ($references !== null) {
            calibration_references_sync($conn, $id, $empresaId, $references);
        }

        if ($certificate !== '') {
            $existingId = null;
            $certLookup = $conn->prepare('SELECT id FROM certificados WHERE calibracion_id = ? ORDER BY fecha_subida DESC, id DESC LIMIT 1');
            if ($certLookup) {
                $certLookup->bind_param('i', $id);
                $certLookup->execute();
                $certLookup->bind_result($existingId);
                $hasCertificate = $certLookup->fetch();
                $certLookup->close();

                if ($hasCertificate && $existingId) {
                    $updateCert = $conn->prepare('UPDATE certificados SET archivo = ? WHERE id = ?');
                    if ($updateCert) {
                        $updateCert->bind_param('si', $certificate, $existingId);
                        $updateCert->execute();
                        $updateCert->close();
                    }
                } else {
                    $tipoCert = 'calibracion';
                    $insertCert = $conn->prepare('INSERT INTO certificados (calibracion_id, archivo, tipo) VALUES (?, ?, ?)');
                    if ($insertCert) {
                        $insertCert->bind_param('iss', $id, $certificate, $tipoCert);
                        $insertCert->execute();
                        $insertCert->close();
                    }
                }
            }
        }

        $deleteRel = $conn->prepare('DELETE FROM calibraciones_planes WHERE calibracion_id = ?');
        if ($deleteRel) {
            $deleteRel->bind_param('i', $id);
            $deleteRel->execute();
            $deleteRel->close();
        }

        if ($planAnteriorId !== null && ($planId === null || $planId !== $planAnteriorId)) {
            $resetPlan = $conn->prepare('UPDATE planes SET estado = ? WHERE id = ? AND empresa_id = ?');
            if ($resetPlan) {
                $pendiente = 'Pendiente';
                $resetPlan->bind_param('sii', $pendiente, $planAnteriorId, $empresaId);
                $resetPlan->execute();
                $resetPlan->close();
            }
        }

        if ($planId !== null) {
            $insertRel = $conn->prepare('INSERT INTO calibraciones_planes (calibracion_id, plan_id) VALUES (?, ?)');
            if ($insertRel) {
                $insertRel->bind_param('ii', $id, $planId);
                $insertRel->execute();
                $insertRel->close();
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

            $certificateNotificationData = [
                'instrumento_id' => $instrumento_id,
                'empresa_id' => $empresaId,
                'empresa_nombre' => $instrumentContext['empresa_nombre'] ?? ('Empresa #' . $empresaId),
                'instrumento_nombre' => $instrumentContext['instrumento_nombre'] ?? ('Instrumento #' . $instrumento_id),
                'instrumento_codigo' => $instrumentContext['instrumento_codigo'] ?? '',
                'certificate_path' => $certificate,
                'fecha_calibracion' => $calibration_date,
            ];
        }

        try {
            calibration_nonconformities_register(
                $conn,
                $id,
                $instrumento_id,
                $empresaId,
                $result,
                $notes !== '' ? $notes : null,
                [$motivoReprogramacion],
                $calibration_date
            );
        } catch (Throwable $e) {
            error_log('[nc-calibraciones][ERROR] No se pudo procesar la no conformidad: ' . $e->getMessage());
        }

        if ($certificateNotificationData !== null) {
            $certificateNotificationData['fecha_proxima'] = $dueDateParam ?? null;
        }

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

        log_activity($nombreOperador, "Edición de calibración $id", 'calibraciones', $correoOperador);

        calibration_nonconformity_handle($conn, [
            'calibracion_id' => $id,
            'instrumento_id' => $instrumento_id,
            'empresa_id' => $empresaId,
            'resultado' => $result,
            'notes' => $notesField !== '' ? $notesField : null,
        ]);

        $mensajeOk = "Calibración actualizada. <a href='list_calibrations.html'>Volver</a>";
        $respond(true, $respondJson ? 'Calibración actualizada.' : $mensajeOk);
    }

    $stmt->close();
    $respond(false, 'Error: ' . $conn->error);
}

?>

<?php if ($cal): ?>
<?php
$referencesJson = json_encode($cal['referencias'] ?? [], JSON_UNESCAPED_UNICODE);
if ($referencesJson === false) {
    $referencesJson = '[]';
}
?>
<form method="POST">
    <input type="hidden" name="id" value="<?php echo (int) $cal['id']; ?>">
    <label>ID Instrumento:</label>
    <input type="number" name="instrumento_id" value="<?php echo (int) $cal['instrumento_id']; ?>" required><br>
    <label>Fecha de calibración:</label>
    <input type="date" name="calibration_date" value="<?php echo htmlspecialchars($cal['fecha_calibracion'] ?? '', ENT_QUOTES, 'UTF-8'); ?>" required><br>
    <label>Fecha de vencimiento:</label>
    <input type="date" name="due_date" value="<?php echo htmlspecialchars($cal['fecha_proxima'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Resultado:</label>

    <?php $resultadoSeleccionado = $cal['resultado_form'] ?? ''; ?>
    <select name="result" required>
        <option value="">Seleccione resultado</option>
        <option value="Conforme" <?php echo ($resultadoSeleccionado === 'Conforme') ? 'selected' : ''; ?>>Conforme</option>
        <option value="No conforme" <?php echo ($resultadoSeleccionado === 'No conforme') ? 'selected' : ''; ?>>No conforme</option>
        <option value="En proceso" <?php echo ($resultadoSeleccionado === 'En proceso') ? 'selected' : ''; ?>>En proceso</option>
    </select><br>
    <label>Modalidad:</label>
    <select name="type" required>
        <option value="Interna" <?php echo (($cal['tipo'] ?? 'Interna') === 'Interna') ? 'selected' : ''; ?>>Interna</option>
        <option value="Externa" <?php echo (($cal['tipo'] ?? '') === 'Externa') ? 'selected' : ''; ?>>Externa</option>
    </select>
    <small class="text-muted-sm">Seleccione si la calibración fue interna o externa.</small><br>
    <label>Duración (horas):</label>
    <input type="number" name="duration_hours" min="0" step="0.1" value="<?php echo htmlspecialchars((isset($cal['duracion_horas']) && $cal['duracion_horas'] !== null && $cal['duracion_horas'] !== '') ? number_format((float)$cal['duracion_horas'], 1, '.', '') : '', ENT_QUOTES, 'UTF-8'); ?>">
    <small class="text-muted-sm">Tiempo total invertido en la calibración.</small><br>
    <label>Costo total (MXN):</label>
    <input type="number" name="cost_total" min="0" step="0.01" value="<?php echo htmlspecialchars((isset($cal['costo_total']) && $cal['costo_total'] !== null && $cal['costo_total'] !== '') ? number_format((float)$cal['costo_total'], 2, '.', '') : '', ENT_QUOTES, 'UTF-8'); ?>">
    <small class="text-muted-sm">Registrar el monto facturado o estimado.</small><br>
    <label>Incertidumbre expandida (U):</label>
    <input type="number" name="u_value" min="0" step="0.000001" value="<?php echo htmlspecialchars((isset($cal['u_value']) && $cal['u_value'] !== null && $cal['u_value'] !== '') ? number_format((float)$cal['u_value'], 6, '.', '') : '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Factor de cobertura (k):</label>
    <input type="number" name="u_k" min="0" step="0.0001" value="<?php echo htmlspecialchars((isset($cal['u_k']) && $cal['u_k'] !== null && $cal['u_k'] !== '') ? number_format((float)$cal['u_k'], 4, '.', '') : '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Cobertura / nivel de confianza:</label>
    <input type="text" name="u_coverage" maxlength="255" value="<?php echo htmlspecialchars($cal['u_coverage'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Método para estimar la incertidumbre:</label>
    <input type="text" name="u_method" maxlength="255" value="<?php echo htmlspecialchars($cal['u_method'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Periodo:</label>
    <select name="periodo" required>
        <option value="">Seleccione periodo</option>
        <?php foreach ($periodosValidos as $periodoVal): ?>
            <option value="<?php echo $periodoVal; ?>" <?php echo (($cal['periodo'] ?? '') === $periodoVal) ? 'selected' : ''; ?>>
                <?php
                    switch ($periodoVal) {
                        case 'P1':
                            echo 'Periodo 1';
                            break;
                        case 'P2':
                            echo 'Periodo 2';
                            break;
                        default:
                            echo 'Extraordinario';
                            break;
                    }
                ?>
            </option>
        <?php endforeach; ?>
    </select><br>
    <label>ID de patrón:</label>
    <input type="number" name="patron_id" value="<?php echo isset($cal['patron_id']) ? (int) $cal['patron_id'] : ''; ?>"><br>
    <label>Certificado del patrón:</label>
    <input type="text" name="patron_certificado" value="<?php echo htmlspecialchars($cal['patron_certificado'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Certificado:</label>
    <input type="text" name="certificate" value="<?php echo htmlspecialchars($cal['certificado'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br><br>
    <label>Referencias vinculadas (JSON):</label>
    <textarea name="references_payload" rows="3" cols="50"><?php echo htmlspecialchars($referencesJson, ENT_QUOTES, 'UTF-8'); ?></textarea>
    <small class="text-muted-sm">Este campo acepta un arreglo JSON con objetos que contengan "type", "label", "link" y/o "reference_id". Usa la interfaz principal para gestionar referencias de forma visual.</small><br><br>
    <button type="submit">Actualizar</button>
</form>
<?php endif; ?>
