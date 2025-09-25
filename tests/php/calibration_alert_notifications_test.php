<?php

declare(strict_types=1);

try {
    require_once __DIR__ . '/../../app/Core/db.php';
} catch (Throwable $connectionError) {
    fwrite(STDOUT, "Prueba de alertas de calibración omitida: " . $connectionError->getMessage() . PHP_EOL);
    exit(0);
}
require_once __DIR__ . '/../../app/Core/helpers/calibration_alerts.php';
require_once __DIR__ . '/../../app/Core/helpers/company.php';

if (session_status() === PHP_SESSION_NONE) {
    session_start();
}

function fail_and_exit(string $message, mysqli $conn, array $cleanupIds): void {
    cleanup_calibration_alerts_test($conn, $cleanupIds);
    fwrite(STDERR, $message . PHP_EOL);
    $conn->close();
    exit(1);
}

function cleanup_calibration_alerts_test(mysqli $conn, array $ids): void {
    if (!empty($ids['alerts'])) {
        $placeholders = implode(',', array_fill(0, count($ids['alerts']), '?'));
        $stmt = $conn->prepare("DELETE FROM calibration_alert_notifications WHERE id IN ($placeholders)");
        if ($stmt) {
            $types = str_repeat('i', count($ids['alerts']));
            $stmt->bind_param($types, ...$ids['alerts']);
            $stmt->execute();
            $stmt->close();
        }
    }
    if (!empty($ids['instrumento'])) {
        $stmt = $conn->prepare('DELETE FROM instrumentos WHERE id = ?');
        if ($stmt) {
            $stmt->bind_param('i', $ids['instrumento']);
            $stmt->execute();
            $stmt->close();
        }
    }
    if (!empty($ids['usuario'])) {
        $stmt = $conn->prepare('DELETE FROM usuarios WHERE id = ?');
        if ($stmt) {
            $stmt->bind_param('i', $ids['usuario']);
            $stmt->execute();
            $stmt->close();
        }
    }
    if (!empty($ids['empresa'])) {
        $stmt = $conn->prepare('DELETE FROM empresas WHERE id = ?');
        if ($stmt) {
            $stmt->bind_param('i', $ids['empresa']);
            $stmt->execute();
            $stmt->close();
        }
    }
    if (!empty($ids['audit_min_id']) && !empty($ids['usuario'])) {
        $stmt = $conn->prepare('DELETE FROM audit_trail WHERE usuario_id = ? AND id > ?');
        if ($stmt) {
            $stmt->bind_param('ii', $ids['usuario'], $ids['audit_min_id']);
            $stmt->execute();
            $stmt->close();
        }
    }
}

$cleanup = [
    'alerts' => [],
    'instrumento' => null,
    'usuario' => null,
    'empresa' => null,
    'audit_min_id' => null,
];

try {
    $empresaNombre = 'Empresa Alertas ' . bin2hex(random_bytes(3));
    $empresaStmt = $conn->prepare('INSERT INTO empresas (nombre, contacto, direccion, telefono, email) VALUES (?, NULL, NULL, NULL, NULL)');
    if (!$empresaStmt) {
        throw new RuntimeException('No se pudo preparar la inserción de la empresa.');
    }
    $empresaStmt->bind_param('s', $empresaNombre);
    if (!$empresaStmt->execute()) {
        throw new RuntimeException('No se pudo registrar la empresa de prueba.');
    }
    $empresaStmt->close();
    $empresaId = (int) $conn->insert_id;
    $cleanup['empresa'] = $empresaId;

    $roleId = 1;
    if ($result = $conn->query('SELECT id FROM roles ORDER BY id ASC LIMIT 1')) {
        if ($row = $result->fetch_assoc()) {
            $roleId = (int) $row['id'];
        }
        $result->close();
    }

    $usuarioCorreo = 'alertas.' . bin2hex(random_bytes(3)) . '@example.test';
    $usuarioNombre = 'Prueba';
    $usuarioApellidos = 'Alertas';
    $usuarioStmt = $conn->prepare('INSERT INTO usuarios (usuario, correo, nombre, apellidos, puesto, telefono, contrasena, empresa_id, role_id, activo, sso) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 1, 0)');
    if (!$usuarioStmt) {
        throw new RuntimeException('No se pudo preparar la inserción del usuario.');
    }
    $usuarioLogin = 'alertas_' . bin2hex(random_bytes(3));
    $hashedPassword = password_hash('Secreto123', PASSWORD_BCRYPT);
    $puesto = 'Analista de Calidad';
    $usuarioStmt->bind_param('ssssssii', $usuarioLogin, $usuarioCorreo, $usuarioNombre, $usuarioApellidos, $puesto, $hashedPassword, $empresaId, $roleId);
    if (!$usuarioStmt->execute()) {
        throw new RuntimeException('No se pudo registrar el usuario de prueba.');
    }
    $usuarioStmt->close();
    $usuarioId = (int) $conn->insert_id;
    $cleanup['usuario'] = $usuarioId;

    $_SESSION['usuario_id'] = $usuarioId;
    $_SESSION['empresa_id'] = $empresaId;
    $_SESSION['nombre'] = $usuarioNombre;
    $_SESSION['apellidos'] = $usuarioApellidos;
    $_SESSION['usuario'] = $usuarioCorreo;

    $serie = 'ALERT-' . bin2hex(random_bytes(3));
    $codigo = 'COD-' . bin2hex(random_bytes(2));
    $instrumentStmt = $conn->prepare('INSERT INTO instrumentos (catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id, ubicacion, fecha_alta, proxima_calibracion, estado, programado, empresa_id) VALUES (NULL, NULL, NULL, ?, ?, NULL, "Área de prueba", CURDATE(), DATE_ADD(CURDATE(), INTERVAL 90 DAY), "activo", 0, ?)');
    if (!$instrumentStmt) {
        throw new RuntimeException('No se pudo preparar el instrumento de prueba.');
    }
    $instrumentStmt->bind_param('ssi', $serie, $codigo, $empresaId);
    if (!$instrumentStmt->execute()) {
        throw new RuntimeException('No se pudo insertar el instrumento de prueba.');
    }
    $instrumentStmt->close();
    $instrumentoId = (int) $conn->insert_id;
    $cleanup['instrumento'] = $instrumentoId;

    calibration_alerts_ensure_log_table($conn);

    $dueUpcoming = (new DateTimeImmutable('+5 days'))->format('Y-m-d');
    $dueOverdue = (new DateTimeImmutable('-3 days'))->format('Y-m-d');

    $insertAlert = $conn->prepare('INSERT INTO calibration_alert_notifications (instrumento_id, empresa_id, due_date, alert_type) VALUES (?, ?, ?, ?)');
    if (!$insertAlert) {
        throw new RuntimeException('No se pudo preparar la inserción de alertas.');
    }
    $alertTypeUpcoming = 'upcoming';
    $insertAlert->bind_param('iiss', $instrumentoId, $empresaId, $dueUpcoming, $alertTypeUpcoming);
    if (!$insertAlert->execute()) {
        throw new RuntimeException('No se pudo insertar la alerta próxima.');
    }
    $alertUpcomingId = (int) $conn->insert_id;
    $cleanup['alerts'][] = $alertUpcomingId;

    $alertTypeOverdue = 'overdue';
    $insertAlert->bind_param('iiss', $instrumentoId, $empresaId, $dueOverdue, $alertTypeOverdue);
    if (!$insertAlert->execute()) {
        throw new RuntimeException('No se pudo insertar la alerta vencida.');
    }
    $alertOverdueId = (int) $conn->insert_id;
    $cleanup['alerts'][] = $alertOverdueId;
    $insertAlert->close();

    $auditResult = $conn->query('SELECT MAX(id) AS max_id FROM audit_trail');
    $auditRow = $auditResult ? $auditResult->fetch_assoc() : null;
    if ($auditResult instanceof mysqli_result) {
        $auditResult->close();
    }
    $auditBaseline = (int) ($auditRow['max_id'] ?? 0);
    $cleanup['audit_min_id'] = $auditBaseline;

    $allAlerts = calibration_alerts_list_notifications($conn, $empresaId, []);
    if (count($allAlerts) !== 2) {
        throw new RuntimeException('Se esperaban 2 alertas registradas.');
    }

    $upcomingAlerts = calibration_alerts_list_notifications($conn, $empresaId, ['alert_type' => 'upcoming']);
    if (count($upcomingAlerts) !== 1 || (int) $upcomingAlerts[0]['id'] !== $alertUpcomingId) {
        throw new RuntimeException('El filtro de alertas próximas no devolvió el resultado esperado.');
    }

    $overdueAlerts = calibration_alerts_list_notifications($conn, $empresaId, ['alert_type' => 'overdue']);
    if (count($overdueAlerts) !== 1 || (int) $overdueAlerts[0]['id'] !== $alertOverdueId) {
        throw new RuntimeException('El filtro de alertas vencidas no devolvió el resultado esperado.');
    }

    $ack = calibration_alerts_attend_notification(
        $conn,
        $alertUpcomingId,
        $empresaId,
        'acknowledge',
        $usuarioId,
        $usuarioNombre . ' ' . $usuarioApellidos,
        $usuarioCorreo,
        ['notes' => 'Reconocida en prueba']
    );
    if (($ack['attention_status'] ?? '') !== 'reconocida') {
        throw new RuntimeException('La alerta no se marcó como reconocida correctamente.');
    }

    $ackFilter = calibration_alerts_list_notifications($conn, $empresaId, ['attention_status' => 'reconocida']);
    if (count($ackFilter) !== 1 || (int) $ackFilter[0]['id'] !== $alertUpcomingId) {
        throw new RuntimeException('La consulta de alertas reconocidas no es consistente.');
    }

    $closed = calibration_alerts_attend_notification(
        $conn,
        $alertOverdueId,
        $empresaId,
        'close',
        $usuarioId,
        $usuarioNombre . ' ' . $usuarioApellidos,
        $usuarioCorreo,
        ['notes' => 'Cierre de prueba']
    );
    if (($closed['attention_status'] ?? '') !== 'cerrada') {
        throw new RuntimeException('La alerta vencida no se marcó como cerrada.');
    }

    $closedFilter = calibration_alerts_list_notifications($conn, $empresaId, ['attention_status' => 'cerrada']);
    if (count($closedFilter) !== 1 || (int) $closedFilter[0]['id'] !== $alertOverdueId) {
        throw new RuntimeException('La consulta de alertas cerradas no es consistente.');
    }

    $auditCheckStmt = $conn->prepare('SELECT COUNT(*) AS total FROM audit_trail WHERE usuario_id = ? AND id > ? AND seccion = "Alertas de Calibración"');
    if (!$auditCheckStmt) {
        throw new RuntimeException('No se pudo preparar la verificación de auditoría.');
    }
    $auditCheckStmt->bind_param('ii', $usuarioId, $auditBaseline);
    if (!$auditCheckStmt->execute()) {
        throw new RuntimeException('No se pudo verificar el registro de auditoría.');
    }
    $auditResult = $auditCheckStmt->get_result();
    $auditCount = $auditResult && ($row = $auditResult->fetch_assoc()) ? (int) ($row['total'] ?? 0) : 0;
    if ($auditResult instanceof mysqli_result) {
        $auditResult->close();
    }
    $auditCheckStmt->close();

    if ($auditCount < 2) {
        throw new RuntimeException('Los cambios de estado no quedaron registrados en la auditoría.');
    }

    echo "✓ Prueba de alertas de calibración completada" . PHP_EOL;
    cleanup_calibration_alerts_test($conn, $cleanup);
    $conn->close();
    exit(0);
} catch (Throwable $e) {
    fail_and_exit('Error en la prueba de alertas de calibración: ' . $e->getMessage(), $conn, $cleanup);
}
