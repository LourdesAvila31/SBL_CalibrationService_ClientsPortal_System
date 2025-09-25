<?php

require_once __DIR__ . '/mail_helper.php';
require_once __DIR__ . '/tenant_notifications.php';
require_once dirname(__DIR__) . '/services/AlertDispatcher.php';
require_once dirname(__DIR__, 2) . '/Modules/Internal/Auditoria/audit.php';

if (!function_exists('calibration_alerts_fetch_rows')) {
    /**
     * Ejecuta la consulta base de calibraciones aplicando una condición de fecha específica.
     *
     * @param mysqli                $conn            Conexión activa a la base de datos.
     * @param string                $dateCondition   Condición SQL para filtrar por fecha (sin incluir la cláusula WHERE).
     * @param array<int,int|string> $params          Parámetros a enlazar en la consulta.
     * @param string                $types           Tipos de los parámetros para bind_param.
     * @param int|null              $empresaId       Identificador de empresa para filtrar (opcional).
     * @param string                $orderByClause   Cláusula ORDER BY que se añadirá al final de la consulta.
     *
     * @return array<int,array<string,mixed>>
     */
    function calibration_alerts_fetch_rows(
        mysqli $conn,
        string $dateCondition,
        array $params,
        string $types,
        ?int $empresaId,
        string $orderByClause
    ): array {
        $sql = <<<SQL
SELECT
    c.instrumento_id,
    c.empresa_id,
    e.nombre AS empresa_nombre,
    COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) AS instrumento_nombre,
    i.codigo AS instrumento_codigo,
    DATE_FORMAT(c.fecha_calibracion, '%Y-%m-%d') AS fecha_calibracion,
    DATE_FORMAT(c.fecha_proxima, '%Y-%m-%d') AS fecha_proxima,
    c.resultado,
    DATEDIFF(c.fecha_proxima, CURDATE()) AS dias_restantes,
    c.usuario_id AS tecnico_id,
    CONCAT_WS(' ', tech.nombre, tech.apellidos) AS tecnico_nombre,
    log.estado AS log_estado,
    log.proveedor_externo AS log_proveedor_externo,
    log.transportista AS log_transportista,
    log.numero_guia AS log_numero_guia,
    log.orden_servicio AS log_orden_servicio,
    log.comentarios AS log_comentarios,
    DATE_FORMAT(log.fecha_envio, '%Y-%m-%d') AS log_fecha_envio,
    DATE_FORMAT(log.fecha_en_transito, '%Y-%m-%d') AS log_fecha_en_transito,
    DATE_FORMAT(log.fecha_recibido, '%Y-%m-%d') AS log_fecha_recibido,
    DATE_FORMAT(log.fecha_retorno, '%Y-%m-%d') AS log_fecha_retorno,
    c.patron_id,
    c.patron_certificado,
    pat.nombre AS patron_nombre,
    pat.codigo AS patron_codigo,
    pat.certificado_numero AS patron_certificado_numero,
    pat.certificado_archivo AS patron_certificado_archivo
FROM calibraciones c
JOIN instrumentos i ON i.id = c.instrumento_id AND i.empresa_id = c.empresa_id
JOIN empresas e ON e.id = c.empresa_id
LEFT JOIN catalogo_instrumentos ci ON ci.id = i.catalogo_id
LEFT JOIN logistica_calibraciones log ON log.calibracion_id = c.id
LEFT JOIN usuarios tech ON tech.id = c.usuario_id
LEFT JOIN patrones pat ON pat.id = c.patron_id
WHERE c.fecha_proxima IS NOT NULL
  AND ($dateCondition)
  AND c.id = (
        SELECT c2.id
        FROM calibraciones c2
        WHERE c2.instrumento_id = c.instrumento_id
          AND c2.empresa_id = c.empresa_id
          AND c2.fecha_proxima IS NOT NULL
        ORDER BY c2.fecha_proxima DESC, c2.fecha_calibracion DESC, c2.id DESC
        LIMIT 1
    )
  AND (i.estado IS NULL OR TRIM(LOWER(i.estado)) <> 'baja')
SQL;

        $queryParams = $params;
        $queryTypes = $types;

        if ($empresaId !== null) {
            $sql .= ' AND c.empresa_id = ?';
            $queryParams[] = (int) $empresaId;
            $queryTypes .= 'i';
        }

        $sql .= ' ' . trim($orderByClause);

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de calibraciones: ' . $conn->error);
        }

        if (!$stmt->bind_param($queryTypes, ...$queryParams)) {
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros de la consulta de calibraciones: ' . $stmt->error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron obtener las calibraciones: ' . $error);
        }

        $result = $stmt->get_result();
        if ($result === false) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron leer los resultados de las calibraciones: ' . $error);
        }

        $rows = [];
        $downloadBase = '/backend/patrones/download_certificate.php?id=';
        while ($row = $result->fetch_assoc()) {
            $row['dias_restantes'] = isset($row['dias_restantes']) ? (int) $row['dias_restantes'] : null;
            $row['tecnico_id'] = isset($row['tecnico_id']) ? (int) $row['tecnico_id'] : null;
            $row['esta_vencida'] = $row['dias_restantes'] !== null && $row['dias_restantes'] < 0;
            $row['esta_vencida_hoy'] = $row['dias_restantes'] !== null && $row['dias_restantes'] === 0;
            $logistica = logistics_response_payload($row);
            $row['logistica'] = $logistica;
            $row['logistica_estado'] = $logistica['estado'];
            $row['logistica_fecha_envio'] = $logistica['fecha_envio'];
            $row['logistica_fecha_retorno'] = $logistica['fecha_retorno'];

            if (!empty($row['patron_id']) && !empty($row['patron_certificado_archivo'])) {
                $row['patron_certificado_url'] = $downloadBase . (int) $row['patron_id'];
            } else {
                $row['patron_certificado_url'] = null;
            }

            $rows[] = $row;
        }

        $stmt->close();
        return $rows;
    }
}

if (!function_exists('calibration_alerts_fetch_upcoming')) {
    /**
     * Obtiene las calibraciones próximas a vencer para las empresas.
     *
     * @param mysqli    $conn       Conexión activa a la base de datos.
     * @param int       $daysAhead  Número de días hacia adelante a considerar.
     * @param int|null  $empresaId  Identificador de la empresa para filtrar (opcional).
     *
     * @return array<int,array<string,mixed>>
     */
    function calibration_alerts_fetch_upcoming(mysqli $conn, int $daysAhead = 60, ?int $empresaId = null): array
    {
        $daysAhead = max($daysAhead, 0);

        $today = new DateTimeImmutable('today');
        $endDate = $today->modify('+' . $daysAhead . ' days');
        $startDate = $today->modify('+1 day');

        $startString = $startDate->format('Y-m-d');
        $endString = $endDate->format('Y-m-d');

        return calibration_alerts_fetch_rows(
            $conn,
            'c.fecha_proxima BETWEEN ? AND ?',
            [$startString, $endString],
            'ss',
            $empresaId,
            'ORDER BY c.fecha_proxima ASC, c.instrumento_id ASC'
        );
    }
}

if (!function_exists('calibration_alerts_fetch_overdue')) {
    /**
     * Obtiene las calibraciones vencidas para las empresas.
     *
     * @param mysqli   $conn      Conexión activa a la base de datos.
     * @param int|null $empresaId Identificador de la empresa para filtrar (opcional).
     *
     * @return array<int,array<string,mixed>>
     */
    function calibration_alerts_fetch_overdue(mysqli $conn, ?int $empresaId = null): array
    {
        $today = new DateTimeImmutable('today');
        $startString = $today->format('Y-m-d');

        return calibration_alerts_fetch_rows(
            $conn,
            'c.fecha_proxima < ?',
            [$startString],
            's',
            $empresaId,
            'ORDER BY c.fecha_proxima ASC, c.instrumento_id ASC'
        );
    }
}

if (!function_exists('calibration_alerts_fetch_due_today')) {
    /**
     * Obtiene las calibraciones que vencen en la fecha actual.
     *
     * @param mysqli   $conn      Conexión activa a la base de datos.
     * @param int|null $empresaId Identificador de la empresa para filtrar (opcional).
     *
     * @return array<int,array<string,mixed>>
     */
    function calibration_alerts_fetch_due_today(mysqli $conn, ?int $empresaId = null): array
    {
        $today = new DateTimeImmutable('today');
        $todayString = $today->format('Y-m-d');

        return calibration_alerts_fetch_rows(
            $conn,
            'c.fecha_proxima = ?',
            [$todayString],
            's',
            $empresaId,
            'ORDER BY c.fecha_proxima ASC, c.instrumento_id ASC'
        );
    }
}

if (!function_exists('calibration_alerts_ensure_log_table')) {
    /**
     * Crea la tabla de notificaciones de calibraciones si no existe.
     */
    function calibration_alerts_ensure_log_table(mysqli $conn): void
    {
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS calibration_alert_notifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    due_date DATE NOT NULL,
    alert_type VARCHAR(20) NOT NULL DEFAULT 'upcoming',
    attention_status VARCHAR(20) NOT NULL DEFAULT 'pendiente',
    attention_notes TEXT DEFAULT NULL,
    attention_user_id INT DEFAULT NULL,
    attention_at DATETIME DEFAULT NULL,
    notified_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_alert (instrumento_id, empresa_id, due_date),
    INDEX idx_alert_due_date (due_date),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (attention_user_id) REFERENCES usuarios(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
SQL;

        if (!$conn->query($sql)) {
            throw new RuntimeException('No se pudo asegurar la tabla de notificaciones de calibraciones: ' . $conn->error);
        }

        $databaseNameResult = $conn->query('SELECT DATABASE() AS db');
        if (!$databaseNameResult) {
            throw new RuntimeException('No se pudo identificar el esquema de base de datos activo: ' . $conn->error);
        }

        $databaseRow = $databaseNameResult->fetch_assoc();
        $databaseNameResult->close();

        $schema = $databaseRow['db'] ?? null;
        if ($schema === null || $schema === '') {
            throw new RuntimeException('No se pudo determinar el nombre de la base de datos para validar la tabla de alertas.');
        }

        $columns = [
            'alert_type' => "ALTER TABLE calibration_alert_notifications ADD COLUMN alert_type VARCHAR(20) NOT NULL DEFAULT 'upcoming' AFTER due_date",
            'attention_status' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_status VARCHAR(20) NOT NULL DEFAULT 'pendiente' AFTER alert_type",
            'attention_notes' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_notes TEXT DEFAULT NULL AFTER attention_status",
            'attention_user_id' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_user_id INT DEFAULT NULL AFTER attention_notes",
            'attention_at' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_at DATETIME DEFAULT NULL AFTER attention_user_id",
            'updated_at' => "ALTER TABLE calibration_alert_notifications ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER notified_at",
        ];

        foreach ($columns as $column => $alterSql) {
            if (!calibration_alerts_column_exists($conn, 'calibration_alert_notifications', $column, $schema)) {
                if (!$conn->query($alterSql)) {
                    throw new RuntimeException('No se pudo ajustar la tabla de notificaciones de calibraciones: ' . $conn->error);
                }

                if ($column === 'attention_user_id') {
                    $fkSql = "ALTER TABLE calibration_alert_notifications ADD FOREIGN KEY (attention_user_id) REFERENCES usuarios(id)";
                    if (!$conn->query($fkSql)) {
                        // Ignorar error si la restricción ya existe
                        if ($conn->errno !== 1826 && $conn->errno !== 1832 && $conn->errno !== 1022) {
                            throw new RuntimeException('No se pudo crear la relación de usuario en alertas de calibración: ' . $conn->error);
                        }
                    }
                }
            }
        }

        $updateSql = "UPDATE calibration_alert_notifications SET alert_type = CASE WHEN due_date < CURDATE() THEN 'overdue' WHEN due_date = CURDATE() THEN 'due_today' ELSE 'upcoming' END";
        if (!$conn->query($updateSql)) {
            throw new RuntimeException('No se pudo sincronizar el tipo de alerta de calibración: ' . $conn->error);
        }
    }
}

if (!function_exists('calibration_alerts_column_exists')) {
    function calibration_alerts_column_exists(mysqli $conn, string $table, string $column, string $schema): bool
    {
        $stmt = $conn->prepare('SELECT COUNT(*) AS total FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?');
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la verificación de columnas para alertas: ' . $conn->error);
        }

        if (!$stmt->bind_param('sss', $schema, $table, $column)) {
            $stmt->close();
            throw new RuntimeException('No se pudo enlazar la verificación de columnas para alertas: ' . $stmt->error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la verificación de columnas para alertas: ' . $error);
        }

        $result = $stmt->get_result();
        if (!$result) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo leer la verificación de columnas para alertas: ' . $error);
        }

        $exists = false;
        if ($row = $result->fetch_assoc()) {
            $exists = ((int) ($row['total'] ?? 0)) > 0;
        }

        $result->close();
        $stmt->close();

        return $exists;
    }
}

if (!function_exists('calibration_alerts_valid_attention_statuses')) {
    /**
     * @return array<int,string>
     */
    function calibration_alerts_valid_attention_statuses(): array
    {
        return ['pendiente', 'reconocida', 'cerrada'];
    }
}

if (!function_exists('calibration_alerts_map_action_to_status')) {
    function calibration_alerts_map_action_to_status(string $action): ?string
    {
        $normalized = strtolower(trim($action));
        $map = [
            'acknowledge' => 'reconocida',
            'ack' => 'reconocida',
            'close' => 'cerrada',
            'cerrar' => 'cerrada',
            'reopen' => 'pendiente',
            'reopen_alert' => 'pendiente',
            'pendiente' => 'pendiente',
            'reconocer' => 'reconocida',
        ];

        return $map[$normalized] ?? null;
    }
}

if (!function_exists('calibration_alerts_fetch_notification_by_id')) {
    /**
     * @return array<string,mixed>|null
     */
    function calibration_alerts_fetch_notification_by_id(mysqli $conn, int $notificationId, int $empresaId): ?array
    {
        $sql = <<<SQL
SELECT
    n.id,
    n.instrumento_id,
    n.empresa_id,
    n.due_date,
    n.alert_type,
    n.attention_status,
    n.attention_notes,
    n.attention_user_id,
    n.attention_at,
    n.notified_at,
    n.updated_at,
    i.codigo AS instrumento_codigo,
    i.serie AS instrumento_serie,
    i.modelo_id,
    COALESCE(ci.nombre, CONCAT('Instrumento #', i.id)) AS instrumento_nombre,
    e.nombre AS empresa_nombre,
    DATEDIFF(n.due_date, CURDATE()) AS dias_restantes,
    CASE
        WHEN n.due_date < CURDATE() THEN 'overdue'
        WHEN n.due_date = CURDATE() THEN 'due_today'
        ELSE 'upcoming'
    END AS computed_alert_type,
    u.correo AS attention_user_correo,
    CONCAT(IFNULL(u.nombre, ''), IF(u.apellidos IS NULL OR u.apellidos = '', '', CONCAT(' ', u.apellidos))) AS attention_user_nombre
FROM calibration_alert_notifications n
JOIN instrumentos i ON i.id = n.instrumento_id
JOIN empresas e ON e.id = n.empresa_id
LEFT JOIN catalogo_instrumentos ci ON ci.id = i.catalogo_id
LEFT JOIN usuarios u ON u.id = n.attention_user_id
WHERE n.id = ? AND n.empresa_id = ?
LIMIT 1
SQL;

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de una alerta de calibración: ' . $conn->error);
        }

        if (!$stmt->bind_param('ii', $notificationId, $empresaId)) {
            $stmt->close();
            throw new RuntimeException('No se pudo enlazar la consulta de una alerta de calibración: ' . $stmt->error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta de una alerta de calibración: ' . $error);
        }

        $result = $stmt->get_result();
        if (!$result) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo leer la alerta de calibración: ' . $error);
        }

        $row = $result->fetch_assoc() ?: null;
        $result->close();
        $stmt->close();

        if ($row) {
            $row['dias_restantes'] = isset($row['dias_restantes']) ? (int) $row['dias_restantes'] : null;
            $row['instrumento_id'] = (int) $row['instrumento_id'];
            $row['empresa_id'] = (int) $row['empresa_id'];
            $row['attention_user_id'] = isset($row['attention_user_id']) ? (int) $row['attention_user_id'] : null;
            $row['alert_type'] = $row['computed_alert_type'] ?? ($row['alert_type'] ?? 'upcoming');
            $row['esta_vencida'] = $row['dias_restantes'] !== null && $row['dias_restantes'] < 0;
            $row['esta_vencida_hoy'] = $row['dias_restantes'] !== null && $row['dias_restantes'] === 0;
        }

        return $row;
    }
}

if (!function_exists('calibration_alerts_list_notifications')) {
    /**
     * @param array<string,mixed> $filters
     * @return array<int,array<string,mixed>>
     */
    function calibration_alerts_list_notifications(mysqli $conn, int $empresaId, array $filters = []): array
    {
        calibration_alerts_ensure_log_table($conn);

        $alertType = isset($filters['alert_type']) ? strtolower(trim((string) $filters['alert_type'])) : null;
        $attentionStatus = isset($filters['attention_status']) ? strtolower(trim((string) $filters['attention_status'])) : null;

        $validStatuses = calibration_alerts_valid_attention_statuses();

        $sql = <<<SQL
SELECT
    n.id,
    n.instrumento_id,
    n.empresa_id,
    DATE_FORMAT(n.due_date, '%Y-%m-%d') AS due_date,
    n.alert_type,
    n.attention_status,
    n.attention_notes,
    n.attention_user_id,
    DATE_FORMAT(n.attention_at, '%Y-%m-%d %H:%i:%s') AS attention_at,
    DATE_FORMAT(n.notified_at, '%Y-%m-%d %H:%i:%s') AS notified_at,
    DATE_FORMAT(n.updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at,
    i.codigo AS instrumento_codigo,
    i.serie AS instrumento_serie,
    COALESCE(ci.nombre, CONCAT('Instrumento #', i.id)) AS instrumento_nombre,
    i.modelo_id,
    e.nombre AS empresa_nombre,
    DATEDIFF(n.due_date, CURDATE()) AS dias_restantes,
    CASE
        WHEN n.due_date < CURDATE() THEN 'overdue'
        WHEN n.due_date = CURDATE() THEN 'due_today'
        ELSE 'upcoming'
    END AS computed_alert_type,
    u.correo AS attention_user_correo,
    CONCAT(IFNULL(u.nombre, ''), IF(u.apellidos IS NULL OR u.apellidos = '', '', CONCAT(' ', u.apellidos))) AS attention_user_nombre
FROM calibration_alert_notifications n
JOIN instrumentos i ON i.id = n.instrumento_id
JOIN empresas e ON e.id = n.empresa_id
LEFT JOIN catalogo_instrumentos ci ON ci.id = i.catalogo_id
LEFT JOIN usuarios u ON u.id = n.attention_user_id
WHERE n.empresa_id = ?
SQL;

        $params = [$empresaId];
        $types = 'i';

        if ($attentionStatus && in_array($attentionStatus, $validStatuses, true)) {
            $sql .= ' AND n.attention_status = ?';
            $params[] = $attentionStatus;
            $types .= 's';
        }

        if ($alertType === 'upcoming') {
            $sql .= ' AND n.due_date > CURDATE()';
        } elseif ($alertType === 'due_today') {
            $sql .= ' AND n.due_date = CURDATE()';
        } elseif ($alertType === 'overdue') {
            $sql .= ' AND n.due_date < CURDATE()';
        }

        $sql .= ' ORDER BY n.due_date ASC, n.id ASC';

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de alertas de calibración: ' . $conn->error);
        }

        if (!$stmt->bind_param($types, ...$params)) {
            $stmt->close();
            throw new RuntimeException('No se pudo enlazar la consulta de alertas de calibración: ' . $stmt->error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta de alertas de calibración: ' . $error);
        }

        $result = $stmt->get_result();
        if (!$result) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo leer las alertas de calibración: ' . $error);
        }

        $rows = [];
        while ($row = $result->fetch_assoc()) {
            $row['dias_restantes'] = isset($row['dias_restantes']) ? (int) $row['dias_restantes'] : null;
            $row['instrumento_id'] = (int) $row['instrumento_id'];
            $row['empresa_id'] = (int) $row['empresa_id'];
            $row['attention_user_id'] = isset($row['attention_user_id']) ? (int) $row['attention_user_id'] : null;
            $row['alert_type'] = $row['computed_alert_type'] ?? ($row['alert_type'] ?? 'upcoming');
            $row['esta_vencida'] = $row['dias_restantes'] !== null && $row['dias_restantes'] < 0;
            $row['esta_vencida_hoy'] = $row['dias_restantes'] !== null && $row['dias_restantes'] === 0;
            $rows[] = $row;
        }

        $result->close();
        $stmt->close();

        return $rows;
    }
}

if (!function_exists('calibration_alerts_attend_notification')) {
    /**
     * @param array<string,mixed> $options
     * @return array<string,mixed>
     */
    function calibration_alerts_attend_notification(
        mysqli $conn,
        int $notificationId,
        int $empresaId,
        string $action,
        int $usuarioId,
        string $usuarioNombre,
        string $usuarioCorreo,
        array $options = []
    ): array {
        calibration_alerts_ensure_log_table($conn);

        $status = calibration_alerts_map_action_to_status($action);
        if ($status === null) {
            throw new InvalidArgumentException('Acción de alerta desconocida: ' . $action);
        }

        $validStatuses = calibration_alerts_valid_attention_statuses();
        if (!in_array($status, $validStatuses, true)) {
            throw new InvalidArgumentException('Estado de atención inválido: ' . $status);
        }

        $current = calibration_alerts_fetch_notification_by_id($conn, $notificationId, $empresaId);
        if ($current === null) {
            throw new RuntimeException('No se encontró la alerta solicitada.');
        }

        $notesProvided = array_key_exists('notes', $options);
        $notes = $notesProvided ? trim((string) $options['notes']) : ($current['attention_notes'] ?? '');
        $notesFinal = $notes === '' ? null : $notes;

        if ($current['attention_status'] === $status && $notesFinal === ($current['attention_notes'] ?? null)) {
            return $current;
        }

        if (!$conn->begin_transaction()) {
            throw new RuntimeException('No se pudo iniciar la transacción para actualizar la alerta: ' . $conn->error);
        }

        try {
            $sql = 'UPDATE calibration_alert_notifications SET attention_status = ?, attention_notes = ?, attention_user_id = ?, attention_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND empresa_id = ?';
            $stmt = $conn->prepare($sql);
            if (!$stmt) {
                throw new RuntimeException('No se pudo preparar la actualización de la alerta de calibración: ' . $conn->error);
            }

            $attentionAt = (new DateTimeImmutable())->format('Y-m-d H:i:s');
            $userId = $usuarioId > 0 ? $usuarioId : null;

            if (!$stmt->bind_param('ssissi', $status, $notesFinal, $userId, $attentionAt, $notificationId, $empresaId)) {
                $stmt->close();
                throw new RuntimeException('No se pudo enlazar la actualización de la alerta de calibración: ' . $stmt->error);
            }

            if (!$stmt->execute()) {
                $error = $stmt->error;
                $stmt->close();
                throw new RuntimeException('No se pudo ejecutar la actualización de la alerta de calibración: ' . $error);
            }

            if ($stmt->affected_rows === 0) {
                $stmt->close();
                throw new RuntimeException('La alerta de calibración no se pudo actualizar.');
            }

            $stmt->close();

            $conn->commit();
        } catch (Throwable $e) {
            $conn->rollback();
            throw $e;
        }

        $updated = calibration_alerts_fetch_notification_by_id($conn, $notificationId, $empresaId);
        if ($updated === null) {
            throw new RuntimeException('No se pudo leer la alerta de calibración después de actualizarla.');
        }

        $before = [
            'status' => $current['attention_status'],
            'notes' => $current['attention_notes'] ?? null,
            'user_id' => $current['attention_user_id'] ?? null,
            'attention_at' => $current['attention_at'] ?? null,
        ];

        $after = [
            'status' => $updated['attention_status'],
            'notes' => $updated['attention_notes'] ?? null,
            'user_id' => $updated['attention_user_id'] ?? null,
            'attention_at' => $updated['attention_at'] ?? null,
        ];

        $rangoReferencia = 'Instrumento #' . $updated['instrumento_id'];
        if (!empty($updated['instrumento_codigo'])) {
            $rangoReferencia .= ' (' . $updated['instrumento_codigo'] . ')';
        }

        log_activity($usuarioNombre, [
            'seccion' => 'Alertas de Calibración',
            'rango_referencia' => $rangoReferencia,
            'instrumento_id' => (int) $updated['instrumento_id'],
            'valor_anterior' => json_encode($before, JSON_UNESCAPED_UNICODE),
            'valor_nuevo' => json_encode($after, JSON_UNESCAPED_UNICODE),
            'usuario_correo' => $usuarioCorreo,
            'usuario_id' => $usuarioId,
        ]);

        return $updated;
    }
}

if (!function_exists('calibration_alerts_get_company_recipients')) {
    /**
     * Obtiene la lista de destinatarios para las alertas de una empresa.
     *
     * @return array<int,array{nombre:string,correo:string}>
     */
    function calibration_alerts_get_company_recipients(mysqli $conn, int $empresaId): array
    {
        $stmt = $conn->prepare(
            "SELECT u.nombre, u.apellidos, u.correo\n             FROM usuarios u\n             JOIN roles r ON r.id = u.role_id\n             WHERE u.empresa_id = ?\n               AND u.activo = 1\n               AND r.nombre IN ('Superadministrador', 'Administrador', 'Supervisor')\n               AND u.correo IS NOT NULL\n               AND u.correo <> ''"
        );
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de destinatarios: ' . $conn->error);
        }

        if (!$stmt->bind_param('i', $empresaId)) {
            $stmt->close();
            throw new RuntimeException('No se pudo enlazar la consulta de destinatarios: ' . $stmt->error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo obtener la lista de destinatarios: ' . $error);
        }

        $result = $stmt->get_result();
        if (!$result) {
            $stmt->close();
            throw new RuntimeException('No se pudo leer la lista de destinatarios.');
        }

        $recipients = [];
        while ($row = $result->fetch_assoc()) {
            $email = strtolower(trim((string) ($row['correo'] ?? '')));
            if ($email === '') {
                continue;
            }
            if (isset($recipients[$email])) {
                continue;
            }
            $name = trim(((string) ($row['nombre'] ?? '')) . ' ' . ((string) ($row['apellidos'] ?? '')));
            if ($name === '') {
                $name = $row['correo'] ?? $email;
            }
            $recipients[$email] = [
                'nombre' => $name,
                'correo' => $email,
            ];
        }

        $stmt->close();
        return array_values($recipients);
    }
}

if (!function_exists('calibration_alerts_send_notifications')) {
    /**
     * Envía las notificaciones de calibraciones próximas y vencidas y registra los envíos realizados.
     *
     * @param callable|null $logger Función opcional para registrar mensajes (nivel, mensaje).
     *
     * @return array<string,int>
     */
    function calibration_alerts_send_notifications(mysqli $conn, int $daysAhead = 60, ?callable $logger = null): array
    {
        $daysAhead = max($daysAhead, 1);

        /** @var callable $log */
        $log = $logger ?? static function (string $level, string $message): void {
            $prefix = strtoupper($level);
            error_log("[calibration-alerts][$prefix] $message");
        };

        calibration_alerts_ensure_log_table($conn);

        $jobName = 'calibration_alerts';
        $jobStartedAt = new DateTimeImmutable();

        try {
            tenant_notifications_jobs_touch($conn, $jobName, 'running', 'Inicio de ejecución', null, $jobStartedAt);
        } catch (Throwable $e) {
            $log('warning', 'No se pudo actualizar el estado inicial del job: ' . $e->getMessage());
        }

        try {
            $upcomingAlerts = calibration_alerts_fetch_upcoming($conn, $daysAhead);
            $dueTodayAlerts = calibration_alerts_fetch_due_today($conn);
            $overdueAlerts = calibration_alerts_fetch_overdue($conn);

            foreach ($upcomingAlerts as &$alert) {
                $alert['alert_type'] = 'upcoming';
            }
            unset($alert);

            foreach ($dueTodayAlerts as &$alert) {
                $alert['alert_type'] = 'due_today';
                $alert['esta_vencida_hoy'] = true;
            }
            unset($alert);

            foreach ($overdueAlerts as &$alert) {
                $alert['alert_type'] = 'overdue';
            }
            unset($alert);

            $allAlerts = array_merge($upcomingAlerts, $dueTodayAlerts, $overdueAlerts);

            $result = [
                'alerts_found' => count($allAlerts),
                'alerts_found_upcoming' => count($upcomingAlerts),
                'alerts_found_due_today' => count($dueTodayAlerts),
                'alerts_found_overdue' => count($overdueAlerts),
                'pending_alerts' => 0,
                'pending_upcoming' => 0,
                'pending_due_today' => 0,
                'pending_overdue' => 0,
                'companies_attempted' => 0,
                'companies_notified' => 0,
                'notifications_sent' => 0,
                'notifications_sent_upcoming' => 0,
                'notifications_sent_due_today' => 0,
                'notifications_sent_overdue' => 0,
                'notifications_recorded' => 0,
                'channel_failures' => [],
            ];

            if (!$allAlerts) {
                $log('info', 'No se encontraron calibraciones próximas o vencidas que notificar.');

                try {
                    tenant_notifications_jobs_touch($conn, $jobName, 'success', 'Sin alertas pendientes', $result, new DateTimeImmutable());
                } catch (Throwable $touchError) {
                    $log('error', 'No se pudo actualizar la tabla de jobs tras finalizar sin alertas: ' . $touchError->getMessage());
                }

                return $result;
            }

            $checkStmt = $conn->prepare('SELECT 1 FROM calibration_alert_notifications WHERE instrumento_id = ? AND empresa_id = ? AND due_date = ? LIMIT 1');
            if (!$checkStmt) {
                throw new RuntimeException('No se pudo preparar la consulta de verificación de notificaciones: ' . $conn->error);
            }

            $pendingAlerts = [];
            $pendingCounts = [
                'upcoming' => 0,
                'due_today' => 0,
                'overdue' => 0,
            ];

            foreach ($allAlerts as $alert) {
                $dueDate = $alert['fecha_proxima'] ?? null;
                if (!$dueDate) {
                    continue;
                }

                $instrumentoId = (int) $alert['instrumento_id'];
                $empresaId = (int) $alert['empresa_id'];
                $alertType = (string) ($alert['alert_type'] ?? 'upcoming');

                if (!$checkStmt->bind_param('iis', $instrumentoId, $empresaId, $dueDate)) {
                    $checkStmt->close();
                    throw new RuntimeException('No se pudo enlazar la consulta de verificación: ' . $conn->error);
                }

                if (!$checkStmt->execute()) {
                    $error = $checkStmt->error;
                    $checkStmt->close();
                    throw new RuntimeException('No se pudo ejecutar la consulta de verificación: ' . $error);
                }

                $checkStmt->store_result();
                $alreadyNotified = $checkStmt->num_rows > 0;
                $checkStmt->free_result();

                if ($alreadyNotified) {
                    continue;
                }

                $pendingAlerts[] = $alert;
                if (!isset($pendingCounts[$alertType])) {
                    $pendingCounts[$alertType] = 0;
                }
                $pendingCounts[$alertType]++;
            }
            $checkStmt->close();

            $result['pending_upcoming'] = $pendingCounts['upcoming'] ?? 0;
            $result['pending_due_today'] = $pendingCounts['due_today'] ?? 0;
            $result['pending_overdue'] = $pendingCounts['overdue'] ?? 0;
            $result['pending_alerts'] = $result['pending_upcoming'] + $result['pending_due_today'] + $result['pending_overdue'];

            if (!$pendingAlerts) {
                $log('info', 'Todos los instrumentos ya fueron notificados previamente.');

                try {
                    tenant_notifications_jobs_touch($conn, $jobName, 'success', 'Alertas duplicadas', $result, new DateTimeImmutable());
                } catch (Throwable $touchError) {
                    $log('error', 'No se pudo actualizar la tabla de jobs tras detectar duplicados: ' . $touchError->getMessage());
                }

                return $result;
            }

            $grouped = [];
            foreach ($pendingAlerts as $alert) {
                $empresaId = (int) $alert['empresa_id'];
                $alertType = (string) ($alert['alert_type'] ?? 'upcoming');
                if (!isset($grouped[$empresaId])) {
                    $grouped[$empresaId] = [
                        'empresa_nombre' => $alert['empresa_nombre'] ?? ('Empresa #' . $empresaId),
                        'items' => [
                            'upcoming' => [],
                            'due_today' => [],
                            'overdue' => [],
                        ],
                    ];
                }
                $grouped[$empresaId]['items'][$alertType][] = $alert;
            }

            $insertStmt = $conn->prepare('INSERT INTO calibration_alert_notifications (instrumento_id, empresa_id, due_date, alert_type) VALUES (?, ?, ?, ?)');
            if (!$insertStmt) {
                throw new RuntimeException('No se pudo preparar el registro de notificaciones: ' . $conn->error);
            }

            $now = (new DateTimeImmutable())->format('Y-m-d H:i');

            $dispatcher = new AlertDispatcher($conn, $log);

            foreach ($grouped as $empresaId => $group) {
                $result['companies_attempted']++;
                $empresaNombre = $group['empresa_nombre'];

                try {
                    $recipients = tenant_notifications_fetch_company_recipients(
                        $conn,
                        $empresaId,
                        ['Superadministrador', 'Administrador', 'Supervisor', 'Cliente']
                    );
                } catch (Throwable $e) {
                    $log('error', 'No se pudo obtener la lista de destinatarios para la empresa ' . $empresaId . ': ' . $e->getMessage());
                    continue;
                }

                if (!$recipients) {
                    $log('warning', 'Sin destinatarios para la empresa ' . $empresaNombre);
                    continue;
                }

                $sections = [
                    'overdue' => [],
                    'due_today' => [],
                    'le30' => [],
                    'gt30' => [],
                ];

                foreach ($group['items'] as $type => $items) {
                    foreach ($items as $item) {
                        $dias = (int) ($item['dias_restantes'] ?? 0);

                        if ($type === 'overdue' || (!empty($item['esta_vencida']) && $type !== 'due_today')) {
                            $sections['overdue'][] = $item;
                            continue;
                        }

                        if ($type === 'due_today' || !empty($item['esta_vencida_hoy'])) {
                            $sections['due_today'][] = $item;
                            continue;
                        }

                        if ($dias <= 30) {
                            $sections['le30'][] = $item;
                        } else {
                            $sections['gt30'][] = $item;
                        }
                    }
                }

                $sectionHtml = '';
                $sectionText = '';

                if ($sections['overdue']) {
                    $sectionHtml .= '<h2>Calibraciones vencidas</h2>' . build_alerts_html_table($sections['overdue']);
                    $sectionText .= "Calibraciones vencidas:\n" . build_alerts_text_table($sections['overdue']) . "\n\n";
                }

                if ($sections['due_today']) {
                    $sectionHtml .= '<h2>Calibraciones vencidas hoy</h2>' . build_alerts_html_table($sections['due_today']);
                    $sectionText .= "Calibraciones vencidas hoy:\n" . build_alerts_text_table($sections['due_today']) . "\n\n";
                }

                if ($sections['le30']) {
                    $sectionHtml .= '<h2>Calibraciones en ≤30 días</h2>' . build_alerts_html_table($sections['le30']);
                    $sectionText .= "Calibraciones en ≤30 días:\n" . build_alerts_text_table($sections['le30']) . "\n\n";
                }

                if ($group['items']['upcoming']) {
                    $sectionHtml .= '<h2>Calibraciones próximas</h2>' . build_alerts_html_table($group['items']['upcoming']);
                    $sectionText .= "Calibraciones próximas:\n" . build_alerts_text_table($group['items']['upcoming']) . "\n\n";
                }

                $htmlBody = '<p>Hola,</p>'
                    . '<p>Se encontraron instrumentos con calibraciones vencidas o próximas a vencer en la empresa <strong>' . htmlspecialchars($empresaNombre, ENT_QUOTES, 'UTF-8') . '</strong>.</p>'
                    . $sectionHtml
                    . '<p>Este aviso fue generado automáticamente el ' . htmlspecialchars($now, ENT_QUOTES, 'UTF-8') . '. Si un instrumento ya fue atendido, ignora este mensaje.</p>';

                $textBody = "Hola,\n\n"
                    . 'Se encontraron instrumentos con calibraciones vencidas o próximas a vencer en la empresa ' . $empresaNombre . ".\n\n"
                    . $sectionText
                    . 'Este aviso fue generado automáticamente el ' . $now . '. Si un instrumento ya fue atendido, ignora este mensaje.';

                $subject = 'Alertas de calibraciones (próximas, vencen hoy y vencidas) - ' . $empresaNombre;

                $channelResults = $dispatcher->dispatch($empresaId, [
                    'empresa_nombre' => $empresaNombre,
                    'subject' => $subject,
                    'html_body' => $htmlBody,
                    'text_body' => $textBody,
                    'recipients' => $recipients,
                    'items' => $group['items'],
                ]);

                $successfulChannels = [];
                $emailChannelStatus = null;
                foreach ($channelResults as $channelName => $channelInfo) {
                    $status = $channelInfo['status'] ?? 'unknown';
                    if ($channelName === 'email') {
                        $emailChannelStatus = $status;
                    }
                    if ($status === 'sent') {
                        $successfulChannels[] = $channelName;
                        continue;
                    }

                    if ($status === 'failed') {
                        $result['channel_failures'][] = [
                            'empresa_id' => $empresaId,
                            'empresa_nombre' => $empresaNombre,
                            'channel' => $channelName,
                            'error' => (string) ($channelInfo['error'] ?? 'Fallo desconocido'),
                        ];
                        $log('error', sprintf('Fallo en canal %s para la empresa %s: %s', $channelName, $empresaNombre, $channelInfo['error'] ?? 'Fallo desconocido'));
                    }
                }

                if ($emailChannelStatus !== 'sent') {
                    if ($emailChannelStatus === 'failed') {
                        $log('error', 'Se registró un fallo en el canal de correo para la empresa ' . $empresaNombre . '. Se omite el registro de notificaciones para permitir reintento.');
                    } else {
                        $log('error', 'El canal de correo no confirmó el envío para la empresa ' . $empresaNombre . ' (estado: ' . ($emailChannelStatus ?? 'desconocido') . '). Se omite el registro de notificaciones.');
                    }
                    continue;
                }

                if (!$successfulChannels) {
                    $log('error', 'No se pudo enviar ninguna alerta para la empresa ' . $empresaNombre . ' en los canales configurados.');
                    continue;
                }

                $log('info', 'Alerta de calibraciones enviada para la empresa ' . $empresaNombre . ' a través de: ' . implode(', ', $successfulChannels));

                $companyTotal = 0;
                $companyUpcoming = 0;
                $companyDueToday = 0;
                $companyOverdue = 0;

                foreach ($group['items'] as $items) {
                    foreach ($items as $item) {
                        $dueDate = $item['fecha_proxima'] ?? null;
                        if (!$dueDate) {
                            continue;
                        }

                        $instrumentoId = (int) $item['instrumento_id'];
                        $alertType = 'upcoming';

                        try {
                            $todayDate = new DateTimeImmutable('today');
                            $dueDateObject = new DateTimeImmutable($dueDate);

                            if ($dueDateObject < $todayDate) {
                                $alertType = 'overdue';
                            } elseif ($dueDateObject > $todayDate) {
                                $alertType = 'upcoming';
                            } else {
                                $alertType = 'due_today';
                            }
                        } catch (Throwable $dateError) {
                            $alertType = 'upcoming';
                        }

                        if (!$insertStmt->bind_param('iiss', $instrumentoId, $empresaId, $dueDate, $alertType)) {
                            $log('error', 'No se pudo registrar la notificación del instrumento ' . $instrumentoId);
                            continue;
                        }

                        if (!$insertStmt->execute()) {
                            $log('error', 'No se pudo registrar la notificación del instrumento ' . $instrumentoId . ': ' . $insertStmt->error);
                            continue;
                        }

                        $companyTotal++;
                        if ($alertType === 'overdue') {
                            $companyOverdue++;
                        } elseif ($alertType === 'due_today') {
                            $companyDueToday++;
                        } else {
                            $companyUpcoming++;
                        }
                        $result['notifications_recorded']++;
                    }
                }

                $result['companies_notified']++;
                $result['notifications_sent'] += $companyTotal;
                $result['notifications_sent_upcoming'] += $companyUpcoming;
                $result['notifications_sent_due_today'] += $companyDueToday;
                $result['notifications_sent_overdue'] += $companyOverdue;
            }

            $insertStmt->close();

            try {
                tenant_notifications_jobs_touch($conn, $jobName, 'success', null, $result, new DateTimeImmutable());
            } catch (Throwable $touchError) {
                $log('error', 'No se pudo actualizar la tabla de jobs tras completar el envío: ' . $touchError->getMessage());
            }

            return $result;
        } catch (Throwable $exception) {
            try {
                tenant_notifications_jobs_touch($conn, $jobName, 'error', $exception->getMessage(), null, new DateTimeImmutable());
            } catch (Throwable $touchError) {
                $log('error', 'Fallo adicional al registrar el error del job: ' . $touchError->getMessage());
            }

            throw $exception;
        }
    }
}

if (!function_exists('build_alerts_html_table')) {
    /**
     * Construye una tabla HTML para el cuerpo del correo de alertas.
     *
     * @param array<int,array<string,mixed>> $items
     */
    function build_alerts_html_table(array $items): string
    {
        $rows = '';
        foreach ($items as $item) {
            $instrumento = htmlspecialchars((string) ($item['instrumento_nombre'] ?? ''), ENT_QUOTES, 'UTF-8');
            $codigo = htmlspecialchars((string) ($item['instrumento_codigo'] ?? ''), ENT_QUOTES, 'UTF-8');
            $fechaCalibracion = htmlspecialchars((string) ($item['fecha_calibracion'] ?? '-'), ENT_QUOTES, 'UTF-8');
            $fechaProxima = htmlspecialchars((string) ($item['fecha_proxima'] ?? '-'), ENT_QUOTES, 'UTF-8');
            $dias = (int) ($item['dias_restantes'] ?? 0);
            $resultado = htmlspecialchars((string) ($item['resultado'] ?? ''), ENT_QUOTES, 'UTF-8');

            $rows .= '<tr>' .
                '<td>' . $instrumento . '</td>' .
                '<td>' . ($codigo !== '' ? $codigo : '-') . '</td>' .
                '<td>' . ($fechaCalibracion !== '' ? $fechaCalibracion : '-') . '</td>' .
                '<td>' . ($fechaProxima !== '' ? $fechaProxima : '-') . '</td>' .
                '<td>' . $dias . '</td>' .
                '<td>' . ($resultado !== '' ? $resultado : '-') . '</td>' .
            '</tr>';
        }

        return '<table border="1" cellpadding="6" cellspacing="0">'
            . '<thead><tr>'
            . '<th>Instrumento</th>'
            . '<th>Código</th>'
            . '<th>Última calibración</th>'
            . '<th>Próxima calibración</th>'
            . '<th>Días restantes</th>'
            . '<th>Resultado</th>'
            . '</tr></thead><tbody>' . $rows . '</tbody></table>';
    }
}

if (!function_exists('build_alerts_text_table')) {
    /**
     * Construye una representación en texto plano de las alertas.
     *
     * @param array<int,array<string,mixed>> $items
     */
    function build_alerts_text_table(array $items): string
    {
        $lines = [];
        foreach ($items as $item) {
            $instrumento = (string) ($item['instrumento_nombre'] ?? 'Instrumento');
            $codigo = (string) ($item['instrumento_codigo'] ?? '');
            $fechaCalibracion = (string) ($item['fecha_calibracion'] ?? '-');
            $fechaProxima = (string) ($item['fecha_proxima'] ?? '-');
            $dias = (int) ($item['dias_restantes'] ?? 0);
            $resultado = (string) ($item['resultado'] ?? '-');

            $lines[] = sprintf(
                '- %s%s | Última: %s | Próxima: %s | Días restantes: %d | Resultado: %s',
                $instrumento,
                $codigo !== '' ? ' (' . $codigo . ')' : '',
                $fechaCalibracion !== '' ? $fechaCalibracion : '-',
                $fechaProxima !== '' ? $fechaProxima : '-',
                $dias,
                $resultado !== '' ? $resultado : '-'
            );
        }

        return implode("\n", $lines);
    }
}
