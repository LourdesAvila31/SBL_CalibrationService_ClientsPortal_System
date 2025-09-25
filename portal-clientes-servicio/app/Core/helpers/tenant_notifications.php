<?php

declare(strict_types=1);

require_once __DIR__ . '/mail_helper.php';

if (!function_exists('tenant_notifications_deduplicate_recipients')) {
    /**
     * Deduplica destinatarios utilizando el correo electrónico en minúsculas.
     *
     * @param array<int,array<string,string>> $recipients
     * @return array<int,array<string,string>>
     */
    function tenant_notifications_deduplicate_recipients(array $recipients): array
    {
        $unique = [];

        foreach ($recipients as $recipient) {
            $email = strtolower(trim($recipient['correo'] ?? ''));
            if ($email === '') {
                continue;
            }
            if (isset($unique[$email])) {
                continue;
            }

            $unique[$email] = [
                'nombre' => trim($recipient['nombre'] ?? '') ?: $email,
                'correo' => $email,
            ];
        }

        return array_values($unique);
    }
}

if (!function_exists('tenant_notifications_fetch_company_recipients')) {
    /**
     * Obtiene los destinatarios activos de una empresa filtrando por roles.
     *
     * @param array<int,string> $roleNames
     *
     * @return array<int,array<string,string>> Lista de destinatarios con llaves `nombre` y `correo`.
     */
    function tenant_notifications_fetch_company_recipients(mysqli $conn, int $empresaId, array $roleNames = []): array
    {
        if ($empresaId <= 0) {
            throw new InvalidArgumentException('El identificador de la empresa debe ser positivo.');
        }

        if (!$roleNames) {
            $roleNames = ['Administrador', 'Supervisor', 'Cliente'];
        }

        $roleNames = array_values(array_unique(array_filter(array_map('trim', $roleNames), static fn ($name) => $name !== '')));
        if (!$roleNames) {
            throw new InvalidArgumentException('Se requiere al menos un rol para obtener destinatarios.');
        }

        $placeholders = implode(',', array_fill(0, count($roleNames), '?'));
        $sql = <<<SQL
SELECT u.nombre, u.apellidos, u.correo
FROM usuarios u
JOIN roles r ON r.id = u.role_id
WHERE u.empresa_id = ?
  AND u.activo = 1
  AND r.nombre IN ($placeholders)
  AND u.correo IS NOT NULL
  AND u.correo <> ''
SQL;

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de destinatarios: ' . $conn->error);
        }

        $types = 'i' . str_repeat('s', count($roleNames));
         $bindArgs = [$types];
        $bindArgs[] = &$empresaId;
        foreach ($roleNames as $idx => $roleName) {
            $bindArgs[] = &$roleNames[$idx];
        }

        if (!call_user_func_array([$stmt, 'bind_param'], $bindArgs)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros de destinatarios: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta de destinatarios: ' . $error);
        }

        $result = $stmt->get_result();
        if ($result === false) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron leer los destinatarios: ' . $error);
        }

        $recipients = [];
        while ($row = $result->fetch_assoc()) {
            $nombre = trim(($row['nombre'] ?? '') . ' ' . ($row['apellidos'] ?? ''));
            $recipients[] = [
                'nombre' => $nombre !== '' ? $nombre : (string) ($row['correo'] ?? ''),
                'correo' => (string) ($row['correo'] ?? ''),
            ];
        }

        $result->free();
        $stmt->close();

        return tenant_notifications_deduplicate_recipients($recipients);
    }
}

if (!function_exists('tenant_notifications_send_bulk_mail')) {
    /**
     * Envía un conjunto de correos y devuelve el resumen del envío.
     *
     * @param array<int,array<string,string>> $recipients
     * @return array{attempted:int,sent:int,failures:array<int,array{correo:string,message:string}>}
     */
    function tenant_notifications_send_bulk_mail(array $recipients, string $subject, string $htmlBody, ?string $textBody = null, ?callable $logger = null): array
    {
        /** @var callable $log */
        $log = $logger ?? static function (string $level, string $message): void {
            error_log('[tenant-notifications][' . strtoupper($level) . '] ' . $message);
        };

        $attempted = 0;
        $sent = 0;
        $failures = [];

        foreach ($recipients as $recipient) {
            $email = trim($recipient['correo'] ?? '');
            $name = trim($recipient['nombre'] ?? '');

            if ($email === '') {
                continue;
            }

            $attempted++;

            try {
                mail_helper_send($email, $name !== '' ? $name : $email, $subject, $htmlBody, $textBody);
                $sent++;
                $log('info', 'Correo enviado a ' . $email);
            } catch (Throwable $e) {
                $log('error', 'No se pudo enviar el correo a ' . $email . ': ' . $e->getMessage());
                $failures[] = [
                    'correo' => $email,
                    'message' => $e->getMessage(),
                ];
            }
        }

        return [
            'attempted' => $attempted,
            'sent' => $sent,
            'failures' => $failures,
        ];
    }
}

if (!function_exists('tenant_notifications_fetch_instrument_context')) {
    /**
     * Obtiene datos descriptivos del instrumento y la empresa.
     *
     * @return array<string,string>|null
     */
    function tenant_notifications_fetch_instrument_context(mysqli $conn, int $instrumentoId, int $empresaId): ?array
    {
        $sql = <<<SQL
SELECT e.nombre AS empresa_nombre,
       COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) AS instrumento_nombre,
       i.codigo AS instrumento_codigo
FROM instrumentos i
JOIN empresas e ON e.id = i.empresa_id
LEFT JOIN catalogo_instrumentos ci ON ci.id = i.catalogo_id
WHERE i.id = ?
  AND i.empresa_id = ?
LIMIT 1
SQL;

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de contexto de instrumento: ' . $conn->error);
        }

        if (!$stmt->bind_param('ii', $instrumentoId, $empresaId)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros del contexto de instrumento: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta del contexto de instrumento: ' . $error);
        }

        $result = $stmt->get_result();
        if ($result === false) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo obtener el contexto del instrumento: ' . $error);
        }

        $row = $result->fetch_assoc() ?: null;
        $result->free();
        $stmt->close();

        if ($row === null) {
            return null;
        }

        return [
            'empresa_nombre' => (string) ($row['empresa_nombre'] ?? ''),
            'instrumento_nombre' => (string) ($row['instrumento_nombre'] ?? ''),
            'instrumento_codigo' => (string) ($row['instrumento_codigo'] ?? ''),
        ];
    }
}

if (!function_exists('tenant_notifications_get_certificate_notification')) {
    /**
     * Obtiene una notificación de certificado previamente registrada.
     *
     * @return array<string,mixed>|null
     */
    function tenant_notifications_get_certificate_notification(mysqli $conn, int $instrumentoId, int $empresaId, string $referenceDate): ?array
    {
        $stmt = $conn->prepare(
            'SELECT id, reference_date, calibration_date, certificate_path, notified_at, updated_at'
            . ' FROM certificate_alert_notifications'
            . ' WHERE instrumento_id = ? AND empresa_id = ? AND reference_date = ?'
            . ' LIMIT 1'
        );

        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de certificados registrados: ' . $conn->error);
        }

        if (!$stmt->bind_param('iis', $instrumentoId, $empresaId, $referenceDate)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros de certificados registrados: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta de certificados registrados: ' . $error);
        }

        $result = $stmt->get_result();
        if ($result === false) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo leer la información de certificados registrados: ' . $error);
        }

        $row = $result->fetch_assoc() ?: null;
        $result->free();
        $stmt->close();

        return $row;
    }
}

if (!function_exists('tenant_notifications_update_certificate_notification')) {
    /**
     * Actualiza información adicional de una notificación previa sin modificar la fecha original.
     */
    function tenant_notifications_update_certificate_notification(mysqli $conn, int $notificationId, ?string $calibrationDate, ?string $certificatePath): void
    {
        $stmt = $conn->prepare(
            'UPDATE certificate_alert_notifications'
            . '   SET calibration_date = ?, certificate_path = ?, updated_at = ?'
            . ' WHERE id = ?'
        );

        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la actualización de certificados: ' . $conn->error);
        }

        $now = (new DateTimeImmutable())->format('Y-m-d H:i:s');

        if (!$stmt->bind_param('sssi', $calibrationDate, $certificatePath, $now, $notificationId)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros de actualización de certificados: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo actualizar la notificación de certificado: ' . $error);
        }

        $stmt->close();
    }
}

if (!function_exists('tenant_notifications_record_certificate_notification')) {
    /**
     * Registra una notificación de certificado enviada.
     */
    function tenant_notifications_record_certificate_notification(mysqli $conn, int $instrumentoId, int $empresaId, string $referenceDate, ?string $calibrationDate, ?string $certificatePath): int
    {
        $stmt = $conn->prepare(
            'INSERT INTO certificate_alert_notifications'
            . ' (instrumento_id, empresa_id, reference_date, calibration_date, certificate_path, notified_at, updated_at)'
            . ' VALUES (?, ?, ?, ?, ?, ?, ?)'
        );

        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar el registro de certificados: ' . $conn->error);
        }

        $now = (new DateTimeImmutable())->format('Y-m-d H:i:s');

        if (!$stmt->bind_param('iisssss', $instrumentoId, $empresaId, $referenceDate, $calibrationDate, $certificatePath, $now, $now)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros de registro de certificados: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo registrar la notificación de certificado: ' . $error);
        }

        $stmt->close();

        return $conn->insert_id;
    }
}

if (!function_exists('tenant_notifications_send_certificate_notification')) {
    /**
     * Envía un aviso de nuevo certificado y registra el envío evitando duplicados.
     *
     * @param array<string,mixed> $data
     * @return array<string,mixed>
     */
    function tenant_notifications_send_certificate_notification(mysqli $conn, array $data, ?callable $logger = null): array
    {
        /** @var callable $log */
        $log = $logger ?? static function (string $level, string $message): void {
            error_log('[tenant-notifications][' . strtoupper($level) . '] ' . $message);
        };

        $instrumentoId = (int) ($data['instrumento_id'] ?? 0);
        $empresaId = (int) ($data['empresa_id'] ?? 0);

        if ($instrumentoId <= 0 || $empresaId <= 0) {
            throw new InvalidArgumentException('Se requieren los identificadores de instrumento y empresa para enviar la notificación.');
        }

        $empresaNombre = trim((string) ($data['empresa_nombre'] ?? ''));
        $instrumentoNombre = trim((string) ($data['instrumento_nombre'] ?? ''));
        $instrumentoCodigo = trim((string) ($data['instrumento_codigo'] ?? ''));
        $calibrationDate = $data['fecha_calibracion'] ?? null;
        $dueDate = $data['fecha_proxima'] ?? null;
        $certificatePath = $data['certificate_path'] ?? null;
        $baseUrl = mail_helper_base_url();
        $certificateUrl = $data['certificate_url'] ?? null;

        $referenceDate = $dueDate ?: ($calibrationDate ?: (new DateTimeImmutable())->format('Y-m-d'));

        $existing = tenant_notifications_get_certificate_notification($conn, $instrumentoId, $empresaId, $referenceDate);
        if ($existing) {
            $log('info', sprintf('La notificación para el instrumento %d con fecha %s ya había sido registrada.', $instrumentoId, $referenceDate));
            tenant_notifications_update_certificate_notification($conn, (int) $existing['id'], $calibrationDate, $certificatePath);

            return [
                'sent' => false,
                'already_recorded' => true,
                'notification_id' => (int) $existing['id'],
            ];
        }

        try {
            $recipients = tenant_notifications_fetch_company_recipients($conn, $empresaId, [
                'Administrador',
                'Supervisor',
                'Cliente',
            ]);
        } catch (Throwable $e) {
            $log('error', 'No se pudo obtener la lista de destinatarios de certificados: ' . $e->getMessage());
            return [
                'sent' => false,
                'recipients' => [],
                'error' => $e->getMessage(),
            ];
        }

        if (!$recipients) {
            $log('warning', 'No se encontraron destinatarios para la empresa ' . $empresaId . ' al enviar el certificado.');
            return [
                'sent' => false,
                'recipients' => [],
            ];
        }

        $empresaDisplay = $empresaNombre !== '' ? $empresaNombre : ('Empresa #' . $empresaId);
        $instrumentoDisplay = $instrumentoNombre !== '' ? $instrumentoNombre : ('Instrumento #' . $instrumentoId);
        $codigoDisplay = $instrumentoCodigo !== '' ? (' (' . $instrumentoCodigo . ')') : '';

        if (!$certificateUrl && $certificatePath) {
            $certificateUrl = rtrim($baseUrl, '/') . '/backend/calibraciones/certificates/' . ltrim((string) $certificatePath, '/');
        }

        $subject = 'Nuevo certificado disponible - ' . $instrumentoDisplay;

        $htmlBody = '<p>Hola,</p>'
            . '<p>Se ha registrado un nuevo certificado de calibración para el instrumento <strong>'
            . htmlspecialchars($instrumentoDisplay, ENT_QUOTES, 'UTF-8') . '</strong>' . htmlspecialchars($codigoDisplay, ENT_QUOTES, 'UTF-8')
            . ' de la empresa <strong>' . htmlspecialchars($empresaDisplay, ENT_QUOTES, 'UTF-8') . '</strong>.</p>'
            . '<ul>'
            . '<li><strong>Fecha de calibración:</strong> ' . htmlspecialchars((string) ($calibrationDate ?: 'No especificada'), ENT_QUOTES, 'UTF-8') . '</li>'
            . '<li><strong>Próxima calibración:</strong> ' . htmlspecialchars((string) ($dueDate ?: 'No especificada'), ENT_QUOTES, 'UTF-8') . '</li>'
            . '</ul>';

        if ($certificateUrl) {
            $htmlBody .= '<p>Puedes descargar el certificado desde el siguiente enlace: '
                . '<a href="' . htmlspecialchars($certificateUrl, ENT_QUOTES, 'UTF-8') . '">' . htmlspecialchars($certificateUrl, ENT_QUOTES, 'UTF-8') . '</a></p>';
        } elseif ($certificatePath) {
            $htmlBody .= '<p>Ruta del archivo: <strong>' . htmlspecialchars((string) $certificatePath, ENT_QUOTES, 'UTF-8') . '</strong></p>';
        }

        $htmlBody .= '<p>Este mensaje fue generado automáticamente el '
            . htmlspecialchars((new DateTimeImmutable())->format('Y-m-d H:i'), ENT_QUOTES, 'UTF-8') . '.</p>';

        $textBodyLines = [
            'Hola,',
            '',
            'Se ha registrado un nuevo certificado de calibración para el instrumento ' . $instrumentoDisplay . $codigoDisplay,
            'de la empresa ' . $empresaDisplay . '.',
            '',
            'Fecha de calibración: ' . ($calibrationDate ?: 'No especificada'),
            'Próxima calibración: ' . ($dueDate ?: 'No especificada'),
        ];

        if ($certificateUrl) {
            $textBodyLines[] = 'Descarga: ' . $certificateUrl;
        } elseif ($certificatePath) {
            $textBodyLines[] = 'Ruta del archivo: ' . $certificatePath;
        }

        $textBodyLines[] = '';
        $textBodyLines[] = 'Mensaje generado automáticamente el ' . (new DateTimeImmutable())->format('Y-m-d H:i');
        $textBody = implode("\n", $textBodyLines);

        $sendSummary = tenant_notifications_send_bulk_mail($recipients, $subject, $htmlBody, $textBody, $log);

        if ($sendSummary['sent'] === 0) {
            $log('error', 'No se pudo enviar el aviso de certificado, se registraron ' . count($sendSummary['failures']) . ' errores.');
            return array_merge($sendSummary, ['sent' => false]);
        }

        $notificationId = tenant_notifications_record_certificate_notification(
            $conn,
            $instrumentoId,
            $empresaId,
            $referenceDate,
            $calibrationDate,
            $certificatePath
        );

        return array_merge($sendSummary, [
            'sent' => true,
            'notification_id' => $notificationId,
            'reference_date' => $referenceDate,
        ]);
    }
}

if (!function_exists('tenant_notifications_jobs_touch')) {
    /**
     * Actualiza la tabla de control de ejecuciones de procesos.
     */
    function tenant_notifications_jobs_touch(
        mysqli $conn,
        string $jobName,
        string $status,
        ?string $message = null,
        ?array $result = null,
        ?DateTimeInterface $runAt = null
    ): void {
        $stmt = $conn->prepare(
            'INSERT INTO system_jobs (job_name, last_run_at, last_status, last_message, last_result, updated_at)'
            . ' VALUES (?, ?, ?, ?, ?, ?)'
            . ' ON DUPLICATE KEY UPDATE'
            . '   last_run_at = VALUES(last_run_at),'
            . '   last_status = VALUES(last_status),'
            . '   last_message = VALUES(last_message),'
            . '   last_result = VALUES(last_result),'
            . '   updated_at = VALUES(updated_at)'
        );

        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar el registro del job: ' . $conn->error);
        }

        $runAt = $runAt ?? new DateTimeImmutable();
        $timestamp = $runAt->format('Y-m-d H:i:s');
        $updatedAt = (new DateTimeImmutable())->format('Y-m-d H:i:s');
        $resultJson = $result !== null ? json_encode($result, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : null;

        if (!$stmt->bind_param('ssssss', $jobName, $timestamp, $status, $message, $resultJson, $updatedAt)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros del registro del job: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo actualizar la tabla de jobs: ' . $error);
        }

        $stmt->close();
    }
}

if (!function_exists('tenant_notifications_jobs_get')) {
    /**
     * Obtiene la última ejecución registrada de un job.
     *
     * @return array<string,mixed>|null
     */
    function tenant_notifications_jobs_get(mysqli $conn, string $jobName): ?array
    {
        $stmt = $conn->prepare(
            'SELECT job_name, last_run_at, last_status, last_message, last_result, updated_at'
            . ' FROM system_jobs WHERE job_name = ? LIMIT 1'
        );

        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de jobs: ' . $conn->error);
        }

        if (!$stmt->bind_param('s', $jobName)) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudieron enlazar los parámetros de jobs: ' . $error);
        }

        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo obtener el estado del job: ' . $error);
        }

        $result = $stmt->get_result();
        if ($result === false) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo leer el estado del job: ' . $error);
        }

        $row = $result->fetch_assoc() ?: null;
        $result->free();
        $stmt->close();

        return $row;
    }
}

if (!function_exists('tenant_notifications_jobs_should_run')) {
    /**
     * Determina si ha transcurrido el intervalo mínimo desde la última ejecución del job.
     */
    function tenant_notifications_jobs_should_run(mysqli $conn, string $jobName, int $minIntervalSeconds, ?DateTimeImmutable $now = null): bool
    {
        if ($minIntervalSeconds <= 0) {
            return true;
        }

        $now = $now ?? new DateTimeImmutable();
        $state = tenant_notifications_jobs_get($conn, $jobName);
        if (!$state) {
            return true;
        }

        $lastRunAt = $state['last_run_at'] ?? null;
        if (!$lastRunAt) {
            return true;
        }

        try {
            $lastRun = new DateTimeImmutable((string) $lastRunAt);
        } catch (Exception $e) {
            return true;
        }

        if (strtolower((string) ($state['last_status'] ?? '')) === 'error') {
            return true;
        }

        $elapsed = $now->getTimestamp() - $lastRun->getTimestamp();
        return $elapsed >= $minIntervalSeconds;
    }
}
