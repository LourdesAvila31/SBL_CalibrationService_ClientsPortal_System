<?php

require_once __DIR__ . '/tenant_notifications.php';

if (!function_exists('calibration_nonconformities_ensure_table')) {
    function calibration_nonconformities_ensure_table(mysqli $conn): void
    {
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS calibraciones_no_conformidades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT NOT NULL,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    estado ENUM('abierta','en_proceso','cerrada') NOT NULL DEFAULT 'abierta',
    descripcion TEXT NOT NULL,
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at DATETIME NULL,
    notified_at DATETIME NULL,
    corrective_url VARCHAR(255) DEFAULT NULL,
    UNIQUE KEY uniq_calibracion (calibracion_id),
    KEY idx_empresa (empresa_id),
    KEY idx_estado (estado),
    CONSTRAINT fk_nc_calibracion FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE CASCADE,
    CONSTRAINT fk_nc_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE,
    CONSTRAINT fk_nc_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
SQL;

        if (!$conn->query($sql)) {
            throw new RuntimeException('No se pudo asegurar la tabla de no conformidades: ' . $conn->error);
        }
    }
}

if (!function_exists('calibration_nonconformities_should_flag')) {
    /**
     * Determina si los datos recibidos indican una no conformidad.
     *
     * @param string $resultado Resultado registrado de la calibración.
     * @param string|null $observaciones Observaciones capturadas durante la calibración.
     * @param array<int,string> $extraTexts Textos adicionales a evaluar.
     *
     * @return array{descripcion:string,fuente:string}|null
     */
    function calibration_nonconformities_should_flag(string $resultado, ?string $observaciones = null, array $extraTexts = []): ?array
    {
        $resultadoNorm = mb_strtolower(trim($resultado), 'UTF-8');
        if ($resultadoNorm !== '') {
            if ($resultadoNorm === 'no conforme' || str_contains($resultadoNorm, 'no conform')) {
                return [
                    'descripcion' => 'Resultado registrado como "No conforme".',
                    'fuente' => 'resultado',
                ];
            }
        }

        $targets = [];
        if ($observaciones !== null) {
            $targets[] = $observaciones;
        }
        foreach ($extraTexts as $text) {
            if ($text !== null && $text !== '') {
                $targets[] = $text;
            }
        }

        foreach ($targets as $text) {
            $normalized = mb_strtolower($text, 'UTF-8');
            if (str_contains($normalized, 'fuera de tolerancia') || str_contains($normalized, 'fuera de especificación')) {
                return [
                    'descripcion' => 'Se detectó la frase "fuera de tolerancia/especificación" en las observaciones.',
                    'fuente' => 'observaciones',
                ];
            }
        }

        return null;
    }
}

if (!function_exists('calibration_nonconformities_register')) {
    /**
     * Registra o actualiza una no conformidad asociada a la calibración y dispara notificaciones.
     *
     * @param callable|null $logger Callback opcional para registrar mensajes.
     *
     * @return array<string,mixed>|null
     */
    function calibration_nonconformities_register(
        mysqli $conn,
        int $calibracionId,
        int $instrumentoId,
        int $empresaId,
        string $resultado,
        ?string $observaciones,
        array $extraTexts = [],
        ?string $fechaCalibracion = null,
        ?callable $logger = null
    ): ?array {
        calibration_nonconformities_ensure_table($conn);

        $analysis = calibration_nonconformities_should_flag($resultado, $observaciones, $extraTexts);
        $log = $logger ?? static function (string $level, string $message): void {
            error_log('[nc-calibraciones][' . strtoupper($level) . '] ' . $message);
        };

        $existingStmt = $conn->prepare('SELECT id, estado, descripcion, notified_at FROM calibraciones_no_conformidades WHERE calibracion_id = ? LIMIT 1');
        if (!$existingStmt) {
            throw new RuntimeException('No se pudo consultar las no conformidades existentes: ' . $conn->error);
        }

        if (!$existingStmt->bind_param('i', $calibracionId)) {
            $error = $existingStmt->error;
            $existingStmt->close();
            throw new RuntimeException('No se pudo enlazar la consulta de no conformidades: ' . $error);
        }

        if (!$existingStmt->execute()) {
            $error = $existingStmt->error;
            $existingStmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta de no conformidades: ' . $error);
        }

        $existing = $existingStmt->get_result();
        $existingRow = $existing ? $existing->fetch_assoc() : null;
        if ($existing) {
            $existing->free();
        }
        $existingStmt->close();

        if ($analysis === null) {
            if ($existingRow && $existingRow['estado'] !== 'cerrada') {
                $closeStmt = $conn->prepare('UPDATE calibraciones_no_conformidades SET estado = ?, resolved_at = NOW() WHERE id = ?');
                if ($closeStmt) {
                    $estado = 'cerrada';
                    $closeStmt->bind_param('si', $estado, $existingRow['id']);
                    $closeStmt->execute();
                    $closeStmt->close();
                }
            }
            return null;
        }

        $descripcion = $analysis['descripcion'];
        $shouldNotify = false;
        $nonConformityId = null;

        if ($existingRow) {
            $nonConformityId = (int) $existingRow['id'];
            $updateStmt = $conn->prepare('UPDATE calibraciones_no_conformidades SET estado = ?, descripcion = ?, resolved_at = NULL WHERE id = ?');
            if (!$updateStmt) {
                throw new RuntimeException('No se pudo actualizar la no conformidad existente: ' . $conn->error);
            }

            $estado = 'abierta';
            $updateStmt->bind_param('ssi', $estado, $descripcion, $nonConformityId);
            if (!$updateStmt->execute()) {
                $error = $updateStmt->error;
                $updateStmt->close();
                throw new RuntimeException('No se pudo guardar la no conformidad existente: ' . $error);
            }
            $updateStmt->close();

            if ($existingRow['estado'] === 'cerrada' || $existingRow['descripcion'] !== $descripcion || $existingRow['notified_at'] === null) {
                $shouldNotify = true;
            }
        } else {
            $insertStmt = $conn->prepare('INSERT INTO calibraciones_no_conformidades (calibracion_id, instrumento_id, empresa_id, descripcion) VALUES (?, ?, ?, ?)');
            if (!$insertStmt) {
                throw new RuntimeException('No se pudo crear el registro de no conformidad: ' . $conn->error);
            }

            $insertStmt->bind_param('iiis', $calibracionId, $instrumentoId, $empresaId, $descripcion);
            if (!$insertStmt->execute()) {
                $error = $insertStmt->error;
                $insertStmt->close();
                throw new RuntimeException('No se pudo guardar la no conformidad: ' . $error);
            }
            $nonConformityId = $insertStmt->insert_id;
            $insertStmt->close();
            $shouldNotify = true;
        }

        if (!$shouldNotify || !$nonConformityId) {
            return [
                'id' => $nonConformityId,
                'descripcion' => $descripcion,
            ];
        }

        try {
            $context = tenant_notifications_fetch_instrument_context($conn, $instrumentoId, $empresaId) ?? [];
        } catch (Throwable $e) {
            $log('error', 'No se pudo obtener el contexto del instrumento: ' . $e->getMessage());
            $context = [];
        }

        try {
            $recipients = tenant_notifications_fetch_company_recipients($conn, $empresaId, ['Superadministrador', 'Administrador', 'Supervisor']);
        } catch (Throwable $e) {
            $log('error', 'No se pudo obtener la lista de destinatarios para la no conformidad: ' . $e->getMessage());
            $recipients = [];
        }

        if (!$recipients) {
            $log('warning', 'No se encontraron destinatarios para notificar la no conformidad.');
        } else {
            $instrumentoNombre = $context['instrumento_nombre'] ?? ('Instrumento #' . $instrumentoId);
            $empresaNombre = $context['empresa_nombre'] ?? ('Empresa #' . $empresaId);
            $codigo = $context['instrumento_codigo'] ?? '';
            $fechaTexto = $fechaCalibracion ?: 'sin fecha registrada';
            $subject = sprintf('No conformidad en calibración de %s', $instrumentoNombre);
            $link = '/SISTEMA-COMPUTARIZADO-ISO-17025/public/apps/tenant/calibraciones/non_conformity_flow.html?nc_id=' . $nonConformityId . '&calibracion_id=' . $calibracionId;

            $htmlBody = '<p>Se detectó una no conformidad en una calibración reciente.</p>'
                . '<ul>'
                . '<li><strong>Empresa:</strong> ' . htmlspecialchars($empresaNombre, ENT_QUOTES, 'UTF-8') . '</li>'
                . '<li><strong>Instrumento:</strong> ' . htmlspecialchars($instrumentoNombre, ENT_QUOTES, 'UTF-8') . '</li>'
                . ($codigo !== '' ? '<li><strong>Código interno:</strong> ' . htmlspecialchars($codigo, ENT_QUOTES, 'UTF-8') . '</li>' : '')
                . '<li><strong>Fecha de calibración:</strong> ' . htmlspecialchars($fechaTexto, ENT_QUOTES, 'UTF-8') . '</li>'
                . '<li><strong>Resultado:</strong> ' . htmlspecialchars($resultado, ENT_QUOTES, 'UTF-8') . '</li>'
                . '<li><strong>Descripción:</strong> ' . htmlspecialchars($descripcion, ENT_QUOTES, 'UTF-8') . '</li>'
                . '</ul>'
                . '<p>Revisa y atiende la no conformidad en el flujo correctivo: '
                . '<a href="' . htmlspecialchars($link, ENT_QUOTES, 'UTF-8') . '">' . htmlspecialchars($link, ENT_QUOTES, 'UTF-8') . '</a></p>';

            $textBody = "Se detectó una no conformidad en una calibración.\n"
                . 'Empresa: ' . $empresaNombre . "\n"
                . 'Instrumento: ' . $instrumentoNombre . ($codigo !== '' ? ' (' . $codigo . ')' : '') . "\n"
                . 'Fecha de calibración: ' . $fechaTexto . "\n"
                . 'Resultado: ' . $resultado . "\n"
                . 'Descripción: ' . $descripcion . "\n"
                . 'Flujo correctivo: ' . $link . "\n";

            try {
                tenant_notifications_send_bulk_mail($recipients, $subject, $htmlBody, $textBody, $log);
            } catch (Throwable $e) {
                $log('error', 'No se pudieron enviar los correos de la no conformidad: ' . $e->getMessage());
            }
        }

        $updateNotify = $conn->prepare('UPDATE calibraciones_no_conformidades SET notified_at = NOW(), corrective_url = ? WHERE id = ?');
        if ($updateNotify) {
            $linkValue = '/SISTEMA-COMPUTARIZADO-ISO-17025/public/apps/tenant/calibraciones/non_conformity_flow.html?nc_id=' . $nonConformityId . '&calibracion_id=' . $calibracionId;
            $updateNotify->bind_param('si', $linkValue, $nonConformityId);
            $updateNotify->execute();
            $updateNotify->close();
        }

        return [
            'id' => $nonConformityId,
            'descripcion' => $descripcion,
        ];
    }
}

if (!function_exists('calibration_nonconformities_get_by_calibration')) {
    function calibration_nonconformities_get_by_calibration(mysqli $conn, int $calibracionId): ?array
    {
        calibration_nonconformities_ensure_table($conn);

        $stmt = $conn->prepare('SELECT id, estado, descripcion, detected_at, resolved_at, corrective_url FROM calibraciones_no_conformidades WHERE calibracion_id = ? LIMIT 1');
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de no conformidades: ' . $conn->error);
        }

        $stmt->bind_param('i', $calibracionId);
        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta de no conformidades: ' . $error);
        }

        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        if ($result) {
            $result->free();
        }
        $stmt->close();

        return $row ?: null;
    }
}

if (!function_exists('calibration_nonconformities_get')) {
    function calibration_nonconformities_get(mysqli $conn, int $nonConformityId, int $empresaId): ?array
    {
        calibration_nonconformities_ensure_table($conn);
        $sql = 'SELECT nc.id, nc.calibracion_id, nc.estado, nc.descripcion, nc.detected_at, nc.resolved_at, nc.corrective_url, '
            . 'c.fecha_calibracion, c.fecha_proxima, c.resultado, '
            . 'e.nombre AS empresa_nombre, '
            . 'i.id AS instrumento_id, i.codigo AS instrumento_codigo, '
            . 'COALESCE(ci.nombre, i.codigo, CONCAT("Instrumento #", i.id)) AS instrumento_nombre '
            . 'FROM calibraciones_no_conformidades nc '
            . 'JOIN calibraciones c ON c.id = nc.calibracion_id '
            . 'JOIN instrumentos i ON i.id = nc.instrumento_id '
            . 'JOIN empresas e ON e.id = nc.empresa_id '
            . 'LEFT JOIN catalogo_instrumentos ci ON ci.id = i.catalogo_id '
            . 'WHERE nc.id = ? AND nc.empresa_id = ? LIMIT 1';

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta del detalle de no conformidad: ' . $conn->error);
        }

        $stmt->bind_param('ii', $nonConformityId, $empresaId);
        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('No se pudo ejecutar la consulta del detalle de no conformidad: ' . $error);
        }

        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        if ($result) {
            $result->free();
        }
        $stmt->close();

        return $row ?: null;
    }
}
