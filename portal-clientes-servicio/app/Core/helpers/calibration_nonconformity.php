<?php

declare(strict_types=1);

require_once __DIR__ . '/instrument_status.php';
require_once __DIR__ . '/mail_helper.php';
require_once __DIR__ . '/company.php';

/**
 * Procesa la creación o cierre de no conformidades asociadas a calibraciones.
 *
 * @param array $contexto {
 *     @var int         $calibracion_id
 *     @var int         $instrumento_id
 *     @var int         $empresa_id
 *     @var string|null $resultado
 *     @var string|null $notes
 * }
 * @param callable      $repositorio    Función que atiende acciones de almacenamiento.
 * @param callable      $envioCorreo    Función responsable de enviar correos.
 * @param callable      $resolverContacto Función que retorna la información del responsable de calidad.
 * @param callable      $resolverInstrumento Función que retorna datos descriptivos del instrumento.
 * @param callable      $ahoraProvider  Generador de fecha/hora actual.
 */
function calibration_nonconformity_process(
    array $contexto,
    callable $repositorio,
    callable $envioCorreo,
    callable $resolverContacto,
    callable $resolverInstrumento,
    callable $ahoraProvider
): void {
    $resultado = trim((string) ($contexto['resultado'] ?? ''));
    $calibracionId = isset($contexto['calibracion_id']) ? (int) $contexto['calibracion_id'] : 0;

    if (!esTextoRechazado($resultado)) {
        if ($calibracionId > 0) {
            $repositorio('close', ['calibracion_id' => $calibracionId]);
        }
        return;
    }

    $ahora = $ahoraProvider();
    if (!$ahora instanceof DateTimeInterface) {
        $ahora = new DateTimeImmutable('now');
    }
    $ahoraTexto = $ahora->format('Y-m-d H:i:s');

    if ($calibracionId <= 0) {
        return;
    }

    $instrumentoId = (int) ($contexto['instrumento_id'] ?? 0);
    $empresaId = (int) ($contexto['empresa_id'] ?? 0);
    $notas = $contexto['notes'] ?? null;

    $registro = $repositorio('upsert', [
        'calibracion_id' => $calibracionId,
        'instrumento_id' => $instrumentoId,
        'empresa_id' => $empresaId,
        'notes' => $notas,
        'detected_at' => $ahoraTexto,
    ]);

    $registroId = is_array($registro) ? (int) ($registro['id'] ?? 0) : 0;
    $contacto = $resolverContacto($empresaId);
    if (!$contacto || !filter_var($contacto['email'] ?? null, FILTER_VALIDATE_EMAIL)) {
        return;
    }

    $instrumento = $resolverInstrumento($instrumentoId, $empresaId) ?? [];

    [$asunto, $html, $textoPlano] = calibration_nonconformity_compose_message($instrumento, $contexto, $ahora);

    $envioCorreo(
        (string) $contacto['email'],
        trim((string) ($contacto['nombre'] ?? $contacto['email'])),
        $asunto,
        $html,
        $textoPlano
    );

    if ($registroId > 0) {
        $repositorio('mark_notified', [
            'id' => $registroId,
            'notified_at' => $ahoraTexto,
        ]);
    }
}

/**
 * Maneja las operaciones de no conformidad contra la base de datos.
 */
function calibration_nonconformity_handle(
    mysqli $conn,
    array $contexto,
    ?callable $envioCorreo = null,
    ?callable $ahoraProvider = null
): void {
    $envioCorreo = $envioCorreo ?? static function (string $correo, string $nombre, string $asunto, string $html, ?string $texto): void {
        mail_helper_send($correo, $nombre, $asunto, $html, $texto);
    };

    $ahoraProvider = $ahoraProvider ?? static function (): DateTimeImmutable {
        return new DateTimeImmutable('now');
    };

    $repositorio = static function (string $accion, array $payload = []) use ($conn) {
        switch ($accion) {
            case 'close':
                $calibracionId = (int) ($payload['calibracion_id'] ?? 0);
                if ($calibracionId <= 0) {
                    return null;
                }
                if ($stmt = $conn->prepare("UPDATE calibration_nonconformities SET estado = 'cerrada' WHERE calibracion_id = ?")) {
                    $stmt->bind_param('i', $calibracionId);
                    $stmt->execute();
                    $stmt->close();
                }
                return null;
            case 'mark_notified':
                $registroId = (int) ($payload['id'] ?? 0);
                if ($registroId <= 0) {
                    return null;
                }
                $notifiedAt = (string) ($payload['notified_at'] ?? date('Y-m-d H:i:s'));
                if ($stmt = $conn->prepare('UPDATE calibration_nonconformities SET notified_at = ? WHERE id = ?')) {
                    $stmt->bind_param('si', $notifiedAt, $registroId);
                    $stmt->execute();
                    $stmt->close();
                }
                return null;
            case 'upsert':
                $calibracionId = (int) ($payload['calibracion_id'] ?? 0);
                $instrumentoId = (int) ($payload['instrumento_id'] ?? 0);
                $empresaId = (int) ($payload['empresa_id'] ?? 0);
                $notes = $payload['notes'] ?? null;
                $detectedAt = (string) ($payload['detected_at'] ?? date('Y-m-d H:i:s'));

                if ($calibracionId <= 0) {
                    return ['id' => 0];
                }

                $existingId = null;
                $existingDetected = null;

                $lookup = $conn->prepare('SELECT id, detected_at FROM calibration_nonconformities WHERE calibracion_id = ? LIMIT 1');
                if ($lookup) {
                    $lookup->bind_param('i', $calibracionId);
                    if ($lookup->execute()) {
                        $lookup->bind_result($idDb, $detectedDb);
                        if ($lookup->fetch()) {
                            $existingId = (int) $idDb;
                            $existingDetected = $detectedDb;
                        }
                    }
                    $lookup->close();
                }

                $notesParam = $notes !== null && trim((string) $notes) !== '' ? $notes : null;
                $detectedValue = $existingDetected ?: $detectedAt;

                if ($existingId !== null) {
                    $update = $conn->prepare('UPDATE calibration_nonconformities SET instrumento_id = ?, empresa_id = ?, estado = \'abierta\', notes = ?, detected_at = ? WHERE id = ?');
                    if ($update) {
                        $update->bind_param('iissi', $instrumentoId, $empresaId, $notesParam, $detectedValue, $existingId);
                        $update->execute();
                        $update->close();
                    }
                    return ['id' => $existingId];
                }

                $insert = $conn->prepare('INSERT INTO calibration_nonconformities (calibracion_id, instrumento_id, empresa_id, estado, detected_at, notes) VALUES (?, ?, ?, \'abierta\', ?, ?)');
                if ($insert) {
                    $insert->bind_param('iiiss', $calibracionId, $instrumentoId, $empresaId, $detectedAt, $notesParam);
                    $insert->execute();
                    $nuevoId = $insert->insert_id;
                    $insert->close();
                    return ['id' => (int) $nuevoId];
                }

                return ['id' => $existingId ?? 0];
            default:
                return null;
        }
    };

    $resolverContacto = static function (int $empresaId) use ($conn) {
        return company_get_quality_responsible_contact($conn, $empresaId);
    };

    $resolverInstrumento = static function (int $instrumentoId, int $empresaId) use ($conn): array {
        $stmt = $conn->prepare('SELECT i.codigo, i.serie, ci.nombre AS nombre_instrumento, e.nombre AS nombre_empresa FROM instrumentos i LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id LEFT JOIN empresas e ON e.id = i.empresa_id WHERE i.id = ? AND i.empresa_id = ? LIMIT 1');
        if (!$stmt) {
            return [];
        }
        $stmt->bind_param('ii', $instrumentoId, $empresaId);
        if (!$stmt->execute()) {
            $stmt->close();
            return [];
        }
        $stmt->bind_result($codigo, $serie, $nombreInstrumento, $nombreEmpresa);
        $stmt->fetch();
        $stmt->close();
        return [
            'codigo' => $codigo,
            'serie' => $serie,
            'instrumento_nombre' => $nombreInstrumento,
            'empresa_nombre' => $nombreEmpresa,
        ];
    };

    calibration_nonconformity_process(
        $contexto,
        $repositorio,
        $envioCorreo,
        $resolverContacto,
        $resolverInstrumento,
        $ahoraProvider
    );
}

/**
 * Construye el contenido del mensaje de notificación para la no conformidad.
 */
function calibration_nonconformity_compose_message(array $instrumento, array $contexto, DateTimeInterface $detectedAt): array
{
    $nombreInstrumento = trim((string) ($instrumento['instrumento_nombre'] ?? 'Instrumento'));
    if ($nombreInstrumento === '') {
        $nombreInstrumento = 'Instrumento';
    }
    $codigo = trim((string) ($instrumento['codigo'] ?? ''));
    $serie = trim((string) ($instrumento['serie'] ?? ''));
    $empresaNombre = trim((string) ($instrumento['empresa_nombre'] ?? ''));

    $asunto = sprintf(
        'No conformidad detectada en %s%s',
        $nombreInstrumento,
        $codigo !== '' ? ' [' . $codigo . ']' : ''
    );

    $resultado = trim((string) ($contexto['resultado'] ?? ''));
    $notas = trim((string) ($contexto['notes'] ?? ''));
    $detectedAtText = $detectedAt->format('d/m/Y H:i');

    $listaDetalles = [
        'Empresa' => $empresaNombre !== '' ? $empresaNombre : 'Sin especificar',
        'Instrumento' => $nombreInstrumento,
        'Código' => $codigo !== '' ? $codigo : 'No asignado',
        'Serie' => $serie !== '' ? $serie : 'No asignada',
        'Resultado' => $resultado !== '' ? $resultado : 'Sin resultado',
        'Detectado el' => $detectedAtText,
    ];

    if ($notas !== '') {
        $listaDetalles['Notas'] = $notas;
    }

    $html = '<p>Se registró una no conformidad asociada a una calibración reciente.</p><ul>';
    foreach ($listaDetalles as $clave => $valor) {
        $html .= '<li><strong>' . htmlspecialchars($clave, ENT_QUOTES, 'UTF-8') . ':</strong> ' . htmlspecialchars($valor, ENT_QUOTES, 'UTF-8') . '</li>';
    }
    $html .= '</ul><p>Por favor atiende esta no conformidad a la brevedad.</p>';

    $lineas = [];
    foreach ($listaDetalles as $clave => $valor) {
        $lineas[] = $clave . ': ' . $valor;
    }
    $textoPlano = "Se registró una no conformidad asociada a una calibración reciente." . PHP_EOL . implode(PHP_EOL, $lineas) . PHP_EOL . 'Por favor atiende esta no conformidad a la brevedad.';

    return [$asunto, $html, $textoPlano];
}
