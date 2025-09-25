<?php

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/mail_helper.php';

if (!function_exists('mensajeria_role_alias')) {
    function mensajeria_role_alias(): string
    {
        return session_role_alias() ?? '';
    }
}

if (!function_exists('mensajeria_is_support')) {
    function mensajeria_is_support(string $alias): bool
    {
        return $alias !== '' && $alias !== 'cliente';
    }
}

if (!function_exists('mensajeria_resolve_empresa_id')) {
    function mensajeria_resolve_empresa_id(?int $prefered = null): ?int
    {
        $alias = mensajeria_role_alias();
        if ($alias === 'cliente') {
            return obtenerEmpresaId();
        }

        if ($prefered !== null && $prefered > 0) {
            return $prefered;
        }

        $requested = requested_empresa_id();
        if ($requested !== null && $requested > 0) {
            return $requested;
        }

        $sessionEmpresa = ensure_session_empresa_id();
        if ($sessionEmpresa !== null && $sessionEmpresa > 0) {
            return $sessionEmpresa;
        }

        return null;
    }
}

if (!function_exists('mensajeria_storage_dir')) {
    function mensajeria_storage_dir(): string
    {
        static $dir = null;
        if ($dir !== null) {
            return $dir;
        }

        $base = dirname(__DIR__, 3) . '/storage/mensajeria';
        if (!is_dir($base)) {
            @mkdir($base, 0755, true);
        }
        $dir = $base;
        return $dir;
    }
}

if (!function_exists('mensajeria_save_attachments')) {
    function mensajeria_save_attachments(string $field = 'adjuntos'): array
    {
        if (!isset($_FILES[$field])) {
            return [];
        }

        $files = $_FILES[$field];
        if (!is_array($files) || !isset($files['name'])) {
            return [];
        }

        $names = (array) $files['name'];
        $tmpNames = (array) ($files['tmp_name'] ?? []);
        $errors = (array) ($files['error'] ?? []);
        $sizes = (array) ($files['size'] ?? []);

        $maxAttachments = 5;
        $maxAttachmentSize = 10 * 1024 * 1024;
        $allowedExtensions = [
            'pdf'  => ['application/pdf'],
            'jpg'  => ['image/jpeg'],
            'jpeg' => ['image/jpeg'],
            'png'  => ['image/png'],
            'gif'  => ['image/gif'],
            'bmp'  => ['image/bmp'],
            'tif'  => ['image/tiff', 'image/x-tiff'],
            'tiff' => ['image/tiff', 'image/x-tiff'],
            'webp' => ['image/webp'],
            'doc'  => ['application/msword', 'application/octet-stream'],
            'docx' => ['application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'application/zip'],
            'xls'  => ['application/vnd.ms-excel', 'application/octet-stream'],
            'xlsx' => ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/zip'],
            'txt'  => ['text/plain'],
            'csv'  => ['text/csv', 'text/plain'],
            'zip'  => ['application/zip', 'application/x-zip-compressed', 'multipart/x-zip'],
            'rar'  => ['application/x-rar', 'application/x-rar-compressed', 'application/vnd.rar'],
            '7z'   => ['application/x-7z-compressed'],
        ];

        $selected = 0;
        $saved = [];
        $uploadDir = mensajeria_storage_dir();
        $finfo = class_exists('finfo') ? new finfo(FILEINFO_MIME_TYPE) : null;

        foreach ($names as $index => $originalName) {
            $error = $errors[$index] ?? UPLOAD_ERR_NO_FILE;
            if ($error === UPLOAD_ERR_NO_FILE) {
                continue;
            }

            $selected++;
            if ($selected > $maxAttachments) {
                throw new RuntimeException('Se permiten máximo ' . $maxAttachments . ' archivos adjuntos.');
            }

            if ($error !== UPLOAD_ERR_OK) {
                throw new RuntimeException('Ocurrió un error al cargar uno de los archivos adjuntos.');
            }

            $tmpName = $tmpNames[$index] ?? '';
            if (!is_uploaded_file($tmpName)) {
                throw new RuntimeException('Archivo adjunto no válido.');
            }

            $size = isset($sizes[$index]) ? (int) $sizes[$index] : 0;
            if ($size > $maxAttachmentSize) {
                throw new RuntimeException('Cada archivo debe ser menor a 10 MB.');
            }

            $extension = strtolower((string) pathinfo((string) $originalName, PATHINFO_EXTENSION));
            if ($extension === '' || !array_key_exists($extension, $allowedExtensions)) {
                throw new RuntimeException('Tipo de archivo no permitido.');
            }

            $mimeType = null;
            if ($finfo instanceof finfo) {
                $detected = $finfo->file($tmpName);
                if ($detected === false) {
                    throw new RuntimeException('No se pudo validar el archivo adjunto.');
                }
                if (!in_array($detected, $allowedExtensions[$extension], true)) {
                    throw new RuntimeException('El archivo adjunto no coincide con un tipo permitido.');
                }
                $mimeType = $detected;
            }

            if (!is_dir($uploadDir)) {
                if (!mkdir($uploadDir, 0755, true) && !is_dir($uploadDir)) {
                    throw new RuntimeException('No se pudo preparar el directorio de carga.');
                }
            }

            try {
                $randomName = bin2hex(random_bytes(16));
            } catch (Exception $e) {
                throw new RuntimeException('No se pudo preparar el archivo adjunto.');
            }
            $storedName = $randomName . '.' . $extension;
            $targetPath = rtrim($uploadDir, '/\\') . DIRECTORY_SEPARATOR . $storedName;

            if (!move_uploaded_file($tmpName, $targetPath)) {
                throw new RuntimeException('No se pudo guardar uno de los adjuntos.');
            }

            $cleanOriginal = trim(preg_replace('/[\r\n\t]+/', ' ', (string) $originalName) ?? '');
            if ($cleanOriginal === '') {
                $cleanOriginal = $storedName;
            }

            $saved[] = [
                'original_name' => $cleanOriginal,
                'stored_name' => $storedName,
                'mime_type' => $mimeType,
                'size' => $size,
                'path' => $targetPath,
            ];
        }

        return $saved;
    }
}

if (!function_exists('mensajeria_strip_attachment_paths')) {
    function mensajeria_strip_attachment_paths(array $attachments): array
    {
        return array_values(array_map(static function (array $item): array {
            return [
                'original_name' => (string) ($item['original_name'] ?? ''),
                'stored_name' => (string) ($item['stored_name'] ?? ''),
                'mime_type' => $item['mime_type'] ?? null,
                'size' => isset($item['size']) ? (int) $item['size'] : null,
            ];
        }, $attachments));
    }
}

if (!function_exists('mensajeria_cleanup_attachments')) {
    function mensajeria_cleanup_attachments(array $attachments): void
    {
        foreach ($attachments as $item) {
            $path = $item['path'] ?? null;
            if ($path && is_file($path)) {
                @unlink($path);
            }
        }
    }
}

if (!function_exists('mensajeria_decode_attachments')) {
    function mensajeria_decode_attachments(?string $payload): array
    {
        if ($payload === null || $payload === '') {
            return [];
        }
        $decoded = json_decode($payload, true);
        if (!is_array($decoded)) {
            return [];
        }
        return array_values(array_filter(array_map(static function ($item) {
            if (!is_array($item)) {
                return null;
            }
            if (empty($item['stored_name']) || empty($item['original_name'])) {
                return null;
            }
            return [
                'original_name' => (string) $item['original_name'],
                'stored_name' => (string) $item['stored_name'],
                'mime_type' => isset($item['mime_type']) ? (string) $item['mime_type'] : null,
                'size' => isset($item['size']) ? (int) $item['size'] : null,
            ];
        }, $decoded)));
    }
}

if (!function_exists('mensajeria_present_attachments')) {
    function mensajeria_present_attachments(int $messageId, array $attachments): array
    {
        $present = [];
        foreach ($attachments as $index => $attachment) {
            $present[] = [
                'nombre' => (string) ($attachment['original_name'] ?? ''),
                'size' => isset($attachment['size']) ? (int) $attachment['size'] : null,
                'mime_type' => $attachment['mime_type'] ?? null,
                'download_url' => '/backend/mensajeria/download_attachment.php?message_id=' . $messageId . '&index=' . $index,
            ];
        }
        return $present;
    }
}

if (!function_exists('mensajeria_author_type')) {
    function mensajeria_author_type(string $alias): string
    {
        return $alias === 'cliente' ? 'tenant' : 'soporte';
    }
}

if (!function_exists('mensajeria_normalize_message')) {
    function mensajeria_normalize_message(array $row): array
    {
        $messageId = (int) ($row['id'] ?? 0);
        $attachments = mensajeria_decode_attachments($row['adjuntos'] ?? null);
        return [
            'id' => $messageId,
            'conversation_id' => (int) ($row['conversation_id'] ?? 0),
            'autor_id' => isset($row['autor_id']) ? (int) $row['autor_id'] : null,
            'autor_nombre' => (string) ($row['autor_nombre'] ?? ($row['autor'] ?? '')),
            'autor_tipo' => (string) ($row['autor_tipo'] ?? ''),
            'mensaje' => (string) ($row['mensaje'] ?? ''),
            'leido_empresa' => isset($row['leido_empresa']) ? (bool) $row['leido_empresa'] : false,
            'created_at' => (string) ($row['created_at'] ?? ''),
            'adjuntos' => mensajeria_present_attachments($messageId, $attachments),
        ];
    }
}

if (!function_exists('mensajeria_fetch_conversation')) {
    function mensajeria_fetch_conversation(mysqli $conn, int $conversationId): ?array
    {
        $sql = "SELECT id, empresa_id, asunto, estado, created_at, updated_at FROM tenant_conversations WHERE id = ? LIMIT 1";
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return null;
        }
        $stmt->bind_param('i', $conversationId);
        if (!$stmt->execute()) {
            $stmt->close();
            return null;
        }
        $result = $stmt->get_result();
        $conversation = $result ? $result->fetch_assoc() : null;
        $stmt->close();
        return $conversation ?: null;
    }
}

if (!function_exists('mensajeria_conversation_accessible')) {
    function mensajeria_conversation_accessible(array $conversation, string $alias, ?int $empresaId): bool
    {
        $empresaConversacion = isset($conversation['empresa_id']) ? (int) $conversation['empresa_id'] : 0;
        if ($empresaConversacion <= 0) {
            return false;
        }
        if ($alias === 'cliente') {
            return $empresaId !== null && $empresaId === $empresaConversacion;
        }
        if ($empresaId !== null && $empresaId > 0) {
            return $empresaId === $empresaConversacion;
        }
        return true;
    }
}

if (!function_exists('mensajeria_count_unread')) {
    function mensajeria_count_unread(mysqli $conn, int $empresaId): array
    {
        $sql = "SELECT
                    COALESCE(SUM(CASE WHEN tm.autor_tipo = 'soporte' AND tm.leido_empresa = 0 THEN 1 ELSE 0 END), 0) AS mensajes,
                    COUNT(DISTINCT CASE WHEN tm.autor_tipo = 'soporte' AND tm.leido_empresa = 0 THEN tm.conversation_id END) AS conversaciones
                FROM tenant_conversations tc
                LEFT JOIN tenant_messages tm ON tm.conversation_id = tc.id
                WHERE tc.empresa_id = ?";
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return ['mensajes' => 0, 'conversaciones' => 0];
        }
        $stmt->bind_param('i', $empresaId);
        if (!$stmt->execute()) {
            $stmt->close();
            return ['mensajes' => 0, 'conversaciones' => 0];
        }
        $result = $stmt->get_result();
        $counts = $result ? $result->fetch_assoc() : null;
        $stmt->close();
        return [
            'mensajes' => isset($counts['mensajes']) ? (int) $counts['mensajes'] : 0,
            'conversaciones' => isset($counts['conversaciones']) ? (int) $counts['conversaciones'] : 0,
        ];
    }
}

if (!function_exists('mensajeria_notify_new_support_message')) {
    function mensajeria_notify_new_support_message(mysqli $conn, array $conversation, array $message): void
    {
        if (!function_exists('mail_helper_send')) {
            return;
        }

        $empresaId = isset($conversation['empresa_id']) ? (int) $conversation['empresa_id'] : 0;
        if ($empresaId <= 0) {
            return;
        }

        $empresaNombre = null;
        $empresaEmail = null;
        $empresaContacto = null;

        if ($stmt = $conn->prepare('SELECT nombre, contacto, email FROM empresas WHERE id = ? LIMIT 1')) {
            $stmt->bind_param('i', $empresaId);
            if ($stmt->execute()) {
                $res = $stmt->get_result();
                if ($res) {
                    $empresaData = $res->fetch_assoc();
                    if ($empresaData) {
                        $empresaNombre = $empresaData['nombre'] ?? null;
                        $empresaContacto = $empresaData['contacto'] ?? null;
                        $empresaEmail = $empresaData['email'] ?? null;
                    }
                }
            }
            $stmt->close();
        }

        $recipients = [];
        if ($empresaEmail) {
            $recipients[$empresaEmail] = $empresaContacto ?: ($empresaNombre ?: $empresaEmail);
        }

        if ($stmt = $conn->prepare("SELECT nombre, correo FROM usuarios WHERE empresa_id = ? AND activo = 1 AND correo IS NOT NULL AND correo <> ''")) {
            $stmt->bind_param('i', $empresaId);
            if ($stmt->execute()) {
                $res = $stmt->get_result();
                while ($row = $res->fetch_assoc()) {
                    $correo = $row['correo'] ?? null;
                    if (!$correo) {
                        continue;
                    }
                    $recipients[$correo] = $row['nombre'] ?? $correo;
                }
            }
            $stmt->close();
        }

        if (!$recipients) {
            return;
        }

        $subject = 'Nuevo mensaje de soporte: ' . (string) ($conversation['asunto'] ?? ('Conversación #' . ($conversation['id'] ?? '')));
        $mensaje = (string) ($message['mensaje'] ?? '');
        $autor = (string) ($message['autor_nombre'] ?? 'Equipo de soporte');
        $fecha = $message['created_at'] ?? date('Y-m-d H:i:s');

        $mensajeHtml = '<p>Hola' . ($empresaNombre ? ' ' . htmlspecialchars($empresaNombre) : '') . ',</p>' .
            '<p>El equipo de soporte ha enviado un nuevo mensaje en la conversación <strong>' .
            htmlspecialchars((string) ($conversation['asunto'] ?? '')) . '</strong>.</p>' .
            '<p><strong>Autor:</strong> ' . htmlspecialchars($autor) . '<br>' .
            '<strong>Fecha:</strong> ' . htmlspecialchars($fecha) . '</p>' .
            '<blockquote style="border-left:4px solid #0d575a;padding-left:12px;color:#333;">' .
            nl2br(htmlspecialchars($mensaje)) . '</blockquote>' .
            '<p>Ingresa al portal para responder cuanto antes.</p>';

        $mensajeTexto = "Nuevo mensaje en la conversación '" . ($conversation['asunto'] ?? '') . "'\n" .
            "Autor: {$autor}\n" .
            "Fecha: {$fecha}\n\n" .
            $mensaje . "\n\nIngresa al portal para continuar la conversación.";

        foreach ($recipients as $email => $name) {
            try {
                mail_helper_send((string) $email, (string) $name, $subject, $mensajeHtml, $mensajeTexto);
            } catch (Throwable $e) {
                // Ignorar fallos individuales de envío para no interrumpir el flujo principal.
                continue;
            }
        }
    }
}
