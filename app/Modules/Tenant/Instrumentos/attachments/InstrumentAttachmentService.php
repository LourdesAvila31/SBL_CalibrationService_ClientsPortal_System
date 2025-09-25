<?php

class InstrumentAttachmentService
{
    public const STORAGE_RELATIVE_PATH = 'backend/instrumentos/attachments/files';
    public const MAX_FILE_SIZE = 15 * 1024 * 1024; // 15 MB

    private const ALLOWED_EXTENSIONS = [
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
        'txt'  => ['text/plain', 'application/octet-stream'],
        'csv'  => ['text/csv', 'text/plain', 'application/vnd.ms-excel'],
        'zip'  => ['application/zip', 'application/x-zip-compressed', 'multipart/x-zip'],
        'rar'  => ['application/x-rar', 'application/x-rar-compressed', 'application/vnd.rar'],
        '7z'   => ['application/x-7z-compressed'],
    ];

    /**
     * Devuelve la ruta física del directorio de almacenamiento.
     */
    public static function storageDirectory(): string
    {
        $base = dirname(__DIR__, 5) . '/public/' . self::STORAGE_RELATIVE_PATH;
        return rtrim($base, '/\\') . DIRECTORY_SEPARATOR;
    }

    /**
     * Asegura que el directorio de almacenamiento exista.
     */
    public static function ensureStorageDirectory(): void
    {
        $dir = self::storageDirectory();
        if (!is_dir($dir)) {
            if (!mkdir($dir, 0755, true) && !is_dir($dir)) {
                throw new RuntimeException('No se pudo crear el directorio de adjuntos.');
            }
        }
    }

    /**
     * Normaliza el nombre visible del archivo.
     */
    public static function sanitizeVisibleName(string $name): string
    {
        $trimmed = trim(str_replace(['\\', '/'], ' ', $name));
        $clean = preg_replace('/[\r\n\t\x00-\x1F\x7F]+/u', ' ', $trimmed);
        if ($clean === null || $clean === '') {
            return 'adjunto';
        }
        $clean = preg_replace('/\s{2,}/', ' ', $clean);
        $clean = $clean === null ? '' : trim($clean);
        if ($clean === '') {
            return 'adjunto';
        }
        if (function_exists('mb_substr')) {
            $clean = mb_substr($clean, 0, 255);
        } else {
            $clean = substr($clean, 0, 255);
        }
        return $clean === '' ? 'adjunto' : $clean;
    }

    public static function normalizeTipo(?string $tipo): ?string
    {
        if ($tipo === null) {
            return null;
        }
        $value = trim($tipo);
        if ($value === '') {
            return null;
        }
        $filtered = preg_replace('/[^\p{L}\p{N}\s._-]/u', '', $value);
        if ($filtered === null) {
            return null;
        }
        $filtered = trim($filtered);
        if ($filtered === '') {
            return null;
        }
        if (function_exists('mb_substr')) {
            $filtered = mb_substr($filtered, 0, 60);
        } else {
            $filtered = substr($filtered, 0, 60);
        }
        return $filtered === '' ? null : $filtered;
    }

    public static function normalizeDescripcion(?string $descripcion): ?string
    {
        if ($descripcion === null) {
            return null;
        }
        $value = trim($descripcion);
        if ($value === '') {
            return null;
        }
        $clean = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]+/u', ' ', $value);
        if ($clean === null) {
            return null;
        }
        $clean = trim($clean);
        if ($clean === '') {
            return null;
        }
        if (function_exists('mb_substr')) {
            $clean = mb_substr($clean, 0, 1000);
        } else {
            $clean = substr($clean, 0, 1000);
        }
        return $clean === '' ? null : $clean;
    }

    /**
     * Obtiene la empresa asociada al instrumento y valida pertenencia.
     */
    private static function assertInstrument(mysqli $conn, int $instrumentoId, int $empresaId): void
    {
        $stmt = $conn->prepare('SELECT empresa_id FROM instrumentos WHERE id = ? LIMIT 1');
        if (!$stmt) {
            throw new RuntimeException('No se pudo validar el instrumento.');
        }
        $stmt->bind_param('i', $instrumentoId);
        $stmt->execute();
        $stmt->bind_result($empresaInstrumento);
        if (!$stmt->fetch()) {
            $stmt->close();
            throw new RuntimeException('Instrumento no encontrado.');
        }
        $stmt->close();
        if ((int) $empresaInstrumento !== $empresaId) {
            throw new RuntimeException('El instrumento no pertenece a la empresa indicada.');
        }
    }

    private static function detectMime(string $path): ?string
    {
        if (!is_readable($path)) {
            return null;
        }
        if (class_exists('finfo')) {
            $finfo = new finfo(FILEINFO_MIME_TYPE);
            $detected = $finfo->file($path);
            if (is_string($detected) && $detected !== '') {
                return $detected;
            }
        }
        return null;
    }

    /**
     * @param array $fileInfo [tmp_path, original_name, size, mime]
     * @return array
     */
    public static function validateFile(array $fileInfo): array
    {
        $tmpPath = $fileInfo['tmp_path'] ?? '';
        $originalName = $fileInfo['original_name'] ?? '';
        $size = isset($fileInfo['size']) ? (int) $fileInfo['size'] : 0;
        $declaredMime = isset($fileInfo['mime']) && $fileInfo['mime'] !== '' ? (string) $fileInfo['mime'] : null;

        if (!is_string($tmpPath) || $tmpPath === '' || !file_exists($tmpPath)) {
            throw new RuntimeException('Archivo temporal inválido.');
        }

        if ($size <= 0) {
            $size = filesize($tmpPath) ?: 0;
        }

        if ($size <= 0) {
            throw new RuntimeException('El archivo adjunto está vacío.');
        }

        if ($size > self::MAX_FILE_SIZE) {
            throw new RuntimeException('El archivo excede el tamaño permitido (15 MB).');
        }

        $extension = strtolower((string) pathinfo($originalName, PATHINFO_EXTENSION));
        if ($extension === '' || !array_key_exists($extension, self::ALLOWED_EXTENSIONS)) {
            throw new RuntimeException('Tipo de archivo no permitido.');
        }

        $detectedMime = self::detectMime($tmpPath);
        $mime = $detectedMime ?: $declaredMime;
        $allowedMimes = self::ALLOWED_EXTENSIONS[$extension];
        if ($mime !== null && !in_array($mime, $allowedMimes, true)) {
            throw new RuntimeException('El archivo adjunto no coincide con el tipo esperado.');
        }

        return [
            'tmp_path' => $tmpPath,
            'original_name' => $originalName,
            'extension' => $extension,
            'size' => $size,
            'mime' => $mime,
        ];
    }

    /**
     * Inserta un adjunto nuevo y devuelve sus metadatos.
     */
    public static function createAttachment(
        mysqli $conn,
        int $instrumentoId,
        int $empresaId,
        array $fileInfo,
        ?string $tipo,
        ?string $descripcion,
        ?int $usuarioId
    ): array {
        self::assertInstrument($conn, $instrumentoId, $empresaId);
        $validated = self::validateFile($fileInfo);
        self::ensureStorageDirectory();

        $nombreVisible = self::sanitizeVisibleName($validated['original_name']);
        $tipo = self::normalizeTipo($tipo);
        $descripcion = self::normalizeDescripcion($descripcion);

        try {
            $random = bin2hex(random_bytes(16));
        } catch (Exception $e) {
            throw new RuntimeException('No se pudo generar un nombre seguro para el archivo.');
        }

        $storedName = $random . '.' . $validated['extension'];
        $storageDir = self::storageDirectory();
        $targetPath = $storageDir . $storedName;

        if (!self::moveFile($validated['tmp_path'], $targetPath)) {
            throw new RuntimeException('No se pudo guardar el archivo adjunto.');
        }

        $relativePath = self::STORAGE_RELATIVE_PATH . '/' . $storedName;
        $mime = $validated['mime'];
        $size = $validated['size'];

        $stmt = $conn->prepare('INSERT INTO instrumento_adjuntos (instrumento_id, empresa_id, nombre_visible, ruta_archivo, tipo, descripcion, usuario_id, mime_type, tamano) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
        if (!$stmt) {
            @unlink($targetPath);
            throw new RuntimeException('No se pudo preparar la inserción de adjuntos.');
        }

        $usuarioParam = $usuarioId !== null ? $usuarioId : null;
        $mimeParam = $mime !== null ? $mime : null;
        $sizeParam = $size > 0 ? $size : null;
        $stmt->bind_param(
            'iissssiss',
            $instrumentoId,
            $empresaId,
            $nombreVisible,
            $relativePath,
            $tipo,
            $descripcion,
            $usuarioParam,
            $mimeParam,
            $sizeParam
        );

        if (!$stmt->execute()) {
            $stmt->close();
            @unlink($targetPath);
            throw new RuntimeException('No se pudo registrar el adjunto.');
        }
        $stmt->close();

        $id = (int) $conn->insert_id;
        return self::fetchAttachment($conn, $id, $instrumentoId, $empresaId);
    }

    private static function moveFile(string $tmpPath, string $targetPath): bool
    {
        $directory = dirname($targetPath);
        if (!is_dir($directory)) {
            if (!mkdir($directory, 0755, true) && !is_dir($directory)) {
                return false;
            }
        }
        if (is_uploaded_file($tmpPath)) {
            return move_uploaded_file($tmpPath, $targetPath);
        }
        if (@rename($tmpPath, $targetPath)) {
            return true;
        }
        if (@copy($tmpPath, $targetPath)) {
            @unlink($tmpPath);
            return true;
        }
        return false;
    }

    private static function fetchAttachment(mysqli $conn, int $id, int $instrumentoId, int $empresaId): array
    {
        $sql = 'SELECT a.id, a.instrumento_id, a.empresa_id, a.nombre_visible, a.ruta_archivo, a.tipo, a.descripcion, a.usuario_id, '
            . 'a.mime_type, a.tamano, a.created_at, a.updated_at, u.usuario, CONCAT_WS(" ", u.nombre, u.apellidos) AS usuario_nombre, '
            . '(SELECT fi.firma_interna FROM usuario_firmas_internas fi '
            . '  WHERE fi.usuario_id = a.usuario_id '
            . '    AND fi.empresa_id = a.empresa_id '
            . '    AND fi.vigente_desde <= COALESCE(a.created_at, NOW()) '
            . '    AND (fi.vigente_hasta IS NULL OR fi.vigente_hasta > COALESCE(a.created_at, NOW())) '
            . '  ORDER BY fi.vigente_desde DESC LIMIT 1) AS usuario_firma_interna '
            . 'FROM instrumento_adjuntos a '
            . 'LEFT JOIN usuarios u ON a.usuario_id = u.id '
            . 'WHERE a.id = ? AND a.instrumento_id = ? AND a.empresa_id = ? LIMIT 1';
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo recuperar el adjunto.');
        }
        $stmt->bind_param('iii', $id, $instrumentoId, $empresaId);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        $stmt->close();
        if (!$row) {
            throw new RuntimeException('No se pudo recuperar el adjunto.');
        }
        return self::formatAttachmentRow($row);
    }

    private static function formatAttachmentRow(array $row): array
    {
        $relativePath = $row['ruta_archivo'] ?? '';
        $cleanRelative = ltrim((string) $relativePath, '/');
        $downloadUrl = '/' . $cleanRelative;
        return [
            'id' => (int) $row['id'],
            'instrumento_id' => (int) $row['instrumento_id'],
            'empresa_id' => (int) $row['empresa_id'],
            'nombre_visible' => $row['nombre_visible'] ?? '',
            'ruta_archivo' => $cleanRelative,
            'download_url' => $downloadUrl,
            'tipo' => $row['tipo'] ?? null,
            'descripcion' => $row['descripcion'] ?? null,
            'usuario_id' => $row['usuario_id'] !== null ? (int) $row['usuario_id'] : null,
            'usuario' => $row['usuario'] ?? null,
            'usuario_nombre' => $row['usuario_nombre'] ?? null,
            'usuario_firma_interna' => $row['usuario_firma_interna'] ?? null,
            'mime_type' => $row['mime_type'] ?? null,
            'tamano' => $row['tamano'] !== null ? (int) $row['tamano'] : null,
            'created_at' => $row['created_at'] ?? null,
            'updated_at' => $row['updated_at'] ?? null,
        ];
    }

    public static function listAttachments(mysqli $conn, int $instrumentoId, int $empresaId): array
    {
        self::assertInstrument($conn, $instrumentoId, $empresaId);
        $sql = 'SELECT a.id, a.instrumento_id, a.empresa_id, a.nombre_visible, a.ruta_archivo, a.tipo, a.descripcion, '
            . 'a.usuario_id, a.mime_type, a.tamano, a.created_at, a.updated_at, u.usuario, CONCAT_WS(" ", u.nombre, u.apellidos) AS usuario_nombre, '
            . '(SELECT fi.firma_interna FROM usuario_firmas_internas fi '
            . '  WHERE fi.usuario_id = a.usuario_id '
            . '    AND fi.empresa_id = a.empresa_id '
            . '    AND fi.vigente_desde <= COALESCE(a.created_at, NOW()) '
            . '    AND (fi.vigente_hasta IS NULL OR fi.vigente_hasta > COALESCE(a.created_at, NOW())) '
            . '  ORDER BY fi.vigente_desde DESC LIMIT 1) AS usuario_firma_interna '
            . 'FROM instrumento_adjuntos a '
            . 'LEFT JOIN usuarios u ON a.usuario_id = u.id '
            . 'WHERE a.instrumento_id = ? AND a.empresa_id = ? '
            . 'ORDER BY a.created_at DESC, a.id DESC';
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudieron obtener los adjuntos.');
        }
        $stmt->bind_param('ii', $instrumentoId, $empresaId);
        $stmt->execute();
        $result = $stmt->get_result();
        $adjuntos = [];
        if ($result) {
            while ($row = $result->fetch_assoc()) {
                $adjuntos[] = self::formatAttachmentRow($row);
            }
        }
        $stmt->close();
        return $adjuntos;
    }

    public static function deleteAttachment(mysqli $conn, int $attachmentId, int $instrumentoId, int $empresaId): bool
    {
        self::assertInstrument($conn, $instrumentoId, $empresaId);
        $stmt = $conn->prepare('SELECT ruta_archivo FROM instrumento_adjuntos WHERE id = ? AND instrumento_id = ? AND empresa_id = ? LIMIT 1');
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la eliminación del adjunto.');
        }
        $stmt->bind_param('iii', $attachmentId, $instrumentoId, $empresaId);
        $stmt->execute();
        $stmt->bind_result($ruta);
        $found = $stmt->fetch();
        $stmt->close();
        if (!$found) {
            throw new RuntimeException('Adjunto no encontrado.');
        }

        $deleteStmt = $conn->prepare('DELETE FROM instrumento_adjuntos WHERE id = ? AND instrumento_id = ? AND empresa_id = ?');
        if (!$deleteStmt) {
            throw new RuntimeException('No se pudo eliminar el adjunto.');
        }
        $deleteStmt->bind_param('iii', $attachmentId, $instrumentoId, $empresaId);
        $success = $deleteStmt->execute();
        $deleteStmt->close();

        if ($success && $ruta) {
            $path = dirname(__DIR__, 5) . '/public/' . ltrim($ruta, '/');
            if (is_file($path)) {
                @unlink($path);
            }
        }

        return $success;
    }

    public static function countByInstrument(mysqli $conn, int $instrumentoId, int $empresaId): int
    {
        $stmt = $conn->prepare('SELECT COUNT(*) AS total FROM instrumento_adjuntos WHERE instrumento_id = ? AND empresa_id = ?');
        if (!$stmt) {
            return 0;
        }
        $stmt->bind_param('ii', $instrumentoId, $empresaId);
        $stmt->execute();
        $stmt->bind_result($total);
        $stmt->fetch();
        $stmt->close();
        return isset($total) ? (int) $total : 0;
    }
}

