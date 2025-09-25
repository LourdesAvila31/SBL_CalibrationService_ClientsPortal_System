<?php

declare(strict_types=1);

$helpersPath = dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';
if (!file_exists($helpersPath)) {
    $helpersPath = dirname(__DIR__, 4) . '/Core/helpers/instrument_status.php';
}
require_once $helpersPath;

/**
 * Servicio para importar instrumentos desde archivos CSV o Excel.
 */
class BulkGageImporter
{
    /** @var mysqli|object|null */
    protected $conn;
    protected int $empresaId;
    protected ?int $usuarioId;

    /** @var array<string,int> */
    protected array $catalogoCache = [];
    /** @var array<string,int> */
    protected array $marcaCache = [];
    /** @var array<string,int> */
    protected array $modeloCache = [];
    /** @var array<string,int> */
    protected array $departamentoCache = [];

    /** @var array<string,int> */
    protected array $headerAliasCache;

    /** @var array<string,int> */
    protected array $archivoCodigos = [];

    protected ?bool $logTableAvailable = null;

    public function __construct($conn, int $empresaId, ?int $usuarioId = null)
    {
        $this->conn = $conn;
        $this->empresaId = $empresaId;
        $this->usuarioId = $usuarioId;
        $this->headerAliasCache = $this->buildHeaderAliasCache();
    }
    /**
     * Importa un archivo recibido mediante un formulario.
     */
    public function importUploadedFile(array $file): array
    {
        $tmpPath = $file['tmp_name'] ?? '';
        if (!is_string($tmpPath) || $tmpPath === '') {
            throw new RuntimeException('No se recibió ningún archivo para procesar.');
        }

        if (!is_uploaded_file($tmpPath) && !is_readable($tmpPath)) {
            throw new RuntimeException('El archivo no está disponible para lectura.');
        }

        $originalName = $file['name'] ?? basename($tmpPath);
        $extension = strtolower((string) ($file['extension'] ?? pathinfo($originalName, PATHINFO_EXTENSION)));
        $mime = $file['type'] ?? null;

        return $this->importFromFilePath($tmpPath, [
            'originalName' => $originalName,
            'extension'    => $extension,
            'mime'         => is_string($mime) ? $mime : null,
        ]);
    }

    /**
     * Importa un archivo en disco (utilizado en pruebas automatizadas).
     */
    public function importFromFilePath(string $path, array $meta = []): array
    {
        if (!is_readable($path)) {
            throw new RuntimeException('El archivo especificado no se puede leer.');
        }

        $extension = strtolower((string) ($meta['extension'] ?? pathinfo($meta['originalName'] ?? $path, PATHINFO_EXTENSION)));
        $mime = $meta['mime'] ?? null;

        $rows = $this->parseSpreadsheet($path, $extension, $mime);
        $resultado = $this->processRows($rows);

        $this->logImport($meta['originalName'] ?? basename($path), $resultado);

        return $resultado;
    }
    /**
     * Lee el archivo y devuelve filas asociativas con los encabezados normalizados.
     *
     * @return array<int,array<string,mixed>>
     */
    protected function parseSpreadsheet(string $path, string $extension, ?string $mime = null): array
    {
        $rawRows = [];

        if ($extension === 'csv' || ($mime && stripos($mime, 'csv') !== false)) {
            $rawRows = $this->parseCsv($path);
        } elseif (in_array($extension, ['xlsx', 'xls', 'ods', 'xlsm', 'xltx', 'xltm'], true)) {
            if (!class_exists('PhpOffice\\PhpSpreadsheet\\IOFactory')) {
                throw new RuntimeException('PhpSpreadsheet no está disponible. Sube el archivo en formato CSV.');
            }
            $rawRows = $this->parseSpreadsheetWithPhpSpreadsheet($path, $extension);
        } else {
            throw new RuntimeException('Formato de archivo no soportado. Usa CSV o Excel (.xlsx).');
        }

        if (empty($rawRows)) {
            throw new RuntimeException('El archivo no contiene información para importar.');
        }

        $headers = array_shift($rawRows);
        if (!is_array($headers)) {
            throw new RuntimeException('No se encontraron encabezados en el archivo.');
        }

        $headerMap = $this->mapHeaders($headers);
        $rows = [];

        foreach ($rawRows as $index => $row) {
            if (!is_array($row)) {
                continue;
            }
            $assoc = [];
            foreach ($headerMap as $key => $position) {
                $assoc[$key] = array_key_exists($position, $row) ? trim((string) $row[$position]) : '';
            }
            $assoc['_rowNumber'] = $index + 2; // +1 por base 0, +1 por encabezados

            if ($this->rowHasContent($assoc)) {
                $rows[] = $assoc;
            }
        }

        if (empty($rows)) {
            throw new RuntimeException('El archivo no contiene registros válidos para importar.');
        }

        return $rows;
    }
    /**
     * @return array<int,array<int,string|null>>
     */
    protected function parseCsv(string $path): array
    {
        $delimiter = $this->detectDelimiter($path);
        $rows = [];
        $handle = fopen($path, 'r');
        if (!$handle) {
            throw new RuntimeException('No se pudo leer el archivo CSV.');
        }

        while (($data = fgetcsv($handle, 0, $delimiter, '"', '\\')) !== false) {
            $rows[] = $data;
        }

        fclose($handle);
        return $rows;
    }

    protected function detectDelimiter(string $path): string
    {
        $delimiters = [',', ';', '\t', '|'];
        $sample = '';
        $handle = fopen($path, 'r');
        if ($handle) {
            $sample = (string) fgets($handle);
            fclose($handle);
        }

        $bestDelimiter = ',';
        $bestCount = 0;
        foreach ($delimiters as $delimiter) {
            $count = substr_count($sample, $delimiter);
            if ($count > $bestCount) {
                $bestCount = $count;
                $bestDelimiter = $delimiter;
            }
        }

        return $bestDelimiter;
    }

    /**
     * @return array<int,array<int,string|null>>
     */
    protected function parseSpreadsheetWithPhpSpreadsheet(string $path, string $extension): array
    {
        $tempFile = null;
        $pathToRead = $path;

        if (pathinfo($path, PATHINFO_EXTENSION) === '') {
            $tempFile = tempnam(sys_get_temp_dir(), 'bulk_import');
            if ($tempFile === false) {
                throw new RuntimeException('No se pudo preparar el archivo para lectura.');
            }
            $pathToRead = $tempFile . '.' . $extension;
            if (!copy($path, $pathToRead)) {
                throw new RuntimeException('No se pudo preparar el archivo temporal.');
            }
        }

        try {
            $reader = \PhpOffice\PhpSpreadsheet\IOFactory::createReaderForFile($pathToRead);
            $reader->setReadDataOnly(true);
            $spreadsheet = $reader->load($pathToRead);
            $sheet = $spreadsheet->getActiveSheet();
            $rows = $sheet->toArray(null, true, true, false);
        } finally {
            if ($tempFile !== null) {
                @unlink($tempFile);
                @unlink($pathToRead);
            }
        }

        return $rows;
    }
    /**
     * @param array<int,string|null> $headers
     * @return array<string,int>
     */
    protected function mapHeaders(array $headers): array
    {
        $mapping = [];
        foreach ($headers as $index => $header) {
            $normalized = self::normalizeText($header);
            if ($normalized === '') {
                continue;
            }
            foreach ($this->headerAliasCache as $key => $aliases) {
                if (in_array($normalized, $aliases, true)) {
                    if (!isset($mapping[$key])) {
                        $mapping[$key] = (int) $index;
                    }
                }
            }
        }

        $required = ['catalogo', 'marca', 'modelo', 'serie', 'codigo', 'departamento', 'ubicacion', 'fecha_alta'];
        $missing = [];
        foreach ($required as $key) {
            if (!isset($mapping[$key])) {
                $missing[] = $this->humanizeKey($key);
            }
        }

        if (!empty($missing)) {
            throw new RuntimeException('Faltan columnas obligatorias en la plantilla: ' . implode(', ', $missing));
        }

        return $mapping;
    }

    protected function rowHasContent(array $row): bool
    {
        foreach ($row as $key => $value) {
            if ($key === '_rowNumber') {
                continue;
            }
            if (is_string($value) && trim($value) !== '') {
                return true;
            }
        }
        return false;
    }
    /**
     * @param array<int,array<string,mixed>> $rows
     */
    protected function processRows(array $rows): array
    {
        $results = [];
        $inserted = 0;
        $failed = 0;
        $processed = 0;
        $this->archivoCodigos = [];

        foreach ($rows as $row) {
            $processed++;
            $rowNumber = (int) ($row['_rowNumber'] ?? ($processed + 1));
            unset($row['_rowNumber']);
            $normalized = $this->normalizeRow($row);
            $errors = $this->validateRow($normalized, $rowNumber);

            if (!empty($errors)) {
                $failed++;
                $results[] = [
                    'row'    => $rowNumber,
                    'status' => 'error',
                    'errors' => $errors,
                    'data'   => $normalized,
                ];
                continue;
            }

            try {
                $instrumento = $this->createInstrumentFromRow($normalized);
                $results[] = [
                    'row'        => $rowNumber,
                    'status'     => 'success',
                    'instrument' => $instrumento,
                ];
                $inserted++;
            } catch (Throwable $e) {
                $failed++;
                $results[] = [
                    'row'    => $rowNumber,
                    'status' => 'error',
                    'errors' => [$e->getMessage()],
                    'data'   => $normalized,
                ];
            }
        }

        $summary = [
            'total'     => count($rows),
            'processed' => $processed,
            'inserted'  => $inserted,
            'failed'    => $failed,
        ];

        return [
            'summary'     => $summary,
            'results'     => $results,
            'hasFailures' => $failed > 0,
            'success'     => $inserted > 0 && $failed === 0,
            'message'     => $this->buildSummaryMessage($summary),
        ];
    }

    protected function buildSummaryMessage(array $summary): string
    {
        if (($summary['inserted'] ?? 0) === 0) {
            return 'No se pudo importar ningún instrumento.';
        }
        if (($summary['failed'] ?? 0) > 0) {
            return sprintf('Se importaron %d instrumentos con algunas incidencias.', $summary['inserted']);
        }
        return sprintf('Se importaron %d instrumentos correctamente.', $summary['inserted']);
    }

    /**
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    protected function normalizeRow(array $row): array
    {
        $normalized = [];
        foreach ($row as $key => $value) {
            if ($key === '_rowNumber') {
                continue;
            }
            $normalized[$key] = is_string($value) ? trim($value) : $value;
        }
        return $normalized;
    }

    /**
     * @param array<string,mixed> $row
     * @return string[]
     */
    protected function validateRow(array &$row, int $rowNumber): array
    {
        $errors = [];
        $required = ['catalogo', 'marca', 'modelo', 'serie', 'codigo', 'departamento', 'ubicacion', 'fecha_alta'];
        foreach ($required as $field) {
            if (!isset($row[$field]) || trim((string) $row[$field]) === '') {
                $errors[] = 'El campo ' . $this->humanizeKey($field) . ' es obligatorio.';
            }
        }

        $codigo = trim((string) ($row['codigo'] ?? ''));
        if ($codigo !== '') {
            $codeKey = mb_strtolower($codigo);
            if (isset($this->archivoCodigos[$codeKey])) {
                $errors[] = sprintf('Código duplicado en el archivo (primera aparición en la fila %d).', $this->archivoCodigos[$codeKey]);
            } else {
                $this->archivoCodigos[$codeKey] = $rowNumber;
            }
        }

        $fechaAlta = $this->normalizeDate($row['fecha_alta'] ?? '');
        if ($fechaAlta === null) {
            $errors[] = 'La fecha de alta es inválida. Usa formatos como YYYY-MM-DD o DD/MM/YYYY.';
        } else {
            $row['fecha_alta'] = $fechaAlta;
        }

        if (!empty($row['fecha_baja'])) {
            $fechaBaja = $this->normalizeDate($row['fecha_baja']);
            if ($fechaBaja === null) {
                $errors[] = 'La fecha de baja es inválida.';
            } else {
                $row['fecha_baja'] = $fechaBaja;
            }
        }

        return $errors;
    }
    /**
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    protected function createInstrumentFromRow(array $row): array
    {
        $catalogoId = $this->ensureCatalogoId($row['catalogo']);
        $marcaId = $this->ensureMarcaId($row['marca']);
        $modeloId = $this->ensureModeloId($row['modelo'], $marcaId);
        $departamentoId = $this->ensureDepartamentoId($row['departamento']);
        $codigo = trim((string) $row['codigo']);

        if ($codigo === '') {
            throw new RuntimeException('El código del instrumento es obligatorio.');
        }

        if ($this->findExistingInstrumento($codigo)) {
            throw new RuntimeException(sprintf('Ya existe un instrumento con el código "%s".', $codigo));
        }

        $fechaAlta = $row['fecha_alta'];
        $fechaBaja = $row['fecha_baja'] ?? null;
        $estado = derivarEstadoInstrumento([
            'estadoActual'   => '',
            'observaciones'  => $row['observaciones'] ?? '',
            'fechaBaja'      => $fechaBaja,
            'departamentoId' => $departamentoId,
            'enStock'        => false,
        ]);

        $instrumento = [
            'catalogo_id'     => $catalogoId,
            'marca_id'        => $marcaId,
            'modelo_id'       => $modeloId,
            'serie'           => $row['serie'],
            'codigo'          => $codigo,
            'departamento_id' => $departamentoId,
            'ubicacion'       => $row['ubicacion'],
            'fecha_alta'      => $fechaAlta,
            'fecha_baja'      => $fechaBaja,
            'estado'          => $estado,
            'empresa_id'      => $this->empresaId,
        ];

        $instrumento['id'] = $this->insertInstrumento($instrumento);
        $this->afterInsert($instrumento);

        return $instrumento;
    }

    /**
     * Punto de extensión para pruebas.
     *
     * @param array<string,mixed> $instrumento
     */
    protected function afterInsert(array $instrumento): void
    {
        // Implementado en pruebas si se requiere.
    }

    /**
     * @param array<string,mixed> $instrumento
     */
    protected function insertInstrumento(array $instrumento): int
    {
        if (!$this->conn) {
            throw new RuntimeException('No hay conexión a la base de datos.');
        }

        $sql = 'INSERT INTO instrumentos (
                catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id,
                ubicacion, fecha_alta, fecha_baja, estado, empresa_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)';

        $stmt = $this->conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la inserción de instrumentos.');
        }

        $catalogoId = (int) $instrumento['catalogo_id'];
        $marcaId = (int) $instrumento['marca_id'];
        $modeloId = (int) $instrumento['modelo_id'];
        $serie = (string) $instrumento['serie'];
        $codigo = (string) $instrumento['codigo'];
        $departamentoId = (int) $instrumento['departamento_id'];
        $ubicacion = (string) $instrumento['ubicacion'];
        $fechaAlta = (string) $instrumento['fecha_alta'];
        $fechaBaja = $instrumento['fecha_baja'] ?? null;
        $estado = (string) $instrumento['estado'];
        $empresaId = (int) $instrumento['empresa_id'];

        $stmt->bind_param(
            'iiississssi',
            $catalogoId,
            $marcaId,
            $modeloId,
            $serie,
            $codigo,
            $departamentoId,
            $ubicacion,
            $fechaAlta,
            $fechaBaja,
            $estado,
            $empresaId
        );

        if (!$stmt->execute()) {
            $error = $stmt->error ?: 'Error desconocido al insertar el instrumento.';
            $stmt->close();
            throw new RuntimeException($error);
        }

        $insertId = (int) $stmt->insert_id;
        $stmt->close();

        return $insertId;
    }
    protected function findExistingInstrumento(string $codigo): ?int
    {
        if (!$this->conn) {
            return null;
        }

        $stmt = $this->conn->prepare('SELECT id FROM instrumentos WHERE codigo = ? AND empresa_id = ? LIMIT 1');
        if (!$stmt) {
            return null;
        }
        $empresaId = $this->empresaId;
        $stmt->bind_param('si', $codigo, $empresaId);
        if (!$stmt->execute()) {
            $stmt->close();
            return null;
        }
        $stmt->bind_result($id);
        $found = $stmt->fetch();
        $stmt->close();
        return $found ? (int) $id : null;
    }

    protected function ensureCatalogoId(string $nombre): int
    {
        return $this->ensureSimpleLookup($nombre, 'catalogo_instrumentos', $this->catalogoCache);
    }

    protected function ensureMarcaId(string $nombre): int
    {
        return $this->ensureSimpleLookup($nombre, 'marcas', $this->marcaCache);
    }

    protected function ensureDepartamentoId(string $nombre): int
    {
        return $this->ensureSimpleLookup($nombre, 'departamentos', $this->departamentoCache);
    }

    protected function ensureModeloId(string $nombre, int $marcaId): int
    {
        $normalized = self::normalizeText($nombre) . '|' . $marcaId;
        if (isset($this->modeloCache[$normalized])) {
            return $this->modeloCache[$normalized];
        }

        if (!$this->conn) {
            throw new RuntimeException('No hay conexión disponible para resolver modelos.');
        }

        $stmt = $this->conn->prepare('SELECT id FROM modelos WHERE LOWER(nombre) = LOWER(?) AND (marca_id = ? OR marca_id IS NULL) LIMIT 1');
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de modelos.');
        }
        $stmt->bind_param('si', $nombre, $marcaId);
        $stmt->execute();
        $stmt->bind_result($id);
        $found = $stmt->fetch();
        $stmt->close();

        if ($found) {
            $this->modeloCache[$normalized] = (int) $id;
            return (int) $id;
        }

        $stmt = $this->conn->prepare('INSERT INTO modelos (nombre, marca_id) VALUES (?, ?)');
        if (!$stmt) {
            throw new RuntimeException('No se pudo registrar el modelo.');
        }
        $stmt->bind_param('si', $nombre, $marcaId);
        if (!$stmt->execute()) {
            $error = $stmt->error ?: 'No se pudo registrar el modelo.';
            $stmt->close();
            throw new RuntimeException($error);
        }
        $newId = (int) $stmt->insert_id;
        $stmt->close();
        $this->modeloCache[$normalized] = $newId;

        return $newId;
    }

    /**
     * @param array<string,int> $cache
     */
    protected function ensureSimpleLookup(string $nombre, string $tabla, array &$cache): int
    {
        $normalized = self::normalizeText($nombre);
        if ($normalized === '') {
            throw new RuntimeException('El valor para ' . $tabla . ' es inválido.');
        }

        if (isset($cache[$normalized])) {
            return $cache[$normalized];
        }

        if (!$this->conn) {
            throw new RuntimeException('No hay conexión disponible para completar la importación.');
        }

        $stmt = $this->conn->prepare("SELECT id FROM {$tabla} WHERE LOWER(nombre) = LOWER(?) LIMIT 1");
        if ($stmt) {
            $stmt->bind_param('s', $nombre);
            $stmt->execute();
            $stmt->bind_result($id);
            if ($stmt->fetch()) {
                $cache[$normalized] = (int) $id;
                $stmt->close();
                return (int) $id;
            }
            $stmt->close();
        }

        $stmt = $this->conn->prepare("INSERT INTO {$tabla} (nombre) VALUES (?)");
        if (!$stmt) {
            throw new RuntimeException('No se pudo registrar el valor en ' . $tabla . '.');
        }
        $stmt->bind_param('s', $nombre);
        if (!$stmt->execute()) {
            $error = $stmt->error ?: 'No se pudo registrar el valor en ' . $tabla . '.';
            $stmt->close();
            throw new RuntimeException($error);
        }
        $newId = (int) $stmt->insert_id;
        $stmt->close();

        $cache[$normalized] = $newId;
        return $newId;
    }
    protected function logImport(string $filename, array $resultado): void
    {
        if (!$this->conn || !$this->shouldLogImports()) {
            return;
        }

        $stmt = $this->conn->prepare('INSERT INTO bulk_import_logs (empresa_id, usuario_id, archivo_nombre, total_registros, procesados, exitosos, fallidos, errores_json) VALUES (?,?,?,?,?,?,?,?)');
        if (!$stmt) {
            return;
        }

        $summary = $resultado['summary'] ?? [];
        $errores = array_filter(
            $resultado['results'] ?? [],
            static fn ($item) => isset($item['status']) && $item['status'] === 'error'
        );

        $erroresJson = json_encode($errores, JSON_UNESCAPED_UNICODE);
        $usuarioId = $this->usuarioId;
        $total = (int) ($summary['total'] ?? 0);
        $procesados = (int) ($summary['processed'] ?? 0);
        $exitosos = (int) ($summary['inserted'] ?? 0);
        $fallidos = (int) ($summary['failed'] ?? 0);

        $stmt->bind_param(
            'iisiiiis',
            $this->empresaId,
            $usuarioId,
            $filename,
            $total,
            $procesados,
            $exitosos,
            $fallidos,
            $erroresJson
        );

        $stmt->execute();
        $stmt->close();
    }

    protected function shouldLogImports(): bool
    {
        if ($this->logTableAvailable !== null) {
            return $this->logTableAvailable;
        }

        if (!$this->conn) {
            $this->logTableAvailable = false;
            return false;
        }

        $stmt = $this->conn->prepare("SHOW TABLES LIKE 'bulk_import_logs'");
        if (!$stmt) {
            $this->logTableAvailable = false;
            return false;
        }

        $stmt->execute();
        $stmt->store_result();
        $this->logTableAvailable = $stmt->num_rows > 0;
        $stmt->close();

        return $this->logTableAvailable;
    }

    /**
     * @return array<string,array<int,string>>
     */
    protected function buildHeaderAliasCache(): array
    {
        $aliases = [
            'catalogo'     => ['catalogo', 'nombre', 'instrumento', 'nombre del instrumento'],
            'marca'        => ['marca'],
            'modelo'       => ['modelo', 'modelo equipo'],
            'serie'        => ['serie', 'numero de serie', 'n de serie', 'número de serie'],
            'codigo'       => ['codigo', 'codigo interno', 'identificador', 'codigo equipo', 'clave'],
            'departamento' => ['departamento', 'area', 'área', 'responsable'],
            'ubicacion'    => ['ubicacion', 'ubicación', 'localizacion', 'localización'],
            'fecha_alta'   => ['fecha alta', 'fecha de alta', 'alta'],
            'fecha_baja'   => ['fecha baja', 'fecha de baja'],
            'observaciones'=> ['observaciones', 'comentarios', 'nota'],
        ];

        $normalized = [];
        foreach ($aliases as $key => $values) {
            $normalized[$key] = array_values(array_unique(array_map([self::class, 'normalizeText'], $values)));
            $normalized[$key][] = $key;
            $normalized[$key] = array_values(array_unique($normalized[$key]));
        }

        return $normalized;
    }

    protected function humanizeKey(string $key): string
    {
        return match ($key) {
            'catalogo' => 'catálogo',
            'fecha_alta' => 'fecha de alta',
            'fecha_baja' => 'fecha de baja',
            default => str_replace('_', ' ', $key),
        };
    }

    public static function normalizeText($value): string
    {
        if (!is_string($value)) {
            $value = (string) $value;
        }
        $value = trim($value);
        if ($value === '') {
            return '';
        }

        if (class_exists('Normalizer')) {
            $value = \Normalizer::normalize($value, \Normalizer::FORM_D);
            $value = preg_replace('/\p{Mn}+/u', '', $value);
        } else {
            $transliterated = @iconv('UTF-8', 'ASCII//TRANSLIT', $value);
            if ($transliterated !== false) {
                $value = $transliterated;
            }
        }

        $value = mb_strtolower($value, 'UTF-8');
        $value = preg_replace('/[^a-z0-9]+/u', ' ', $value);
        $value = preg_replace('/\s+/u', ' ', $value);

        return trim($value ?? '');
    }

    protected function normalizeDate($value): ?string
    {
        if ($value === null) {
            return null;
        }

        if (is_numeric($value)) {
            $numeric = (float) $value;
            if (class_exists('PhpOffice\PhpSpreadsheet\Shared\Date')) {
                try {
                    $date = \PhpOffice\PhpSpreadsheet\Shared\Date::excelToDateTimeObject($numeric);
                    return $date->format('Y-m-d');
                } catch (Throwable $e) {
                    // Ignorar y continuar con otros métodos.
                }
            }
            $base = DateTimeImmutable::createFromFormat('Y-m-d', '1899-12-30');
            if ($base) {
                return $base->modify('+' . (int) $numeric . ' days')->format('Y-m-d');
            }
        }

        $value = trim((string) $value);
        if ($value === '') {
            return null;
        }

        $normalized = str_replace(['\\', '.'], ['/', '/'], $value);
        $formats = ['Y-m-d', 'd/m/Y', 'd-m-Y', 'm/d/Y', 'Y/m/d', 'd/m/y', 'd-m-y'];
        foreach ($formats as $format) {
            $date = DateTimeImmutable::createFromFormat($format, $normalized);
            if ($date && $date->format($format) === $normalized) {
                return $date->format('Y-m-d');
            }
        }

        $timestamp = strtotime($value);
        if ($timestamp !== false) {
            return date('Y-m-d', $timestamp);
        }

        return null;
    }
}
