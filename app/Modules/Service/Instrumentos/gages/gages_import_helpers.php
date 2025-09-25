<?php

declare(strict_types=1);

const GAGES_IMPORT_NA_VALUES = ['NA', 'ND', 'N/A'];
const GAGES_IMPORT_MONTH_MAP = [
    'ENE' => 1,
    'FEB' => 2,
    'MAR' => 3,
    'ABR' => 4,
    'MAY' => 5,
    'JUN' => 6,
    'JUL' => 7,
    'AGO' => 8,
    'SEP' => 9,
    'SET' => 9,
    'OCT' => 10,
    'NOV' => 11,
    'DIC' => 12,
];

/**
 * Lee y normaliza un archivo CSV de instrumentos.
 *
 * @param string $path Ruta del archivo CSV subido.
 * @return array{records: array<int, array<string, mixed>>, errors: array<int, array<string, mixed>>}
 */
function parse_gages_inventory_csv(string $path): array
{
    if (!is_readable($path)) {
        throw new RuntimeException('No se puede leer el archivo subido.');
    }

    $file = new SplFileObject($path, 'r');
    $file->setFlags(SplFileObject::READ_CSV | SplFileObject::SKIP_EMPTY);
    $file->setCsvControl(',', '"', '\\');

    $headers = $file->fgetcsv();
    if ($headers === false) {
        throw new InvalidArgumentException('El archivo CSV está vacío.');
    }

    if (count($headers) === 1) {
        $file->rewind();
        $file->setCsvControl(';', '"', '\\');
        $headers = $file->fgetcsv();
        if ($headers === false) {
            throw new InvalidArgumentException('El archivo CSV está vacío.');
        }
    }

    $normalizedHeaders = [];
    foreach ($headers as $header) {
        if ($header === null) {
            $normalizedHeaders[] = '';
            continue;
        }
        $clean = trim((string) $header);
        $clean = preg_replace('/^\xEF\xBB\xBF/u', '', $clean ?? '') ?? '';
        $normalizedHeaders[] = $clean;
    }

    $lowercaseHeaders = [];
    foreach ($normalizedHeaders as $header) {
        if ($header === '') {
            continue;
        }
        $lower = mb_strtolower($header, 'UTF-8');
        $lowercaseHeaders[$lower] = true;
    }

    validate_required_columns($lowercaseHeaders);

    $records = [];
    $errors = [];
    $rowNumber = 1; // incluye encabezado

    while (!$file->eof()) {
        $row = $file->fgetcsv();
        if ($row === false || $row === null) {
            continue;
        }
        $rowNumber++;

        // Ignora filas vacías
        if (count($row) === 1 && ($row[0] === null || trim((string) $row[0]) === '')) {
            continue;
        }

        $assoc = [];
        foreach ($normalizedHeaders as $index => $header) {
            if ($header === '') {
                continue;
            }
            $key = mb_strtolower($header, 'UTF-8');
            $assoc[$key] = $row[$index] ?? null;
        }

        try {
            $records[] = normalize_inventory_row($assoc, $rowNumber);
        } catch (Throwable $e) {
            $codigo = obtener_columna($assoc, ['código', 'codigo']);
            $errors[] = [
                'row'    => $rowNumber,
                'codigo' => $codigo !== null ? trim((string) $codigo) : null,
                'error'  => $e->getMessage(),
            ];
        }
    }

    return ['records' => $records, 'errors' => $errors];
}

/**
 * Procesa los registros normalizados y actualiza la base de datos.
 *
 * @param mysqli $conn
 * @param int    $empresaId
 * @param array<int, array<string, mixed>> $records
 * @param array<int, array<string, mixed>> $rowErrors
 *
 * @return array{created: int, updated: int}
 */
function process_gages_import(mysqli $conn, int $empresaId, array $records, array &$rowErrors): array
{
    $created = 0;
    $updated = 0;

    $catalogoCache = load_catalog_cache($conn, $empresaId);
    $marcaCache    = load_brand_cache($conn, $empresaId);
    $modeloCache   = load_model_cache($conn, $empresaId);
    $deptCache     = load_department_cache($conn, $empresaId);

    $selectInstrument = $conn->prepare('SELECT id FROM instrumentos WHERE empresa_id = ? AND codigo = ? LIMIT 1');
    $insertInstrument = $conn->prepare(
        'INSERT INTO instrumentos (
            catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id,
            ubicacion, fecha_alta, fecha_baja, proxima_calibracion, estado,
            programado, empresa_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
    );
    $updateInstrument = $conn->prepare(
        'UPDATE instrumentos SET
            catalogo_id = ?, marca_id = ?, modelo_id = ?, serie = ?, codigo = ?,
            departamento_id = ?, ubicacion = ?, fecha_alta = ?, fecha_baja = ?,
            proxima_calibracion = ?, estado = ?, programado = ?
        WHERE id = ?'
    );

    foreach ($records as $record) {
        try {
            $catalogoId = ensure_catalog_id($conn, $empresaId, $record['instrumento'] ?? null, $catalogoCache);
            $marcaId    = ensure_brand_id($conn, $empresaId, $record['marca'] ?? null, $marcaCache);
            $modeloId   = ensure_model_id($conn, $empresaId, $marcaId, $record['modelo'] ?? null, $modeloCache);
            $deptId     = ensure_department_id($conn, $empresaId, $record['departamento'] ?? null, $deptCache);

            $codigo = $record['codigo'] ?? null;
            if ($codigo === null || $codigo === '') {
                throw new RuntimeException('El código del instrumento es obligatorio.');
            }

            $instrumentId = find_instrument_id($selectInstrument, $empresaId, $codigo);
            $programado = $record['programado'] ?? null;
            $programado = $programado === null ? null : (int) $programado;

            if ($instrumentId === null) {
                bind_instrument_params(
                    $insertInstrument,
                    $catalogoId,
                    $marcaId,
                    $modeloId,
                    $record['serie'] ?? null,
                    $codigo,
                    $deptId,
                    $record['ubicacion'] ?? null,
                    $record['fecha_alta'] ?? null,
                    $record['fecha_baja'] ?? null,
                    $record['proxima_calibracion'] ?? null,
                    $record['estado'] ?? null,
                    $programado,
                    $empresaId
                );
                if (!$insertInstrument->execute()) {
                    throw new RuntimeException('No se pudo insertar el instrumento: ' . $insertInstrument->error);
                }
                $created++;
            } else {
                bind_instrument_params(
                    $updateInstrument,
                    $catalogoId,
                    $marcaId,
                    $modeloId,
                    $record['serie'] ?? null,
                    $codigo,
                    $deptId,
                    $record['ubicacion'] ?? null,
                    $record['fecha_alta'] ?? null,
                    $record['fecha_baja'] ?? null,
                    $record['proxima_calibracion'] ?? null,
                    $record['estado'] ?? null,
                    $programado,
                    $instrumentId
                );
                if (!$updateInstrument->execute()) {
                    throw new RuntimeException('No se pudo actualizar el instrumento: ' . $updateInstrument->error);
                }
                $updated++;
            }
        } catch (Throwable $e) {
            $rowErrors[] = [
                'row'    => $record['row'] ?? null,
                'codigo' => $record['codigo'] ?? null,
                'error'  => $e->getMessage(),
            ];
        }
    }

    $selectInstrument->close();
    $insertInstrument->close();
    $updateInstrument->close();

    return ['created' => $created, 'updated' => $updated];
}

/** @param array<string, bool> $lowercaseHeaders */
function validate_required_columns(array $lowercaseHeaders): void
{
    $requiredSets = [
        ['código', 'codigo'],
        ['estado'],
        ['programado'],
    ];

    foreach ($requiredSets as $set) {
        $found = false;
        foreach ($set as $candidate) {
            if (isset($lowercaseHeaders[$candidate])) {
                $found = true;
                break;
            }
        }
        if (!$found) {
            throw new InvalidArgumentException('El CSV no contiene la columna obligatoria: ' . $set[0]);
        }
    }
}

/**
 * @param array<string, mixed> $assoc
 * @return array<string, mixed>
 */
function normalize_inventory_row(array $assoc, int $rowNumber): array
{
    $instrumento = normalize_text(obtener_columna($assoc, ['instrumento']));
    $marca       = normalize_text(obtener_columna($assoc, ['marca']));
    $modelo      = normalize_text(obtener_columna($assoc, ['modelo']));
    $serie       = normalize_text(obtener_columna($assoc, ['serie']), true);
    $codigo      = normalize_codigo(obtener_columna($assoc, ['código', 'codigo']));
    $departamento = normalize_text(obtener_columna($assoc, ['departamento responsable', 'departamento', 'departamento responsable']));
    $ubicacion    = normalize_text(obtener_columna($assoc, ['ubicación', 'ubicacion']), true);
    $fechaAlta    = normalize_fecha(obtener_columna($assoc, ['fecha de alta']));
    $fechaBaja    = normalize_fecha(obtener_columna($assoc, ['fecha de baja']));
    $proxima      = normalize_fecha(obtener_columna($assoc, ['próxima calibración', 'proxima calibracion', 'próxima calibracion', 'proxima calibración', 'proxima_calibracion']));
    $estado       = normalize_estado(obtener_columna($assoc, ['estado']));
    $programado   = normalize_programado(obtener_columna($assoc, ['programado']));

    if ($codigo === null) {
        throw new InvalidArgumentException('La columna Código es obligatoria.');
    }
    if ($estado === null) {
        throw new InvalidArgumentException("No se pudo determinar el estado del instrumento con código '{$codigo}'.");
    }
    if ($programado === null) {
        throw new InvalidArgumentException("No se pudo determinar el indicador 'programado' del instrumento con código '{$codigo}'.");
    }

    return [
        'row'                  => $rowNumber,
        'instrumento'          => $instrumento,
        'marca'                => $marca,
        'modelo'               => $modelo,
        'serie'                => $serie,
        'codigo'               => $codigo,
        'departamento'         => $departamento,
        'ubicacion'            => $ubicacion,
        'fecha_alta'           => $fechaAlta,
        'fecha_baja'           => $fechaBaja,
        'proxima_calibracion'  => $proxima,
        'estado'               => $estado,
        'programado'           => $programado,
    ];
}

function normalize_text(?string $value, bool $allowEmpty = false): ?string
{
    if ($value === null) {
        return null;
    }
    $trimmed = trim($value);
    if ($trimmed === '') {
        return null;
    }
    return $trimmed;
}

function normalize_codigo(?string $value): ?string
{
    if ($value === null) {
        return null;
    }
    $trimmed = trim($value);
    return $trimmed === '' ? null : $trimmed;
}

function normalize_fecha(?string $value): ?string
{
    if ($value === null) {
        return null;
    }
    $text = trim($value);
    if ($text === '') {
        return null;
    }
    $upper = mb_strtoupper($text, 'UTF-8');
    if (in_array($upper, GAGES_IMPORT_NA_VALUES, true)) {
        return null;
    }

    $formats = ['Y-m-d', 'd/m/Y'];
    foreach ($formats as $format) {
        $dt = DateTime::createFromFormat($format, $text);
        if ($dt instanceof DateTimeInterface && $dt->format($format) === $text) {
            return $dt->format('Y-m-d');
        }
    }

    $text = str_replace("\xC2\xA0", ' ', $text);
    $parts = explode('-', $text);
    if (count($parts) === 3) {
        [$day, $month, $year] = $parts;
        $dayInt = (int) trim($day);
        try {
            $monthInt = translate_month($month);
            $yearInt = normalize_year((int) trim($year));
            $normalized = sprintf('%04d-%02d-%02d', $yearInt, $monthInt, $dayInt);
            $dt = DateTime::createFromFormat('Y-m-d', $normalized);
            if ($dt instanceof DateTimeInterface) {
                return $dt->format('Y-m-d');
            }
        } catch (Throwable $e) {
            // Ignorado, se lanzará excepción más abajo.
        }
    }

    throw new InvalidArgumentException('Formato de fecha no reconocido: ' . $value);
}

function normalize_estado(?string $value): ?string
{
    $text = normalize_text($value, true);
    if ($text === null) {
        return null;
    }
    $upper = mb_strtoupper($text, 'UTF-8');
    if (in_array($upper, GAGES_IMPORT_NA_VALUES, true)) {
        return null;
    }
    return $text;
}

function normalize_programado(?string $value): ?int
{
    if ($value === null) {
        return null;
    }
    $text = trim($value);
    if ($text === '') {
        return null;
    }
    $upper = mb_strtoupper($text, 'UTF-8');
    if (in_array($upper, GAGES_IMPORT_NA_VALUES, true)) {
        return null;
    }

    $trueValues = ['SI', 'SÍ', 'YES', 'TRUE', 'VERDADERO'];
    $falseValues = ['NO', 'FALSE', 'FALSO'];

    if (in_array($upper, $trueValues, true)) {
        return 1;
    }
    if (in_array($upper, $falseValues, true)) {
        return 0;
    }
    if (!is_numeric($text)) {
        throw new InvalidArgumentException('Valor de programado no válido: ' . $value);
    }
    $number = (int) $text;
    if ($number !== 0 && $number !== 1) {
        throw new InvalidArgumentException('Valor de programado fuera de rango (esperado 0 o 1): ' . $value);
    }
    return $number;
}

function translate_month(string $value): int
{
    $key = mb_strtoupper(trim($value), 'UTF-8');
    if (mb_strlen($key, 'UTF-8') > 3) {
        $key = mb_substr($key, 0, 3, 'UTF-8');
    }
    if (!isset(GAGES_IMPORT_MONTH_MAP[$key])) {
        throw new InvalidArgumentException('Mes no reconocido: ' . $value);
    }
    return GAGES_IMPORT_MONTH_MAP[$key];
}

function normalize_year(int $value): int
{
    if ($value < 100) {
        return 2000 + $value;
    }
    return $value;
}

/**
 * @param array<string, mixed> $assoc
 * @param array<int, string>   $candidates
 */
function obtener_columna(array $assoc, array $candidates): ?string
{
    foreach ($candidates as $candidate) {
        $key = mb_strtolower($candidate, 'UTF-8');
        if (array_key_exists($key, $assoc) && $assoc[$key] !== null) {
            return (string) $assoc[$key];
        }
    }
    return null;
}

function load_catalog_cache(mysqli $conn, int $empresaId): array
{
    $cache = [];
    $stmt = $conn->prepare('SELECT id, nombre FROM catalogo_instrumentos WHERE empresa_id = ?');
    $stmt->bind_param('i', $empresaId);
    $stmt->execute();
    $result = $stmt->get_result();
    while ($row = $result->fetch_assoc()) {
        $cache[normalize_cache_key($row['nombre'])] = (int) $row['id'];
    }
    $stmt->close();
    return $cache;
}

function load_brand_cache(mysqli $conn, int $empresaId): array
{
    $cache = [];
    $stmt = $conn->prepare('SELECT id, nombre FROM marcas WHERE empresa_id = ?');
    $stmt->bind_param('i', $empresaId);
    $stmt->execute();
    $result = $stmt->get_result();
    while ($row = $result->fetch_assoc()) {
        $cache[normalize_cache_key($row['nombre'])] = (int) $row['id'];
    }
    $stmt->close();
    return $cache;
}

function load_model_cache(mysqli $conn, int $empresaId): array
{
    $cache = [];
    $stmt = $conn->prepare('SELECT id, nombre, marca_id FROM modelos WHERE empresa_id = ?');
    $stmt->bind_param('i', $empresaId);
    $stmt->execute();
    $result = $stmt->get_result();
    while ($row = $result->fetch_assoc()) {
        $key = build_model_cache_key((int) $row['marca_id'], $row['nombre']);
        $cache[$key] = (int) $row['id'];
    }
    $stmt->close();
    return $cache;
}

function load_department_cache(mysqli $conn, int $empresaId): array
{
    $cache = [];
    $stmt = $conn->prepare('SELECT id, nombre FROM departamentos WHERE empresa_id = ?');
    $stmt->bind_param('i', $empresaId);
    $stmt->execute();
    $result = $stmt->get_result();
    while ($row = $result->fetch_assoc()) {
        $cache[normalize_cache_key($row['nombre'])] = (int) $row['id'];
    }
    $stmt->close();
    return $cache;
}

function ensure_catalog_id(mysqli $conn, int $empresaId, ?string $nombre, array &$cache): ?int
{
    if ($nombre === null) {
        return null;
    }
    $key = normalize_cache_key($nombre);
    if (isset($cache[$key])) {
        return $cache[$key];
    }
    $stmt = $conn->prepare('INSERT INTO catalogo_instrumentos (nombre, empresa_id) VALUES (?, ?)');
    $stmt->bind_param('si', $nombre, $empresaId);
    if (!$stmt->execute()) {
        $stmt->close();
        throw new RuntimeException('No se pudo registrar el catálogo: ' . $conn->error);
    }
    $id = $conn->insert_id;
    $stmt->close();
    $cache[$key] = (int) $id;
    return (int) $id;
}

function ensure_brand_id(mysqli $conn, int $empresaId, ?string $nombre, array &$cache): ?int
{
    if ($nombre === null) {
        return null;
    }
    $key = normalize_cache_key($nombre);
    if (isset($cache[$key])) {
        return $cache[$key];
    }
    $stmt = $conn->prepare('INSERT INTO marcas (nombre, empresa_id) VALUES (?, ?)');
    $stmt->bind_param('si', $nombre, $empresaId);
    if (!$stmt->execute()) {
        $stmt->close();
        throw new RuntimeException('No se pudo registrar la marca: ' . $conn->error);
    }
    $id = $conn->insert_id;
    $stmt->close();
    $cache[$key] = (int) $id;
    return (int) $id;
}

function ensure_model_id(mysqli $conn, int $empresaId, ?int $marcaId, ?string $nombre, array &$modeloCache): ?int
{
    if ($nombre === null) {
        return null;
    }
    if ($marcaId === null) {
        throw new RuntimeException('No se puede registrar un modelo sin especificar la marca asociada.');
    }
    $key = build_model_cache_key($marcaId, $nombre);
    if (isset($modeloCache[$key])) {
        return $modeloCache[$key];
    }
    $stmt = $conn->prepare('INSERT INTO modelos (nombre, marca_id, empresa_id) VALUES (?, ?, ?)');
    $stmt->bind_param('sii', $nombre, $marcaId, $empresaId);
    if (!$stmt->execute()) {
        $stmt->close();
        throw new RuntimeException('No se pudo registrar el modelo: ' . $conn->error);
    }
    $id = $conn->insert_id;
    $stmt->close();
    $modeloCache[$key] = (int) $id;
    return (int) $id;
}

function ensure_department_id(mysqli $conn, int $empresaId, ?string $nombre, array &$cache): ?int
{
    if ($nombre === null) {
        return null;
    }
    $key = normalize_cache_key($nombre);
    if (isset($cache[$key])) {
        return $cache[$key];
    }
    $stmt = $conn->prepare('INSERT INTO departamentos (nombre, empresa_id) VALUES (?, ?)');
    $stmt->bind_param('si', $nombre, $empresaId);
    if (!$stmt->execute()) {
        $stmt->close();
        throw new RuntimeException('No se pudo registrar el departamento: ' . $conn->error);
    }
    $id = $conn->insert_id;
    $stmt->close();
    $cache[$key] = (int) $id;
    return (int) $id;
}

function find_instrument_id(mysqli_stmt $stmt, int $empresaId, string $codigo): ?int
{
    $stmt->bind_param('is', $empresaId, $codigo);
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result->fetch_assoc();
    if (!$row) {
        return null;
    }
    return (int) $row['id'];
}

function bind_instrument_params(
    mysqli_stmt $stmt,
    ?int $catalogoId,
    ?int $marcaId,
    ?int $modeloId,
    ?string $serie,
    string $codigo,
    ?int $departamentoId,
    ?string $ubicacion,
    ?string $fechaAlta,
    ?string $fechaBaja,
    ?string $proximaCalibracion,
    ?string $estado,
    ?int $programado,
    int $finalIdentifier
): void {
    $stmt->bind_param(
        'iiississsssii',
        $catalogoId,
        $marcaId,
        $modeloId,
        $serie,
        $codigo,
        $departamentoId,
        $ubicacion,
        $fechaAlta,
        $fechaBaja,
        $proximaCalibracion,
        $estado,
        $programado,
        $finalIdentifier
    );
}

function normalize_cache_key(?string $value): string
{
    return mb_strtolower(trim((string) $value), 'UTF-8');
}

function build_model_cache_key(int $marcaId, ?string $nombre): string
{
    return $marcaId . '|' . normalize_cache_key($nombre);
}
