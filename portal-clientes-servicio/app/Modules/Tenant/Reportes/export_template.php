<?php
declare(strict_types=1);

require_once dirname(__DIR__, 3) . '/Core/auth.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once __DIR__ . '/../configuracion/google_client.php';

// Validación de permisos utilizando el endpoint existente.
$_GET['permiso'] = 'reportes_crear';
ob_start();
include dirname(__DIR__, 3) . '/Core/check_permission.php';
$permissionResponse = json_decode((string) ob_get_clean(), true) ?: [];
unset($_GET['permiso']);

if (!(isset($permissionResponse['allowed']) && $permissionResponse['allowed'] === true)) {
    http_response_code(403);
    header('Content-Type: application/json');
    echo json_encode(['success' => false, 'message' => 'Permiso denegado.']);
    exit;
}

header('Content-Type: application/json; charset=UTF-8');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'message' => 'Método no permitido']);
    exit;
}

$raw = file_get_contents('php://input');
$data = json_decode((string) $raw, true);
if (!is_array($data)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Solicitud inválida']);
    exit;
}

$templateKey = isset($data['template_key']) ? trim((string) $data['template_key']) : '';
if ($templateKey === '') {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Falta el parámetro template_key.']);
    exit;
}

$year = isset($data['year']) ? (int) $data['year'] : 0;
if ($year < 1900 || $year > 2100) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Año fuera de rango.']);
    exit;
}

$revisionNumber = isset($data['revision']) ? (int) $data['revision'] : 0;
if ($revisionNumber < 0) {
    http_response_code(400);
    echo json_encode(['success' => false, 'message' => 'Número de revisión inválido.']);
    exit;
}

$signatures = [];
if (isset($data['signatures']) && is_array($data['signatures'])) {
    $signatures = array_values($data['signatures']);
}

$extraFields = [];
if (isset($data['fields']) && is_array($data['fields'])) {
    $extraFields = $data['fields'];
}

$empresaId = obtenerEmpresaId();
$usuarioId = $_SESSION['usuario_id'] ?? null;
if (!$usuarioId) {
    http_response_code(403);
    echo json_encode(['success' => false, 'message' => 'Sesión inválida.']);
    exit;
}

global $conn;
if (!($conn instanceof mysqli)) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => 'Conexión a base de datos no disponible.']);
    exit;
}

try {
    $template = fetch_report_template($conn, $templateKey, $empresaId);
    if ($template === null) {
        http_response_code(404);
        echo json_encode(['success' => false, 'message' => 'Plantilla no encontrada.']);
        return;
    }

    $result = generate_report_from_template($template, $year, $revisionNumber, $signatures, $extraFields, $usuarioId, $empresaId, $conn);
    echo json_encode(['success' => true] + $result);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['success' => false, 'message' => $e->getMessage()]);
}

/**
 * @return array<string,mixed>|null
 */
function fetch_report_template(mysqli $conn, string $templateKey, ?int $empresaId): ?array
{
    $sql = 'SELECT id, empresa_id, template_key, spreadsheet_id, sheet_id, title_pattern, sheet_title_pattern, config_json, drive_folder_id, descripcion'
         . ' FROM report_templates WHERE template_key = ?';
    $params = [$templateKey];
    $types = 's';
    if ($empresaId !== null) {
        $sql .= ' AND (empresa_id IS NULL OR empresa_id = ?)';
        $types .= 'i';
        $params[] = $empresaId;
    }
    $sql .= ' ORDER BY empresa_id DESC LIMIT 1';

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        throw new RuntimeException('No se pudo preparar la consulta de plantillas.');
    }

    $stmt->bind_param($types, ...$params);
    $stmt->execute();

    $row = null;
    if (method_exists($stmt, 'get_result')) {
        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
    } else {
        $stmt->bind_result(
            $id,
            $empresa,
            $key,
            $spreadsheetId,
            $sheetId,
            $titlePattern,
            $sheetTitlePattern,
            $configJson,
            $driveFolderId,
            $descripcion
        );
        if ($stmt->fetch()) {
            $row = [
                'id'                  => $id,
                'empresa_id'          => $empresa,
                'template_key'        => $key,
                'spreadsheet_id'      => $spreadsheetId,
                'sheet_id'            => $sheetId,
                'title_pattern'       => $titlePattern,
                'sheet_title_pattern' => $sheetTitlePattern,
                'config_json'         => $configJson,
                'drive_folder_id'     => $driveFolderId,
                'descripcion'         => $descripcion,
            ];
        }
    }

    $stmt->close();

    return $row ?: null;
}

/**
 * @param array<int,mixed> $signatures
 * @param array<string,mixed> $extraFields
 * @return array<string,mixed>
 */
function generate_report_from_template(array $template, int $year, int $revisionNumber, array $signatures, array $extraFields, int $usuarioId, ?int $empresaId, mysqli $conn): array
{
    $sheetsClient = google_sheets_client();
    $driveClient = google_drive_client();

    $config = [];
    if (!empty($template['config_json'])) {
        $decoded = json_decode((string) $template['config_json'], true);
        if (is_array($decoded)) {
            $config = $decoded;
        }
    }

    $title = build_from_pattern($template['title_pattern'] ?? '', $template['template_key'], $year, $revisionNumber);
    $sheetTitle = build_from_pattern($template['sheet_title_pattern'] ?? '', $template['template_key'], $year, $revisionNumber);
    if ($sheetTitle === '') {
        $sheetTitle = $title;
    }

    $creation = $sheetsClient->createSpreadsheet($title);
    $spreadsheetId = $creation['spreadsheetId'] ?? null;
    if (!is_string($spreadsheetId) || $spreadsheetId === '') {
        throw new RuntimeException('No se pudo crear la hoja de cálculo destino.');
    }

    $defaultSheetId = null;
    if (isset($creation['sheets'][0]['properties']['sheetId'])) {
        $defaultSheetId = (int) $creation['sheets'][0]['properties']['sheetId'];
    }

    $copyResponse = $sheetsClient->copySheet((string) $template['spreadsheet_id'], (int) $template['sheet_id'], $spreadsheetId);
    $copiedSheetId = null;
    if (isset($copyResponse['sheetId'])) {
        $copiedSheetId = (int) $copyResponse['sheetId'];
    } elseif (isset($copyResponse['properties']['sheetId'])) {
        $copiedSheetId = (int) $copyResponse['properties']['sheetId'];
    }
    if ($copiedSheetId === null) {
        throw new RuntimeException('No se pudo duplicar la hoja de la plantilla.');
    }

    $requests = [];
    if ($defaultSheetId !== null && $defaultSheetId !== $copiedSheetId) {
        $requests[] = ['deleteSheet' => ['sheetId' => $defaultSheetId]];
    }
    if ($sheetTitle !== '') {
        $requests[] = [
            'updateSheetProperties' => [
                'properties' => [
                    'sheetId' => $copiedSheetId,
                    'title'   => $sheetTitle,
                ],
                'fields' => 'title',
            ],
        ];
    }
    if ($title !== '') {
        $requests[] = [
            'updateSpreadsheetProperties' => [
                'properties' => ['title' => $title],
                'fields'     => 'title',
            ],
        ];
    }
    if (!empty($requests)) {
        $sheetsClient->batchUpdate($spreadsheetId, $requests);
    }

    $valueUpdates = build_values_updates($config, $signatures, $extraFields, $year, $revisionNumber);
    if (!empty($valueUpdates)) {
        $sheetsClient->updateValues($spreadsheetId, $valueUpdates);
    }

    if (!empty($template['drive_folder_id'])) {
        try {
            $driveClient->updateFile($spreadsheetId, [], [
                'addParents'    => (string) $template['drive_folder_id'],
                'removeParents' => 'root',
                'fields'        => 'id, parents',
            ]);
        } catch (Throwable $e) {
            // Continuar incluso si mover el archivo falla.
        }
    }

    $pdfData = $driveClient->exportFile($spreadsheetId, 'application/pdf');
    $spreadsheetUrl = 'https://docs.google.com/spreadsheets/d/' . $spreadsheetId;

    log_report_generation($conn, $template, $usuarioId, $empresaId, $year, $revisionNumber, $signatures, $extraFields, $spreadsheetId, $spreadsheetUrl);

    return [
        'file_id'          => $spreadsheetId,
        'spreadsheet_url'  => $spreadsheetUrl,
        'title'            => $title,
        'pdf_base64'       => base64_encode($pdfData),
        'copied_sheet_id'  => $copiedSheetId,
    ];
}

function build_from_pattern(string $pattern, string $key, int $year, int $revisionNumber): string
{
    $pattern = trim($pattern);
    if ($pattern === '') {
        $rev = str_pad((string) $revisionNumber, 2, '0', STR_PAD_LEFT);
        return sprintf('%s %d Rev %s', $key, $year, $rev);
    }

    $replacements = [
        '{{template}}' => $key,
        '{{key}}'      => $key,
        '{{year}}'     => (string) $year,
        '{{revision}}' => str_pad((string) $revisionNumber, 2, '0', STR_PAD_LEFT),
    ];

    return strtr($pattern, $replacements);
}

/**
 * @param array<string,mixed> $config
 * @param array<int,mixed> $signatures
 * @param array<string,mixed> $extraFields
 * @return array<int,array<string,mixed>>
 */
function build_values_updates(array $config, array $signatures, array $extraFields, int $year, int $revisionNumber): array
{
    $updates = [];

    $yearRange = isset($config['year_range']) ? trim((string) $config['year_range']) : '';
    if ($yearRange !== '') {
        $updates[] = ['range' => $yearRange, 'values' => [[(string) $year]]];
    }

    $revisionRange = isset($config['revision_range']) ? trim((string) $config['revision_range']) : '';
    if ($revisionRange !== '') {
        $updates[] = ['range' => $revisionRange, 'values' => [[(string) $revisionNumber]]];
    }

    $signatureRanges = [];
    if (isset($config['signature_ranges']) && is_array($config['signature_ranges'])) {
        foreach ($config['signature_ranges'] as $range) {
            if (is_string($range) && trim($range) !== '') {
                $signatureRanges[] = trim($range);
            }
        }
    }

    $signatureStart = isset($config['signature_start']) ? trim((string) $config['signature_start']) : '';

    foreach ($signatures as $index => $signature) {
        $range = $signatureRanges[$index] ?? ($signatureStart !== '' ? offset_range($signatureStart, $index) : null);
        if ($range === null) {
            continue;
        }
        $updates[] = [
            'range'  => $range,
            'values' => [[format_signature_value($signature)]],
        ];
    }

    if (isset($config['fields']) && is_array($config['fields'])) {
        foreach ($config['fields'] as $fieldKey => $range) {
            if (!is_string($fieldKey) || !isset($extraFields[$fieldKey])) {
                continue;
            }
            if (!is_string($range) || trim($range) === '') {
                continue;
            }
            $value = $extraFields[$fieldKey];
            if (is_array($value)) {
                $value = implode(' / ', array_map('strval', $value));
            }
            $updates[] = [
                'range'  => trim($range),
                'values' => [[(string) $value]],
            ];
        }
    }

    return $updates;
}

/**
 * @param mixed $signature
 */
function format_signature_value($signature): string
{
    if (is_array($signature)) {
        $parts = [];
        foreach ($signature as $value) {
            if (is_scalar($value) && trim((string) $value) !== '') {
                $parts[] = trim((string) $value);
            }
        }
        if (!empty($parts)) {
            return implode("\n", $parts);
        }
    }
    if (is_scalar($signature)) {
        return trim((string) $signature);
    }
    return '';
}

function offset_range(string $startRange, int $offset): ?string
{
    $pattern = '/^(?:(?<sheet>[^!]+)!)?(?<column>[A-Z]+)(?<row>\d+)$/';
    if (!preg_match($pattern, $startRange, $matches)) {
        return null;
    }
    $sheet = isset($matches['sheet']) ? (string) $matches['sheet'] : '';
    $column = (string) $matches['column'];
    $row = (int) $matches['row'] + $offset;
    return ($sheet !== '' ? $sheet . '!' : '') . $column . $row;
}

/**
 * @param array<int,mixed> $signatures
 * @param array<string,mixed> $extraFields
 */
function log_report_generation(mysqli $conn, array $template, int $usuarioId, ?int $empresaId, int $year, int $revisionNumber, array $signatures, array $extraFields, string $spreadsheetId, string $spreadsheetUrl): void
{
    $sql = 'INSERT INTO report_template_exports (usuario_id, empresa_id, template_id, template_key, year_value, revision_number, firmas_json, parametros_json, drive_file_id, drive_file_url)'
         . ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        throw new RuntimeException('No se pudo registrar la operación en la bitácora.');
    }

    $firmaJson = json_encode($signatures, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    $parametrosJson = json_encode([
        'year'       => $year,
        'revision'   => $revisionNumber,
        'fields'     => $extraFields,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

    $empresaParam = $empresaId;
    $templateId = (int) $template['id'];
    $templateKey = (string) $template['template_key'];
    $yearValue = $year;
    $revisionValue = $revisionNumber;
    $fileId = $spreadsheetId;
    $fileUrl = $spreadsheetUrl;

    $stmt->bind_param(
        'iiisiissss',
        $usuarioId,
        $empresaParam,
        $templateId,
        $templateKey,
        $yearValue,
        $revisionValue,
        $firmaJson,
        $parametrosJson,
        $fileId,
        $fileUrl
    );

    $stmt->execute();
    $stmt->close();
}
