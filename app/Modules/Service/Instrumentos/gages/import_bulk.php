<?php

declare(strict_types=1);

use RuntimeException;
use Throwable;

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';

ensure_portal_access('service');

if (!check_permission('instrumentos_crear')) {
    http_response_code(403);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['success' => false, 'error' => 'Acceso denegado.']);
    exit;
}

require_once dirname(__DIR__, 4) . '/Core/db.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once __DIR__ . '/BulkGageImporter.php';

header('Content-Type: application/json; charset=utf-8');

try {
    if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
        http_response_code(405);
        throw new RuntimeException('Método no permitido.');
    }

    if (!isset($_FILES['archivo'])) {
        http_response_code(400);
        throw new RuntimeException('Selecciona un archivo CSV o Excel para continuar.');
    }

    $archivo = $_FILES['archivo'];
    if (!is_array($archivo) || ($archivo['error'] ?? UPLOAD_ERR_OK) !== UPLOAD_ERR_OK) {
        http_response_code(400);
        $code = $archivo['error'] ?? UPLOAD_ERR_NO_FILE;
        $mensaje = match ($code) {
            UPLOAD_ERR_INI_SIZE, UPLOAD_ERR_FORM_SIZE => 'El archivo excede el tamaño permitido.',
            UPLOAD_ERR_NO_FILE => 'No se recibió ningún archivo para procesar.',
            default => 'No se pudo cargar el archivo proporcionado.',
        };
        throw new RuntimeException($mensaje);
    }

    $empresaId = obtenerEmpresaId();
    $usuarioId = $_SESSION['usuario_id'] ?? null;
    $usuarioId = is_numeric($usuarioId) ? (int) $usuarioId : null;

    $importer = new BulkGageImporter($conn, $empresaId, $usuarioId);
    $resultado = $importer->importUploadedFile($archivo);

    $response = [
        'success'     => (bool) ($resultado['success'] ?? true),
        'summary'     => $resultado['summary'] ?? [],
        'results'     => $resultado['results'] ?? [],
        'hasFailures' => (bool) ($resultado['hasFailures'] ?? false),
        'message'     => $resultado['message'] ?? null,
        'filename'    => $archivo['name'] ?? null,
    ];

    echo json_encode($response, JSON_UNESCAPED_UNICODE);
} catch (RuntimeException $e) {
    $payload = ['success' => false, 'error' => $e->getMessage()];
    echo json_encode($payload, JSON_UNESCAPED_UNICODE);
} catch (Throwable $e) {
    http_response_code(500);
    $payload = ['success' => false, 'error' => 'Error inesperado: ' . $e->getMessage()];
    echo json_encode($payload, JSON_UNESCAPED_UNICODE);
} finally {
    if (isset($conn) && $conn instanceof mysqli) {
        $conn->close();
    }
}
