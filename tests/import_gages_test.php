<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/app/Modules/Tenant/Instrumentos/gages/gages_import_helpers.php';

$fixturePath = __DIR__ . '/fixtures/instrumentos_import.csv';
if (!file_exists($fixturePath)) {
    echo 'ERROR: No se encontró la fixture de instrumentos.' . PHP_EOL;
    exit(1);
}

try {
    $parsed = parse_gages_inventory_csv($fixturePath);
} catch (Throwable $e) {
    echo 'ERROR al parsear la plantilla: ' . $e->getMessage() . PHP_EOL;
    exit(1);
}

$records = $parsed['records'] ?? [];
$rowErrors = $parsed['errors'] ?? [];

if (!empty($rowErrors)) {
    echo 'ERROR: La fixture generó errores de normalización:' . PHP_EOL;
    foreach ($rowErrors as $error) {
        $row = $error['row'] ?? '?';
        $codigo = $error['codigo'] ?? 'sin código';
        echo "  - Fila {$row} ({$codigo}): {$error['error']}" . PHP_EOL;
    }
    exit(1);
}

echo '✓ CSV normalizado correctamente. Filas: ' . count($records) . PHP_EOL;

try {
    require_once dirname(__DIR__) . '/app/Core/db.php';
} catch (Throwable $e) {
    echo '⚠ Prueba de base de datos omitida: ' . $e->getMessage() . PHP_EOL;
    exit(0);
}

if (!isset($conn) || !($conn instanceof mysqli)) {
    echo '⚠ Prueba de base de datos omitida: conexión no disponible.' . PHP_EOL;
    exit(0);
}

$empresaId = 1;
$conn->begin_transaction();

try {
    $dbErrors = [];
    $summary = process_gages_import($conn, $empresaId, $records, $dbErrors);
    $totalProcesado = (int) ($summary['created'] ?? 0) + (int) ($summary['updated'] ?? 0);
    $esperado = count($records);
    if ($totalProcesado !== $esperado) {
        throw new RuntimeException("Se esperaban {$esperado} operaciones de importación, se registraron {$totalProcesado}.");
    }
    if (!empty($dbErrors)) {
        throw new RuntimeException('Se detectaron errores durante la importación: ' . json_encode($dbErrors, JSON_UNESCAPED_UNICODE));
    }

    $codigoValidacion = $records[0]['codigo'] ?? '';
    $stmt = $conn->prepare('SELECT codigo, estado, programado FROM instrumentos WHERE codigo = ? AND empresa_id = ? LIMIT 1');
    $stmt->bind_param('si', $codigoValidacion, $empresaId);
    $stmt->execute();
    $result = $stmt->get_result();
    $fila = $result ? $result->fetch_assoc() : null;
    $stmt->close();
    if (!$fila) {
        throw new RuntimeException('No se encontró el instrumento importado para validación.');
    }

    echo '✓ Importación de prueba ejecutada. Altas: ' . ($summary['created'] ?? 0);
    echo ' · Actualizaciones: ' . ($summary['updated'] ?? 0) . PHP_EOL;
    $conn->rollback();
    exit(0);
} catch (Throwable $e) {
    $conn->rollback();
    echo 'ERROR durante la importación de prueba: ' . $e->getMessage() . PHP_EOL;
    exit(1);
}
