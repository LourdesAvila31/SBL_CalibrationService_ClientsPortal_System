<?php

declare(strict_types=1);

require_once __DIR__ . '/bulk_import_harness.php';

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

try {
    $datos = run_bulk_import_harness();
    $resultado = $datos['resultado'];
    $insertados = $datos['insertados'];

    $summary = $resultado['summary'] ?? [];
    assert_true(($summary['total'] ?? null) === 3, 'Se esperaban 3 filas en la muestra.');
    assert_true(($summary['inserted'] ?? null) === 2, 'Se esperaban 2 registros exitosos.');
    assert_true(($summary['failed'] ?? null) === 1, 'Se esperaba 1 registro con error.');
    assert_true(count($insertados) === 2, 'El arreglo de insertados debe contener 2 elementos.');

    foreach ($insertados as $instrumento) {
        assert_true(($instrumento['empresa_id'] ?? null) === 7, 'Los instrumentos deben vincularse a la empresa de prueba.');
        assert_true(($instrumento['estado'] ?? '') === 'activo', 'El estado debe derivarse automáticamente como "activo".');
    }

    $errores = array_filter($resultado['results'] ?? [], static fn ($row) => ($row['status'] ?? '') === 'error');
    assert_true(count($errores) === 1, 'Debe existir un único registro con error.');
    $detalleError = array_values($errores)[0];
    $mensajeError = implode(' ', $detalleError['errors'] ?? []);
    assert_true(stripos($mensajeError, 'Departamento') !== false, 'El mensaje de error debe mencionar el departamento obligatorio.');

    echo "✓ Prueba de importación masiva superada" . PHP_EOL;
    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, '✗ Prueba de importación masiva fallida: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
