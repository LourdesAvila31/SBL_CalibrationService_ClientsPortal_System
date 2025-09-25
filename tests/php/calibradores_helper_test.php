<?php

declare(strict_types=1);

require_once __DIR__ . '/../../app/Core/helpers/calibradores.php';

function assertEquals($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        fwrite(STDERR, sprintf("✗ %s. Esperado: %s, obtenido: %s\n", $message, var_export($expected, true), var_export($actual, true)));
        exit(1);
    }
}

$uuid = '123e4567-e89b-12d3-a456-426614174000';
$timestamp = '2024-05-01T12:00:00Z';
$payload = json_encode(['magnitud' => 'temperatura', 'valor' => 23.5, 'unidad' => '°C'], JSON_UNESCAPED_UNICODE);
$token = 'prueba-token';

$signature = calibrator_compute_signature($uuid, $timestamp, $payload, $token);
assertEquals(true, calibrator_verify_signature($uuid, $timestamp, $payload, $token, $signature), 'La firma generada debe validarse correctamente');
assertEquals(false, calibrator_verify_signature($uuid, $timestamp, $payload, $token, 'firma-invalida'), 'Una firma alterada debe rechazarse');

assertEquals(true, calibrator_validate_selection(null, null), 'Sin medición previa se permite cualquier selección');
assertEquals(true, calibrator_validate_selection(5, null), 'Una medición puede asignarse si no hay selección manual');
assertEquals(true, calibrator_validate_selection(7, 7), 'La selección coincide con la medición');
assertEquals(false, calibrator_validate_selection(2, 3), 'Calibrador distinto debe rechazarse');

echo "✓ Pruebas de helpers de calibradores superadas" . PHP_EOL;
