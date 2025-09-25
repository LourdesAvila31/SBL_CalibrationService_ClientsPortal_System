<?php

declare(strict_types=1);

require_once __DIR__ . '/../../app/Core/helpers/calibration_status.php';

function assertSameValue($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        fwrite(STDERR, sprintf("Assertion failed: %s. Expected %s, got %s\n", $message, var_export($expected, true), var_export($actual, true)));
        exit(1);
    }
}

$timezone = new DateTimeZone('UTC');
$reference = new DateTimeImmutable('2024-01-10', $timezone);

$past = calcularEstadoCalibracion('2024-01-05', $timezone, $reference);
assertSameValue('vencido', $past['estado_calibracion'], 'Past date should be marked as vencido');
assertSameValue('vencido', $past['calibration_status'], 'Past date should expose vencido calibration status');
assertSameValue(-5, $past['dias_restantes'], 'Past date should return negative days');

$upcoming = calcularEstadoCalibracion('2024-01-25', $timezone, $reference);
assertSameValue('proximo', $upcoming['estado_calibracion'], 'Upcoming date within 30 days should be proximo');
assertSameValue('por_vencer', $upcoming['calibration_status'], 'Upcoming date within 30 days should be por_vencer');
assertSameValue(15, $upcoming['dias_restantes'], 'Upcoming date should calculate remaining days');

$future = calcularEstadoCalibracion('2024-02-25', $timezone, $reference);
assertSameValue('vigente', $future['estado_calibracion'], 'Future date beyond 30 days should be vigente');
assertSameValue('vigente', $future['calibration_status'], 'Future date beyond 30 days should expose vigente');
assertSameValue(46, $future['dias_restantes'], 'Future date should calculate remaining days');

$missing = calcularEstadoCalibracion(null, $timezone, $reference);
assertSameValue('sin_programar', $missing['estado_calibracion'], 'Null date should be sin_programar');
assertSameValue('sin_programar', $missing['calibration_status'], 'Null date should expose sin_programar');
assertSameValue(null, $missing['dias_restantes'], 'Null date should have null days');

echo "All calibration status assertions passed\n";
