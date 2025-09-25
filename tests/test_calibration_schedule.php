<?php
require_once __DIR__ . '/../app/Core/helpers/calibration_schedule.php';

$cases = [
    [['2024-01-01', '2024-01-05', null], 4],
    [['2024-01-01', '2023-12-31', null], 0],
    [['2024-01-01', '2024-01-10', '2024-01-08'], 2],
    [['2024-01-01', null, null], null],
];

foreach ($cases as $idx => $case) {
    [$input, $expected] = $case;
    [$programada, $real, $reprogramada] = $input;
    $result = calibration_compute_delay($programada, $real, $reprogramada);
    if ($result !== $expected) {
        echo "Caso $idx: esperado $expected, obtenido $result\n";
        exit(1);
    }
}

$states = [
    ['Completada', 0, false],
    ['Reprogramada', 0, true],
    ['Atrasada', null, true],
    ['completada', 3, true],
];

foreach ($states as $idx => $stateCase) {
    [$estado, $dias, $expected] = $stateCase;
    $result = calibration_requires_justification($estado, $dias);
    if ($result !== $expected) {
        echo "Justificación $idx falló\n";
        exit(1);
    }
}

echo "OK";
