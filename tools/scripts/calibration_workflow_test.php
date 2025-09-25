#!/usr/bin/env php
<?php
require_once dirname(__DIR__, 2) . '/app/Core/helpers/calibration_results.php';

$scenarios = [];

$scenarios['operador captura'] = function () {
    $resultado = calibration_prepare_decision_output(null, 'Conforme', null);
    if ($resultado['estado'] !== 'Pendiente') {
        throw new RuntimeException('El estado inicial debe ser Pendiente.');
    }
    if ($resultado['resultado'] !== 'Conforme') {
        throw new RuntimeException('El resultado preliminar debe conservarse.');
    }
    if ($resultado['estado_resultado'] !== 'Pendiente') {
        throw new RuntimeException('El estado del resultado debe permanecer Pendiente.');
    }
};

$scenarios['supervisor aprueba'] = function () {
    $resultado = calibration_prepare_decision_output('Aprobado', 'Conforme', 'Conforme');
    if ($resultado['estado_resultado'] !== 'Liberado') {
        throw new RuntimeException('Un resultado aprobado debe marcarse como Liberado.');
    }
    if ($resultado['resultado'] !== 'Conforme') {
        throw new RuntimeException('El resultado liberado debe coincidir con el preliminar.');
    }
};

$scenarios['rechazo'] = function () {
    $resultado = calibration_prepare_decision_output('Rechazado', 'No conforme', null);
    if ($resultado['estado_resultado'] !== 'Rechazado') {
        throw new RuntimeException('El estado de resultado debe indicar Rechazado.');
    }
    if ($resultado['resultado'] !== 'No conforme') {
        throw new RuntimeException('El rechazo debe conservar el resultado capturado.');
    }
};

$scenarios['consultas filtran por estado'] = function () {
    $data = [
        ['estado' => 'Pendiente', 'pre' => 'Conforme', 'liberado' => null],
        ['estado' => 'Aprobado', 'pre' => 'Conforme', 'liberado' => 'Conforme'],
        ['estado' => 'Rechazado', 'pre' => 'No conforme', 'liberado' => null],
    ];
    $evaluated = array_map(function ($row) {
        return calibration_prepare_decision_output($row['estado'], $row['pre'], $row['liberado']);
    }, $data);
    $liberados = array_filter($evaluated, function ($row) {
        return $row['estado_resultado'] === 'Liberado';
    });
    if (count($liberados) !== 1) {
        throw new RuntimeException('Las consultas deben regresar únicamente calibraciones liberadas.');
    }
    $liberado = array_values($liberados)[0];
    if ($liberado['estado'] !== 'Aprobado') {
        throw new RuntimeException('El único registro liberado debe provenir de un estado Aprobado.');
    }
};

$failures = 0;
foreach ($scenarios as $name => $scenario) {
    try {
        $scenario();
        fwrite(STDOUT, "[OK] $name" . PHP_EOL);
    } catch (Throwable $exception) {
        $failures++;
        fwrite(STDERR, "[FALLO] $name: " . $exception->getMessage() . PHP_EOL);
    }
}

if ($failures > 0) {
    fwrite(STDERR, "Se detectaron $failures escenarios con errores." . PHP_EOL);
    exit(1);
}

fwrite(STDOUT, "Todos los escenarios se validaron correctamente." . PHP_EOL);
exit(0);
