<?php

declare(strict_types=1);

/**
 * Determina si una calibración requiere registrar datos de incertidumbre
 * de acuerdo con el estado operativo y el resultado indicado.
 */
function calibration_requires_uncertainty(?string $estadoEjecucion, ?string $resultado): bool
{
    if ($estadoEjecucion === null) {
        return false;
    }

    $estadoEjecucion = trim($estadoEjecucion);
    if ($estadoEjecucion === '') {
        return false;
    }

    if (strcasecmp($estadoEjecucion, 'Completada') !== 0) {
        return false;
    }

    if ($resultado === null) {
        return false;
    }

    $resultado = trim($resultado);
    if ($resultado === '') {
        return false;
    }

    return strcasecmp($resultado, 'En proceso') !== 0;
}
