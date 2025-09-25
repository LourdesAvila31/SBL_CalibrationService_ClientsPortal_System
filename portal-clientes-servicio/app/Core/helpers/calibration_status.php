<?php

declare(strict_types=1);

/**
 * Calcula el estado de calibración a partir de la próxima fecha disponible.
 *
 * @param string|null $proximaFecha Fecha próxima almacenada en base de datos.
 * @param DateTimeZone|null $zonaHoraria Zona horaria a utilizar. Si es null se usa la zona por defecto del sistema.
 * @param DateTimeInterface|null $momentoReferencia Fecha de referencia para el cálculo. Si es null se usa "ahora".
 *
 * @return array{estado_calibracion: string, calibration_status: string, dias_restantes: int|null}
 */
function calcularEstadoCalibracion(
    ?string $proximaFecha,
    ?DateTimeZone $zonaHoraria = null,
    ?DateTimeInterface $momentoReferencia = null
): array {
    $estado = 'sin_programar';
    $calibrationStatus = 'sin_programar';
    $diasRestantes = null;

    $fechaLimpia = trim((string)($proximaFecha ?? ''));
    if ($fechaLimpia === '') {
        return [
            'estado_calibracion' => $estado,
            'calibration_status' => $calibrationStatus,
            'dias_restantes' => $diasRestantes,
        ];
    }

    if ($zonaHoraria === null) {
        $zonaPredeterminada = date_default_timezone_get();
        try {
            $zonaHoraria = new DateTimeZone($zonaPredeterminada ?: 'UTC');
        } catch (Throwable $e) {
            $zonaHoraria = new DateTimeZone('UTC');
        }
    }

    try {
        $fechaObjetivo = new DateTimeImmutable($fechaLimpia, $zonaHoraria);
    } catch (Throwable $e) {
        try {
            $fechaObjetivo = (new DateTimeImmutable($fechaLimpia))->setTimezone($zonaHoraria);
        } catch (Throwable $inner) {
            return [
                'estado_calibracion' => 'desconocido',
                'calibration_status' => 'desconocido',
                'dias_restantes' => null,
            ];
        }
    }

    if ($momentoReferencia === null) {
        $momentoReferencia = new DateTimeImmutable('now', $zonaHoraria);
    } else {
        $momentoReferencia = $momentoReferencia->setTimezone($zonaHoraria);
    }

    $inicio = $momentoReferencia->setTime(0, 0, 0, 0);
    $destino = $fechaObjetivo->setTime(0, 0, 0, 0);
    $diasRestantes = (int)$inicio->diff($destino)->format('%r%a');

    if ($diasRestantes < 0) {
        $estado = 'vencido';
        $calibrationStatus = 'vencido';
    } elseif ($diasRestantes <= 30) {
        $estado = 'proximo';
        $calibrationStatus = 'por_vencer';
    } else {
        $estado = 'vigente';
        $calibrationStatus = 'vigente';
    }

    return [
        'estado_calibracion' => $estado,
        'calibration_status' => $calibrationStatus,
        'dias_restantes' => $diasRestantes,
    ];
}

/**
 * Normaliza la respuesta del helper para integrarla con filas de base de datos.
 *
 * @param array<string, mixed> $registro Fila de base de datos que contiene la columna proxima_calibracion.
 * @param DateTimeZone|null $zonaHoraria Zona horaria a utilizar.
 *
 * @return array<string, mixed>
 */
function anexarEstadoCalibracion(array $registro, ?DateTimeZone $zonaHoraria = null): array
{
    $resultado = calcularEstadoCalibracion($registro['proxima_calibracion'] ?? null, $zonaHoraria);
    $registro['estado_calibracion'] = $resultado['estado_calibracion'];
    $registro['calibration_status'] = $resultado['calibration_status'];
    $registro['dias_restantes'] = $resultado['dias_restantes'];

    return $registro;
}
