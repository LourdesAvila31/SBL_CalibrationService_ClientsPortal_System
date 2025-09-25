<?php
if (!function_exists('calibration_allowed_statuses')) {
    /**
     * Estados válidos para la ejecución de una calibración.
     *
     * @return string[]
     */
    function calibration_allowed_statuses(): array
    {
        return [
            'Programada',
            'En proceso',
            'Completada',
            'Reprogramada',
            'Atrasada',
            'Cancelada',
        ];
    }
}

if (!function_exists('calibration_normalize_status')) {
    /**
     * Normaliza un estado recibido desde formularios.
     */
    function calibration_normalize_status(?string $estado): string
    {
        $estado = $estado !== null ? trim($estado) : '';
        if ($estado === '') {
            return 'Completada';
        }

        $allowed = calibration_allowed_statuses();
        foreach ($allowed as $candidate) {
            if (strcasecmp($candidate, $estado) === 0) {
                return $candidate;
            }
        }

        return 'Completada';
    }
}

if (!function_exists('calibration_create_date')) {
    /**
     * Convierte una cadena YYYY-MM-DD en DateTimeImmutable.
     */
    function calibration_create_date(?string $value): ?DateTimeImmutable
    {
        if ($value === null) {
            return null;
        }
        $value = trim($value);
        if ($value === '') {
            return null;
        }

        $date = DateTimeImmutable::createFromFormat('!Y-m-d', $value);
        if ($date instanceof DateTimeImmutable) {
            return $date;
        }

        return null;
    }
}

if (!function_exists('calibration_compute_delay')) {
    /**
     * Calcula los días de atraso tomando como referencia la fecha programada o reprogramada.
     */
    function calibration_compute_delay(?string $fechaProgramada, ?string $fechaReal, ?string $fechaReprogramada = null): ?int
    {
        $real = calibration_create_date($fechaReal);
        if (!$real) {
            return null;
        }

        $referencia = calibration_create_date($fechaReprogramada) ?? calibration_create_date($fechaProgramada);
        if (!$referencia) {
            return null;
        }

        $diff = (int) $referencia->diff($real)->format('%r%a');
        return $diff > 0 ? $diff : 0;
    }
}

if (!function_exists('calibration_requires_justification')) {
    /**
     * Determina si se requiere justificación según el estado o los días de atraso.
     */
    function calibration_requires_justification(?string $estado, ?int $diasAtraso): bool
    {
        $estado = calibration_normalize_status($estado);
        $needsByState = in_array($estado, ['Reprogramada', 'Atrasada'], true);
        $needsByDelay = is_int($diasAtraso) && $diasAtraso > 0;

        return $needsByState || $needsByDelay;
    }
}
