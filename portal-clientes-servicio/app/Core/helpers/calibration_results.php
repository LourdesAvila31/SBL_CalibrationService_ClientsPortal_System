<?php

if (!function_exists('calibration_normalize_decision_state')) {
    function calibration_normalize_decision_state(?string $estado): string
    {
        if (!is_string($estado)) {
            return 'Pendiente';
        }
        $normalized = strtolower(trim($estado));
        switch ($normalized) {
            case 'aprobado':
                return 'Aprobado';
            case 'rechazado':
                return 'Rechazado';
            default:
                return 'Pendiente';
        }
    }
}

if (!function_exists('calibration_prepare_decision_output')) {
    function calibration_prepare_decision_output(?string $estado, ?string $resultadoPreliminar, ?string $resultadoLiberado): array
    {
        $estadoNormalizado = calibration_normalize_decision_state($estado);

        $preliminar = trim((string) ($resultadoPreliminar ?? ''));
        $preliminar = $preliminar !== '' ? $preliminar : null;

        $liberado = trim((string) ($resultadoLiberado ?? ''));
        $liberado = $liberado !== '' ? $liberado : null;

        if ($estadoNormalizado === 'Aprobado') {
            $resultado = $liberado ?? $preliminar;
            $estadoResultado = 'Liberado';
        } elseif ($estadoNormalizado === 'Rechazado') {
            $resultado = $preliminar ?? $liberado;
            $estadoResultado = 'Rechazado';
        } else {
            $resultado = $preliminar ?? $liberado;
            $estadoResultado = 'Pendiente';
        }

        return [
            'estado' => $estadoNormalizado,
            'resultado' => $resultado,
            'resultado_preliminar' => $preliminar,
            'resultado_liberado' => $liberado,
            'estado_resultado' => $estadoResultado,
        ];
    }
}
