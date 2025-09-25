<?php


if (!function_exists('logistics_state_sequence')) {
    /**
     * Estados válidos para la trazabilidad logística.
     *
     * @return array<int,string>
     */
    function logistics_state_sequence(): array
    {
        return ['Pendiente', 'Enviado', 'En tránsito', 'Recibido'];
    }
}

if (!function_exists('logistics_normalize_state')) {
    /**
     * Normaliza cualquier valor recibido a un estado válido.
     */
    function logistics_normalize_state(?string $state): string
    {
        if ($state === null) {
            return 'Pendiente';
        }

        $clean = trim($state);
        if ($clean === '') {
            return 'Pendiente';
        }

        $lower = strtolower($clean);
        switch ($lower) {
            case 'enviado':
                return 'Enviado';
            case 'en transito':
            case 'en tránsito':
            case 'en-transito':
            case 'en-tránsito':
                return 'En tránsito';
            case 'recibido':
            case 'devuelto':
            case 'retornado':
                return 'Recibido';
            case 'pendiente':
            default:
                return 'Pendiente';
        }
    }
}

if (!function_exists('logistics_date_from_input')) {
    /**
     * Valida y normaliza fechas en formato YYYY-MM-DD.
     */
    function logistics_date_from_input(?string $value): ?string
    {
        if ($value === null) {
            return null;
        }
        $trimmed = trim($value);
        if ($trimmed === '') {
            return null;
        }
        $date = DateTime::createFromFormat('Y-m-d', $trimmed);
        if ($date === false) {
            return null;
        }
        return $date->format('Y-m-d');
    }
}

if (!function_exists('logistics_infer_state_from_dates')) {
    /**
     * Determina el estado más avanzado en función de las fechas capturadas.
     *
     * @param array<string,?string> $dates
     */
    function logistics_infer_state_from_dates(array $dates, string $state): string
    {
        $normalized = logistics_normalize_state($state);
        $sequence = logistics_state_sequence();
        $position = array_search($normalized, $sequence, true);
        if ($position === false) {
            $position = 0;
        }

        if (!empty($dates['fecha_retorno']) || !empty($dates['fecha_recibido'])) {
            return 'Recibido';
        }
        if (!empty($dates['fecha_en_transito'])) {
            return max_state($sequence, $position, 'En tránsito');
        }
        if (!empty($dates['fecha_envio'])) {
            return max_state($sequence, $position, 'Enviado');
        }

        return $sequence[$position] ?? 'Pendiente';
    }
}

if (!function_exists('max_state')) {
    /**
     * Devuelve el estado más avanzado entre el estado actual y uno candidato.
     *
     * @param array<int,string> $sequence
     */
    function max_state(array $sequence, int $currentIndex, string $candidate): string
    {
        $candidateIndex = array_search($candidate, $sequence, true);
        if ($candidateIndex === false) {
            return $sequence[$currentIndex] ?? 'Pendiente';
        }

        return $sequence[max($currentIndex, $candidateIndex)] ?? $candidate;
    }
}

if (!function_exists('logistics_extract_from_row')) {
    /**
     * Convierte una fila plana en una estructura legible para el frontend.
     *
     * @param array<string,mixed> $row
     */
    function logistics_extract_from_row(array $row, string $prefix = 'log_'): array
    {
        $estadoKey = $prefix . 'estado';
        $dates = [
            'fecha_envio' => $row[$prefix . 'fecha_envio'] ?? null,
            'fecha_en_transito' => $row[$prefix . 'fecha_en_transito'] ?? null,
            'fecha_recibido' => $row[$prefix . 'fecha_recibido'] ?? null,
            'fecha_retorno' => $row[$prefix . 'fecha_retorno'] ?? null,
        ];

        foreach ($dates as $key => $value) {
            if ($value instanceof DateTimeInterface) {
                $dates[$key] = $value->format('Y-m-d');
            } elseif (is_string($value)) {
                $dates[$key] = logistics_date_from_input($value);
            } else {
                $dates[$key] = null;
            }
        }

        $estado = logistics_infer_state_from_dates($dates, (string) ($row[$estadoKey] ?? 'Pendiente'));

        return [
            'estado' => $estado,
            'proveedor' => $row[$prefix . 'proveedor_externo'] ?? null,
            'transportista' => $row[$prefix . 'transportista'] ?? null,
            'numero_guia' => $row[$prefix . 'numero_guia'] ?? null,
            'orden_servicio' => $row[$prefix . 'orden_servicio'] ?? null,
            'comentarios' => $row[$prefix . 'comentarios'] ?? null,
            'fecha_envio' => $dates['fecha_envio'],
            'fecha_en_transito' => $dates['fecha_en_transito'],
            'fecha_recibido' => $dates['fecha_recibido'],
            'fecha_retorno' => $dates['fecha_retorno'],
        ];
    }
}

if (!function_exists('logistics_build_timeline')) {
    /**
     * Construye un arreglo ordenado para representar la línea de tiempo.
     *
     * @param array<string,mixed> $logistica
     * @return array<int,array<string,mixed>>
     */
    function logistics_build_timeline(array $logistica): array
    {
        $estadoActual = logistics_normalize_state($logistica['estado'] ?? 'Pendiente');
        $sequence = logistics_state_sequence();
        $currentIndex = array_search($estadoActual, $sequence, true);
        if ($currentIndex === false) {
            $currentIndex = 0;
        }

        $timeline = [];
        $dateMap = [
            'Pendiente' => null,
            'Enviado' => $logistica['fecha_envio'] ?? null,
            'En tránsito' => $logistica['fecha_en_transito'] ?? null,
            'Recibido' => $logistica['fecha_retorno'] ?? ($logistica['fecha_recibido'] ?? null),
        ];

        foreach ($sequence as $index => $estado) {
            $timeline[] = [
                'estado' => $estado,
                'fecha' => $dateMap[$estado] ?? null,
                'completado' => $index <= $currentIndex,
            ];
        }

        return $timeline;
    }
}

if (!function_exists('logistics_response_payload')) {
    /**
     * Prepara la estructura final enviada como respuesta JSON.
     *
     * @param array<string,mixed> $row
     */
    function logistics_response_payload(array $row, string $prefix = 'log_'): array
    {
        $data = logistics_extract_from_row($row, $prefix);
        $data['timeline'] = logistics_build_timeline($data);
        return $data;
    }
}
