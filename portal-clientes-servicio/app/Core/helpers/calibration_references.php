<?php
/**
 * Utilidades para relacionar registros de calibración con elementos de calidad.
 */

if (!function_exists('calibration_references_allowed_types')) {
    /**
     * Devuelve los tipos de referencia disponibles para las calibraciones.
     *
     * @return array<string, string> Mapa tipo => descripción legible.
     */
    function calibration_references_allowed_types(): array
    {
        return [
            'documento_calidad' => 'Documento de calidad',
            'capacitacion' => 'Capacitación',
            'accion_correctiva' => 'Acción correctiva',
        ];
    }
}

if (!function_exists('calibration_references_parse_payload')) {
    /**
     * Normaliza la carga útil recibida desde formularios o APIs.
     *
     * @param mixed $payload Cadena JSON o arreglo.
     * @return array<int, array{type: string, reference_id: int|null, label: string|null, link: string|null}>
     */
    function calibration_references_parse_payload($payload): array
    {
        if ($payload === null || $payload === '') {
            return [];
        }

        if (is_string($payload)) {
            $decoded = json_decode($payload, true);
            if (!is_array($decoded)) {
                return [];
            }
        } elseif (is_array($payload)) {
            $decoded = $payload;
        } else {
            return [];
        }

        $allowed = array_keys(calibration_references_allowed_types());
        $result = [];

        foreach ($decoded as $item) {
            if (!is_array($item)) {
                continue;
            }

            $type = isset($item['type']) ? (string) $item['type'] : '';
            if ($type === '' || !in_array($type, $allowed, true)) {
                continue;
            }

            $referenceId = null;
            if (array_key_exists('reference_id', $item) && $item['reference_id'] !== null && $item['reference_id'] !== '') {
                $validated = filter_var($item['reference_id'], FILTER_VALIDATE_INT);
                if ($validated === false) {
                    continue;
                }
                $referenceId = (int) $validated;
            }

            $label = trim((string) ($item['label'] ?? ''));
            $link = trim((string) ($item['link'] ?? ''));

            if ($label === '') {
                $label = null;
            } elseif (mb_strlen($label) > 190) {
                $label = mb_substr($label, 0, 190);
            }

            if ($link === '') {
                $link = null;
            } elseif (mb_strlen($link) > 500) {
                $link = mb_substr($link, 0, 500);
            }

            if ($label === null && $link === null && $referenceId === null) {
                continue;
            }

            $result[] = [
                'type' => $type,
                'reference_id' => $referenceId,
                'label' => $label,
                'link' => $link,
            ];
        }

        return $result;
    }
}

if (!function_exists('calibration_references_sync')) {
    /**
     * Reemplaza las referencias asociadas a una calibración.
     *
     * @param array<int, array{type: string, reference_id: int|null, label: string|null, link: string|null}> $references
     */
    function calibration_references_sync(mysqli $conn, int $calibrationId, int $empresaId, array $references): void
    {
        $delete = $conn->prepare('DELETE FROM calibracion_referencias WHERE calibracion_id = ? AND empresa_id = ?');
        if ($delete) {
            $delete->bind_param('ii', $calibrationId, $empresaId);
            $delete->execute();
            $delete->close();
        }

        if ($references === []) {
            return;
        }

        $insert = $conn->prepare('INSERT INTO calibracion_referencias (calibracion_id, empresa_id, tipo, referencia_id, etiqueta, enlace) VALUES (?, ?, ?, ?, ?, ?)');
        if (!$insert) {
            return;
        }

        $calIdParam = $calibrationId;
        $empresaParam = $empresaId;
        $tipoParam = '';
        $referenciaParam = null;
        $etiquetaParam = null;
        $enlaceParam = null;

        $insert->bind_param('iisiss', $calIdParam, $empresaParam, $tipoParam, $referenciaParam, $etiquetaParam, $enlaceParam);

        foreach ($references as $reference) {
            $tipoParam = (string) $reference['type'];
            $referenciaParam = $reference['reference_id'];
            $etiquetaParam = $reference['label'];
            $enlaceParam = $reference['link'];
            $insert->execute();
        }

        $insert->close();
    }
}

if (!function_exists('calibration_references_fetch')) {
    /**
     * Obtiene las referencias agrupadas por identificador de calibración.
     *
     * @param array<int, int> $calibrationIds
     * @return array<int, array<int, array{type: string, reference_id: int|null, label: string|null, link: string|null}>>
     */
    function calibration_references_fetch(mysqli $conn, array $calibrationIds): array
    {
        $ids = array_values(array_unique(array_filter(array_map('intval', $calibrationIds))));
        if ($ids === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $types = str_repeat('i', count($ids));
        $sql = 'SELECT calibracion_id, tipo, referencia_id, etiqueta, enlace FROM calibracion_referencias WHERE calibracion_id IN (' . $placeholders . ') ORDER BY id';

        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return [];
        }

        $params = [];
        foreach ($ids as $index => $value) {
            $params[$index] = &$ids[$index];
        }
        array_unshift($params, $types);

        call_user_func_array([$stmt, 'bind_param'], $params);

        if (!$stmt->execute()) {
            $stmt->close();
            return [];
        }

        $result = $stmt->get_result();
        $grouped = [];
        if ($result) {
            while ($row = $result->fetch_assoc()) {
                $calibrationId = (int) $row['calibracion_id'];
                if (!isset($grouped[$calibrationId])) {
                    $grouped[$calibrationId] = [];
                }
                $grouped[$calibrationId][] = [
                    'type' => (string) $row['tipo'],
                    'reference_id' => isset($row['referencia_id']) ? ($row['referencia_id'] !== null ? (int) $row['referencia_id'] : null) : null,
                    'label' => isset($row['etiqueta']) && $row['etiqueta'] !== null ? (string) $row['etiqueta'] : null,
                    'link' => isset($row['enlace']) && $row['enlace'] !== null ? (string) $row['enlace'] : null,
                ];
            }
        }

        $stmt->close();

        return $grouped;
    }
}
