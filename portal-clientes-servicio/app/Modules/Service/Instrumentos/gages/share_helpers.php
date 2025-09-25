<?php

require_once dirname(__DIR__, 4) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 4) . '/Core/helpers/calibration_status.php';

if (!function_exists('tenant_get_gage_share_summary')) {
    function tenant_get_gage_share_summary(mysqli $conn, int $empresaId, int $instrumentoId): ?array
    {
        $stmt = $conn->prepare(
            'SELECT i.id,
                    COALESCE(ci.nombre, "") AS nombre,
                    COALESCE(i.codigo, "") AS codigo,
                    COALESCE(i.ubicacion, "") AS ubicacion,
                    COALESCE(i.estado, "") AS estado,
                    COALESCE(d.nombre, "") AS departamento,
                    i.proxima_calibracion
               FROM instrumentos i
               LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
               LEFT JOIN departamentos d ON i.departamento_id = d.id
              WHERE i.id = ? AND i.empresa_id = ?
              LIMIT 1'
        );

        if (!$stmt) {
            return null;
        }

        $stmt->bind_param('ii', $instrumentoId, $empresaId);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        $stmt->close();

        if (!$row) {
            return null;
        }

        $zonaHoraria = obtenerZonaHorariaEmpresa($conn, $empresaId);
        if ($zonaHoraria instanceof DateTimeZone) {
            $row = anexarEstadoCalibracion($row, $zonaHoraria);
        } else {
            $row = anexarEstadoCalibracion($row);
        }

        return [
            'id' => (int) ($row['id'] ?? 0),
            'nombre' => trim((string) ($row['nombre'] ?? '')) ?: 'Instrumento',
            'codigo' => trim((string) ($row['codigo'] ?? '')),
            'ubicacion' => trim((string) ($row['ubicacion'] ?? '')),
            'estado' => trim((string) ($row['estado'] ?? '')),
            'departamento' => trim((string) ($row['departamento'] ?? '')),
            'proxima_calibracion' => $row['proxima_calibracion'] ?? null,
            'estado_calibracion' => $row['estado_calibracion'] ?? null,
            'dias_restantes' => isset($row['dias_restantes']) ? (int) $row['dias_restantes'] : null,
        ];
    }
}

if (!function_exists('tenant_get_gage_share_certificate')) {
    function tenant_get_gage_share_certificate(mysqli $conn, int $empresaId, int $instrumentoId, int $calibracionId, string $archivo): ?array
    {
        $stmt = $conn->prepare(
            'SELECT cal.fecha_calibracion,
                    cal.fecha_proxima,
                    COALESCE(cal.resultado, "") AS resultado,
                    CONCAT(TRIM(COALESCE(u.nombre, "")), " ", TRIM(COALESCE(u.apellidos, ""))) AS responsable
               FROM calibraciones cal
               JOIN instrumentos i ON i.id = cal.instrumento_id
               JOIN certificados cert ON cert.calibracion_id = cal.id
               LEFT JOIN usuarios u ON u.id = cal.liberado_por
              WHERE cal.id = ?
                AND cal.instrumento_id = ?
                AND i.empresa_id = ?
                AND cert.archivo = ?
              LIMIT 1'
        );

        if (!$stmt) {
            return null;
        }

        $stmt->bind_param('iiis', $calibracionId, $instrumentoId, $empresaId, $archivo);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        $stmt->close();

        if (!$row) {
            return null;
        }

        return [
            'fecha_calibracion' => $row['fecha_calibracion'] ?? null,
            'fecha_proxima' => $row['fecha_proxima'] ?? null,
            'resultado' => trim((string) ($row['resultado'] ?? '')),
            'responsable' => trim((string) ($row['responsable'] ?? '')),
        ];
    }
}
