<?php

if (!function_exists('tenant_latest_certificate')) {
    function tenant_latest_certificate(mysqli $conn, int $instrumentoId, int $empresaId): ?array
    {
        $sql = "SELECT cert.id AS certificado_id, cert.archivo, cal.id AS calibracion_id, cal.fecha_calibracion\n                FROM calibraciones cal\n                JOIN certificados cert ON cert.calibracion_id = cal.id\n                WHERE cal.instrumento_id = ? AND cal.empresa_id = ?\n                ORDER BY cal.fecha_calibracion DESC, cal.id DESC, cert.id DESC\n                LIMIT 1";

        $stmt = $conn->prepare($sql);
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

        $archivo = basename((string) ($row['archivo'] ?? ''));
        if ($archivo === '') {
            return null;
        }

        return [
            'certificado_id' => isset($row['certificado_id']) ? (int) $row['certificado_id'] : 0,
            'archivo' => $archivo,
            'calibration_id' => isset($row['calibracion_id']) ? (int) $row['calibracion_id'] : 0,
            'fecha_calibracion' => $row['fecha_calibracion'] ?? null,
        ];
    }
}
