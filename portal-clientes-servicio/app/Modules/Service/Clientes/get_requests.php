<?php
session_start();

header('Content-Type: application/json; charset=utf-8');

require_once dirname(__DIR__, 3) . '/Core/permissions.php';
ensure_portal_access('service');
if (!check_permission('clientes_gestionar')) {
    http_response_code(403);
    echo json_encode(['error' => 'Acceso no autorizado']);
    exit;
}

require_once dirname(__DIR__, 3) . '/Core/db.php';

$empresaId = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);
$estado = trim((string) ($_GET['estado'] ?? ''));
$estado = $estado !== '' ? $estado : null;
$desde = trim((string) ($_GET['desde'] ?? ''));
$hasta = trim((string) ($_GET['hasta'] ?? ''));

$dateFilter = function (?string $value): ?string {
    if (!$value) {
        return null;
    }
    $parsed = DateTime::createFromFormat('Y-m-d', $value) ?: DateTime::createFromFormat('d/m/Y', $value);
    return $parsed ? $parsed->format('Y-m-d') : null;
};

$desde = $dateFilter($desde);
$hasta = $dateFilter($hasta);

try {
    $sql = "SELECT s.id, s.empresa_id, s.instrumento_id, s.usuario_id, s.tipo, s.fecha_deseada, s.estado,
                   s.fecha_solicitud, s.comentarios, s.instrucciones_cliente,
                   e.nombre AS empresa_nombre,
                   i.codigo AS instrumento_codigo,
                   COALESCE(ci.nombre, '') AS instrumento_nombre,
                   CONCAT(u.nombre, ' ', u.apellidos) AS solicitante
            FROM solicitudes_calibracion s
            LEFT JOIN empresas e ON e.id = s.empresa_id
            LEFT JOIN instrumentos i ON i.id = s.instrumento_id
            LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
            LEFT JOIN usuarios u ON u.id = s.usuario_id
            WHERE 1 = 1";

    $types = '';
    $params = [];

    if ($empresaId) {
        $sql .= ' AND s.empresa_id = ?';
        $types .= 'i';
        $params[] = $empresaId;
    }

    if ($estado) {
        $sql .= ' AND s.estado = ?';
        $types .= 's';
        $params[] = $estado;
    }

    if ($desde) {
        $sql .= ' AND DATE(s.fecha_solicitud) >= ?';
        $types .= 's';
        $params[] = $desde;
    }

    if ($hasta) {
        $sql .= ' AND DATE(s.fecha_solicitud) <= ?';
        $types .= 's';
        $params[] = $hasta;
    }

    $sql .= ' ORDER BY s.fecha_solicitud DESC, s.id DESC';

    if ($types) {
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No fue posible consultar las solicitudes.');
        }
        $stmt->bind_param($types, ...$params);
        $stmt->execute();
        $result = $stmt->get_result();
    } else {
        $result = $conn->query($sql);
    }

    $solicitudes = [];
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $solicitudes[] = [
                'id' => (int) ($row['id'] ?? 0),
                'empresa_id' => (int) ($row['empresa_id'] ?? 0),
                'instrumento_id' => isset($row['instrumento_id']) ? (int) $row['instrumento_id'] : null,
                'usuario_id' => isset($row['usuario_id']) ? (int) $row['usuario_id'] : null,
                'tipo' => $row['tipo'] ?? '',
                'fecha_deseada' => $row['fecha_deseada'] ?? null,
                'estado' => $row['estado'] ?? '',
                'fecha_solicitud' => $row['fecha_solicitud'] ?? null,
                'comentarios' => $row['comentarios'] ?? '',
                'empresa_nombre' => $row['empresa_nombre'] ?? '',
                'instrumento_codigo' => $row['instrumento_codigo'] ?? '',
                'instrumento_nombre' => $row['instrumento_nombre'] ?? '',
                'solicitante' => trim((string) ($row['solicitante'] ?? '')),
                'instrucciones_cliente' => $row['instrucciones_cliente'] ?? null,
            ];
        }
    }

    echo json_encode(['solicitudes' => $solicitudes]);
} catch (Throwable $e) {
    error_log('Error al listar solicitudes de clientes: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible recuperar las solicitudes.']);
}
