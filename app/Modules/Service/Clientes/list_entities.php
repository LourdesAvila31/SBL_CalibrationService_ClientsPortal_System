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

$empresaFiltro = filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]]);

try {
    $empresas = [];

    $sql = "SELECT e.id, e.nombre, e.contacto, e.telefono, e.email, e.direccion, e.activo,
                   (
                        SELECT COUNT(*) FROM usuarios u
                        JOIN roles r ON u.role_id = r.id
                        WHERE u.empresa_id = e.id AND LOWER(r.nombre) = 'cliente'
                   ) AS total_clientes,
                   (
                        SELECT COUNT(*) FROM instrumentos i WHERE i.empresa_id = e.id
                   ) AS total_instrumentos,
                   (
                        SELECT COUNT(*) FROM calibraciones c WHERE c.empresa_id = e.id
                   ) AS total_calibraciones,
                   (
                        SELECT COUNT(*) FROM solicitudes_calibracion s
                        WHERE s.empresa_id = e.id AND (s.estado IS NULL OR s.estado = '' OR s.estado = 'Pendiente')
                   ) AS solicitudes_pendientes
            FROM empresas e";

    $params = [];
    $types = '';
    if ($empresaFiltro) {
        $sql .= ' WHERE e.id = ?';
        $params[] = $empresaFiltro;
        $types .= 'i';
    }

    $sql .= ' ORDER BY e.nombre ASC';

    if ($types) {
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            throw new RuntimeException('No fue posible preparar la consulta principal.');
        }
        $stmt->bind_param($types, ...$params);
        $stmt->execute();
        $result = $stmt->get_result();
    } else {
        $result = $conn->query($sql);
    }

    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $empresaId = (int) ($row['id'] ?? 0);
            $empresas[$empresaId] = [
                'id' => $empresaId,
                'nombre' => $row['nombre'] ?? '',
                'contacto' => $row['contacto'] ?? '',
                'telefono' => $row['telefono'] ?? '',
                'email' => $row['email'] ?? '',
                'direccion' => $row['direccion'] ?? '',
                'activo' => (int) ($row['activo'] ?? 0) === 1,
                'totales' => [
                    'clientes' => (int) ($row['total_clientes'] ?? 0),
                    'instrumentos' => (int) ($row['total_instrumentos'] ?? 0),
                    'calibraciones' => (int) ($row['total_calibraciones'] ?? 0),
                    'solicitudes_pendientes' => (int) ($row['solicitudes_pendientes'] ?? 0),
                ],
                'clientes' => [],
            ];
        }
    }

    if (!empty($empresas)) {
        $clienteSql = "SELECT u.id, u.nombre, u.apellidos, u.correo, u.telefono, u.puesto, u.empresa_id
                        FROM usuarios u
                        JOIN roles r ON u.role_id = r.id
                        WHERE LOWER(r.nombre) = 'cliente'";
        if ($empresaFiltro) {
            $clienteSql .= ' AND u.empresa_id = ?';
        }
        $clienteSql .= ' ORDER BY u.nombre ASC, u.apellidos ASC';

        if ($empresaFiltro) {
            $clienteStmt = $conn->prepare($clienteSql);
            if (!$clienteStmt) {
                throw new RuntimeException('No fue posible recuperar los clientes.');
            }
            $clienteStmt->bind_param('i', $empresaFiltro);
            $clienteStmt->execute();
            $clienteRes = $clienteStmt->get_result();
        } else {
            $clienteRes = $conn->query($clienteSql);
        }

        if ($clienteRes) {
            while ($row = $clienteRes->fetch_assoc()) {
                $empresaId = (int) ($row['empresa_id'] ?? 0);
                if (!isset($empresas[$empresaId])) {
                    continue;
                }
                $empresas[$empresaId]['clientes'][] = [
                    'id' => (int) ($row['id'] ?? 0),
                    'nombre' => trim(($row['nombre'] ?? '') . ' ' . ($row['apellidos'] ?? '')),
                    'correo' => $row['correo'] ?? '',
                    'telefono' => $row['telefono'] ?? '',
                    'puesto' => $row['puesto'] ?? '',
                ];
            }
        }
    }

    echo json_encode([
        'empresas' => array_values($empresas),
        'generated_at' => (new DateTimeImmutable())->format(DateTimeInterface::ATOM),
    ]);
} catch (Throwable $e) {
    error_log('Error en list_entities.php: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No fue posible obtener la información de clientes.']);
}
