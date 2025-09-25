<?php
declare(strict_types=1);

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function assert_same_sets(array $expected, array $actual, string $message): void
{
    sort($expected);
    sort($actual);
    if ($expected !== $actual) {
        throw new RuntimeException($message . ' Esperado: ' . json_encode($expected) . ' Obtenido: ' . json_encode($actual));
    }
}

try {
    require_once __DIR__ . '/../../app/Core/db.php';
    require_once __DIR__ . '/../../app/Core/helpers/company_provisioning.php';

    global $conn;
    $conn->begin_transaction();

    $empresaNombre = 'Empresa de Prueba ' . uniqid();
    $stmt = $conn->prepare('INSERT INTO empresas (nombre) VALUES (?)');
    if (!$stmt) {
        throw new RuntimeException('No se pudo preparar el registro de empresa de prueba.');
    }
    $stmt->bind_param('s', $empresaNombre);
    $stmt->execute();
    $empresaId = (int) $stmt->insert_id;
    $stmt->close();

    assert_true($empresaId > 0, 'No se pudo insertar la empresa de prueba.');

    company_provision_roles($conn, $empresaId);
    assert_true($conn->errno === 0, 'Error durante la provisión inicial de roles.');

    $rolesDelegadosEsperados = [
        'Delegado Calidad' => ['auditoria_leer', 'instrumentos_leer', 'planeacion_leer'],
        'Delegado Planta' => ['calibraciones_leer', 'instrumentos_leer', 'mensajeria_leer'],
    ];

    foreach ($rolesDelegadosEsperados as $nombreRol => $permisosEsperados) {
        $stmt = $conn->prepare('SELECT id, delegated FROM roles WHERE empresa_id = ? AND nombre = ? LIMIT 1');
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de roles.');
        }
        $stmt->bind_param('is', $empresaId, $nombreRol);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        $stmt->close();

        assert_true(is_array($row), 'No se encontró el rol esperado ' . $nombreRol);
        assert_true((int) ($row['delegated'] ?? 0) === 1, 'El rol ' . $nombreRol . ' debe estar marcado como delegado.');

        $roleId = (int) ($row['id'] ?? 0);
        assert_true($roleId > 0, 'El rol ' . $nombreRol . ' no tiene identificador válido.');

        $permStmt = $conn->prepare('SELECT p.nombre FROM role_permissions rp JOIN permissions p ON p.id = rp.permission_id WHERE rp.role_id = ?');
        if (!$permStmt) {
            throw new RuntimeException('No se pudo preparar la consulta de permisos del rol.');
        }
        $permStmt->bind_param('i', $roleId);
        $permStmt->execute();
        $permResult = $permStmt->get_result();
        $permisosObtenidos = [];
        if ($permResult) {
            while ($permRow = $permResult->fetch_assoc()) {
                $permisosObtenidos[] = (string) $permRow['nombre'];
            }
        }
        $permStmt->close();

        assert_same_sets($permisosEsperados, $permisosObtenidos, 'Los permisos del rol ' . $nombreRol . ' no coinciden.');
    }

    $rolesTenantEsperados = [
        'responsable' => [
            'adjuntos_actualizar',
            'adjuntos_crear',
            'adjuntos_eliminar',
            'adjuntos_leer',
            'auditoria_actualizar',
            'auditoria_crear',
            'auditoria_eliminar',
            'auditoria_leer',
            'calibraciones_actualizar',
            'calibraciones_crear',
            'calibraciones_eliminar',
            'calibraciones_leer',
            'clientes_actualizar',
            'clientes_crear',
            'clientes_eliminar',
            'clientes_leer',
            'configuracion_actualizar',
            'configuracion_leer',
            'instrumentos_actualizar',
            'instrumentos_crear',
            'instrumentos_eliminar',
            'instrumentos_leer',
            'mensajeria_leer',
            'mensajeria_responder',
            'planeacion_actualizar',
            'planeacion_crear',
            'planeacion_eliminar',
            'planeacion_leer',
            'proveedores_actualizar',
            'proveedores_crear',
            'proveedores_eliminar',
            'proveedores_leer',
            'reportes_actualizar',
            'reportes_crear',
            'reportes_eliminar',
            'reportes_leer',
            'usuarios_add',
            'usuarios_delete',
            'usuarios_edit',
            'usuarios_view',
        ],
        'consulta' => [
            'adjuntos_leer',
            'auditoria_leer',
            'calibraciones_leer',
            'clientes_leer',
            'instrumentos_leer',
            'mensajeria_leer',
            'planeacion_leer',
            'proveedores_leer',
            'reportes_leer',
            'usuarios_view',
        ],
    ];

    foreach ($rolesTenantEsperados as $nombreRol => $permisosEsperados) {
        $stmt = $conn->prepare('SELECT id, nombre_visible FROM tenant_roles WHERE empresa_id = ? AND nombre = ? LIMIT 1');
        if (!$stmt) {
            throw new RuntimeException('No se pudo preparar la consulta de roles tenant.');
        }
        $stmt->bind_param('is', $empresaId, $nombreRol);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result ? $result->fetch_assoc() : null;
        $stmt->close();

        assert_true(is_array($row), 'No se encontró el rol tenant ' . $nombreRol);
        assert_true(trim((string) ($row['nombre_visible'] ?? '')) !== '', 'El rol tenant debe tener nombre visible.');

        $tenantRoleId = (int) ($row['id'] ?? 0);
        assert_true($tenantRoleId > 0, 'El rol tenant ' . $nombreRol . ' no tiene identificador válido.');

        $permStmt = $conn->prepare('SELECT p.nombre FROM tenant_role_permissions trp JOIN permissions p ON p.id = trp.permission_id WHERE trp.tenant_role_id = ?');
        if (!$permStmt) {
            throw new RuntimeException('No se pudo preparar la consulta de permisos del rol tenant.');
        }
        $permStmt->bind_param('i', $tenantRoleId);
        $permStmt->execute();
        $permResult = $permStmt->get_result();
        $permisosObtenidos = [];
        if ($permResult) {
            while ($permRow = $permResult->fetch_assoc()) {
                $permisosObtenidos[] = (string) $permRow['nombre'];
            }
        }
        $permStmt->close();

        assert_same_sets($permisosEsperados, $permisosObtenidos, 'Los permisos del rol tenant ' . $nombreRol . ' no coinciden.');
    }

    company_provision_roles($conn, $empresaId);
    assert_true($conn->errno === 0, 'La provisión de roles debe ser idempotente.');

    foreach (array_keys($rolesDelegadosEsperados) as $nombreRol) {
        $count = $conn->query(
            "SELECT COUNT(*) AS total FROM role_permissions rp JOIN roles r ON r.id = rp.role_id WHERE r.empresa_id = {$empresaId} AND r.nombre = '" . $conn->real_escape_string($nombreRol) . "'"
        );
        $row = $count ? $count->fetch_assoc() : null;
        $count?->close();
        assert_true((int) ($row['total'] ?? 0) === count($rolesDelegadosEsperados[$nombreRol]), 'El rol ' . $nombreRol . ' no debe duplicar permisos.');
    }

    foreach (array_keys($rolesTenantEsperados) as $nombreRol) {
        $count = $conn->query(
            "SELECT COUNT(*) AS total FROM tenant_role_permissions trp JOIN tenant_roles tr ON tr.id = trp.tenant_role_id WHERE tr.empresa_id = {$empresaId} AND tr.nombre = '" . $conn->real_escape_string($nombreRol) . "'"
        );
        $row = $count ? $count->fetch_assoc() : null;
        $count?->close();
        assert_true((int) ($row['total'] ?? 0) === count($rolesTenantEsperados[$nombreRol]), 'El rol tenant ' . $nombreRol . ' no debe duplicar permisos.');
    }

    $conn->rollback();
    echo "\u2713 Provisión de empresas validada" . PHP_EOL;
} catch (Throwable $e) {
    if (strpos($e->getMessage(), 'No se pudo conectar a la base de datos MySQL') !== false
        || strpos($e->getMessage(), 'No such file or directory') !== false) {
        fwrite(STDERR, "⚠ Prueba de provisión de empresas omitida: {$e->getMessage()}" . PHP_EOL);
        exit(0);
    }

    if (isset($conn) && $conn instanceof mysqli && $conn->in_transaction) {
        $conn->rollback();
    }
    fwrite(STDERR, '✗ Prueba de provisión de empresas fallida: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
