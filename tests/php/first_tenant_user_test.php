<?php
declare(strict_types=1);

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

try {
    require_once __DIR__ . '/../../app/Core/db.php';
    require_once __DIR__ . '/../../app/Core/permissions.php';

    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    global $conn;
    $conn->begin_transaction();

    $empresaAdmin = 1;
    $adminRoleId = resolve_role_id('Administrador', $empresaAdmin);
    assert_true($adminRoleId !== null, 'No se encontró el rol Administrador.');

    $actorUsuario = 'primer_actor_' . uniqid();
    $actorCorreo = $actorUsuario . '@pruebas.local';
    $passwordHash = password_hash('Actor123*', PASSWORD_DEFAULT);

    $stmtActor = $conn->prepare('INSERT INTO usuarios (usuario, correo, nombre, apellidos, contrasena, role_id, empresa_id, activo) VALUES (?, ?, ?, ?, ?, ?, ?, 1)');
    assert_true($stmtActor !== false, 'No se pudo preparar el usuario actor.');
    $nombreActor = 'Actor';
    $apellidosActor = 'Pruebas';
    $stmtActor->bind_param('sssssii', $actorUsuario, $actorCorreo, $nombreActor, $apellidosActor, $passwordHash, $adminRoleId, $empresaAdmin);
    $stmtActor->execute();
    $actorId = (int) $stmtActor->insert_id;
    $stmtActor->close();
    assert_true($actorId > 0, 'No se pudo crear el usuario actor.');

    $_SESSION['usuario_id'] = $actorId;
    $_SESSION['usuario'] = $actorCorreo;
    $_SESSION['nombre'] = $nombreActor;
    $_SESSION['apellidos'] = $apellidosActor;
    $_SESSION['rol'] = 'Administrador';
    $_SESSION['empresa_id'] = $empresaAdmin;

    $empresaNombre = 'Empresa Test ' . uniqid();
    $stmtEmpresa = $conn->prepare('INSERT INTO empresas (nombre, contacto, direccion, telefono, email) VALUES (?, NULL, NULL, NULL, NULL)');
    assert_true($stmtEmpresa !== false, 'No se pudo preparar la empresa.');
    $stmtEmpresa->bind_param('s', $empresaNombre);
    $stmtEmpresa->execute();
    $empresaId = (int) $stmtEmpresa->insert_id;
    $stmtEmpresa->close();
    assert_true($empresaId > 0, 'No se pudo registrar la empresa de prueba.');

    $stmtTenantRole = $conn->prepare('INSERT INTO tenant_roles (empresa_id, nombre, nombre_visible) VALUES (?, ?, ?)');
    assert_true($stmtTenantRole !== false, 'No se pudo preparar el rol delegado.');
    $tenantRoleSlug = 'responsable';
    $tenantRoleVisible = 'Responsable';
    $stmtTenantRole->bind_param('iss', $empresaId, $tenantRoleSlug, $tenantRoleVisible);
    $stmtTenantRole->execute();
    $responsableTenantRoleId = (int) $stmtTenantRole->insert_id;
    $stmtTenantRole->close();
    assert_true($responsableTenantRoleId > 0, 'No se pudo registrar el rol delegado responsable.');

    $permisosUsuarios = ['usuarios_view', 'usuarios_add', 'usuarios_edit', 'usuarios_delete'];
    $quotedPermisos = array_map([$conn, 'real_escape_string'], $permisosUsuarios);
    $quotedPermisos = array_map(static fn ($nombre) => "'{$nombre}'", $quotedPermisos);
    $permsResult = $conn->query('SELECT id, nombre FROM permissions WHERE nombre IN (' . implode(',', $quotedPermisos) . ')');
    assert_true($permsResult instanceof mysqli_result, 'No se pudieron obtener los permisos de usuarios.');
    $permissionIds = [];
    while ($permRow = $permsResult->fetch_assoc()) {
        $permissionIds[$permRow['nombre']] = (int) $permRow['id'];
    }
    $permsResult->close();
    foreach ($permisosUsuarios as $permiso) {
        assert_true(isset($permissionIds[$permiso]), 'Falta el permiso ' . $permiso . '.');
    }

    $stmtPerm = $conn->prepare('INSERT INTO tenant_role_permissions (tenant_role_id, permission_id) VALUES (?, ?)');
    assert_true($stmtPerm !== false, 'No se pudo preparar la asignación de permisos delegados.');
    foreach ($permisosUsuarios as $permiso) {
        $permId = $permissionIds[$permiso];
        $stmtPerm->bind_param('ii', $responsableTenantRoleId, $permId);
        $stmtPerm->execute();
    }
    $stmtPerm->close();

    $_POST = [
        'usuario'    => 'primer_usuario_' . uniqid(),
        'correo'     => 'primer.usuario.' . uniqid() . '@empresa.test',
        'nombre'     => 'Primer',
        'apellidos'  => 'Usuario',
        'puesto'     => 'Responsable de laboratorio',
        'telefono'   => '555-000-1234',
        'contrasena' => 'ClaveSegura1*',
        'role_id'    => 'Operador',
        'empresa_id' => (string) $empresaId,
        'activo'     => '1',
    ];
    $primerUsuarioCorreo = $_POST['correo'];

    ob_start();
    require __DIR__ . '/../../app/Modules/Internal/Usuarios/create_user.php';
    $responseRaw = ob_get_clean();
    $payload = json_decode($responseRaw, true);
    $_POST = [];

    assert_true(is_array($payload), 'La respuesta del registro de usuario no es válida.');
    assert_true(($payload['success'] ?? false) === true, 'El registro del primer usuario no fue exitoso.');
    $primerUsuarioId = (int) ($payload['usuario_id'] ?? 0);
    assert_true($primerUsuarioId > 0, 'El identificador del primer usuario es inválido.');

    $clienteRoleId = resolve_role_id('Cliente', $empresaId);
    assert_true($clienteRoleId !== null, 'No se pudo resolver el rol Cliente.');

    $stmtCheckUsuario = $conn->prepare('SELECT role_id FROM usuarios WHERE id = ? LIMIT 1');
    $stmtCheckUsuario->bind_param('i', $primerUsuarioId);
    $stmtCheckUsuario->execute();
    $stmtCheckUsuario->bind_result($roleAsignado);
    $stmtCheckUsuario->fetch();
    $stmtCheckUsuario->close();
    assert_true((int) $roleAsignado === (int) $clienteRoleId, 'El primer usuario no quedó asignado al rol Cliente.');

    $stmtRolesDelegados = $conn->prepare('SELECT tenant_role_id FROM tenant_user_roles WHERE usuario_id = ?');
    $stmtRolesDelegados->bind_param('i', $primerUsuarioId);
    $stmtRolesDelegados->execute();
    $rolesResult = $stmtRolesDelegados->get_result();
    $tenantRolesAsignados = [];
    while ($roleRow = $rolesResult->fetch_assoc()) {
        $tenantRolesAsignados[] = (int) $roleRow['tenant_role_id'];
    }
    $stmtRolesDelegados->close();
    assert_true(in_array($responsableTenantRoleId, $tenantRolesAsignados, true), 'El rol delegado responsable no se asignó al primer usuario.');

    $_SESSION['usuario_id'] = $primerUsuarioId;
    $_SESSION['usuario'] = $primerUsuarioCorreo;
    $_SESSION['nombre'] = 'Primer';
    $_SESSION['apellidos'] = 'Usuario';
    $_SESSION['rol'] = 'Cliente';
    $_SESSION['empresa_id'] = $empresaId;

    foreach ($permisosUsuarios as $permiso) {
        assert_true(check_permission($permiso) === true, 'El primer usuario no cuenta con el permiso ' . $permiso . '.');
    }

    $conn->rollback();
    echo "\u2713 Prueba de primer usuario delega roles completada" . PHP_EOL;
} catch (Throwable $e) {
    if (isset($conn) && $conn instanceof mysqli) {
        $conn->rollback();
    }
    if (strpos($e->getMessage(), 'No se pudo conectar a la base de datos MySQL') !== false
        || strpos($e->getMessage(), 'No such file or directory') !== false) {
        fwrite(STDERR, "⚠ Prueba de primer usuario omitida: {$e->getMessage()}" . PHP_EOL);
        exit(0);
    }
    fwrite(STDERR, '✗ Prueba de primer usuario fallida: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
