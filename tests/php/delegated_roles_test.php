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
    require_once __DIR__ . '/../../app/Core/delegated_roles.php';
    require_once __DIR__ . '/../../app/Core/permissions.php';

    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    global $conn;
    $conn->begin_transaction();

    $empresaPrincipal = 1;
    $empresaSecundaria = 2;

    $adminRoleId = resolve_role_id('Administrador', $empresaPrincipal);
    assert_true($adminRoleId !== null, 'No se encontró el rol Administrador.');

    $actorUsuario = 'delegado_actor_' . uniqid();
    $actorCorreo = $actorUsuario . '@pruebas.local';
    $passwordHash = password_hash('Delegado123*', PASSWORD_DEFAULT);

    $stmt = $conn->prepare('INSERT INTO usuarios (usuario, correo, nombre, apellidos, contrasena, role_id, empresa_id, activo) VALUES (?, ?, ?, ?, ?, ?, ?, 1)');
    $nombreActor = 'Delegado';
    $apellidosActor = 'Pruebas';
    $stmt->bind_param('sssssii', $actorUsuario, $actorCorreo, $nombreActor, $apellidosActor, $passwordHash, $adminRoleId, $empresaPrincipal);
    $stmt->execute();
    $actorId = (int) $stmt->insert_id;
    $stmt->close();
    assert_true($actorId > 0, 'No se pudo insertar el actor de prueba.');

    $_SESSION['usuario_id'] = $actorId;
    $_SESSION['usuario'] = $actorCorreo;
    $_SESSION['nombre'] = $nombreActor;
    $_SESSION['apellidos'] = $apellidosActor;
    $_SESSION['empresa_id'] = $empresaPrincipal;

    $delegado = create_delegated_role(
        $empresaPrincipal,
        'Delegado Temporal',
        ['instrumentos_leer', 'planeacion_leer'],
        $actorId
    );
    assert_true(isset($delegado['id']), 'No se creó el rol delegado.');

    $roleId = (int) $delegado['id'];
    $query = $conn->prepare('SELECT nombre, empresa_id, delegated FROM roles WHERE id = ?');
    $query->bind_param('i', $roleId);
    $query->execute();
    $datosRol = $query->get_result()->fetch_assoc();
    $query->close();
    assert_true(($datosRol['delegated'] ?? 0) == 1, 'El rol creado debe marcarse como delegado.');
    assert_true((int) $datosRol['empresa_id'] === $empresaPrincipal, 'El rol delegado debe pertenecer a la empresa principal.');

    update_delegated_role($roleId, 'Delegado Actualizado', ['instrumentos_leer'], $actorId);

    $query = $conn->prepare('SELECT nombre FROM roles WHERE id = ?');
    $query->bind_param('i', $roleId);
    $query->execute();
    $datosActualizados = $query->get_result()->fetch_assoc();
    $query->close();
    assert_true(($datosActualizados['nombre'] ?? '') === 'Delegado Actualizado', 'El rol delegado no se actualizó.');

    $permCount = $conn->query('SELECT COUNT(*) AS total FROM role_permissions rp JOIN permissions p ON p.id = rp.permission_id WHERE rp.role_id = ' . $roleId)->fetch_assoc();
    assert_true((int) ($permCount['total'] ?? 0) === 1, 'El rol delegado debe tener exactamente un permiso tras la actualización.');

    $usuarioObjetivo = 'usuario_objetivo_' . uniqid();
    $correoObjetivo = $usuarioObjetivo . '@pruebas.local';
    $stmt = $conn->prepare('INSERT INTO usuarios (usuario, correo, nombre, apellidos, contrasena, role_id, empresa_id, activo) VALUES (?, ?, ?, ?, ?, ?, ?, 1)');
    $stmt->bind_param('sssssii', $usuarioObjetivo, $correoObjetivo, 'Usuario', 'Objetivo', $passwordHash, $adminRoleId, $empresaPrincipal);
    $stmt->execute();
    $usuarioObjetivoId = (int) $stmt->insert_id;
    $stmt->close();

    assign_delegated_role_to_user($roleId, $usuarioObjetivoId, $actorId);

    $query = $conn->prepare('SELECT role_id FROM usuarios WHERE id = ?');
    $query->bind_param('i', $usuarioObjetivoId);
    $query->execute();
    $rolAsignado = $query->get_result()->fetch_assoc();
    $query->close();
    assert_true((int) ($rolAsignado['role_id'] ?? 0) === $roleId, 'El usuario no quedó asignado al rol delegado.');

    $bitacora = $conn->prepare("SELECT COUNT(*) AS total FROM audit_trail WHERE usuario_id = ? AND seccion = 'roles_delegados' AND valor_nuevo LIKE '%Delegado Actualizado%'");
    $bitacora->bind_param('i', $actorId);
    $bitacora->execute();
    $auditoria = $bitacora->get_result()->fetch_assoc();
    $bitacora->close();
    assert_true((int) ($auditoria['total'] ?? 0) >= 1, 'No se registró la auditoría esperada.');

    $usuarioOtraEmpresa = 'usuario_ajeno_' . uniqid();
    $correoAjeno = $usuarioOtraEmpresa . '@pruebas.local';
    $stmt = $conn->prepare('INSERT INTO usuarios (usuario, correo, nombre, apellidos, contrasena, role_id, empresa_id, activo) VALUES (?, ?, ?, ?, ?, ?, ?, 1)');
    $stmt->bind_param('sssssii', $usuarioOtraEmpresa, $correoAjeno, 'Usuario', 'Externo', $passwordHash, $adminRoleId, $empresaSecundaria);
    $stmt->execute();
    $usuarioAjenoId = (int) $stmt->insert_id;
    $stmt->close();

    $excepcionCapturada = false;
    try {
        assign_delegated_role_to_user($roleId, $usuarioAjenoId, $actorId);
    } catch (Throwable $fallo) {
        $excepcionCapturada = true;
    }
    assert_true($excepcionCapturada, 'Debe impedirse asignar roles delegados a usuarios de otra empresa.');

    $conn->rollback();
    echo "\u2713 Prueba de roles delegados completada" . PHP_EOL;
} catch (Throwable $e) {
    if (strpos($e->getMessage(), 'No se pudo conectar a la base de datos MySQL') !== false
        || strpos($e->getMessage(), 'No such file or directory') !== false) {
        fwrite(STDERR, "⚠ Prueba de roles delegados omitida: {$e->getMessage()}" . PHP_EOL);
        exit(0);
    }

    if (isset($conn) && $conn instanceof mysqli) {
        $conn->rollback();
    }
    fwrite(STDERR, '✗ Prueba de roles delegados fallida: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
