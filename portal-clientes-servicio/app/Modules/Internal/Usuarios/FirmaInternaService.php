<?php
declare(strict_types=1);

/**
 * Normaliza una firma interna eliminando espacios duplicados y limitando su longitud.
 */
function firmas_internas_normalizar(?string $firma): ?string
{
    if ($firma === null) {
        return null;
    }

    $firma = trim($firma);
    if ($firma === '') {
        return null;
    }

    $firma = preg_replace('/\s+/', ' ', $firma);
    if ($firma === null) {
        $firma = '';
    }

    if ($firma === '') {
        return null;
    }

    if (function_exists('mb_substr')) {
        $firma = mb_substr($firma, 0, 150);
    } else {
        $firma = substr($firma, 0, 150);
    }

    return $firma !== '' ? $firma : null;
}

/**
 * Genera una firma interna predeterminada usando los datos básicos del usuario.
 */
function firmas_internas_generar_predeterminada(array $usuario): ?string
{
    $nombre = trim((string) ($usuario['nombre'] ?? ''));
    $apellidos = trim((string) ($usuario['apellidos'] ?? ''));
    $nombreCompleto = trim($nombre . ' ' . $apellidos);
    if ($nombreCompleto !== '') {
        return firmas_internas_normalizar($nombreCompleto);
    }

    $usuarioLogin = trim((string) ($usuario['usuario'] ?? ''));
    if ($usuarioLogin !== '') {
        return firmas_internas_normalizar($usuarioLogin);
    }

    $correo = trim((string) ($usuario['correo'] ?? ''));
    if ($correo !== '') {
        return firmas_internas_normalizar($correo);
    }

    return null;
}

/**
 * Obtiene la firma interna vigente para un usuario, opcionalmente filtrando por correo y fecha.
 */
function firmas_internas_obtener_actual(mysqli $conn, int $usuarioId, int $empresaId, ?string $correo = null, ?string $fechaReferencia = null): ?array
{
    $fechaConsulta = $fechaReferencia !== null
        ? date('Y-m-d H:i:s', strtotime($fechaReferencia))
        : date('Y-m-d H:i:s');

    if ($fechaConsulta === false) {
        $fechaConsulta = date('Y-m-d H:i:s');
    }

    $empresaId = max(0, $empresaId);

    if ($correo !== null && $correo !== '') {
        $stmt = $conn->prepare(
            'SELECT id, firma_interna, correo, vigente_desde, vigente_hasta '
            . 'FROM usuario_firmas_internas '
            . 'WHERE usuario_id = ? AND empresa_id = ? '
            . 'AND LOWER(correo) = LOWER(?) '
            . 'AND vigente_desde <= ? '
            . 'AND (vigente_hasta IS NULL OR vigente_hasta > ?) '
            . 'ORDER BY vigente_desde DESC LIMIT 1'
        );
        if ($stmt) {
            $stmt->bind_param('iisss', $usuarioId, $empresaId, $correo, $fechaConsulta, $fechaConsulta);
            $stmt->execute();
            $result = $stmt->get_result();
            $row = $result ? $result->fetch_assoc() : null;
            $stmt->close();
            if ($row) {
                return $row;
            }
        }
    }

    $stmt = $conn->prepare(
        'SELECT id, firma_interna, correo, vigente_desde, vigente_hasta '
        . 'FROM usuario_firmas_internas '
        . 'WHERE usuario_id = ? AND empresa_id = ? '
        . 'AND vigente_desde <= ? '
        . 'AND (vigente_hasta IS NULL OR vigente_hasta > ?) '
        . 'ORDER BY vigente_desde DESC LIMIT 1'
    );
    if (!$stmt) {
        return null;
    }

    $stmt->bind_param('iiss', $usuarioId, $empresaId, $fechaConsulta, $fechaConsulta);
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result ? $result->fetch_assoc() : null;
    $stmt->close();

    return $row ?: null;
}

/**
 * Registra una nueva firma interna para el usuario, cerrando la anterior si aplica.
 */
function firmas_internas_registrar(mysqli $conn, int $usuarioId, int $empresaId, string $firma, ?string $correo = null, ?int $autorId = null, ?string $vigenteDesde = null): bool
{
    $firmaNormalizada = firmas_internas_normalizar($firma);
    if ($firmaNormalizada === null) {
        return false;
    }

    $empresaId = max(0, $empresaId);
    $ahora = $vigenteDesde !== null
        ? date('Y-m-d H:i:s', strtotime($vigenteDesde))
        : date('Y-m-d H:i:s');
    if ($ahora === false) {
        $ahora = date('Y-m-d H:i:s');
    }

    $actual = firmas_internas_obtener_actual($conn, $usuarioId, $empresaId, $correo, $ahora);
    if ($actual && isset($actual['firma_interna']) && $actual['firma_interna'] === $firmaNormalizada) {
        return true;
    }

    $stmt = $conn->prepare(
        'UPDATE usuario_firmas_internas '
        . 'SET vigente_hasta = ?, updated_at = CURRENT_TIMESTAMP '
        . 'WHERE usuario_id = ? AND empresa_id = ? AND vigente_hasta IS NULL'
    );
    if ($stmt) {
        $stmt->bind_param('sii', $ahora, $usuarioId, $empresaId);
        $stmt->execute();
        $stmt->close();
    }

    $stmt = $conn->prepare(
        'INSERT INTO usuario_firmas_internas '
        . '(empresa_id, usuario_id, correo, firma_interna, vigente_desde, vigente_hasta, creado_por, created_at, updated_at) '
        . 'VALUES (?, ?, ?, ?, ?, NULL, ?, NOW(), NOW())'
    );
    if (!$stmt) {
        return false;
    }

    $correoParam = $correo !== null && $correo !== '' ? $correo : null;
    $autorParam = $autorId !== null && $autorId > 0 ? $autorId : null;
    $stmt->bind_param('iisssi', $empresaId, $usuarioId, $correoParam, $firmaNormalizada, $ahora, $autorParam);
    $ok = $stmt->execute();
    $stmt->close();

    return $ok;
}

/**
 * Obtiene la firma interna vigente para un usuario basándose en un registro completo de usuario.
 */
function firmas_internas_resolver_desde_row(mysqli $conn, array $usuarioRow, bool $crearSiNoExiste = true): ?string
{
    $usuarioId = isset($usuarioRow['id']) ? (int) $usuarioRow['id'] : 0;
    if ($usuarioId <= 0) {
        return null;
    }

    $empresaId = isset($usuarioRow['empresa_id']) ? (int) $usuarioRow['empresa_id'] : 0;
    $correo = trim((string) ($usuarioRow['correo'] ?? ''));

    $registro = firmas_internas_obtener_actual($conn, $usuarioId, $empresaId, $correo);
    if ($registro && isset($registro['firma_interna'])) {
        return (string) $registro['firma_interna'];
    }

    if (!$crearSiNoExiste) {
        return null;
    }

    $firmaPredeterminada = firmas_internas_generar_predeterminada($usuarioRow);
    if ($firmaPredeterminada === null) {
        return null;
    }

    firmas_internas_registrar($conn, $usuarioId, $empresaId, $firmaPredeterminada, $correo, $usuarioId);
    return $firmaPredeterminada;
}

/**
 * Obtiene la firma interna vigente para un usuario conocido por su ID. Puede crear un registro si no existe.
 */
function firmas_internas_resolver_por_id(mysqli $conn, int $usuarioId, ?int $empresaId = null, ?string $correo = null, bool $crearSiNoExiste = false): ?string
{
    $usuarioId = max(0, $usuarioId);
    if ($usuarioId <= 0) {
        return null;
    }

    $empresaId = $empresaId !== null ? max(0, $empresaId) : null;
    $correo = $correo !== null ? trim($correo) : null;

    if ($empresaId !== null) {
        $registro = firmas_internas_obtener_actual($conn, $usuarioId, $empresaId, $correo);
        if ($registro && isset($registro['firma_interna'])) {
            return (string) $registro['firma_interna'];
        }
    } else {
        $registro = firmas_internas_obtener_actual($conn, $usuarioId, 0, $correo);
        if ($registro && isset($registro['firma_interna'])) {
            return (string) $registro['firma_interna'];
        }
    }

    if (!$crearSiNoExiste) {
        return null;
    }

    $stmt = $conn->prepare('SELECT id, empresa_id, correo, usuario, nombre, apellidos FROM usuarios WHERE id = ? LIMIT 1');
    if (!$stmt) {
        return null;
    }

    $stmt->bind_param('i', $usuarioId);
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result ? $result->fetch_assoc() : null;
    $stmt->close();

    if (!$row) {
        return null;
    }

    if ($empresaId !== null && (!isset($row['empresa_id']) || (int) $row['empresa_id'] !== $empresaId)) {
        $row['empresa_id'] = $empresaId;
    }

    if ($correo !== null) {
        $row['correo'] = $correo;
    }

    return firmas_internas_resolver_desde_row($conn, $row, $crearSiNoExiste);
}

/**
 * Sincroniza la firma interna dentro de la sesión activa utilizando un registro de usuario conocido.
 */
function firmas_internas_sincronizar_sesion(mysqli $conn, array $usuarioRow): ?string
{
    $firma = firmas_internas_resolver_desde_row($conn, $usuarioRow, true);
    if ($firma !== null) {
        $_SESSION['usuario_firma_interna'] = $firma;
    } else {
        unset($_SESSION['usuario_firma_interna']);
    }

    return $firma;
}
