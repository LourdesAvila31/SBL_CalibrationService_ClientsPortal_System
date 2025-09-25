<?php

/**
 * Obtiene el identificador de la empresa asociada a la sesión actual.
 *
 * La función revisa diferentes fuentes (sesión, base de datos asociada al
 * usuario autenticado) y garantiza un identificador positivo. Si no se
 * encuentra ninguno, detiene la ejecución del endpoint con un mensaje de
 * error (HTTP 400) o lanza una excepción en entornos CLI.
 */
function obtenerEmpresaId(): int
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    require_once __DIR__ . '/../permissions.php';
    $empresaSesion = ensure_session_empresa_id();
    if ($empresaSesion !== null && $empresaSesion > 0) {
        $empresaId = (int) $empresaSesion;
        $_SESSION['empresa_id'] = $empresaId;
        return $empresaId;
    }

    unset($_SESSION['empresa_id']);

    $candidatos = [
        filter_input(INPUT_POST, 'empresa_id', FILTER_VALIDATE_INT),
        filter_input(INPUT_GET, 'empresa_id', FILTER_VALIDATE_INT),
    ];

    foreach ($candidatos as $candidato) {
        if ($candidato === null) {
            continue;
        }

        $empresaId = (int) $candidato;
        if ($empresaId > 0) {
            $_SESSION['empresa_id'] = $empresaId;
            return $empresaId;
        }
    }
    responderEmpresaNoEspecificada();
    return 0;
}

/**
 * Detiene la petición si no es posible resolver la empresa actual.
 */
function responderEmpresaNoEspecificada(): void
{
    $mensaje = 'Empresa no especificada';

    if (PHP_SAPI === 'cli') {
        throw new RuntimeException($mensaje);
    }

    if (!headers_sent()) {
        http_response_code(400);
    }

    $acceptHeader      = $_SERVER['HTTP_ACCEPT'] ?? '';
    $contentTypeHeader = $_SERVER['CONTENT_TYPE'] ?? '';
    $quiereJson        = stripos($acceptHeader, 'application/json') !== false
        || stripos($contentTypeHeader, 'application/json') !== false;

    if ($quiereJson) {
        if (!headers_sent()) {
            header('Content-Type: application/json; charset=UTF-8');
        }
        echo json_encode(['error' => $mensaje], JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
    } else {
        if (!headers_sent()) {
            header('Content-Type: text/plain; charset=UTF-8');
        }
        echo $mensaje;
    }

    exit;
}

/**
 * Obtiene la zona horaria configurada para una empresa.
 *
 * La configuración se lee desde la tabla "empresas" (columna "zona_horaria").
 * Si la columna no existe, no hay registro o el valor no es válido, retorna null.
 */
function obtenerZonaHorariaEmpresa(mysqli $conn, int $empresaId): ?DateTimeZone
{
    if ($empresaId <= 0) {
        return null;
    }

    static $cache = [];
    if (array_key_exists($empresaId, $cache)) {
        return $cache[$empresaId];
    }

    $zonaHoraria = null;

    try {
        $stmt = $conn->prepare('SELECT zona_horaria FROM empresas WHERE id = ? LIMIT 1');
    } catch (Throwable $e) {
        return $cache[$empresaId] = null;
    }

    if (!$stmt) {
        return $cache[$empresaId] = null;
    }

    try {
        $stmt->bind_param('i', $empresaId);
        if ($stmt->execute()) {
            $resultado = $stmt->get_result();
            if ($resultado) {
                $fila = $resultado->fetch_assoc();
                if ($fila !== null) {
                    $valor = trim((string)($fila['zona_horaria'] ?? ''));
                    if ($valor !== '') {
                        try {
                            $zonaHoraria = new DateTimeZone($valor);
                        } catch (Throwable $e) {
                            $zonaHoraria = null;
                        }
                    }
                }
                $resultado->free();
            }
        }
    } catch (Throwable $e) {
        $zonaHoraria = null;
    } finally {
        $stmt->close();
    }

    return $cache[$empresaId] = $zonaHoraria;
}
