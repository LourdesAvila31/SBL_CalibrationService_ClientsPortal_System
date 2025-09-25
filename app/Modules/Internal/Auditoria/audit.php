<?php
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__) . '/Usuarios/FirmaInternaService.php';

function normalizarSegmentoActor(?string $segmento = null): string
{
    $catalogo = [
        'admin' => 'Administradores',
        'administrador' => 'Administradores',
        'administradores' => 'Administradores',
        'sistema' => 'Sistemas',
        'sistemas' => 'Sistemas',
        'system' => 'Sistemas',
        'developer' => 'Developer',
        'developers' => 'Developer',
        'dev' => 'Developer',
    ];

    if ($segmento !== null) {
        $clave = strtolower(trim($segmento));
        if ($clave !== '') {
            foreach ($catalogo as $patron => $canonico) {
                if (strpos($clave, $patron) !== false) {
                    return $canonico;
                }
            }
        }
    }

    $roleName = strtolower(trim((string) ($_SESSION['role_name'] ?? '')));
    $roleNum = isset($_SESSION['role_num']) ? (int) $_SESSION['role_num'] : null;

    if ($roleName !== '') {
        if (strpos($roleName, 'sistema') !== false || strpos($roleName, 'soporte') !== false || strpos($roleName, 'infra') !== false) {
            return 'Sistemas';
        }
        if (strpos($roleName, 'developer') !== false || strpos($roleName, 'dev') !== false) {
            return 'Developer';
        }
        if (strpos($roleName, 'admin') !== false) {
            return 'Administradores';
        }
    }

    if ($roleNum !== null) {
        if (in_array($roleNum, [900, 901, 950], true)) {
            return 'Developer';
        }
        if (in_array($roleNum, [500, 550, 580], true)) {
            return 'Sistemas';
        }
    }

    return 'Administradores';
}

function log_activity($usuarioNombre, $detalle, $legacySeccion = null, $legacyCorreo = null) {
    global $conn;

    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    $ahora = date('Y-m-d H:i:s');
    $empresaPorDefecto = obtenerEmpresaId();

    $defaults = [
        'empresa_id' => $empresaPorDefecto,
        'segmento_actor' => null,
        'fecha_evento' => $ahora,
        'seccion' => 'General',
        'valor_anterior' => null,
        'valor_nuevo' => null,
        'usuario_correo' => $_SESSION['usuario'] ?? null,
        'usuario_nombre' => $usuarioNombre,
        'usuario_firma_interna' => $_SESSION['usuario_firma_interna'] ?? null,
    ];

    if (is_array($detalle)) {
        $data = array_merge($defaults, $detalle);
    } else {
        $data = $defaults;
        $data['valor_nuevo'] = $detalle !== null ? (string) $detalle : '';

        if (is_array($legacySeccion)) {
            $data = array_merge($data, $legacySeccion);
            $legacySeccion = null;
        }

        if ($legacySeccion !== null && $legacySeccion !== '') {
            $data['seccion'] = (string) $legacySeccion;
        }

        if ($legacyCorreo !== null && $legacyCorreo !== '') {
            $data['usuario_correo'] = $legacyCorreo;
        }
    }

    if (is_object($data['fecha_evento']) && $data['fecha_evento'] instanceof \DateTimeInterface) {
        $data['fecha_evento'] = $data['fecha_evento']->format('Y-m-d H:i:s');
    }

    if (is_string($data['fecha_evento']) && $data['fecha_evento'] !== '') {
        $fechaEvento = date('Y-m-d H:i:s', strtotime($data['fecha_evento']));
        $data['fecha_evento'] = $fechaEvento ?: $ahora;
    } else {
        $data['fecha_evento'] = $ahora;
    }

    $nombreFinal = trim((string) ($data['usuario_nombre'] ?? ''));
    if ($nombreFinal === '') {
        $nombreFinal = trim(((string) ($_SESSION['nombre'] ?? '')) . ' ' . ((string) ($_SESSION['apellidos'] ?? '')));
    }
    if ($nombreFinal === '') {
        $nombreFinal = 'Desconocido';
    }
    $data['usuario_nombre'] = $nombreFinal;

    $correoFinal = trim((string) ($data['usuario_correo'] ?? ''));
    if ($correoFinal === '') {
        $correoFinal = $legacyCorreo !== null ? trim((string) $legacyCorreo) : '';
    }
    if ($correoFinal === '') {
        $correoFinal = 'desconocido@sistema.local';
    }
    $data['usuario_correo'] = $correoFinal;

    $data['seccion'] = trim((string) ($data['seccion'] ?? '')) ?: 'General';

    $firmaInterna = $data['usuario_firma_interna'] ?? null;
    if (is_string($firmaInterna)) {
        $firmaInterna = trim($firmaInterna);
        if ($firmaInterna === '') {
            $firmaInterna = null;
        }
    } else {
        $firmaInterna = null;
    }

    $usuarioReferencia = isset($data['usuario_id']) && $data['usuario_id'] !== null
        ? (int) $data['usuario_id']
        : (isset($_SESSION['usuario_id']) ? (int) $_SESSION['usuario_id'] : 0);

    $empresaReferencia = isset($data['empresa_id']) && $data['empresa_id'] > 0
        ? (int) $data['empresa_id']
        : $empresaPorDefecto;

    $correoReferencia = $data['usuario_correo'] ?? ($_SESSION['usuario'] ?? null);

    if ($firmaInterna === null && $usuarioReferencia > 0) {
        $firmaResuelta = firmas_internas_resolver_por_id($conn, $usuarioReferencia, $empresaReferencia, $correoReferencia, true);
        if ($firmaResuelta !== null && $firmaResuelta !== '') {
            $firmaInterna = $firmaResuelta;
            if ($usuarioReferencia === (int) ($_SESSION['usuario_id'] ?? 0)) {
                $_SESSION['usuario_firma_interna'] = $firmaResuelta;
            }
        }
    }

    $data['usuario_firma_interna'] = $firmaInterna;

    $metadata = [];
    if (isset($data['rango_referencia']) && $data['rango_referencia'] !== '') {
        $metadata['rango_referencia'] = (string) $data['rango_referencia'];
    }
    if (isset($data['instrumento_id']) && $data['instrumento_id'] !== '' && $data['instrumento_id'] !== null) {
        $metadata['instrumento_id'] = (int) $data['instrumento_id'];
    }
    if (isset($data['usuario_id']) && $data['usuario_id'] !== '' && $data['usuario_id'] !== null) {
        $metadata['usuario_id'] = (int) $data['usuario_id'];
    }
    if (isset($data['meta']) && is_array($data['meta'])) {
        foreach ($data['meta'] as $metaKey => $metaValue) {
            if ($metaValue === null || $metaValue === '') {
                continue;
            }

            $metadata[$metaKey] = is_scalar($metaValue)
                ? $metaValue
                : json_encode($metaValue, JSON_UNESCAPED_UNICODE);
        }
    }

    $data['valor_anterior'] = audit_normalize_value($data['valor_anterior'] ?? null);
    $data['valor_nuevo'] = audit_normalize_value($data['valor_nuevo'] ?? null);
    $data['valor_nuevo'] = audit_merge_meta($data['valor_nuevo'], $metadata);
    $data['empresa_id'] = isset($data['empresa_id']) && $data['empresa_id'] !== '' ? (int) $data['empresa_id'] : $empresaPorDefecto;
    if ($data['empresa_id'] <= 0) {
        $data['empresa_id'] = $empresaPorDefecto > 0 ? $empresaPorDefecto : 0;
    }

    $segmentoActor = normalizarSegmentoActor($data['segmento_actor'] ?? null);

    $empresaId = $data['empresa_id'];
    $fechaEvento = $data['fecha_evento'];
    $seccion = $data['seccion'];
    $valorAnterior = $data['valor_anterior'];
    $valorNuevo = $data['valor_nuevo'];
    $usuarioCorreo = $data['usuario_correo'];
    $usuarioNombre = $data['usuario_nombre'];
    $usuarioFirmaInterna = $firmaInterna;

    $stmt = $conn->prepare('INSERT INTO audit_trail (empresa_id, segmento_actor, fecha_evento, seccion, valor_anterior, valor_nuevo, usuario_correo, usuario_nombre, usuario_firma_interna) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)');
    $stmt->bind_param(
        'issssssss',
        $empresaId,
        $segmentoActor,
        $fechaEvento,
        $seccion,
        $valorAnterior,
        $valorNuevo,
        $usuarioCorreo,
        $usuarioNombre,
        $usuarioFirmaInterna
    );
    $stmt->execute();
    $stmt->close();
}

function audit_normalize_value($value): ?string
{
    if ($value === null) {
        return null;
    }

    if (is_string($value)) {
        return $value;
    }

    if (is_bool($value)) {
        return $value ? '1' : '0';
    }

    if (is_scalar($value)) {
        return (string) $value;
    }

    if (is_array($value)) {
        return json_encode($value, JSON_UNESCAPED_UNICODE);
    }

    if (is_object($value)) {
        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d H:i:s');
        }

        if (method_exists($value, '__toString')) {
            return (string) $value;
        }

        return json_encode($value, JSON_UNESCAPED_UNICODE);
    }

    return null;
}

function audit_merge_meta(?string $value, array $metadata): ?string
{
    if ($metadata === [] || $metadata === null) {
        return $value;
    }

    $filteredMeta = [];
    foreach ($metadata as $key => $metaValue) {
        if ($metaValue === null || $metaValue === '') {
            continue;
        }
        $filteredMeta[$key] = $metaValue;
    }

    if ($filteredMeta === []) {
        return $value;
    }

    if ($value === null || $value === '') {
        return json_encode(['meta' => $filteredMeta], JSON_UNESCAPED_UNICODE);
    }

    $decoded = json_decode($value, true);
    if (json_last_error() === JSON_ERROR_NONE) {
        if (!is_array($decoded)) {
            $decoded = ['valor' => $decoded];
        }

        if (isset($decoded['meta']) && is_array($decoded['meta'])) {
            $decoded['meta'] = array_merge($decoded['meta'], $filteredMeta);
        } else {
            $decoded['meta'] = $filteredMeta;
        }

        return json_encode($decoded, JSON_UNESCAPED_UNICODE);
    }

    return json_encode([
        'valor' => $value,
        'meta' => $filteredMeta,
    ], JSON_UNESCAPED_UNICODE);
}
