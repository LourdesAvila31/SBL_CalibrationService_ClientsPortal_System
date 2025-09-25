<?php

function log_api_activity(mysqli $db, int $empresaId, string $accion, array $contexto = []): void
{
    $ahora = date('Y-m-d H:i:s');
    $seccion = $contexto['seccion'] ?? 'API';
    $valorAnterior = $contexto['valor_anterior'] ?? null;
    $valorNuevo = $contexto['valor_nuevo'] ?? null;
    $usuarioCorreo = $contexto['usuario_correo'] ?? 'api@sistema.local';
    $usuarioNombre = $contexto['usuario_nombre'] ?? 'API Client';
    $usuarioFirmaInterna = $contexto['usuario_firma_interna'] ?? $usuarioNombre;

    if ($valorNuevo === null && isset($contexto['payload'])) {
        $valorNuevo = $contexto['payload'];
    }

    if ($valorAnterior === null && isset($contexto['before'])) {
        $valorAnterior = $contexto['before'];
    }

    if ($valorNuevo === null) {
        $valorNuevo = $accion;
    }
    $metadata = [];
    if (isset($contexto['rango_referencia']) && $contexto['rango_referencia'] !== '') {
        $metadata['rango_referencia'] = (string) $contexto['rango_referencia'];
    }
    if (isset($contexto['instrumento_id']) && $contexto['instrumento_id'] !== null && $contexto['instrumento_id'] !== '') {
        $metadata['instrumento_id'] = (int) $contexto['instrumento_id'];
    }
    if (isset($contexto['usuario_id']) && $contexto['usuario_id'] !== null && $contexto['usuario_id'] !== '') {
        $metadata['usuario_id'] = (int) $contexto['usuario_id'];
    }
    if (isset($contexto['meta']) && is_array($contexto['meta'])) {
        foreach ($contexto['meta'] as $metaKey => $metaValue) {
            if ($metaValue === null || $metaValue === '') {
                continue;
            }
            $metadata[$metaKey] = is_scalar($metaValue)
                ? $metaValue
                : json_encode($metaValue, JSON_UNESCAPED_UNICODE);
        }
    }

    $valorAnterior = api_audit_normalize_value($valorAnterior);
    $valorNuevo = api_audit_normalize_value($valorNuevo);
    $valorNuevo = api_audit_merge_meta($valorNuevo, $metadata);

    $metadata = [];
    if (isset($contexto['rango_referencia']) && $contexto['rango_referencia'] !== '') {
        $metadata['rango_referencia'] = (string) $contexto['rango_referencia'];
    }
    if (isset($contexto['instrumento_id']) && $contexto['instrumento_id'] !== null && $contexto['instrumento_id'] !== '') {
        $metadata['instrumento_id'] = (int) $contexto['instrumento_id'];
    }
    if (isset($contexto['usuario_id']) && $contexto['usuario_id'] !== null && $contexto['usuario_id'] !== '') {
        $metadata['usuario_id'] = (int) $contexto['usuario_id'];
    }
    if (isset($contexto['meta']) && is_array($contexto['meta'])) {
        foreach ($contexto['meta'] as $metaKey => $metaValue) {
            if ($metaValue === null || $metaValue === '') {
                continue;
            }
            $metadata[$metaKey] = is_scalar($metaValue)
                ? $metaValue
                : json_encode($metaValue, JSON_UNESCAPED_UNICODE);
        }
    }

    $valorAnterior = api_audit_normalize_value($valorAnterior);
    $valorNuevo = api_audit_normalize_value($valorNuevo);
    $valorNuevo = api_audit_merge_meta($valorNuevo, $metadata);

    $stmt = $db->prepare('INSERT INTO audit_trail (empresa_id, fecha_evento, seccion, valor_anterior, valor_nuevo, usuario_correo, usuario_nombre, usuario_firma_interna) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    if (!$stmt) {
        error_log('API Logger: no fue posible preparar el registro de auditoría.');
        return;
    }

    $stmt->bind_param('issssssss', $empresaId, $ahora, $seccion, $valorAnterior, $valorNuevo, $usuarioCorreo, $usuarioNombre, $usuarioFirmaInterna);
    $stmt->execute();
    $stmt->close();
}

function api_audit_normalize_value($value): ?string
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

function api_audit_merge_meta(?string $value, array $metadata): ?string
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
