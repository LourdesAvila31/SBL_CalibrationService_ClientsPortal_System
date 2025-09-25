<?php

if (!function_exists('tenant_roles_table_exists')) {
    function tenant_roles_table_exists(mysqli $conn, string $tableName): bool
    {
        $pattern = str_replace(['\\', '%', '_'], ['\\\\', '\\%', '\\_'], $tableName);
        $table   = $conn->real_escape_string($pattern);
        $result  = $conn->query("SHOW TABLES LIKE '" . $table . "'");
        $exists = $result instanceof mysqli_result && $result->num_rows > 0;
        if ($result instanceof mysqli_result) {
            $result->close();
        }
        return $exists;
    }
}

if (!function_exists('tenant_roles_normalize_int_list')) {
    function tenant_roles_normalize_int_list($raw): array
    {
        if ($raw === null) {
            return [];
        }

        if (is_string($raw)) {
            $raw = trim($raw);
            if ($raw === '') {
                return [];
            }

            $decoded = json_decode($raw, true);
            if (json_last_error() === JSON_ERROR_NONE && is_array($decoded)) {
                $raw = $decoded;
            } else {
                $raw = array_map('trim', explode(',', $raw));
            }
        }

        if (!is_array($raw)) {
            return [];
        }

        $normalized = [];
        foreach ($raw as $value) {
            if (is_array($value)) {
                $value = $value['id'] ?? null;
            }
            $filtered = filter_var($value, FILTER_VALIDATE_INT);
            if ($filtered !== false && $filtered !== null) {
                $intValue = (int) $filtered;
                if ($intValue > 0) {
                    $normalized[$intValue] = $intValue;
                }
            }
        }

        $normalized = array_values($normalized);
        sort($normalized);
        return $normalized;
    }
}

if (!function_exists('tenant_roles_bind_params')) {
    function tenant_roles_bind_params(mysqli_stmt $stmt, string $types, array $values): void
    {
        $values = array_values($values);
        $params = [$types];
        foreach ($values as $key => $value) {
            $params[] = &$values[$key];
        }
        call_user_func_array([$stmt, 'bind_param'], $params);
    }
}

if (!function_exists('tenant_roles_validate_ids')) {
    function tenant_roles_validate_ids(mysqli $conn, array $ids, int $empresaId): array
    {
        $ids = tenant_roles_normalize_int_list($ids);
        if ($ids === [] || !tenant_roles_table_exists($conn, 'tenant_roles')) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $sql = "SELECT id FROM tenant_roles WHERE empresa_id = ? AND id IN ($placeholders)";
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return [];
        }

        $values = array_merge([$empresaId], $ids);
        $types = 'i' . str_repeat('i', count($ids));
        tenant_roles_bind_params($stmt, $types, $values);
        $stmt->execute();
        $result = $stmt->get_result();
        $valid = [];
        while ($row = $result->fetch_assoc()) {
            $valid[] = (int) $row['id'];
        }
        $stmt->close();
        sort($valid);
        return $valid;
    }
}

if (!function_exists('tenant_roles_validate_permission_ids')) {
    function tenant_roles_validate_permission_ids(mysqli $conn, array $ids): array
    {
        $ids = tenant_roles_normalize_int_list($ids);
        if ($ids === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $sql = "SELECT id FROM permissions WHERE id IN ($placeholders)";
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return [];
        }

        $types = str_repeat('i', count($ids));
        tenant_roles_bind_params($stmt, $types, $ids);
        $stmt->execute();
        $result = $stmt->get_result();
        $valid = [];
        while ($row = $result->fetch_assoc()) {
            $valid[] = (int) $row['id'];
        }
        $stmt->close();
        sort($valid);
        return $valid;
    }
}

if (!function_exists('tenant_roles_fetch_names')) {
    function tenant_roles_fetch_names(mysqli $conn, array $ids): array
    {
        $ids = tenant_roles_normalize_int_list($ids);
        if ($ids === [] || !tenant_roles_table_exists($conn, 'tenant_roles')) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $sql = "SELECT id, nombre FROM tenant_roles WHERE id IN ($placeholders)";
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return [];
        }

        $types = str_repeat('i', count($ids));
        tenant_roles_bind_params($stmt, $types, $ids);
        $stmt->execute();
        $result = $stmt->get_result();
        $names  = [];
        while ($row = $result->fetch_assoc()) {
            $names[(int) $row['id']] = $row['nombre'];
        }
        $stmt->close();
        ksort($names);
        return $names;
    }
}

if (!function_exists('tenant_roles_fetch_permission_names')) {
    function tenant_roles_fetch_permission_names(mysqli $conn, array $ids): array
    {
        $ids = tenant_roles_normalize_int_list($ids);
        if ($ids === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $sql = "SELECT id, nombre FROM permissions WHERE id IN ($placeholders)";
        $stmt = $conn->prepare($sql);
        if (!$stmt) {
            return [];
        }

        $types = str_repeat('i', count($ids));
        tenant_roles_bind_params($stmt, $types, $ids);
        $stmt->execute();
        $result = $stmt->get_result();
        $names  = [];
        while ($row = $result->fetch_assoc()) {
            $names[(int) $row['id']] = $row['nombre'];
        }
        $stmt->close();
        ksort($names);
        return $names;
    }
}
