<?php

declare(strict_types=1);

/**
 * Helpers relacionados con la validación de competencias de los usuarios.
 */

/**
 * Verifica si un usuario cuenta con evidencia vigente de competencia para un tipo de instrumento.
 */
function user_has_competency(mysqli $conn, int $usuarioId, int $empresaId, ?int $catalogoId = null): bool
{
    $sql = 'SELECT 1
            FROM usuario_competencias
            WHERE usuario_id = ?
              AND empresa_id = ?
              AND (vigente_desde IS NULL OR vigente_desde <= CURDATE())
              AND (vigente_hasta IS NULL OR vigente_hasta >= CURDATE())';

    $types = 'ii';
    $params = [$usuarioId, $empresaId];

    if ($catalogoId !== null) {
        $sql .= ' AND (catalogo_id = ? OR catalogo_id IS NULL)';
        $types .= 'i';
        $params[] = $catalogoId;
    }

    $sql .= ' LIMIT 1';

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return false;
    }

    if (!$stmt->bind_param($types, ...$params)) {
        $stmt->close();
        return false;
    }

    if (!$stmt->execute()) {
        $stmt->close();
        return false;
    }

    $stmt->store_result();
    $hasCompetency = $stmt->num_rows > 0;
    $stmt->close();

    return $hasCompetency;
}

/**
 * Obtiene el listado de usuarios que cuentan con evidencia de competencia.
 *
 * @return array<int, array<string, mixed>>
 */
function fetch_competent_users(mysqli $conn, int $empresaId, ?int $catalogoId = null): array
{
    $sql = "SELECT u.id,
                   u.nombre,
                   u.apellidos,
                   u.puesto,
                   u.correo,
                   c.id AS competencia_id,
                   c.catalogo_id,
                   c.vigente_desde,
                   c.vigente_hasta
            FROM usuarios u
            INNER JOIN usuario_competencias c ON c.usuario_id = u.id
            WHERE u.empresa_id = ?
              AND c.empresa_id = ?
              AND (c.vigente_desde IS NULL OR c.vigente_desde <= CURDATE())
              AND (c.vigente_hasta IS NULL OR c.vigente_hasta >= CURDATE())";

    $types = 'ii';
    $params = [$empresaId, $empresaId];

    if ($catalogoId !== null) {
        $sql .= ' AND (c.catalogo_id = ? OR c.catalogo_id IS NULL)';
        $types .= 'i';
        $params[] = $catalogoId;
    }

    $sql .= ' ORDER BY u.nombre, u.apellidos, c.vigente_hasta DESC, c.id DESC';

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return [];
    }

    if (!$stmt->bind_param($types, ...$params)) {
        $stmt->close();
        return [];
    }

    if (!$stmt->execute()) {
        $stmt->close();
        return [];
    }

    $result = $stmt->get_result();
    $users = [];
    while ($row = $result->fetch_assoc()) {
        $usuarioId = (int) ($row['id'] ?? 0);
        if ($usuarioId === 0) {
            continue;
        }

        if (!isset($users[$usuarioId])) {
            $users[$usuarioId] = [
                'id' => $usuarioId,
                'nombre' => $row['nombre'] ?? '',
                'apellidos' => $row['apellidos'] ?? '',
                'puesto' => $row['puesto'] ?? '',
                'correo' => $row['correo'] ?? '',
                'evidencias' => 0,
                'catalogo_id' => null,
                'vigente_desde' => null,
                'vigente_hasta' => null,
                'tiene_vigencia_especifica' => false,
            ];
        }

        $users[$usuarioId]['evidencias']++;

        $competenciaCatalogo = isset($row['catalogo_id']) ? (int) $row['catalogo_id'] : null;
        if ($competenciaCatalogo === 0) {
            $competenciaCatalogo = null;
        }
        $vigenteDesde = $row['vigente_desde'] ?? null;
        $vigenteHasta = $row['vigente_hasta'] ?? null;

        $replace = false;
        $tieneEspecifica = $users[$usuarioId]['tiene_vigencia_especifica'];

        if ($catalogoId !== null && $competenciaCatalogo === $catalogoId) {
            if (!$tieneEspecifica) {
                $replace = true;
            } else {
                $replace = should_replace_competency_window($users[$usuarioId]['vigente_hasta'], $vigenteHasta);
            }
            $users[$usuarioId]['tiene_vigencia_especifica'] = true;
        } elseif (!$tieneEspecifica && $catalogoId === null) {
            $replace = should_replace_competency_window($users[$usuarioId]['vigente_hasta'], $vigenteHasta);
        } elseif (!$tieneEspecifica && $competenciaCatalogo === null) {
            $replace = should_replace_competency_window($users[$usuarioId]['vigente_hasta'], $vigenteHasta);
        }

        if ($replace) {
            $users[$usuarioId]['catalogo_id'] = $competenciaCatalogo;
            $users[$usuarioId]['vigente_desde'] = $vigenteDesde;
            $users[$usuarioId]['vigente_hasta'] = $vigenteHasta;
        }
    }
    $stmt->close();

    $today = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d');
    foreach ($users as &$user) {
        $vigenteHasta = $user['vigente_hasta'];
        $user['vigencia_activa'] = $vigenteHasta === null || $vigenteHasta >= $today;
    }
    unset($user);

    return array_values($users);
}

/**
 * Determina si la ventana de vigencia propuesta debe reemplazar a la actual.
 */
function should_replace_competency_window(?string $currentHasta, ?string $candidateHasta): bool
{
    if ($candidateHasta === null) {
        return true;
    }

    if ($currentHasta === null) {
        return false;
    }

    return strcmp($candidateHasta, $currentHasta) > 0;
}
