<?php

declare(strict_types=1);

global $__portal_domain_cache;
if (!isset($__portal_domain_cache) || !is_array($__portal_domain_cache)) {
    $__portal_domain_cache = [];
}

/**
 * Normaliza un dominio eliminando espacios, mayúsculas y caracteres extra.
 */
function normalize_portal_domain(string $domain): string
{
    $domain = strtolower(trim($domain));
    $domain = preg_replace('/^@+/', '', $domain) ?? '';
    $domain = preg_replace('/\s+/', '', $domain) ?? '';

    return trim($domain, " .\t\n\r\0\x0B");
}

/**
 * Devuelve el portal asociado a un correo electrónico, si existe.
 */
function resolve_portal_by_email(string $email): ?array
{
    $email = trim($email);
    if ($email === '' || strpos($email, '@') === false) {
        return null;
    }

    $domain = substr(strrchr($email, '@'), 1);
    if ($domain === false || $domain === null) {
        return null;
    }

    return resolve_portal_by_domain($domain);
}

/**
 * Devuelve el portal asociado a un dominio.
 */
function resolve_portal_by_domain(string $domain): ?array
{
    global $__portal_domain_cache;
    $normalized = normalize_portal_domain($domain);
    if ($normalized === '') {
        return null;
    }

    if (array_key_exists($normalized, $__portal_domain_cache)) {
        return $__portal_domain_cache[$normalized];
    }

    global $conn;
    if (!($conn instanceof mysqli)) {
        require_once __DIR__ . '/db.php';
        $conn = $GLOBALS['conn'] ?? null;
    }

    if (!($conn instanceof mysqli)) {
        $__portal_domain_cache[$normalized] = null;
        return null;
    }

    $sql = "SELECT pd.id, pd.domain, pd.portal_id, pd.is_active, pd.is_primary, pd.created_at, pd.updated_at,"
        . " p.slug AS portal_slug, p.nombre AS portal_nombre"
        . " FROM portal_domains pd"
        . " INNER JOIN portals p ON p.id = pd.portal_id"
        . " WHERE pd.domain = ? LIMIT 1";

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        $__portal_domain_cache[$normalized] = null;
        return null;
    }

    $stmt->bind_param('s', $normalized);
    if (!$stmt->execute()) {
        $stmt->close();
        $__portal_domain_cache[$normalized] = null;
        return null;
    }

    $result = $stmt->get_result();
    $row = $result ? $result->fetch_assoc() : null;
    $stmt->close();

    if (!$row || (int) ($row['is_active'] ?? 0) !== 1) {
        $__portal_domain_cache[$normalized] = null;
        return null;
    }

    $row['id'] = isset($row['id']) ? (int) $row['id'] : null;
    $row['portal_id'] = (int) $row['portal_id'];
    $row['is_active'] = (int) $row['is_active'];
    $row['is_primary'] = isset($row['is_primary']) ? (int) $row['is_primary'] : 0;
    $row['domain'] = $normalized;
    $row['portal_slug'] = isset($row['portal_slug']) ? strtolower((string) $row['portal_slug']) : null;

    $__portal_domain_cache[$normalized] = $row;

    return $row;
}

/**
 * Registra o actualiza un dominio asociado a un portal específico.
 */
function register_portal_domain(int $portalId, string $domain, bool $isPrimary = false, bool $isActive = true): bool
{
    $normalized = normalize_portal_domain($domain);
    if ($normalized === '' || $portalId <= 0) {
        return false;
    }

    global $conn;
    if (!($conn instanceof mysqli)) {
        require_once __DIR__ . '/db.php';
        $conn = $GLOBALS['conn'] ?? null;
    }

    if (!($conn instanceof mysqli)) {
        return false;
    }

    $sql = "INSERT INTO portal_domains (domain, portal_id, is_primary, is_active)"
        . " VALUES (?, ?, ?, ?)"
        . " ON DUPLICATE KEY UPDATE portal_id = VALUES(portal_id),"
        . " is_primary = VALUES(is_primary), is_active = VALUES(is_active)";

    $stmt = $conn->prepare($sql);
    if (!$stmt) {
        return false;
    }

    $isPrimaryInt = $isPrimary ? 1 : 0;
    $isActiveInt = $isActive ? 1 : 0;
    $stmt->bind_param('siii', $normalized, $portalId, $isPrimaryInt, $isActiveInt);

    $ok = $stmt->execute();
    $stmt->close();

    clear_portal_domain_cache($normalized);

    return $ok;
}

/**
 * Limpia la caché interna de dominios para evitar inconsistencias.
 */
function clear_portal_domain_cache(?string $domain = null): void
{
    global $__portal_domain_cache;

    if ($domain === null) {
        $__portal_domain_cache = [];
        return;
    }

    $normalized = normalize_portal_domain($domain);
    unset($__portal_domain_cache[$normalized]);
}

?>
