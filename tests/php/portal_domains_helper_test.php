<?php

declare(strict_types=1);
require_once dirname(__DIR__, 2) . '/app/Core/db.php';
require_once dirname(__DIR__, 2) . '/app/Core/portal_domains.php';

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function assert_same($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        throw new RuntimeException($message . ' Esperado: ' . var_export($expected, true) . ' Obtenido: ' . var_export($actual, true));
    }
}
function fetch_portal_id(mysqli $conn, string $slug): int
{
    $stmt = $conn->prepare('SELECT id FROM portals WHERE slug = ? LIMIT 1');
    if (!$stmt) {
        throw new RuntimeException('No se pudo preparar la consulta para obtener el portal ' . $slug);
    }

    $stmt->bind_param('s', $slug);
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result ? $result->fetch_assoc() : null;
    $stmt->close();

    if (!is_array($row) || empty($row['id'])) {
        throw new RuntimeException('No existe el portal con slug ' . $slug);
    }

    return (int) $row['id'];
}

$conn = $GLOBALS['conn'] ?? null;
try {
    if (!($conn instanceof mysqli)) {
        throw new RuntimeException('La conexión a la base de datos no está disponible para las pruebas.');
    }

    clear_portal_domain_cache();

    $domain = 'clientes.ejemplo.com';
    $conn->query("DELETE FROM portal_domains WHERE domain = '" . $conn->real_escape_string($domain) . "'");

    $tenantPortalId = fetch_portal_id($conn, 'tenant');
    $inserted = register_portal_domain($tenantPortalId, $domain, true);
    assert_true($inserted, 'El dominio de prueba no se pudo registrar.');

    $resolved = resolve_portal_by_email('usuario@CLIENTES.EJEMPLO.com');
    assert_true(is_array($resolved), 'El dominio recién registrado debe resolverse.');
    assert_same('tenant', $resolved['portal_slug'] ?? null, 'El slug del portal debe corresponder a tenant.');
    assert_same($tenantPortalId, $resolved['portal_id'] ?? null, 'El portal asociado al dominio debe coincidir.');

    $conn->query("DELETE FROM portal_domains WHERE domain = '" . $conn->real_escape_string($domain) . "'");

    $cached = resolve_portal_by_email('otro@clientes.ejemplo.com');
    assert_true(is_array($cached), 'La caché debe devolver el portal incluso después de borrar el registro.');
    assert_same('tenant', $cached['portal_slug'] ?? null, 'El slug en caché debe permanecer intacto.');

    clear_portal_domain_cache($domain);
    $missing = resolve_portal_by_email('ultimo@clientes.ejemplo.com');
    assert_true($missing === null, 'Tras limpiar la caché el dominio debe dejar de resolverse.');

    clear_portal_domain_cache();
    $conn->query("DELETE FROM portal_domains WHERE domain = '" . $conn->real_escape_string($domain) . "'");

    echo "✓ portal_domains helper: todas las aserciones pasaron" . PHP_EOL;
    exit(0);
} catch (Throwable $e) {
    clear_portal_domain_cache();
    if ($conn instanceof mysqli) {
        $conn->query("DELETE FROM portal_domains WHERE domain = 'clientes.ejemplo.com'");
    }
    fwrite(STDERR, '✗ portal_domains helper falló: ' . $e->getMessage() . PHP_EOL);
    exit(1);
}
