<?php
session_start();
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
if (!check_permission('usuarios_view')) {
    http_response_code(403);
    echo json_encode([]);
    exit;
}

header('Content-Type: application/json');
require_once dirname(__DIR__, 3) . '/Core/db.php';

$empresas = [];
$esSuper = session_is_superadmin();
$empresaSesion = ensure_session_empresa_id();

if ($esSuper || $empresaSesion === null) {
    $sql = "SELECT id, nombre FROM empresas ORDER BY nombre";
    if ($result = $conn->query($sql)) {
        while ($row = $result->fetch_assoc()) {
            $empresas[] = [
                'id' => (int) $row['id'],
                'nombre' => $row['nombre'],
            ];
        }
        $result->close();
    }
} else {
    $stmt = $conn->prepare('SELECT id, nombre FROM empresas WHERE id = ?');
    $stmt->bind_param('i', $empresaSesion);
    $stmt->execute();
    $result = $stmt->get_result();
    while ($row = $result->fetch_assoc()) {
        $empresas[] = [
            'id' => (int) $row['id'],
            'nombre' => $row['nombre'],
        ];
    }
    $stmt->close();
}

echo json_encode($empresas);
