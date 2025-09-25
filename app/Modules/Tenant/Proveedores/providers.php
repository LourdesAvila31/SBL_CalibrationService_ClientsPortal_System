<?php
// Endpoint que lista y elimina proveedores en formato JSON.
// Requiere una conexión válida a la base de datos local y no
// utiliza datasets de demostración.

require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
header('Content-Type: application/json');

$roleName = $_SESSION['role_id'] ?? '';
$canManage = in_array($roleName, ['Superadministrador', 'Administrador'], true);
$canView   = $canManage || $roleName === 'Supervisor';

try {
    require_once dirname(__DIR__, 3) . '/Core/db.php';
} catch (Throwable $e) {
    error_log('Error de conexión a la base de datos: ' . $e->getMessage());
    http_response_code(500);
    echo json_encode(['error' => 'No se pudo conectar a la base de datos.']);
    exit;
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $accion = $_POST['accion'] ?? '';
    if ($accion === 'eliminar') {
        if (!$canManage) {
            http_response_code(403);
            echo json_encode(['msg' => 'Acceso denegado']);
            $conn->close();
            exit;
        }
        $id = intval($_POST['id'] ?? 0);
        $conn->begin_transaction();
        try {
            $stmt = $conn->prepare('DELETE FROM proveedor_contactos WHERE proveedor_id = ?');
            $stmt->bind_param('i', $id);
            $stmt->execute();
            $stmt->close();

            $stmt = $conn->prepare('DELETE FROM proveedores WHERE id = ?');
            $stmt->bind_param('i', $id);
            $ok = $stmt->execute();
            $stmt->close();

            $conn->commit();
        } catch (Throwable $e) {
            $conn->rollback();
            $ok = false;
        }
        echo json_encode(['msg' => $ok ? 'Proveedor eliminado' : 'Error al eliminar proveedor']);
    } else {
        echo json_encode(['msg' => 'Acción no válida']);
    }
    $conn->close();
    exit;
}

if (!$canView) {
    http_response_code(403);
    echo json_encode([]);
    $conn->close();
    exit;
}

$sql = 'SELECT id, nombre, direccion FROM proveedores ORDER BY id DESC';
$res = $conn->query($sql);
$proveedores = [];
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $row['contactos'] = [];
        $stmt = $conn->prepare('SELECT nombre, puesto, correo FROM proveedor_contactos WHERE proveedor_id = ?');
        $stmt->bind_param('i', $row['id']);
        $stmt->execute();
        $cRes = $stmt->get_result();
        if ($cRes) {
            while ($c = $cRes->fetch_assoc()) {
                $row['contactos'][] = $c;
            }
        }
        $stmt->close();
        $proveedores[] = $row;
    }
}

echo json_encode($proveedores);
$conn->close();
?>
