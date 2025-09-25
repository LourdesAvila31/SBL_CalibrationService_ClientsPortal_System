<?php
require_once dirname(__DIR__, 3) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';
header('Content-Type: application/json');

$sql = "SELECT id, nombre FROM departamentos ORDER BY nombre ASC";
$res = $conn->query($sql);
$options = [];
if ($res) {
    while ($row = $res->fetch_assoc()) {
        if ($row) {
            $options[] = $row;
        } else {
            break;
        }
    }
}
echo json_encode($options, JSON_UNESCAPED_UNICODE);
$conn->close();
?>