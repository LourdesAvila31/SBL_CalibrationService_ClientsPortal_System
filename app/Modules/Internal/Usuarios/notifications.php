<?php
session_start();

header('Content-Type: application/json');

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

function responderJson(array $payload = []): void
{
    echo json_encode($payload);
    exit;
}

function normalizarNotificaciones(array $items): array
{
    foreach ($items as &$item) {
        if (is_array($item) && array_key_exists('orden', $item)) {
            unset($item['orden']);
        }
    }
    unset($item);

    return $items;
}

if (!isset($_SESSION['usuario_id'])) {
    responderJson();
}

$usuarioId = (int) $_SESSION['usuario_id'];
$empresaContexto = (int) obtenerEmpresaId();

$extendedInput = filter_input(INPUT_GET, 'extended', FILTER_DEFAULT);
$extended = $extendedInput !== null
    ? filter_var($extendedInput, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE)
    : false;
$extended = $extended ?? false;

$markAsReadInput = filter_input(INPUT_GET, 'mark_as_read', FILTER_DEFAULT);
if ($markAsReadInput === null) {
    $markAsRead = true;
} else {
    $markAsRead = filter_var($markAsReadInput, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
    $markAsRead = $markAsRead ?? false;
}

$limitInput = filter_input(INPUT_GET, 'limit', FILTER_VALIDATE_INT, [
    'options' => ['min_range' => 1, 'max_range' => 50],
]);
$limit = $limitInput !== false && $limitInput !== null ? (int) $limitInput : 20;

$sqlUsuario = <<<'SQL'
    SELECT
        r.nombre AS role_name,
        COALESCE(u.empresa_id, 0) AS usuario_empresa_id
    FROM usuarios u
    INNER JOIN roles r ON u.role_id = r.id
    WHERE u.id = ?
SQL;

$stmt = $conn->prepare($sqlUsuario);
if (!$stmt) {
    responderJson();
}

$stmt->bind_param('i', $usuarioId);

if (!$stmt->execute()) {
    $stmt->close();
    responderJson();
}

$resultado = $stmt->get_result();
$datosUsuario = $resultado->fetch_assoc();
$stmt->close();

if (!$datosUsuario) {
    responderJson();
}

$roleName = (string) ($datosUsuario['role_name'] ?? '');
$empresaUsuario = (int) ($datosUsuario['usuario_empresa_id'] ?? 0);
$empresaId = $empresaContexto > 0 ? $empresaContexto : 0;

if ($empresaId <= 0 && $empresaUsuario > 0) {
    $empresaId = $empresaUsuario;
}

if ($empresaId <= 0) {
    responderJson();
}

$_SESSION['empresa_id'] = $empresaId;

$userNotifications = [];
$idsSinLeer = [];

$sqlUserNotificaciones = sprintf(
    'SELECT id, titulo, mensaje, tipo, created_at, read_at FROM user_notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT %d',
    $limit
);

$stmt = $conn->prepare($sqlUserNotificaciones);
if ($stmt) {
    $stmt->bind_param('i', $usuarioId);
    if ($stmt->execute()) {
        $resultado = $stmt->get_result();
        while ($row = $resultado->fetch_assoc()) {
            $createdAt = isset($row['created_at']) ? strtotime((string) $row['created_at']) : false;
            $timestamp = $createdAt !== false ? (int) $createdAt : time();
            $userNotifications[] = [
                'titulo'    => (string) ($row['titulo'] ?? ''),
                'mensaje'   => (string) ($row['mensaje'] ?? ''),
                'tipo'      => (string) ($row['tipo'] ?? 'general'),
                'fecha'     => $createdAt ? date('d/m/Y H:i', $createdAt) : null,
                'leido'     => !empty($row['read_at']),
                'orden'     => $timestamp,
            ];
            if (empty($row['read_at']) && isset($row['id'])) {
                $idsSinLeer[] = (int) $row['id'];
            }
        }
    }
    $stmt->close();
}

$unreadBefore = count($idsSinLeer);

if ($idsSinLeer) {
    $idsFiltrados = array_filter($idsSinLeer, static function ($value) {
        return is_int($value) && $value > 0;
    });
} else {
    $idsFiltrados = [];
}

$unreadActual = $unreadBefore;

if ($markAsRead && $idsFiltrados) {
    $idsString = implode(',', $idsFiltrados);
    if ($idsString !== '') {
        $conn->query("UPDATE user_notifications SET read_at = NOW() WHERE id IN ({$idsString})");
        $unreadActual = 0;
    }
}

$sqlNotificaciones = <<<'SQL'
    SELECT
        COALESCE(ci.nombre, CONCAT('Instrumento #', cal.instrumento_id)) AS instrumento,
        cal.fecha_proxima,
        DATEDIFF(cal.fecha_proxima, CURDATE()) AS dias_restantes
    FROM calibraciones cal
    INNER JOIN instrumentos i ON i.id = cal.instrumento_id
    LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
    WHERE cal.empresa_id = ?
      AND cal.fecha_proxima IS NOT NULL
      AND cal.fecha_proxima BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 60 DAY)
    ORDER BY cal.fecha_proxima ASC
SQL;

$stmt = $conn->prepare($sqlNotificaciones);
if (!$stmt) {
    responderJson(normalizarNotificaciones($userNotifications));
}

$stmt->bind_param('i', $empresaId);

if (!$stmt->execute()) {
    $stmt->close();
    responderJson(normalizarNotificaciones($userNotifications));
}

$res = $stmt->get_result();

$notificaciones = $userNotifications;
while ($row = $res->fetch_assoc()) {
    $fechaVencimiento = $row['fecha_proxima'] ?? null;
    $diasRestantes = isset($row['dias_restantes']) ? (int) $row['dias_restantes'] : null;

    if (!$fechaVencimiento || $diasRestantes === null || $diasRestantes < 0) {
        continue;
    }

    $timestamp = strtotime($fechaVencimiento);
    if ($timestamp === false) {
        continue;
    }

    $fecha = date('d/m/Y', $timestamp);
    $titulo = $diasRestantes <= 30 ? 'Vence en ≤30 días' : 'Vence en ≤60 días';
    $mensaje = "El instrumento <b>{$row['instrumento']}</b> vence el <b>{$fecha}</b> (faltan {$diasRestantes} días).";
    if ($roleName !== 'Cliente') {
        $mensaje .= " (Empresa ID: {$empresaId})";
    }

    $notificaciones[] = [
        'titulo'  => $titulo,
        'mensaje' => $mensaje,
        'tipo'    => 'calibration_alert',
        'fecha'   => $fecha,
        'leido'   => false,
        'orden'   => $timestamp,
    ];
}

$stmt->close();

if ($notificaciones) {
    usort($notificaciones, static function (array $a, array $b): int {
        $aOrden = $a['orden'] ?? 0;
        $bOrden = $b['orden'] ?? 0;
        if ($aOrden === $bOrden) {
            return 0;
        }
        return ($aOrden < $bOrden) ? 1 : -1;
    });
    $notificaciones = array_slice($notificaciones, 0, 20);
    foreach ($notificaciones as &$notificacion) {
        unset($notificacion['orden']);
    }
    unset($notificacion);
}

$notificacionesNormalizadas = normalizarNotificaciones($notificaciones);

if ($extended) {
    $payload = [
        'success' => true,
        'items' => $notificacionesNormalizadas,
        'meta' => [
            'limit' => $limit,
            'mark_as_read' => $markAsRead,
            'unread_user_notifications_before' => $unreadBefore,
            'unread_user_notifications' => $markAsRead ? 0 : $unreadActual,
            'total_items' => count($notificacionesNormalizadas),
        ],
    ];
    responderJson($payload);
}

responderJson($notificacionesNormalizadas);
