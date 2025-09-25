<?php
// Usar rutas relativas para evitar dependencias del nombre de carpeta
require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/permissions.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

if (!check_permission('calibraciones_eliminar')) {
    http_response_code(403);
    exit('Acceso denegado');
}

$empresaId = obtenerEmpresaId();
if (!$empresaId) {
    http_response_code(400);
    exit('Empresa no especificada');
}

$periodosValidos = ['P1', 'P2', 'EXTRA'];
$cal = null;

if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    $id = filter_input(INPUT_GET, 'id', FILTER_VALIDATE_INT);
    if ($id) {
        $stmt = $conn->prepare('SELECT * FROM calibraciones WHERE id = ? AND empresa_id = ?');
        $stmt->bind_param('ii', $id, $empresaId);
        $stmt->execute();
        $res = $stmt->get_result();
        $cal = $res->fetch_assoc();
        $stmt->close();

        if ($cal) {
            $certStmt = $conn->prepare('SELECT archivo FROM certificados WHERE calibracion_id = ? ORDER BY fecha_subida DESC, id DESC LIMIT 1');
            $certStmt->bind_param('i', $id);
            $certStmt->execute();
            $certStmt->bind_result($archivoCert);
            if ($certStmt->fetch()) {
                $cal['certificado'] = $archivoCert;
            }
            $certStmt->close();
        }
    }
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $id             = filter_input(INPUT_POST, 'id', FILTER_VALIDATE_INT);
    $instrumento_id = filter_input(INPUT_POST, 'instrumento_id', FILTER_VALIDATE_INT);
    $calibration_date = trim($_POST['calibration_date'] ?? '');
    $due_date         = trim($_POST['due_date'] ?? '');
    $result           = trim($_POST['result'] ?? '');
    $periodo          = strtoupper(trim($_POST['periodo'] ?? ''));
    $certificate      = trim($_POST['certificate'] ?? '');

    if (!$id || !$instrumento_id || $calibration_date === '' || $result === '') {
        exit('Datos incompletos.');
    }

    if (!in_array($periodo, $periodosValidos, true)) {
        exit('Periodo inválido.');
    }

    $check = $conn->prepare('SELECT 1 FROM instrumentos WHERE id = ? AND empresa_id = ?');
    $check->bind_param('ii', $instrumento_id, $empresaId);
    $check->execute();
    $check->store_result();
    if ($check->num_rows === 0) {
        $check->close();
        exit('Instrumento no encontrado');
    }
    $check->close();

    $dueDateParam = $due_date !== '' ? $due_date : null;

    $stmt = $conn->prepare('UPDATE calibraciones SET instrumento_id = ?, fecha_calibracion = ?, fecha_proxima = ?, resultado = ?, periodo = ? WHERE id = ? AND empresa_id = ?');
    $stmt->bind_param('issssii', $instrumento_id, $calibration_date, $dueDateParam, $result, $periodo, $id, $empresaId);

    if ($stmt->execute()) {
        $stmt->close();

        if ($certificate !== '') {
            $existingId = null;
            $certLookup = $conn->prepare('SELECT id FROM certificados WHERE calibracion_id = ? ORDER BY fecha_subida DESC, id DESC LIMIT 1');
            $certLookup->bind_param('i', $id);
            $certLookup->execute();
            $certLookup->bind_result($existingId);
            $hasCertificate = $certLookup->fetch();
            $certLookup->close();

            if ($hasCertificate && $existingId) {
                $updateCert = $conn->prepare('UPDATE certificados SET archivo = ? WHERE id = ?');
                $updateCert->bind_param('si', $certificate, $existingId);
                $updateCert->execute();
                $updateCert->close();
            } else {
                $tipoCert = 'calibracion';
                $insertCert = $conn->prepare('INSERT INTO certificados (calibracion_id, archivo, tipo) VALUES (?, ?, ?)');
                $insertCert->bind_param('iss', $id, $certificate, $tipoCert);
                $insertCert->execute();
                $insertCert->close();
            }
        }

        echo "Calibración actualizada. <a href='list_calibrations.html'>Volver</a>";
        exit;
    }

    $stmt->close();
    exit('Error: ' . $conn->error);
}

?>

<?php if ($cal): ?>
<form method="POST">
    <input type="hidden" name="id" value="<?php echo (int) $cal['id']; ?>">
    <label>ID Instrumento:</label>
    <input type="number" name="instrumento_id" value="<?php echo (int) $cal['instrumento_id']; ?>" required><br>
    <label>Fecha de calibración:</label>
    <input type="date" name="calibration_date" value="<?php echo htmlspecialchars($cal['fecha_calibracion'] ?? '', ENT_QUOTES, 'UTF-8'); ?>" required><br>
    <label>Fecha de vencimiento:</label>
    <input type="date" name="due_date" value="<?php echo htmlspecialchars($cal['fecha_proxima'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br>
    <label>Resultado:</label>
    <input type="text" name="result" value="<?php echo htmlspecialchars($cal['resultado'] ?? '', ENT_QUOTES, 'UTF-8'); ?>" required><br>
    <label>Periodo:</label>
    <select name="periodo" required>
        <option value="">Seleccione periodo</option>
        <?php foreach ($periodosValidos as $periodoVal): ?>
            <option value="<?php echo $periodoVal; ?>" <?php echo (($cal['periodo'] ?? '') === $periodoVal) ? 'selected' : ''; ?>>
                <?php
                    switch ($periodoVal) {
                        case 'P1':
                            echo 'Periodo 1';
                            break;
                        case 'P2':
                            echo 'Periodo 2';
                            break;
                        default:
                            echo 'Extraordinario';
                            break;
                    }
                ?>
            </option>
        <?php endforeach; ?>
    </select><br>
    <label>Certificado:</label>
    <input type="text" name="certificate" value="<?php echo htmlspecialchars($cal['certificado'] ?? '', ENT_QUOTES, 'UTF-8'); ?>"><br><br>
    <button type="submit">Actualizar</button>
</form>
<?php endif; ?>
