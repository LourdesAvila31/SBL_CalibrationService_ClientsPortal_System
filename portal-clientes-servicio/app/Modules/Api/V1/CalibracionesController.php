<?php
namespace App\Modules\Api\V1;

use mysqli;
use mysqli_sql_exception;
use Throwable;
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_schedule.php';
require_once __DIR__ . '/ApiResponse.php';
require_once __DIR__ . '/Controller.php';

class CalibracionesController extends Controller
{
    public function handle(string $method, array $query = []): ApiResponse
    {
        $normalizedMethod = strtoupper($method);
        if ($normalizedMethod === 'HEAD') {
            $normalizedMethod = 'GET';
        }

        if ($normalizedMethod !== 'GET') {
            return $this->methodNotAllowed(['GET', 'OPTIONS']);
        }

        try {
            if (!defined('DB_CONFIG_AUTO_CONNECT')) {
                define('DB_CONFIG_AUTO_CONNECT', false);
            }
            require_once dirname(__DIR__, 3) . '/Core/db_config.php';
            /** @var mysqli $conn */
            $conn = \DatabaseManager::getConnection();
            $GLOBALS['conn'] = $conn;
        } catch (Throwable $e) {
            return $this->error('No se pudo conectar con la base de datos.', 500, [
                'detail' => $e->getMessage(),
            ]);
        }

        $empresaId = obtenerEmpresaId();
        if ($empresaId <= 0) {
            $this->closeConnection($conn);
            return $this->error('Empresa no especificada.', 400);
        }

        $filters = $this->buildFilters($query);
        $where = array_merge(['c.empresa_id = ?'], $filters['conditions']);
        $params = array_merge([$empresaId], $filters['params']);
        $types = 'i' . $filters['types'];

        $sql = <<<SQL
SELECT
    c.id,
    c.instrumento_id,
    ci.nombre AS instrumento,
    i.codigo,
    c.fecha_calibracion,
    c.fecha_proxima,
    c.resultado,
    c.periodo,
    c.tipo,
    c.duracion_horas,
    c.costo_total,
    c.estado_ejecucion,
    c.motivo_reprogramacion,
    c.fecha_reprogramada,
    c.dias_atraso
FROM calibraciones c
LEFT JOIN instrumentos i ON i.id = c.instrumento_id
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
WHERE %s
ORDER BY c.fecha_calibracion DESC, c.id DESC
SQL;

        $sql = sprintf($sql, implode(' AND ', $where));

        try {
            $stmt = $conn->prepare($sql);
        } catch (mysqli_sql_exception $e) {
            $this->closeConnection($conn);
            return $this->error('No se pudo preparar la consulta de calibraciones.', 500, [
                'detail' => $e->getMessage(),
            ]);
        }

        if (!$stmt) {
            $this->closeConnection($conn);
            return $this->error('No se pudo preparar la consulta de calibraciones.', 500);
        }

        $this->bindParams($stmt, $types, $params);

        if (!$stmt->execute()) {
            $stmt->close();
            $this->closeConnection($conn);
            return $this->error('No se pudo ejecutar la consulta de calibraciones.', 500);
        }

        $result = $stmt->get_result();
        $calibraciones = [];

        while ($row = $result->fetch_assoc()) {
            $estado = calibration_normalize_status($row['estado_ejecucion'] ?? null);
            $diasRegistrados = $row['dias_atraso'];
            $diasCalculados = calibration_compute_delay(
                $row['fecha_proxima'] ?? null,
                $row['fecha_calibracion'] ?? null,
                $row['fecha_reprogramada'] ?? null
            );

            $diasAtraso = $diasRegistrados !== null ? (int) $diasRegistrados : ($diasCalculados ?? null);
            $requiereJustificacion = calibration_requires_justification($estado, $diasAtraso);
            $motivo = trim((string) ($row['motivo_reprogramacion'] ?? ''));

            $row['estado_ejecucion'] = $estado;
            $row['dias_atraso'] = $diasAtraso;
            $row['requiere_justificacion'] = $requiereJustificacion;
            $row['justificacion_pendiente'] = $requiereJustificacion && $motivo === '';

            $calibraciones[] = $row;
        }

        $stmt->close();
        $this->closeConnection($conn);

        return $this->ok([
            'calibraciones' => $calibraciones,
        ]);
    }

    /**
     * @param array<string,mixed> $query
     * @return array{conditions: string[], params: array<int,mixed>, types: string}
     */
    private function buildFilters(array $query): array
    {
        $conditions = [];
        $params = [];
        $types = '';

        $instrumentoId = filter_var($query['instrumento_id'] ?? null, FILTER_VALIDATE_INT);
        if ($instrumentoId !== false && $instrumentoId !== null) {
            $conditions[] = 'c.instrumento_id = ?';
            $params[] = (int) $instrumentoId;
            $types .= 'i';
        }

        $estado = $this->stringOrNull($query['estado'] ?? null);
        if ($estado !== null) {
            $conditions[] = 'LOWER(TRIM(c.estado_ejecucion)) = ?';
            $params[] = mb_strtolower($estado, 'UTF-8');
            $types .= 's';
        }

        $desde = $this->stringOrNull($query['desde'] ?? null);
        if ($desde !== null && $this->isValidDate($desde)) {
            $conditions[] = 'c.fecha_calibracion >= ?';
            $params[] = $desde;
            $types .= 's';
        }

        $hasta = $this->stringOrNull($query['hasta'] ?? null);
        if ($hasta !== null && $this->isValidDate($hasta)) {
            $conditions[] = 'c.fecha_calibracion <= ?';
            $params[] = $hasta;
            $types .= 's';
        }

        if ($conditions === []) {
            $conditions[] = '1 = 1';
        }

        return [
            'conditions' => $conditions,
            'params' => $params,
            'types' => $types,
        ];
    }

    private function stringOrNull(mixed $value): ?string
    {
        if (!is_string($value)) {
            return null;
        }

        $trimmed = trim($value);
        return $trimmed === '' ? null : $trimmed;
    }

    private function isValidDate(string $value): bool
    {
        return (bool) preg_match('/^\\d{4}-\\d{2}-\\d{2}$/', $value);
    }
}
