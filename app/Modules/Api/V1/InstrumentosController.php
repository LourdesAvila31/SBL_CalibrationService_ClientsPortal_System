<?php

namespace App\Modules\Api\V1;

use mysqli;
use mysqli_sql_exception;
use Throwable;

require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/calibration_status.php';
require_once __DIR__ . '/ApiResponse.php';
require_once __DIR__ . '/Controller.php';

class InstrumentosController extends Controller
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
        $where = array_merge(['i.empresa_id = ?'], $filters['conditions']);
        $params = array_merge([$empresaId], $filters['params']);
        $types = 'i' . $filters['types'];

        $sql = <<<SQL
SELECT
    i.id,
    COALESCE(ci.nombre, i.codigo, CONCAT('Instrumento #', i.id)) AS nombre,
    m.nombre AS marca,
    mo.nombre AS modelo,
    i.serie,
    i.codigo,
    d.nombre AS departamento,
    i.ubicacion,
    i.fecha_alta,
    i.fecha_baja,
    i.proxima_calibracion,
    i.estado,
    i.programado
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN marcas m ON i.marca_id = m.id
LEFT JOIN modelos mo ON i.modelo_id = mo.id
LEFT JOIN departamentos d ON i.departamento_id = d.id
WHERE %s
ORDER BY i.id ASC
SQL;

        $sql = sprintf($sql, implode(' AND ', $where));

        try {
            $stmt = $conn->prepare($sql);
        } catch (mysqli_sql_exception $e) {
            $this->closeConnection($conn);
            return $this->error('No se pudo preparar la consulta de instrumentos.', 500, [
                'detail' => $e->getMessage(),
            ]);
        }

        if (!$stmt) {
            $this->closeConnection($conn);
            return $this->error('No se pudo preparar la consulta de instrumentos.', 500);
        }

        $this->bindParams($stmt, $types, $params);

        if (!$stmt->execute()) {
            $stmt->close();
            $this->closeConnection($conn);
            return $this->error('No se pudo ejecutar la consulta de instrumentos.', 500);
        }

        $result = $stmt->get_result();
        $instrumentos = [];
        while ($row = $result->fetch_assoc()) {
            $instrumentos[] = anexarEstadoCalibracion($row);
        }

        $stmt->close();
        $this->closeConnection($conn);

        return $this->ok([
            'instrumentos' => $instrumentos,
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

        $estado = $this->stringOrNull($query['estado'] ?? null);
        if ($estado !== null) {
            $conditions[] = 'LOWER(TRIM(i.estado)) = ?';
            $params[] = mb_strtolower($estado, 'UTF-8');
            $types .= 's';
        }

        $departamento = $this->stringOrNull($query['departamento'] ?? null);
        if ($departamento !== null) {
            $conditions[] = 'LOWER(TRIM(d.nombre)) = ?';
            $params[] = mb_strtolower($departamento, 'UTF-8');
            $types .= 's';
        }

        $programadoRaw = $query['programado'] ?? null;
        if ($programadoRaw !== null && $programadoRaw !== '') {
            $programado = filter_var($programadoRaw, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
            if ($programado !== null) {
                $conditions[] = 'i.programado = ?';
                $params[] = $programado ? 1 : 0;
                $types .= 'i';
            }
        }

        $instrumentoId = filter_var($query['instrumento_id'] ?? null, FILTER_VALIDATE_INT);
        if ($instrumentoId !== false && $instrumentoId !== null) {
            $conditions[] = 'i.id = ?';
            $params[] = (int) $instrumentoId;
            $types .= 'i';
        }

        $codigo = $this->stringOrNull($query['codigo'] ?? null);
        if ($codigo !== null) {
            $conditions[] = 'i.codigo = ?';
            $params[] = $codigo;
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
}
