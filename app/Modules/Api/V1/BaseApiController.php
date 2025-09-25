<?php

require_once dirname(__DIR__, 3) . '/Core/db_config.php';

abstract class BaseApiController
{
    /** @var mysqli */
    protected $db;

    /** @var string */
    protected $method;

    /** @var array|null */
    private $jsonInput;

    public function __construct()
    {
        $this->db = DatabaseManager::getConnection();
        $this->method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
    }

    /**
     * Devuelve el método HTTP normalizado.
     */
    protected function getRequestMethod(): string
    {
        return strtoupper($this->method);
    }

    /**
     * Analiza y devuelve el cuerpo JSON de la petición.
     *
     * @return array<string,mixed>
     */
    protected function getJsonInput(): array
    {
        if ($this->jsonInput !== null) {
            return $this->jsonInput;
        }

        $raw = file_get_contents('php://input');
        if ($raw === false || trim($raw) === '') {
            $this->jsonInput = [];
            return $this->jsonInput;
        }

        $data = json_decode($raw, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->respondError('Cuerpo JSON inválido.', 400, [
                'json_error' => json_last_error_msg(),
            ]);
        }

        if (!is_array($data)) {
            $this->respondError('El cuerpo JSON debe ser un objeto o arreglo asociativo.', 400);
        }

        /** @var array<string,mixed> $data */
        $this->jsonInput = $data;
        return $this->jsonInput;
    }

    /**
     * Responde una petición OPTIONS para facilitar CORS/preflight.
     *
     * @param string[] $allowedMethods
     */
    protected function handleOptions(array $allowedMethods): void
    {
        $normalized = array_unique(array_map('strtoupper', $allowedMethods));
        header('Allow: ' . implode(', ', $normalized));
        header('Access-Control-Allow-Methods: ' . implode(', ', $normalized));
        header('Access-Control-Allow-Headers: Content-Type, Authorization, X-Requested-With');
        header('Access-Control-Allow-Origin: *');
        http_response_code(204);
        exit;
    }

    /**
     * Envía una respuesta JSON estándar.
     *
     * @param mixed $payload
     */
    protected function respondJson($payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json');
        echo json_encode($payload, JSON_UNESCAPED_UNICODE);
        exit;
    }

    /**
     * Envía una respuesta de error en formato JSON.
     *
     * @param array<string,mixed> $details
     */
    protected function respondError(string $message, int $status = 400, array $details = []): void
    {
        $payload = ['error' => $message];
        if (!empty($details)) {
            $payload['details'] = $details;
        }
        $this->respondJson($payload, $status);
    }

    protected function beginTransaction(): void
    {
        if (method_exists($this->db, 'begin_transaction')) {
            $this->db->begin_transaction();
        }
    }

    protected function commit(): void
    {
        if (method_exists($this->db, 'commit')) {
            $this->db->commit();
        }
    }

    protected function rollBack(): void
    {
        if (method_exists($this->db, 'rollback')) {
            $this->db->rollback();
        }
    }

    /**
     * Enlaza parámetros dinámicos para consultas preparadas.
     *
     * @param mysqli_stmt $stmt
     * @param string $types
     * @param array<int,mixed> $params
     */
    protected function bindParams(mysqli_stmt $stmt, string $types, array &$params): void
    {
        $refs = [];
        foreach ($params as $index => &$value) {
            $refs[$index] = &$value;
        }
        $stmt->bind_param($types, ...$refs);
    }

    abstract public function handle(): void;
}
