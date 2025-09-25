<?php

namespace App\Modules\Api\V1;

use mysqli;
use mysqli_stmt;

/**
 * Clase base con utilidades compartidas para los controladores de la API.
 */
abstract class Controller
{
    /**
     * @param string $method Método HTTP original.
     * @param array<string,mixed> $query Parámetros de la consulta.
     */
    abstract public function handle(string $method, array $query = []): ApiResponse;

    /**
     * @param array<string,mixed> $payload
     */
    protected function ok(array $payload, int $statusCode = 200): ApiResponse
    {
        return new ApiResponse($statusCode, $payload);
    }

    /**
     * @param array<string,string> $headers
     */
    protected function withHeaders(ApiResponse $response, array $headers): ApiResponse
    {
        return new ApiResponse($response->statusCode, $response->payload, $headers + $response->headers);
    }

    /**
     * @param array<string,mixed> $extra
     */
    protected function error(string $message, int $statusCode, array $extra = []): ApiResponse
    {
        return new ApiResponse($statusCode, ['error' => $message] + $extra);
    }

    /**
     * @param string[] $allowedMethods
     */
    protected function methodNotAllowed(array $allowedMethods): ApiResponse
    {
        $allowHeader = implode(', ', array_unique(array_map('strtoupper', $allowedMethods)));

        return new ApiResponse(
            405,
            [
                'error' => 'Método no permitido. Usa uno de: ' . $allowHeader,
            ],
            ['Allow' => $allowHeader]
        );
    }

    /**
     * Vincula parámetros dinámicos a la sentencia preparada.
     *
     * @param string   $types  Cadena de tipos compatibles con mysqli.
     * @param mixed[]  $params Parámetros a vincular.
     */
    protected function bindParams(mysqli_stmt $stmt, string $types, array $params): void
    {
        if ($types === '' || $params === []) {
            return;
        }

        $references = [];
        foreach ($params as $index => $value) {
            $references[$index] = &$params[$index];
        }

        array_unshift($references, $types);
        call_user_func_array([$stmt, 'bind_param'], $references);
    }

    protected function closeConnection(?mysqli $conn): void
    {
        if ($conn instanceof mysqli) {
            $conn->close();
        }
    }
}
