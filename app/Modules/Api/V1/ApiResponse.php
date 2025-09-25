<?php

namespace App\Modules\Api\V1;

/**
 * Representa la respuesta estandarizada que los controladores de la API
 * devuelven a los front controllers.
 */
final class ApiResponse
{
    /**
     * @param int $statusCode Código HTTP a enviar.
     * @param array<string,mixed> $payload Cuerpo de la respuesta en formato array.
     * @param array<string,string> $headers Cabeceras adicionales a emitir.
     */
    public function __construct(
        public int $statusCode,
        public array $payload,
        public array $headers = []
    ) {
    }
}
