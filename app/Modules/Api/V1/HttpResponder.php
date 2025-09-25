<?php

namespace App\Modules\Api\V1;

use Throwable;

/**
 * Emite una respuesta HTTP a partir del objeto ApiResponse.
 */
final class HttpResponder
{
    public static function emit(ApiResponse $response): void
    {
        foreach ($response->headers as $name => $value) {
            header(trim($name) . ': ' . $value);
        }

        http_response_code($response->statusCode);

        $json = json_encode($response->payload, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
        if ($json === false) {
            $fallback = [
                'error' => 'No se pudo serializar la respuesta de la API.',
                'detail' => json_last_error_msg(),
            ];

            http_response_code(500);
            echo json_encode($fallback, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);
            return;
        }

        echo $json;
    }

    public static function emitException(Throwable $e): void
    {
        $response = new ApiResponse(500, [
            'error' => 'Error interno inesperado.',
            'detail' => $e->getMessage(),
        ]);

        self::emit($response);
    }
}
