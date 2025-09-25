<?php

if (!function_exists('qr_links_secret')) {
    function qr_links_secret(): string
    {
        static $secret = null;
        if ($secret !== null) {
            return $secret;
        }

        $env = getenv('QR_LINK_SECRET');
        if (is_string($env) && $env !== '') {
            $secret = $env;
            return $secret;
        }

        $storagePath = dirname(__DIR__, 2) . '/storage/qr_link_secret.key';
        if (is_readable($storagePath)) {
            $content = trim((string) file_get_contents($storagePath));
            if ($content !== '') {
                $secret = $content;
                return $secret;
            }
        }

        $secret = hash('sha256', __FILE__ . '|' . php_uname());
        return $secret;
    }
}

if (!function_exists('qr_links_base64url_encode')) {
    function qr_links_base64url_encode(string $data): string
    {
        return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
    }
}

if (!function_exists('qr_links_base64url_decode')) {
    function qr_links_base64url_decode(string $data): string
    {
        $remainder = strlen($data) % 4;
        if ($remainder) {
            $data .= str_repeat('=', 4 - $remainder);
        }
        return base64_decode(strtr($data, '-_', '+/'), true) ?: '';
    }
}

if (!function_exists('qr_links_pack')) {
    function qr_links_pack(array $payload): string
    {
        $json = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if ($json === false) {
            throw new RuntimeException('No se pudo serializar el payload para el enlace.');
        }

        $encodedPayload = qr_links_base64url_encode($json);
        $signature = hash_hmac('sha256', $encodedPayload, qr_links_secret(), true);
        $encodedSignature = qr_links_base64url_encode($signature);

        return $encodedPayload . '.' . $encodedSignature;
    }
}

if (!function_exists('qr_links_unpack')) {
    function qr_links_unpack(string $token): ?array
    {
        if ($token === '') {
            return null;
        }

        $parts = explode('.', $token);
        if (count($parts) !== 2) {
            return null;
        }

        [$encodedPayload, $encodedSignature] = $parts;
        if ($encodedPayload === '' || $encodedSignature === '') {
            return null;
        }

        $expectedSignature = qr_links_base64url_encode(hash_hmac('sha256', $encodedPayload, qr_links_secret(), true));
        if (!hash_equals($expectedSignature, $encodedSignature)) {
            return null;
        }

        $decoded = qr_links_base64url_decode($encodedPayload);
        if ($decoded === '') {
            return null;
        }

        $data = json_decode($decoded, true);
        return is_array($data) ? $data : null;
    }
}

if (!function_exists('qr_links_decode_token')) {
    function qr_links_decode_token(?string $token): ?array
    {
        if (!is_string($token) || $token === '') {
            return null;
        }

        $data = qr_links_unpack($token);
        if (!is_array($data)) {
            return null;
        }

        $data['instrument_id'] = isset($data['instrument_id']) ? (int) $data['instrument_id'] : 0;
        $data['calibration_id'] = isset($data['calibration_id']) ? (int) $data['calibration_id'] : 0;
        $data['empresa_id'] = isset($data['empresa_id']) ? (int) $data['empresa_id'] : 0;
        $data['exp'] = isset($data['exp']) ? (int) $data['exp'] : 0;
        $file = isset($data['file']) ? basename((string) $data['file']) : '';
        if ($file === '') {
            return null;
        }
        $data['file'] = $file;

        return $data;
    }
}

if (!function_exists('qr_links_decode_detail_token')) {
    function qr_links_decode_detail_token(?string $token): ?array
    {
        if (!is_string($token) || $token === '') {
            return null;
        }

        $data = qr_links_unpack($token);
        if (!is_array($data)) {
            return null;
        }

        $data['instrument_id'] = isset($data['instrument_id']) ? (int) $data['instrument_id'] : 0;
        $data['empresa_id'] = isset($data['empresa_id']) ? (int) $data['empresa_id'] : 0;
        $data['exp'] = isset($data['exp']) ? (int) $data['exp'] : 0;
        $data['calibration_id'] = isset($data['calibration_id']) ? (int) $data['calibration_id'] : 0;

        if ($data['instrument_id'] <= 0 || $data['empresa_id'] <= 0) {
            return null;
        }

        return $data;
    }
}

if (!function_exists('qr_links_token_is_expired')) {
    function qr_links_token_is_expired(array $payload): bool
    {
        $exp = isset($payload['exp']) ? (int) $payload['exp'] : 0;
        return $exp > 0 ? ($exp < time()) : true;
    }
}

if (!function_exists('qr_links_generate_certificate_token')) {
    function qr_links_generate_certificate_token(int $empresaId, int $instrumentoId, int $calibracionId, string $archivo, int $ttlSeconds = 2592000): array
    {
        $ttlSeconds = max($ttlSeconds, 300);
        $expiresAt = time() + $ttlSeconds;
        $payload = [
            'empresa_id' => $empresaId,
            'instrument_id' => $instrumentoId,
            'calibration_id' => $calibracionId,
            'file' => basename($archivo),
            'exp' => $expiresAt,
        ];

        return [
            'token' => qr_links_pack($payload),
            'expires_at' => $expiresAt,
            'payload' => $payload,
        ];
    }
}

if (!function_exists('qr_links_generate_instrument_detail_token')) {
    function qr_links_generate_instrument_detail_token(int $empresaId, int $instrumentoId, int $ttlSeconds = 2592000): array
    {
        $ttlSeconds = max($ttlSeconds, 300);
        $expiresAt = time() + $ttlSeconds;
        $payload = [
            'empresa_id' => $empresaId,
            'instrument_id' => $instrumentoId,
            'exp' => $expiresAt,
        ];

        return [
            'token' => qr_links_pack($payload),
            'expires_at' => $expiresAt,
            'payload' => $payload,
        ];
    }
}

if (!function_exists('qr_links_certificate_relative_url')) {
    function qr_links_certificate_relative_url(string $token): string
    {
        return 'backend/calibraciones/share_certificate.php?token=' . rawurlencode($token);
    }
}

if (!function_exists('qr_links_instrument_detail_relative_url')) {
    function qr_links_instrument_detail_relative_url(string $token): string
    {
        return 'apps/tenant/instrumentos/qr_details.html?token=' . rawurlencode($token);
    }
}
