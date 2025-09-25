<?php
declare(strict_types=1);

/**
 * Cliente ligero para interactuar con las APIs de Google (Sheets/Drive)
 * utilizando credenciales de cuenta de servicio.
 *
 * La configuración se lee desde la variable de entorno
 * `GOOGLE_APPLICATION_CREDENTIALS_JSON` (contenido JSON) o desde el archivo
 * especificado en `GOOGLE_APPLICATION_CREDENTIALS`. Como último recurso se
 * intenta cargar el archivo `google_service_account.json` ubicado junto a este
 * script.
 */

final class GoogleServiceClient
{
    private const TOKEN_URI = 'https://oauth2.googleapis.com/token';

    /** @var array<string,mixed> */
    private $credentials;

    /**
     * @var array<string,array{token:string,expires:int}>
     */
    private $tokenCache = [];

    /**
     * @param array<string,mixed> $credentials
     */
    public function __construct(array $credentials)
    {
        $this->credentials = $credentials;
    }

    /**
     * Ejecuta una solicitud HTTP autenticada y devuelve el resultado decodificado
     * como arreglo asociativo.
     *
     * @param array<int,string> $scopes
     * @param array<string,string> $headers
     * @return array<string,mixed>
     */
    public function requestJson(string $method, string $url, array $scopes, $body = null, array $headers = []): array
    {
        $response = $this->request($method, $url, $scopes, $body, $headers, true);
        if (!is_array($response)) {
            throw new RuntimeException('Respuesta inesperada de la API de Google.');
        }
        return $response;
    }

    /**
     * Ejecuta una solicitud HTTP autenticada y devuelve el cuerpo tal cual se
     * recibió (útil para descargas de binarios).
     *
     * @param array<int,string> $scopes
     * @param array<string,string> $headers
     * @return string
     */
    public function requestRaw(string $method, string $url, array $scopes, $body = null, array $headers = []): string
    {
        $response = $this->request($method, $url, $scopes, $body, $headers, false);
        if (!is_string($response)) {
            throw new RuntimeException('Se esperaba un contenido binario de la API de Google.');
        }
        return $response;
    }

    /**
     * @param array<int,string> $scopes
     * @param array<string,string> $headers
     * @return array<string,mixed>|string
     */
    private function request(string $method, string $url, array $scopes, $body, array $headers, bool $expectJson)
    {
        $accessToken = $this->fetchAccessToken($scopes);

        $ch = curl_init();
        if ($ch === false) {
            throw new RuntimeException('No se pudo inicializar la conexión cURL.');
        }

        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, strtoupper($method));
        curl_setopt($ch, CURLOPT_HTTPHEADER, $this->buildHeaders($headers, $accessToken, $body));

        if ($body !== null) {
            if (is_array($body)) {
                $body = json_encode($body, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            }
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $responseBody = curl_exec($ch);
        if ($responseBody === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new RuntimeException('Error al comunicar con la API de Google: ' . $error);
        }

        $status = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        if ($status < 200 || $status >= 300) {
            $message = 'La API de Google devolvió un error (' . $status . ')';
            if ($expectJson) {
                $decoded = json_decode($responseBody, true);
                if (is_array($decoded) && isset($decoded['error'])) {
                    if (is_string($decoded['error'])) {
                        $message .= ': ' . $decoded['error'];
                    } elseif (is_array($decoded['error']) && isset($decoded['error']['message'])) {
                        $message .= ': ' . (string) $decoded['error']['message'];
                    }
                }
            }
            throw new RuntimeException($message);
        }

        if ($expectJson) {
            /** @var mixed $decoded */
            $decoded = json_decode($responseBody, true);
            if (!is_array($decoded)) {
                throw new RuntimeException('No se pudo interpretar la respuesta JSON de Google.');
            }
            return $decoded;
        }

        return $responseBody;
    }

    /**
     * @param array<string,string> $headers
     * @return array<int,string>
     */
    private function buildHeaders(array $headers, string $token, $body): array
    {
        $final = ['Authorization: Bearer ' . $token];
        $hasContentType = false;
        foreach ($headers as $key => $value) {
            if (strtolower($key) === 'content-type') {
                $hasContentType = true;
            }
            $final[] = $key . ': ' . $value;
        }

        if (!$hasContentType && $body !== null) {
            $final[] = 'Content-Type: application/json; charset=UTF-8';
        }

        return $final;
    }

    /**
     * @param array<int,string> $scopes
     */
    private function fetchAccessToken(array $scopes): string
    {
        if (empty($scopes)) {
            throw new InvalidArgumentException('Se requiere al menos un scope para solicitar el token de acceso.');
        }
        sort($scopes);
        $cacheKey = sha1(implode(' ', $scopes));
        if (isset($this->tokenCache[$cacheKey])) {
            $cached = $this->tokenCache[$cacheKey];
            if ($cached['expires'] > time() + 60) {
                return $cached['token'];
            }
        }

        $jwt = $this->buildJwt($scopes);
        $postFields = http_build_query([
            'grant_type' => 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion'  => $jwt,
        ], '', '&');

        $ch = curl_init(self::TOKEN_URI);
        if ($ch === false) {
            throw new RuntimeException('No se pudo inicializar la solicitud de token.');
        }
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/x-www-form-urlencoded']);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $postFields);

        $response = curl_exec($ch);
        if ($response === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new RuntimeException('Error al solicitar token OAuth: ' . $error);
        }
        $status = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        if ($status < 200 || $status >= 300) {
            $message = 'Solicitud de token rechazada (' . $status . ')';
            $decoded = json_decode($response, true);
            if (is_array($decoded) && isset($decoded['error_description'])) {
                $message .= ': ' . (string) $decoded['error_description'];
            }
            throw new RuntimeException($message);
        }

        $decoded = json_decode($response, true);
        if (!is_array($decoded) || !isset($decoded['access_token'])) {
            throw new RuntimeException('No se recibió un token de acceso válido.');
        }

        $token = (string) $decoded['access_token'];
        $expiresIn = isset($decoded['expires_in']) ? (int) $decoded['expires_in'] : 3600;
        $this->tokenCache[$cacheKey] = [
            'token'   => $token,
            'expires' => time() + $expiresIn,
        ];

        return $token;
    }

    /**
     * @param array<int,string> $scopes
     */
    private function buildJwt(array $scopes): string
    {
        $now = time();
        $payload = [
            'iss'   => $this->credentials['client_email'] ?? $this->credentials['service_account_email'] ?? null,
            'scope' => implode(' ', $scopes),
            'aud'   => self::TOKEN_URI,
            'iat'   => $now,
            'exp'   => $now + 3600,
        ];

        if (empty($payload['iss'])) {
            throw new RuntimeException('Las credenciales de Google no contienen el campo client_email.');
        }

        $header = ['alg' => 'RS256', 'typ' => 'JWT'];
        $segments = [
            $this->base64UrlEncode(json_encode($header, JSON_UNESCAPED_SLASHES)),
            $this->base64UrlEncode(json_encode($payload, JSON_UNESCAPED_SLASHES)),
        ];
        $signingInput = implode('.', $segments);
        $signature = '';
        $privateKey = $this->credentials['private_key'] ?? null;
        if (!is_string($privateKey) || trim($privateKey) === '') {
            throw new RuntimeException('Las credenciales de Google no contienen una clave privada válida.');
        }

        $success = openssl_sign($signingInput, $signature, $privateKey, OPENSSL_ALGO_SHA256);
        if (!$success) {
            throw new RuntimeException('No fue posible firmar el token JWT.');
        }

        $segments[] = $this->base64UrlEncode($signature);
        return implode('.', $segments);
    }

    private function base64UrlEncode(string $value): string
    {
        return rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
    }
}

final class GoogleSheetsClient
{
    private const API_BASE = 'https://sheets.googleapis.com/v4/spreadsheets';
    private $client;

    public function __construct(GoogleServiceClient $client)
    {
        $this->client = $client;
    }

    /**
     * @return array<string,mixed>
     */
    public function createSpreadsheet(string $title): array
    {
        $url = self::API_BASE;
        $body = ['properties' => ['title' => $title]];
        return $this->client->requestJson('POST', $url, ['https://www.googleapis.com/auth/spreadsheets'], $body);
    }

    /**
     * @return array<string,mixed>
     */
    public function copySheet(string $spreadsheetId, int $sheetId, string $destinationSpreadsheetId): array
    {
        $url = sprintf('%s/%s/sheets/%d:copyTo', self::API_BASE, $spreadsheetId, $sheetId);
        $body = ['destinationSpreadsheetId' => $destinationSpreadsheetId];
        return $this->client->requestJson('POST', $url, ['https://www.googleapis.com/auth/spreadsheets'], $body);
    }

    /**
     * @param array<int,array<string,mixed>> $requests
     * @return array<string,mixed>
     */
    public function batchUpdate(string $spreadsheetId, array $requests): array
    {
        $url = sprintf('%s/%s:batchUpdate', self::API_BASE, $spreadsheetId);
        $body = ['requests' => $requests];
        return $this->client->requestJson('POST', $url, ['https://www.googleapis.com/auth/spreadsheets'], $body);
    }

    /**
     * @param array<int,array<string,mixed>> $data
     * @return array<string,mixed>
     */
    protected function valuesBatchUpdate(string $spreadsheetId, array $data): array
    {
        $url = sprintf('%s/%s/values:batchUpdate', self::API_BASE, $spreadsheetId);
        $body = [
            'valueInputOption' => 'USER_ENTERED',
            'data'             => $data,
        ];
        return $this->client->requestJson('POST', $url, ['https://www.googleapis.com/auth/spreadsheets'], $body);
    }

    /**
     * @param array<int,array<string,mixed>> $data
     */
    public function updateValues(string $spreadsheetId, array $data): void
    {
        if (empty($data)) {
            return;
        }
        $this->valuesBatchUpdate($spreadsheetId, $data);
    }
}

final class GoogleDriveClient
{
    private const API_BASE = 'https://www.googleapis.com/drive/v3/files';
    private $client;

    public function __construct(GoogleServiceClient $client)
    {
        $this->client = $client;
    }

    /**
     * @return array<string,mixed>
     */
    public function updateFile(string $fileId, array $body, array $queryParams = []): array
    {
        $url = self::API_BASE . '/' . $fileId;
        if (!empty($queryParams)) {
            $url .= '?' . http_build_query($queryParams);
        }
        return $this->client->requestJson('PATCH', $url, ['https://www.googleapis.com/auth/drive'], $body, ['Content-Type' => 'application/json; charset=UTF-8']);
    }

    public function exportFile(string $fileId, string $mimeType): string
    {
        $url = sprintf('%s/%s/export?mimeType=%s', self::API_BASE, $fileId, rawurlencode($mimeType));
        return $this->client->requestRaw('GET', $url, ['https://www.googleapis.com/auth/drive']);
    }

    /**
     * @return array<string,mixed>
     */
    public function getFile(string $fileId, array $fields): array
    {
        $url = sprintf('%s/%s?fields=%s', self::API_BASE, $fileId, rawurlencode(implode(',', $fields)));
        return $this->client->requestJson('GET', $url, ['https://www.googleapis.com/auth/drive']);
    }
}

/**
 * Devuelve una instancia compartida del cliente base autenticado.
 */
function google_service_client(): GoogleServiceClient
{
    static $client = null;
    if ($client instanceof GoogleServiceClient) {
        return $client;
    }

    $credentials = load_google_credentials();
    $client = new GoogleServiceClient($credentials);
    return $client;
}

function google_sheets_client(): GoogleSheetsClient
{
    static $client = null;
    if ($client instanceof GoogleSheetsClient) {
        return $client;
    }
    $client = new GoogleSheetsClient(google_service_client());
    return $client;
}

function google_drive_client(): GoogleDriveClient
{
    static $client = null;
    if ($client instanceof GoogleDriveClient) {
        return $client;
    }
    $client = new GoogleDriveClient(google_service_client());
    return $client;
}

/**
 * @return array<string,mixed>
 */
function load_google_credentials(): array
{
    static $credentials = null;
    if ($credentials !== null) {
        return $credentials;
    }

    $json = getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON');
    if ($json && trim($json) !== '') {
        $decoded = json_decode($json, true);
        if (!is_array($decoded)) {
            throw new RuntimeException('No fue posible decodificar GOOGLE_APPLICATION_CREDENTIALS_JSON.');
        }
        $credentials = $decoded;
        return $credentials;
    }

    $path = getenv('GOOGLE_APPLICATION_CREDENTIALS');
    if (!$path || trim($path) === '') {
        $path = __DIR__ . '/google_service_account.json';
    }
    if (!is_file($path) || !is_readable($path)) {
        throw new RuntimeException('No se localizaron las credenciales de Google.');
    }

    $contents = file_get_contents($path);
    if ($contents === false) {
        throw new RuntimeException('No se pudo leer el archivo de credenciales de Google.');
    }

    $decoded = json_decode($contents, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Las credenciales de Google contienen un JSON inválido.');
    }
    $credentials = $decoded;
    return $credentials;
}
?>
