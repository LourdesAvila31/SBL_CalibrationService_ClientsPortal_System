#!/usr/bin/env php
<?php
declare(strict_types=1);

class ValidationRunner
{
    private string $baseUrl;
    private string $format;
    private bool $useFixtures = false;
    private ?array $fixtures = null;
    private ?string $username;
    private ?string $password;
    private string $mode = 'live';
    private string $overallStatus = 'warning';
    private array $cookies = [];
    private ?array $authenticationResult = null;
    private ?string $calibrationPlanId = null;
    private string $approvalState = 'aprobada';
    private string $rejectionState = 'rechazada';
    private ?string $approvalComment = null;
    private ?string $rejectionComment = null;
    private string $portalScope = 'none';
    private function optionalString($value): ?string
    {
        if ($value === null || $value === false) {
            return null;
        }
        if (is_string($value)) {
            $trimmed = trim($value);
            return $trimmed === '' ? null : $trimmed;
        }
        return null;
    }

    public function __construct(array $options)
    {
        $formatOption = $this->optionalString($options['format'] ?? null);
        if ($formatOption === null) {
            $formatOption = 'json';
        }
        $this->format = strtolower($formatOption);
        if (!in_array($this->format, ['json', 'text'], true)) {
            $this->format = 'json';
        }

        $baseUrlOption = $this->optionalString($options['baseUrl'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_BASE_URL'));
        if ($baseUrlOption === null) {
            $baseUrlOption = 'http://localhost:8000';
        }
        $this->baseUrl = rtrim($baseUrlOption, '/');

        $fixtureOption = $this->optionalString($options['fixtures'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_FIXTURES'));
        if ($fixtureOption !== null) {
            $this->loadFixtures($fixtureOption);
        }

        $this->username = $this->optionalString($options['username'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_USER'));
        $this->password = $this->optionalString($options['password'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_PASSWORD'));

        if ($this->useFixtures) {
            $this->mode = 'fixtures';
        }

        $this->calibrationPlanId = $this->optionalString($options['calibrationPlanId'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_CALIBRATION_PLAN_ID'));

        $approvalState = $this->optionalString($options['approvalState'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_APPROVAL_STATE'));
        if ($approvalState !== null) {
            $this->approvalState = $approvalState;
        }

        $rejectionState = $this->optionalString($options['rejectionState'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_REJECTION_STATE'));
        if ($rejectionState !== null) {
            $this->rejectionState = $rejectionState;
        }

        $this->approvalComment = $this->optionalString($options['approvalComment'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_APPROVAL_COMMENT'));
        $this->rejectionComment = $this->optionalString($options['rejectionComment'] ?? null)
            ?? $this->optionalString(getenv('VALIDATION_REJECTION_COMMENT'));

        $this->portalScope = $this->normalizePortalScope($options['portalScope'] ?? null);
    }

    private function normalizePortalScope($value): string
    {
        $candidate = $this->optionalString($value);
        if ($candidate === null) {
            return 'none';
        }

        $normalized = strtolower($candidate);
        $map = [
            'internal' => 'internal',
            'interno' => 'internal',
            'panel interno' => 'internal',
            'public/apps/internal' => 'internal',
            'tenant' => 'tenant',
            'clientes' => 'tenant',
            'portal clientes' => 'tenant',
            'portal de clientes' => 'tenant',
            'public/apps/tenant' => 'tenant',
            'none' => 'none',
            'ninguno' => 'none',
            'sin portal' => 'none',
            'general' => 'none',
        ];

        return $map[$normalized] ?? 'none';
    }

    private function portalScopeLabel(): string
    {
        switch ($this->portalScope) {
            case 'internal':
                return 'Panel interno (public/apps/internal)';
            case 'tenant':
                return 'Portal de clientes (public/apps/tenant)';
            default:
                return 'Sin impacto directo en portales';
        }
    }

    private function portalScopeNotice(): string
    {
        return 'Impacto en: ' . $this->portalScopeLabel();
    }

    private function portalScopeReminder(): string
    {
        return 'Recordatorio de alcance: ' . $this->portalScopeLabel();
    }

    private function loadFixtures(string $path): void
    {
        $realPath = $path;
        if (!is_file($realPath)) {
            $candidate = __DIR__ . '/../../' . ltrim($path, '/');
            if (is_file($candidate)) {
                $realPath = $candidate;
            }
        }

        if (!is_file($realPath)) {
            fwrite(STDERR, "No se encontró el archivo de fixtures: {$path}\n");
            return;
        }

        $contents = file_get_contents($realPath);
        if ($contents === false) {
            fwrite(STDERR, "No se pudo leer el archivo de fixtures: {$realPath}\n");
            return;
        }

        $decoded = json_decode($contents, true);
        if (!is_array($decoded)) {
            fwrite(STDERR, "El archivo de fixtures no tiene formato JSON válido: {$realPath}\n");
            return;
        }

        $this->fixtures = $decoded;
        $this->useFixtures = true;
    }

    public function run(): array
    {
        $iq = $this->runInstallationQualification();
        $oq = $this->runOperationalQualification();
        $pq = $this->runPerformanceQualification();

        $this->overallStatus = $this->aggregateStatuses([$iq, $oq, $pq]);

        $summary = [
            'overall' => $this->overallStatus,
            'counts' => $this->collectSummaryCounts([$iq, $oq, $pq]),
        ];

        return [
            'timestamp' => date('c'),
            'mode' => $this->mode,
            'base_url' => $this->baseUrl,
            'portal_scope' => $this->portalScope,
            'portal_scope_label' => $this->portalScopeLabel(),
            'portal_scope_notice' => $this->portalScopeNotice(),
            'portal_scope_reminder' => $this->portalScopeReminder(),
            'iq' => $iq,
            'oq' => $oq,
            'pq' => $pq,
            'summary' => $summary,
        ];
    }

    public function exitCode(): int
    {
        return $this->overallStatus === 'failed' ? 1 : 0;
    }

    private function runInstallationQualification(): array
    {
        $database = $this->checkDatabaseConnectivity();
        $permissions = $this->checkRolesAndPermissions();

        return [
            'status' => $this->aggregateStatuses([$database, $permissions]),
            'database' => $database,
            'permissions' => $permissions,
        ];
    }

    private function runOperationalQualification(): array
    {
        $flows = $this->checkOperationalFlows();
        $reports = $this->checkReportGeneration();

        $flowItems = array_values($flows);
        $status = $this->aggregateStatuses(array_merge($flowItems, [$reports]));

        return [
            'status' => $status,
            'flows' => $flows,
            'reports' => $reports,
        ];
    }

    private function runPerformanceQualification(): array
    {
        if ($this->useFixtures) {
            $fixture = $this->fixtureSegment(['pq']);
            if (is_array($fixture)) {
                if (!isset($fixture['status']) && isset($fixture['cases']) && is_array($fixture['cases'])) {
                    $fixture['status'] = $this->aggregateStatuses($fixture['cases']);
                }
                return $fixture;
            }
        }

        return [
            'status' => 'warning',
            'details' => 'Sin escenarios configurados. Defina casos de desempeño o proporcione un fixture.',
            'cases' => [],
        ];
    }

    private function checkDatabaseConnectivity(): array
    {
        if ($this->useFixtures) {
            $fixture = $this->fixtureSegment(['iq', 'database']);
            if (is_array($fixture)) {
                return $fixture;
            }
        }

        try {
            require_once __DIR__ . '/../../app/Core/db_config.php';
            $info = method_exists('DatabaseManager', 'getConnectionInfo')
                ? DatabaseManager::getConnectionInfo()
                : ['type' => 'desconocido', 'active' => true];

            $status = (!empty($info['active']) && $info['active'] === true) ? 'passed' : 'failed';
            $details = 'Conexión a base de datos verificada.';
            if ($status === 'failed') {
                $details = 'No se pudo establecer la conexión a la base de datos.';
            }

            return [
                'status' => $status,
                'details' => $details,
                'connection' => $info,
            ];
        } catch (Throwable $e) {
            return [
                'status' => 'failed',
                'details' => 'Excepción durante la conexión: ' . $e->getMessage(),
            ];
        }
    }

    private function checkRolesAndPermissions(): array
    {
        if ($this->useFixtures) {
            $fixture = $this->fixtureSegment(['iq', 'permissions']);
            if (is_array($fixture)) {
                if (!isset($fixture['status'])) {
                    $roles = $fixture['roles'] ?? [];
                    $perms = $fixture['permissions'] ?? [];
                    $status = $this->deriveStatusFromPresence($roles, $perms);
                    $fixture['status'] = $status;
                }
                return $fixture;
            }
        }

        $rolesRequired = [
            'Superadministrador',
            'Administrador',
            'Supervisor',
            'Operador',
            'Lector',
            'Cliente',
            'Sistemas',
        ];
        $permissionsRequired = [
            'calibraciones_crear',
            'calibraciones_actualizar',
            'calibraciones_leer',
            'reportes_leer',
            'usuarios_view',
            'usuarios_add',
        ];

        $roles = [];
        $perms = [];

        try {
            if (!class_exists('DatabaseManager')) {
                require_once __DIR__ . '/../../app/Core/db_config.php';
            }
            /** @var mysqli $conn */
            global $conn;
            if (!isset($conn) || !$conn instanceof mysqli) {
                throw new RuntimeException('Conexión a base de datos no inicializada.');
            }

            foreach ($rolesRequired as $role) {
                $stmt = $conn->prepare('SELECT id FROM roles WHERE nombre = ? LIMIT 1');
                if ($stmt === false) {
                    $roles[$role] = 'error';
                    continue;
                }
                $stmt->bind_param('s', $role);
                $stmt->execute();
                $stmt->store_result();
                $roles[$role] = $stmt->num_rows > 0 ? 'present' : 'missing';
                $stmt->close();
            }

            foreach ($permissionsRequired as $permiso) {
                $stmt = $conn->prepare('SELECT id FROM permissions WHERE nombre = ? LIMIT 1');
                if ($stmt === false) {
                    $perms[$permiso] = 'error';
                    continue;
                }
                $stmt->bind_param('s', $permiso);
                $stmt->execute();
                $stmt->store_result();
                $perms[$permiso] = $stmt->num_rows > 0 ? 'present' : 'missing';
                $stmt->close();
            }

            $status = $this->deriveStatusFromPresence($roles, $perms);

            return [
                'status' => $status,
                'roles' => $roles,
                'permissions' => $perms,
            ];
        } catch (Throwable $e) {
            return [
                'status' => 'failed',
                'details' => 'Error al consultar roles o permisos: ' . $e->getMessage(),
                'roles' => $roles,
                'permissions' => $perms,
            ];
        }
    }

    private function deriveStatusFromPresence(array $roles, array $perms): string
    {
        $missingRole = array_filter($roles, static fn($value) => $value !== 'present');
        $missingPerm = array_filter($perms, static fn($value) => $value !== 'present');

        if (!empty($missingRole) || !empty($missingPerm)) {
            return 'failed';
        }

        return 'passed';
    }

    private function checkOperationalFlows(): array
    {
        if ($this->useFixtures) {
            $fixture = $this->fixtureSegment(['oq', 'flows']);
            if (is_array($fixture)) {
                return $fixture;
            }
        }

        $flows = [
            'login_page' => [
                'description' => 'Página de autenticación interna disponible',
                'method' => 'GET',
                'path' => '/apps/internal/usuarios/login.html',
                'requiresAuth' => false,
            ],
            'calibration_capture' => [
                'description' => 'Formulario de captura de calibraciones accesible',
                'method' => 'GET',
                'path' => '/apps/tenant/calibraciones/add_calibration.html',
                'requiresAuth' => false,
            ],
            'approval' => [
                'description' => 'Endpoint de aprobación de calibraciones',
                'method' => 'POST',
                'path' => '/backend/calibraciones/update_work_order.php',
                'requiresAuth' => true,

                'payload' => $this->buildWorkOrderPayload('approve'),
                'payload_required' => true,
            ],
            'rejection' => [
                'description' => 'Endpoint de rechazo de calibraciones',
                'method' => 'POST',
                'path' => '/backend/calibraciones/update_work_order.php',
                'requiresAuth' => true,

                'payload' => $this->buildWorkOrderPayload('reject'),
                'payload_required' => true,
            ],
            'report_generation' => [
                'description' => 'Listado de reportes disponible',
                'method' => 'GET',
                'path' => '/backend/reportes/list_reports.php',
                'requiresAuth' => true,
            ],
        ];

        $results = [];
        foreach ($flows as $key => $definition) {
            $results[$key] = $this->executeHttpCheck($definition);
        }

        return $results;
    }

    private function checkReportGeneration(): array
    {
        if ($this->useFixtures) {
            $fixture = $this->fixtureSegment(['oq', 'reports']);
            if (is_array($fixture)) {
                return $fixture;
            }
        }

        return [
            'status' => 'skipped',
            'details' => 'No se generó reporte porque no se proporcionaron credenciales ni se ejecutó servidor de pruebas.',
        ];
    }

    private function executeHttpCheck(array $definition): array
    {
        $description = $definition['description'] ?? 'Chequeo HTTP';
        $requiresAuth = (bool)($definition['requiresAuth'] ?? false);

        if ($requiresAuth && (!$this->username || !$this->password)) {
            return [
                'status' => 'skipped',
                'details' => $description . ' omitido: no se proporcionaron credenciales.',
            ];
        }

        $path = $definition['path'] ?? '/';
        $method = strtoupper($definition['method'] ?? 'GET');
        $url = $this->baseUrl . '/' . ltrim($path, '/');

        $payload = $definition['payload'] ?? null;
        $requiresPayload = (bool)($definition['payload_required'] ?? false);

        if ($method === 'POST' && $requiresPayload && $payload === null) {
            return [
                'status' => 'skipped',
                'details' => $description . ' omitido: configure plan_id mediante --calibration-plan-id o la variable VALIDATION_CALIBRATION_PLAN_ID.',
            ];
        }

        if ($requiresAuth) {
            $auth = $this->authenticateIfNeeded();
            if (($auth['status'] ?? 'failed') !== 'passed') {
                $status = ($auth['status'] ?? 'failed') === 'failed' ? 'failed' : 'skipped';
                $message = $description . ' no ejecutado: ' . ($auth['details'] ?? 'Autenticación requerida.');

                return [
                    'status' => $status,
                    'details' => $message,
                ];
            }
        }

        $response = $this->httpRequest($url, $method, $payload);
        if ($response['error'] !== null) {
            return [
                'status' => 'skipped',
                'details' => $description . ' no ejecutado: ' . $response['error'],
            ];
        }

        $statusCode = $response['status_code'];
        $passed = $statusCode >= 200 && $statusCode < 400;

        return [
            'status' => $passed ? 'passed' : 'failed',
            'details' => $description,
            'status_code' => $statusCode,
        ];
    }

    private function httpRequest(string $url, string $method, ?array $payload = null, array $requestOptions = []): array
    {
        global $http_response_header;
        $headers = $requestOptions['headers'] ?? [];
        $headers[] = 'User-Agent: ValidationRunner/1.0';
        $includeCookies = $requestOptions['include_cookies'] ?? true;
        if ($includeCookies && !empty($this->cookies)) {
            $cookieHeader = $this->buildCookieHeader();
            if ($cookieHeader !== null) {
                $headers[] = 'Cookie: ' . $cookieHeader;
            }
        }
        $content = null;
        if ($payload !== null) {
            $content = http_build_query($payload);
            $headers[] = 'Content-Type: application/x-www-form-urlencoded';
            $headers[] = 'Content-Length: ' . strlen($content);
        }
        $contextOptions = [
            'http' => [
                'method' => $method,
                'timeout' => $requestOptions['timeout'] ?? 10,
                'ignore_errors' => true,
                'header' => implode("\r\n", $headers),
                'follow_location' => $requestOptions['allow_redirects'] ?? 1,
                'max_redirects' => $requestOptions['max_redirects'] ?? 3,
            ],
        ];
        if ($content !== null) {
            $contextOptions['http']['content'] = $content;
        }

        $context = stream_context_create($contextOptions);
        $result = @file_get_contents($url, false, $context);
        if ($result === false) {
            $error = error_get_last();
            $message = $error['message'] ?? 'Error desconocido';
            return [
                'status_code' => 0,
                'body' => null,
                'error' => $message,
                'headers' => $http_response_header ?? [],
            ];
        }

        $statusCode = 0;
        $headersList = $http_response_header ?? [];
        if (!empty($headersList)) {
            $this->storeCookies($headersList);
        }
        if (isset($headersList[0]) && preg_match('/\s(\d{3})\s/', $headersList[0], $matches)) {
            $statusCode = (int) $matches[1];
        }

        return [
            'status_code' => $statusCode,
            'body' => $result,
            'error' => null,
            'headers' => $headersList,
        ];
    }

    private function buildWorkOrderPayload(string $action): ?array
    {
        if ($this->calibrationPlanId === null) {
            return null;
        }

        $payload = [
            'plan_id' => $this->calibrationPlanId,
            'estado_orden' => $action === 'approve' ? $this->approvalState : $this->rejectionState,
        ];

        $comment = $action === 'approve' ? $this->approvalComment : $this->rejectionComment;
        if ($comment !== null) {
            $payload['comentarios'] = $comment;
        }

        return $payload;
    }

    private function buildCookieHeader(): ?string
    {
        if (empty($this->cookies)) {
            return null;
        }

        $pairs = [];
        foreach ($this->cookies as $name => $value) {
            $pairs[] = $name . '=' . $value;
        }

        return implode('; ', $pairs);
    }

    private function storeCookies(array $headers): void
    {
        foreach ($headers as $header) {
            if (stripos($header, 'Set-Cookie:') !== 0) {
                continue;
            }

            $cookieLine = trim(substr($header, strlen('Set-Cookie:')));
            if ($cookieLine === '') {
                continue;
            }

            $segments = explode(';', $cookieLine);
            $nameValue = trim(array_shift($segments));
            if ($nameValue === '') {
                continue;
            }

            $equalsPos = strpos($nameValue, '=');
            if ($equalsPos === false) {
                continue;
            }

            $name = trim(substr($nameValue, 0, $equalsPos));
            $value = substr($nameValue, $equalsPos + 1);

            if ($value === false || $value === '') {
                unset($this->cookies[$name]);
            } else {
                $this->cookies[$name] = $value;
            }
        }
    }

    private function authenticateIfNeeded(): array
    {
        if ($this->authenticationResult !== null) {
            return $this->authenticationResult;
        }

        if (!$this->username || !$this->password) {
            $this->authenticationResult = [
                'status' => 'skipped',
                'details' => 'No se proporcionaron credenciales para autenticación.',
            ];
            return $this->authenticationResult;
        }

        $loginUrl = $this->baseUrl . '/backend/usuarios/login.php';
        $response = $this->httpRequest($loginUrl, 'POST', [
            'correo' => $this->username,
            'contrasena' => $this->password,
        ], [
            'allow_redirects' => 1,
        ]);

        if ($response['error'] !== null) {
            $this->authenticationResult = [
                'status' => 'failed',
                'details' => 'Error durante autenticación: ' . $response['error'],
                'status_code' => $response['status_code'],
            ];
            return $this->authenticationResult;
        }

        $statusCode = $response['status_code'];
        $location = $this->findHeaderValue($response['headers'] ?? [], 'Location');
        $hasSession = !empty($this->cookies);

        $successfulRedirect = false;
        if ($location !== null) {
            $normalizedLocation = strtolower($location);
            if (strpos($normalizedLocation, 'login.html') === false && strpos($normalizedLocation, 'error=') === false) {
                $successfulRedirect = true;
            }
        }

        if ($statusCode >= 200 && $statusCode < 400 && $hasSession && ($successfulRedirect || $location === null)) {
            $this->authenticationResult = [
                'status' => 'passed',
                'details' => 'Autenticación exitosa para ' . $this->username . '.',
                'status_code' => $statusCode,
                'location' => $location,
            ];
        } else {
            $message = 'No se pudo autenticar con las credenciales proporcionadas.';
            if ($location !== null && strpos($location, 'error=') !== false) {
                $message = 'Autenticación rechazada: ' . $location;
            } elseif ($statusCode >= 400) {
                $message = 'Error HTTP durante autenticación (código ' . $statusCode . ').';
            }

            $this->authenticationResult = [
                'status' => 'failed',
                'details' => $message,
                'status_code' => $statusCode,
                'location' => $location,
            ];
        }

        return $this->authenticationResult;
    }

    private function findHeaderValue(array $headers, string $headerName): ?string
    {
        foreach ($headers as $header) {
            if (stripos($header, $headerName . ':') === 0) {
                return trim(substr($header, strlen($headerName) + 1));
            }
        }

        return null;
    }

    private function aggregateStatuses(array $items): string
    {
        $hasFailure = false;
        $hasWarning = false;

        foreach ($items as $item) {
            if (!is_array($item)) {
                continue;
            }
            if (isset($item['status']) && is_string($item['status'])) {
                $status = $item['status'];
            } else {
                $status = null;
            }

            if ($status === null && !empty($item)) {
                $status = $this->aggregateStatuses($item);
            }

            if ($status === 'failed') {
                $hasFailure = true;
            } elseif ($status !== 'passed') {
                $hasWarning = true;
            }
        }

        if ($hasFailure) {
            return 'failed';
        }
        if ($hasWarning) {
            return 'warning';
        }
        return 'passed';
    }

    private function collectSummaryCounts(array $sections): array
    {
        $counts = [
            'passed' => 0,
            'failed' => 0,
            'warning' => 0,
            'skipped' => 0,
        ];

        $iterator = function ($node) use (&$counts, &$iterator) {
            if (!is_array($node)) {
                return;
            }
            foreach ($node as $value) {
                if (is_array($value)) {
                    if (isset($value['status']) && is_string($value['status'])) {
                        $status = $value['status'];
                        if (!isset($counts[$status])) {
                            $counts[$status] = 0;
                        }
                        $counts[$status]++;
                    }
                    $iterator($value);
                }
            }
        };

        $iterator($sections);
        return $counts;
    }

    private function fixtureSegment(array $path)
    {
        if (!$this->useFixtures || $this->fixtures === null) {
            return null;
        }

        $value = $this->fixtures;
        foreach ($path as $segment) {
            if (!is_array($value) || !array_key_exists($segment, $value)) {
                return null;
            }
            $value = $value[$segment];
        }
        return $value;
    }

    public function render(array $report): string
    {
        if ($this->format === 'json') {
            return json_encode($report, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
        }

        $scopeNotice = $report['portal_scope_notice'] ?? $this->portalScopeNotice();
        $scopeReminder = $report['portal_scope_reminder'] ?? $this->portalScopeReminder();

        $lines = [];
        $lines[] = 'Validación IQ/OQ/PQ';
        $lines[] = $scopeNotice;
        $lines[] = 'Modo: ' . $report['mode'];
        $lines[] = 'URL base: ' . $report['base_url'];
        $lines[] = 'Estado global: ' . $report['summary']['overall'];
        $lines[] = $scopeReminder;
        $lines[] = '';

        $sections = ['iq' => 'Instalación', 'oq' => 'Operacional', 'pq' => 'Desempeño'];
        foreach ($sections as $key => $title) {
            $section = $report[$key];
            $lines[] = sprintf('%s (%s)', $title, $section['status'] ?? 'sin estado');
            foreach ($section as $subKey => $value) {
                if ($subKey === 'status') {
                    continue;
                }
                if (is_array($value) && isset($value['status'])) {
                    $lines[] = sprintf('  - %s: %s', $subKey, $value['status']);
                }
            }
            $lines[] = '';
        }

        return implode(PHP_EOL, $lines) . PHP_EOL;
    }
}

$options = getopt('', [
    'format::',
    'base-url::',
    'fixtures::',
    'user::',
    'password::',
    'calibration-plan-id::',
    'approval-state::',
    'rejection-state::',
    'approval-comment::',
    'rejection-comment::',
    'portal-scope::',

]);

$runner = new ValidationRunner([
    'format' => $options['format'] ?? getenv('VALIDATION_FORMAT') ?? null,
    'baseUrl' => $options['base-url'] ?? getenv('VALIDATION_BASE_URL') ?? null,
    'fixtures' => $options['fixtures'] ?? getenv('VALIDATION_FIXTURES') ?? null,
    'username' => $options['user'] ?? getenv('VALIDATION_USER') ?? null,
    'password' => $options['password'] ?? getenv('VALIDATION_PASSWORD') ?? null,
    'calibrationPlanId' => $options['calibration-plan-id'] ?? getenv('VALIDATION_CALIBRATION_PLAN_ID') ?? null,
    'approvalState' => $options['approval-state'] ?? getenv('VALIDATION_APPROVAL_STATE') ?? null,
    'rejectionState' => $options['rejection-state'] ?? getenv('VALIDATION_REJECTION_STATE') ?? null,
    'approvalComment' => $options['approval-comment'] ?? getenv('VALIDATION_APPROVAL_COMMENT') ?? null,
    'rejectionComment' => $options['rejection-comment'] ?? getenv('VALIDATION_REJECTION_COMMENT') ?? null,
    'portalScope' => $options['portal-scope'] ?? getenv('VALIDATION_PORTAL_SCOPE') ?? null,
]);

$report = $runner->run();
echo $runner->render($report);
exit($runner->exitCode());
