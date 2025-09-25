<?php

declare(strict_types=1);
require_once __DIR__ . '/../helpers/tenant_notifications.php';

/**
 * Servicio para despachar alertas de calibración a múltiples canales configurables.
 */
final class AlertDispatcher
{
    private mysqli $conn;

    /** @var callable */
    private $logger;

    /**
     * @var array<string,callable>
     */
    private array $connectors;

    /** @var callable */
    private $channelResolver;

    /**
     * @param array<string,callable> $connectors
     */
    public function __construct(mysqli $conn, ?callable $logger = null, array $connectors = [], ?callable $channelResolver = null)
    {
        $this->conn = $conn;
        $this->logger = $logger ?? static function (string $level, string $message): void {
            error_log('[alert-dispatcher][' . strtoupper($level) . '] ' . $message);
        };

        $this->connectors = $connectors ?: $this->buildDefaultConnectors();

        if ($channelResolver !== null) {
            $this->channelResolver = $channelResolver;
            return;
        }

        $this->ensureChannelsTable();
        $this->channelResolver = function (int $empresaId): array {
            return $this->fetchCompanyChannels($empresaId);
        };
    }

    /**
     * @param array<string,mixed> $message
     *
     * @return array<string,array<string,mixed>>
     */
    public function dispatch(int $empresaId, array $message): array
    {
        $channels = ($this->channelResolver)($empresaId);
        if (!$channels) {
            $channels = [];
        }

        if (!isset($channels['email'])) {
            $channels['email'] = [
                'enabled' => true,
                'config' => [],
            ];
        }

        $results = [];
        foreach ($channels as $name => $channel) {
            $enabled = (bool) ($channel['enabled'] ?? false);
            if (!$enabled) {
                $results[$name] = [
                    'status' => 'skipped',
                    'reason' => 'disabled',
                ];
                continue;
            }

            if (!isset($this->connectors[$name])) {
                $this->log('warning', 'No existe conector para el canal ' . $name . ', se omite.');
                $results[$name] = [
                    'status' => 'failed',
                    'error' => 'Connector not available',
                ];
                continue;
            }

            $connector = $this->connectors[$name];

            try {
                $summary = $connector(
                    is_array($channel['config'] ?? null) ? $channel['config'] : [],
                    $message + ['empresa_id' => $empresaId],
                    $this->logger
                );

                $channelResult = [
                    'status' => 'sent',
                    'summary' => $summary,
                ];

                if (is_array($summary) && isset($summary['status'])) {
                    $channelResult['status'] = (string) $summary['status'];
                    if (isset($summary['error']) && !isset($channelResult['error'])) {
                        $channelResult['error'] = (string) $summary['error'];
                    }
                } elseif ($name === 'email' && is_array($summary) && array_key_exists('sent', $summary)) {
                    $sentCount = (int) $summary['sent'];
                    if ($sentCount <= 0) {
                        $errorMessage = 'El conector de correo no envió mensajes (0 enviados).';
                        if (!empty($summary['failures']) && is_array($summary['failures'])) {
                            $errorMessage .= ' Destinatarios con error: ' . count($summary['failures']) . '.';
                        }

                        $channelResult['status'] = 'failed';
                        $channelResult['error'] = $errorMessage;
                    }
                }

                $results[$name] = $channelResult;
            } catch (Throwable $exception) {
                $this->log('error', 'Fallo al enviar por el canal ' . $name . ': ' . $exception->getMessage());
                $results[$name] = [
                    'status' => 'failed',
                    'error' => $exception->getMessage(),
                ];
            }
        }

        return $results;
    }

    private function log(string $level, string $message): void
    {
        ($this->logger)($level, $message);
    }

    /**
     * @return array<string,callable>
     */
    private function buildDefaultConnectors(): array
    {
        return [
            'email' => function (array $config, array $message, callable $logger): array {
                unset($config); // Configuración reservada para futuras extensiones.

                $recipients = $message['recipients'] ?? [];
                $subject = (string) ($message['subject'] ?? '');
                $htmlBody = (string) ($message['html_body'] ?? '');
                $textBody = $message['text_body'] ?? null;

                return tenant_notifications_send_bulk_mail($recipients, $subject, $htmlBody, $textBody, $logger);
            },
            'slack' => function (array $config, array $message, callable $logger): array {
                $webhookUrl = trim((string) ($config['webhook_url'] ?? ''));
                if ($webhookUrl === '') {
                    $empresaId = (int) ($message['empresa_id'] ?? 0);
                    $override = getenv('TENANT_' . $empresaId . '_SLACK_WEBHOOK');
                    if (is_string($override) && trim($override) !== '') {
                        $webhookUrl = trim($override);
                    }
                }

                if ($webhookUrl === '') {
                    $fallback = getenv('SLACK_WEBHOOK_URL');
                    if (is_string($fallback) && trim($fallback) !== '') {
                        $webhookUrl = trim($fallback);
                    }
                }

                if ($webhookUrl === '') {
                    throw new RuntimeException('No se configuró el webhook de Slack para la empresa.');
                }

                $textBody = (string) ($message['text_body'] ?? strip_tags((string) ($message['html_body'] ?? '')));
                $payload = json_encode([
                    'text' => $textBody !== '' ? $textBody : 'Alerta de calibraciones próxima a vencer.',
                ], JSON_THROW_ON_ERROR);

                $context = stream_context_create([
                    'http' => [
                        'method' => 'POST',
                        'header' => "Content-Type: application/json\r\n",
                        'content' => $payload,
                        'timeout' => 10,
                    ],
                ]);

                $result = @file_get_contents($webhookUrl, false, $context);
                if ($result === false) {
                    $error = error_get_last();
                    $messageError = $error['message'] ?? 'Error desconocido al contactar el webhook.';
                    throw new RuntimeException($messageError);
                }

                $logger('info', 'Webhook de Slack invocado para la empresa ' . ($message['empresa_id'] ?? 'desconocida'));

                return [
                    'delivered' => true,
                ];
            },
        ];
    }

    private function ensureChannelsTable(): void
    {
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS tenant_notification_channels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    channel VARCHAR(50) NOT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    settings_json TEXT NULL,
    UNIQUE KEY uniq_company_channel (empresa_id, channel),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
SQL;

        if (!$this->conn->query($sql)) {
            $this->log('warning', 'No se pudo asegurar la tabla de canales de notificación: ' . $this->conn->error);
        }
    }

    /**
     * @return array<string,array<string,mixed>>
     */
    private function fetchCompanyChannels(int $empresaId): array
    {
        $stmt = $this->conn->prepare('SELECT channel, enabled, settings_json FROM tenant_notification_channels WHERE empresa_id = ?');
        if (!$stmt) {
            $this->log('warning', 'No se pudo preparar la lectura de canales: ' . $this->conn->error);
            return [];
        }

        if (!$stmt->bind_param('i', $empresaId)) {
            $this->log('warning', 'No se pudieron enlazar parámetros de canales para la empresa ' . $empresaId . ': ' . $stmt->error);
            $stmt->close();
            return [];
        }

        if (!$stmt->execute()) {
            $this->log('warning', 'No se pudieron obtener los canales para la empresa ' . $empresaId . ': ' . $stmt->error);
            $stmt->close();
            return [];
        }

        $result = $stmt->get_result();
        if ($result === false) {
            $this->log('warning', 'No se pudieron leer los resultados de canales para la empresa ' . $empresaId . ': ' . $stmt->error);
            $stmt->close();
            return [];
        }

        $channels = [];
        while ($row = $result->fetch_assoc()) {
            $settings = [];
            $json = $row['settings_json'] ?? null;
            if (is_string($json) && trim($json) !== '') {
                try {
                    /** @var array<string,mixed> $decoded */
                    $decoded = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
                    $settings = $decoded;
                } catch (Throwable $decodeError) {
                    $this->log('warning', 'Configuración inválida para el canal ' . $row['channel'] . ': ' . $decodeError->getMessage());
                }
            }

            $channels[$row['channel']] = [
                'enabled' => (bool) ($row['enabled'] ?? false),
                'config' => $settings,
            ];
        }

        $result->free();
        $stmt->close();

        return $channels;
    }
}
