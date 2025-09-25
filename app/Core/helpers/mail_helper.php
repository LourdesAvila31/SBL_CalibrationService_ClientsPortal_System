<?php
use PHPMailer\PHPMailer\Exception as PHPMailerException;
use PHPMailer\PHPMailer\PHPMailer;

// Intentar cargar el autoloader de Composer si existe
if (!class_exists(PHPMailer::class)) {
    $autoloadCandidates = [
        __DIR__ . '/../vendor/autoload.php',
        __DIR__ . '/../../vendor/autoload.php',
    ];
    foreach ($autoloadCandidates as $autoload) {
        if (file_exists($autoload)) {
            require_once $autoload;
            break;
        }
    }
}

if (!function_exists('mail_helper_env')) {
    /**
     * Obtiene una variable de entorno considerando diferentes fuentes.
     */
    function mail_helper_env(string $key, $default = null, bool $allowEmpty = false)
    {
        $sources = [
            getenv($key),
            $_ENV[$key] ?? null,
            $_SERVER[$key] ?? null,
        ];

        foreach ($sources as $value) {
            if ($value === false || $value === null) {
                continue;
            }
            if ($value === '' && !$allowEmpty) {
                continue;
            }
            return $value;
        }

        return $allowEmpty ? '' : $default;
    }
}

if (!function_exists('mail_helper_base_url')) {
    /**
     * Devuelve la URL pública configurada para el sistema.
     */
    function mail_helper_base_url(): string
    {
        $candidate = mail_helper_env('APP_PUBLIC_URL')
            ?? mail_helper_env('PUBLIC_APP_URL')
            ?? mail_helper_env('PUBLIC_URL')
            ?? 'http://localhost:8000';

        if (!is_string($candidate) || $candidate === '') {
            $candidate = 'http://localhost:8000';
        }

        return rtrim($candidate, '/');
    }
}

if (!function_exists('mail_helper_send')) {
    /**
     * Envía un correo electrónico utilizando PHPMailer cuando está disponible y
     * hace fallback a la función mail() en entornos sin dependencias.
     *
     * @throws RuntimeException cuando el envío falla.
     */
    function mail_helper_send(string $toEmail, string $toName, string $subject, string $htmlBody, ?string $textBody = null): void
    {
        $fromEmail = (string) mail_helper_env('SMTP_FROM_EMAIL', 'no-reply@localhost');
        $fromName  = (string) mail_helper_env('SMTP_FROM_NAME', 'Sistema ISO 17025');

        if (class_exists(PHPMailer::class)) {
            $mailer = new PHPMailer(true);
            try {
                $mailer->CharSet = 'UTF-8';
                $mailer->setFrom($fromEmail, $fromName);
                $mailer->addAddress($toEmail, $toName ?: $toEmail);
                $mailer->isHTML(true);
                $mailer->Subject = $subject;
                $mailer->Body    = $htmlBody;
                $mailer->AltBody = $textBody ?? strip_tags($htmlBody);

                $mailer->isSMTP();
                $mailer->Host = (string) mail_helper_env('SMTP_HOST', 'localhost');
                $mailer->Port = (int) (mail_helper_env('SMTP_PORT', 587) ?? 587);

                $authOverride = mail_helper_env('SMTP_AUTH');
                $username = mail_helper_env('SMTP_USER', '', true);
                $password = mail_helper_env('SMTP_PASS', '', true);

                if ($authOverride !== null) {
                    $authValue = filter_var($authOverride, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
                    $mailer->SMTPAuth = $authValue ?? ($username !== '' || $password !== '');
                } else {
                    $mailer->SMTPAuth = ($username !== '' || $password !== '');
                }

                if ($mailer->SMTPAuth) {
                    $mailer->Username = (string) $username;
                    $mailer->Password = (string) $password;
                }

                $encryption = strtolower((string) (mail_helper_env('SMTP_ENCRYPTION', 'tls') ?? ''));
                if (in_array($encryption, ['tls', 'starttls'], true)) {
                    $mailer->SMTPSecure = PHPMailer::ENCRYPTION_STARTTLS;
                } elseif ($encryption === 'ssl') {
                    $mailer->SMTPSecure = PHPMailer::ENCRYPTION_SMTPS;
                }

                $mailer->send();
                return;
            } catch (PHPMailerException $e) {
                throw new RuntimeException('No se pudo enviar el correo: ' . $e->getMessage(), 0, $e);
            }
        }

        // Fallback básico usando mail()
        $headers = [];
        $headers[] = 'MIME-Version: 1.0';
        $headers[] = 'Content-type: text/html; charset=UTF-8';
        $headers[] = 'From: ' . mb_encode_mimeheader($fromName) . " <{$fromEmail}>";
        $headers[] = 'Reply-To: ' . $fromEmail;
        $headers[] = 'X-Mailer: PHP/' . phpversion();

        $success = mail($toEmail, $subject, $htmlBody, implode("\r\n", $headers));
        if (!$success) {
            throw new RuntimeException('No se pudo enviar el correo de restablecimiento.');
        }
    }
}

if (!function_exists('send_password_reset_email')) {
    /**
     * Envía el correo con el enlace firmado para restablecer la contraseña.
     */
    function send_password_reset_email(string $email, string $nombre, string $token): void
    {
        $baseUrl = mail_helper_base_url();
        $resetUrl = $baseUrl . '/apps/internal/usuarios/reset_password.html?token=' . urlencode($token);

        $subject = 'Restablecimiento de contraseña - Sistema ISO 17025';
        $htmlBody = <<<HTML
            <p>Hola {$nombre},</p>
            <p>Recibimos una solicitud para restablecer tu contraseña del Sistema ISO 17025.</p>
            <p>Puedes crear una nueva contraseña haciendo clic en el siguiente enlace:</p>
            <p><a href="{$resetUrl}">Restablecer contraseña</a></p>
            <p>Este enlace expirará en una hora y solo se puede usar una vez.</p>
            <p>Si tú no solicitaste este cambio, puedes ignorar este mensaje.</p>
            <p>Saludos,<br>Equipo de Soporte</p>
        HTML;

        $textBody = "Hola {$nombre},\n\n" .
            "Recibimos una solicitud para restablecer tu contraseña del Sistema ISO 17025. " .
            "Utiliza el siguiente enlace para continuar (vigente por una hora):\n{$resetUrl}\n\n" .
            "Si no solicitaste este cambio, ignora este mensaje.";

        mail_helper_send($email, $nombre, $subject, $htmlBody, $textBody);
    }
}
