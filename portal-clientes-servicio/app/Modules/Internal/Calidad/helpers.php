<?php

declare(strict_types=1);

require_once dirname(__DIR__, 3) . '/Core/db.php';
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';

/**
 * Garantiza que las tablas necesarias para el módulo de calidad existan.
 */
function calidadEnsureTables(mysqli $conn): bool
{
    $queries = [
        "CREATE TABLE IF NOT EXISTS calidad_documentos (" .
        "    id INT AUTO_INCREMENT PRIMARY KEY," .
        "    empresa_id INT NOT NULL," .
        "    titulo VARCHAR(255) NOT NULL," .
        "    descripcion TEXT NULL," .
        "    contenido LONGTEXT NULL," .
        "    estado ENUM('borrador','publicado') NOT NULL DEFAULT 'borrador'," .
        "    publicado_en DATETIME NULL," .
        "    publicado_por INT NULL," .
        "    creado_por INT NULL," .
        "    responsable VARCHAR(150) NULL," .
        "    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," .
        "    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," .
        "    INDEX idx_calidad_documentos_empresa_estado (empresa_id, estado)," .
        "    CONSTRAINT fk_calidad_documentos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE" .
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "CREATE TABLE IF NOT EXISTS calidad_capacitaciones (" .
        "    id INT AUTO_INCREMENT PRIMARY KEY," .
        "    empresa_id INT NOT NULL," .
        "    tema VARCHAR(255) NOT NULL," .
        "    descripcion TEXT NULL," .
        "    fecha_programada DATE NULL," .
        "    responsable VARCHAR(150) NULL," .
        "    estado ENUM('borrador','publicado') NOT NULL DEFAULT 'borrador'," .
        "    publicado_en DATETIME NULL," .
        "    publicado_por INT NULL," .
        "    creado_por INT NULL," .
        "    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," .
        "    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," .
        "    INDEX idx_calidad_capacitaciones_empresa_estado (empresa_id, estado)," .
        "    CONSTRAINT fk_calidad_capacitaciones_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE" .
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "CREATE TABLE IF NOT EXISTS calidad_no_conformidades (" .
        "    id INT AUTO_INCREMENT PRIMARY KEY," .
        "    empresa_id INT NOT NULL," .
        "    titulo VARCHAR(255) NOT NULL," .
        "    descripcion TEXT NULL," .
        "    causa_raiz TEXT NULL," .
        "    acciones TEXT NULL," .
        "    responsable VARCHAR(150) NULL," .
        "    estado ENUM('abierta','en_proceso','cerrada') NOT NULL DEFAULT 'abierta'," .
        "    cerrado_en DATETIME NULL," .
        "    cerrado_por INT NULL," .
        "    creado_por INT NULL," .
        "    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," .
        "    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," .
        "    INDEX idx_calidad_nc_empresa_estado (empresa_id, estado)," .
        "    CONSTRAINT fk_calidad_nc_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE" .
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
    ];

    foreach ($queries as $sql) {
        if (!$conn->query($sql)) {
            return false;
        }
    }

    return true;
}

/**
 * Envía una respuesta JSON consistente para el módulo de calidad.
 */
function calidadRespond(int $statusCode, string $status, string $message, $data = null): void
{
    http_response_code($statusCode);
    header('Content-Type: application/json');
    echo json_encode([
        'status' => $status,
        'message' => $message,
        'data' => $data,
    ]);
    exit;
}

/**
 * Obtiene y valida el identificador de la empresa activa.
 */
function calidadEmpresaId(mysqli $conn): int
{
    $empresaId = obtenerEmpresaId();
    if ($empresaId <= 0) {
        calidadRespond(400, 'error', 'Empresa no especificada');
    }

    if (!calidadEnsureTables($conn)) {
        calidadRespond(500, 'error', 'No se pudo preparar el esquema de datos de calidad');
    }

    return $empresaId;
}

/**
 * Decodifica el cuerpo JSON de una petición.
 */
function calidadJsonBody(): array
{
    $raw = file_get_contents('php://input');
    $data = json_decode($raw, true);

    if ($data === null && json_last_error() !== JSON_ERROR_NONE) {
        calidadRespond(400, 'error', 'Formato JSON inválido');
    }

    return is_array($data) ? $data : [];
}

/**
 * Determina el usuario autenticado actual.
 */
function calidadUsuarioId(): int
{
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    return (int) ($_SESSION['usuario_id'] ?? 0);
}

