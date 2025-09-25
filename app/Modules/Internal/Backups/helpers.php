<?php

declare(strict_types=1);

/**
 * Utilidades compartidas para la generación de respaldos.
 */

/**
 * Crea un respaldo de la base de datos configurada en el entorno y devuelve
 * información sobre el archivo generado.
 *
 * @param mysqli $conn Conexión activa hacia la base de datos.
 * @param string $dir  Directorio donde se almacenará el respaldo.
 *
 * @return array{success:bool, filename?:string, filepath?:string, error?:string}
 */
function create_backup(mysqli $conn, string $dir): array
{
    if (!is_dir($dir)) {
        if (!mkdir($dir, 0777, true) && !is_dir($dir)) {
            return [
                'success' => false,
                'error' => 'No se pudo crear el directorio de respaldos.',
            ];
        }
    }

    $dir = rtrim($dir, DIRECTORY_SEPARATOR);

    $dbName = backup_env_value('DB_NAME', '');
    if ($dbName === '') {
        $result = $conn->query('SELECT DATABASE() AS db');
        if ($result instanceof mysqli_result) {
            $row = $result->fetch_assoc();
            if ($row && isset($row['db'])) {
                $dbName = (string) $row['db'];
            }
            $result->free();
        }
    }

    if ($dbName === '') {
        return [
            'success' => false,
            'error' => 'No se pudo determinar el nombre de la base de datos.',
        ];
    }

    $host = backup_env_value('DB_HOST', 'localhost');
    $user = backup_env_value('DB_USER', 'root');
    $pass = backup_env_value('DB_PASS', '', true);

    $filename = 'backup_' . date('Ymd_His') . '.sql';
    $filepath = $dir . DIRECTORY_SEPARATOR . $filename;

    $commandParts = [
        'mysqldump',
        '--host=' . escapeshellarg($host),
        '--user=' . escapeshellarg($user),
    ];

    if ($pass !== '') {
        $commandParts[] = '--password=' . escapeshellarg($pass);
    }

    $commandParts[] = escapeshellarg($dbName);
    $command = implode(' ', $commandParts) . ' > ' . escapeshellarg($filepath);

    exec($command, $output, $retval);

    if ($retval !== 0) {
        if (is_file($filepath)) {
            @unlink($filepath);
        }
        return [
            'success' => false,
            'error' => 'mysqldump finalizó con código ' . $retval . '.',
        ];
    }

    return [
        'success' => true,
        'filename' => $filename,
        'filepath' => $filepath,
    ];
}

/**
 * Obtiene un valor de entorno compatible con distintas configuraciones.
 */
function backup_env_value(string $key, string $default, bool $allowEmpty = false): string
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

        return (string) $value;
    }

    return $allowEmpty ? '' : $default;
}
