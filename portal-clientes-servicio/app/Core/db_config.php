<?php
/**
 * Database Configuration and Connection Manager
 *
 * Esta versión requiere explícitamente una base de datos MySQL disponible
 * localmente. Si la conexión falla se lanza una excepción para que los
 * consumidores puedan gestionar el error y no se utiliza ninguna base de
 * datos de demostración ni datasets de prueba.
 */

class DatabaseManager {
    private static $conn = null;
    private static $isRealDatabase = false;

    /**
     * Obtiene la conexión activa a la base de datos.
     */
    public static function getConnection() {
        if (self::$conn === null) {
            self::initializeConnection();
        }
        $GLOBALS['conn'] = self::$conn;
        return self::$conn;
    }

    /**
     * Indica si la conexión apunta a una base de datos MySQL real.
     */
    public static function isRealDatabase() {
        if (self::$conn === null) {
            self::initializeConnection();
        }
        return self::$isRealDatabase;
    }

    /**
     * Inicializa la conexión asegurando el uso de MySQL.
     *
     * @throws Exception si no es posible conectarse a MySQL.
     */
    private static function initializeConnection() {
        $host = self::envValue('DB_HOST', 'localhost');
        $user = self::envValue('DB_USER', 'root');
        $pass = self::envValue('DB_PASS', '', true);
        $db   = self::envValue('DB_NAME', 'iso17025');

        $conn = new mysqli($host, $user, $pass, $db);
        if ($conn->connect_errno !== 0) {
            throw new Exception('No se pudo conectar a la base de datos MySQL: ' . $conn->connect_error);
        }

        if (!$conn->set_charset('utf8')) {
            error_log('Database Manager: No se pudo establecer el conjunto de caracteres UTF-8.');
        }

        self::$conn = $conn;
        self::$isRealDatabase = true;
        error_log('Database Manager: Conectado a base de datos MySQL local');
    }

    /**
     * Obtiene un valor de entorno compatible con phpMyAdmin.
     *
     * Permite leer variables definidas mediante `putenv`, `$_ENV` o `$_SERVER`
     * tal como suele configurarse en servidores que alojan phpMyAdmin.
     */
    private static function envValue($key, $default, $allowEmpty = false) {
        $sources = [
            getenv($key),
            $_ENV[$key] ?? null,
            $_SERVER[$key] ?? null
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

    /**
     * Información de depuración sobre el estado de la conexión.
     */
    public static function getConnectionInfo() {
        return [
            'type' => self::$isRealDatabase ? 'MySQL' : 'Sin conexión',
            'active' => self::$conn !== null,
            'is_real' => self::$isRealDatabase
        ];
    }
}

if (!defined('DB_CONFIG_AUTO_CONNECT')) {
    define('DB_CONFIG_AUTO_CONNECT', true);
}

if (!array_key_exists('conn', $GLOBALS)) {
    $GLOBALS['conn'] = null;
}

// Para compatibilidad con código existente se expone la conexión como $conn.
if (DB_CONFIG_AUTO_CONNECT) {
    $GLOBALS['conn'] = DatabaseManager::getConnection();
}

?>
