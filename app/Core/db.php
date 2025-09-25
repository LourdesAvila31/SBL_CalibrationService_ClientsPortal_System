<?php
/**
 * Simple compatibility wrapper that exposes the shared database connection.
 *
 * All scripts that previously included this file now rely on `db_config.php`,
 * which únicamente establece conexiones MySQL reales y propaga cualquier
 * error para que puedas corregir la instancia activa.
 */

require_once __DIR__ . '/db_config.php';

// `$conn` is provided by DatabaseManager in db_config.php

?>

