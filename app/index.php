<?php
// Punto de entrada para el backend.
// Devuelve un mensaje sencillo para comprobar que el servidor PHP está activo.

header('Content-Type: application/json');
echo json_encode([
    'status' => 'ok',
    'message' => 'Backend operativo'
]);