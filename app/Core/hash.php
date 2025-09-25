<?php
// Genera el hash de una contraseña recibida por parámetro (?pw=)
$password = $_GET['pw'] ?? 'TuContraseñaSegura';
echo password_hash($password, PASSWORD_DEFAULT);
?>
