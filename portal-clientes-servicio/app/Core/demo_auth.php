<?php
// Demo bypass for authentication - for testing purposes only
session_start();

// Set up a demo session
$_SESSION['usuario_id'] = 1;
$_SESSION['usuario'] = 'demo';
$_SESSION['role_id'] = 1;

// For demo purposes, always return true for permission checks
function check_permission(string $permiso): bool {
    return true; // Allow all permissions for demo
}
?>