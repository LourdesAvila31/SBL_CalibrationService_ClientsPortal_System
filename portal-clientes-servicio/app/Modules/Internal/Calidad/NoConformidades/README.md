# No Conformidades

Servicios orientados al registro, seguimiento y cierre de no conformidades internas.

## Dependencias internas
- `Core/SessionGuard.php` para validar sesión.
- `Core/db.php` para la conexión a la base de datos.
- `Core/helpers/company.php` que expone `obtenerEmpresaId()` y asegura el contexto de empresa.
- `Modules/Internal/Auditoria/audit.php` para centralizar el registro en la bitácora.
- Tablas requeridas: `no_conformidades` y `no_conformidades_acciones`, asociadas a `usuarios`.

## Scripts
- `register_nonconformity.php`: crea una nueva no conformidad con estado inicial.
- `update_followup.php`: agrega acciones de seguimiento y actualiza el estado.
- `close_nonconformity.php`: cierra la no conformidad registrando resultados y verificaciones.
