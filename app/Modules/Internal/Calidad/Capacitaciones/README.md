# Capacitaciones de Calidad

Controladores y servicios para coordinar la formación interna.

## Dependencias internas
- `Core/SessionGuard.php` para garantizar que exista sesión válida.
- `Core/db.php` para acceder a la base de datos.
- `Core/helpers/company.php` con `obtenerEmpresaId()` para aislar datos multitenant.
- `Modules/Internal/Auditoria/audit.php` para enviar cada acción a la bitácora de auditoría.
- Tablas de apoyo: `capacitaciones` y `capacitaciones_participantes` con claves foráneas a `usuarios`.

## Scripts
- `schedule_training.php`: programa una nueva capacitación y define responsable/modalidad.
- `record_attendance.php`: registra la asistencia de los participantes y actualiza la bitácora.
