# Documentos de Calidad

Este submódulo agrupa los controladores encargados de gestionar el ciclo de vida de los documentos del sistema de gestión.

## Dependencias internas
- `Core/SessionGuard.php`: valida la sesión antes de ejecutar cualquier acción.
- `Core/db.php`: expone la conexión mysqli utilizada en los scripts.
- `Core/helpers/company.php`: provee `obtenerEmpresaId()` para aislar los datos por empresa.
- `Modules/Internal/Auditoria/audit.php`: registra cada acción en la bitácora (`log_activity`).
- Tablas esperadas: `documentos_calidad` con columnas para metadatos del documento y auditoría de cambios.

## Scripts
- `register_document.php`: alta de un documento en estado borrador.
- `review_document.php`: registra la revisión (aprobación/rechazo) y comentarios.
- `publish_document.php`: publica un documento aprobado y registra la trazabilidad.
