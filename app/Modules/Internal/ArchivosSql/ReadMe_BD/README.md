# Carga de historiales desde phpMyAdmin

Este instructivo aplica al **panel interno** (`public/apps/internal`). Asegúrate
de trabajar con la empresa correspondiente (usa `obtenerEmpresaId()` en tus
scripts PHP si necesitas validar datos desde el backend).

## 1. Preparar los datos normales del historial

1. Copia `audit_trail.csv` a la carpeta
   `app/Modules/Internal/ArchivosSql/Normalize_Python/`.
2. Desde la terminal posicionada en la raíz del proyecto ejecuta:

   ```bash
   python app/Modules/Internal/ArchivosSql/Normalize_Python/convert_audit_trail_csv.py
   python app/Modules/Internal/ArchivosSql/Normalize_Python/convert_historiales_csv.py
   ```

   - El primer script genera `insert_audit_trail.sql` con los movimientos crudos.
   - El segundo crea los archivos `insert_historial_*.sql` listos para
     importarse en phpMyAdmin sin depender de `LOAD DATA LOCAL INFILE`.

> Consejo ISO 17025 / NOM-059: valida que los horarios del CSV estén en la zona
> correcta antes de importar para que las bitácoras coincidan con la evidencia
> auditada.

## 2. Orden sugerido para la importación

Con los archivos generados, ingresa a phpMyAdmin (empresa correcta) y, desde la
pestaña **SQL**, ejecuta los scripts en este orden:

1. `app/Modules/Internal/ArchivosSql/Archivos_BD_SQL/insert_audit_trail.sql`
   (opcional si ya existen los eventos crudos).
2. `app/Modules/Internal/ArchivosSql/Archivos_BD_SQL/insert_historial_departamentos.sql`.
3. `app/Modules/Internal/ArchivosSql/Archivos_BD_SQL/insert_historial_ubicaciones.sql`.
4. `app/Modules/Internal/ArchivosSql/Archivos_BD_SQL/insert_historial_fecha_alta.sql`.
5. `app/Modules/Internal/ArchivosSql/Archivos_BD_SQL/insert_historial_fecha_baja.sql`.
6. `app/Modules/Internal/ArchivosSql/Archivos_BD_SQL/insert_historial_tipos_instrumento.sql`.

Cada archivo incluye transacciones y validaciones con `NOT EXISTS`, por lo que
puedes ejecutarlos más de una vez sin duplicar registros. Si necesitas forzar el
prefijo del estado histórico, edita `convert_historiales_csv.py` para fijar el
valor deseado antes de ejecutar el script.

## 3. Recomendaciones finales

- Si phpMyAdmin marca errores por códigos inexistentes, revisa primero que
  `insert_instrumentos.sql` y `insert_plan_riesgos.sql` se hayan importado
  correctamente.
- Conserva los CSV originales en la carpeta `Normalize_Python/` para poder
  regenerar los scripts cuando haya auditorías o actualizaciones masivas.
- Documenta en el expediente de calidad cada ejecución indicando fecha, hora y
  usuario que realizó la importación (requisito de trazabilidad NOM-059).
