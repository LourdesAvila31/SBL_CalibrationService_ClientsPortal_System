# Guía de carga de datos SQL

Este directorio concentra los archivos SQL y CSV necesarios para reconstruir por completo la base de datos `iso17025` desde phpMyAdmin sin depender de rutas locales ni de scripts inexistentes. Todos los pasos descritos a continuación se pueden realizar exclusivamente copiando y pegando SQL en la pestaña **SQL** o importando archivos `.sql`/`.csv` desde la pestaña **Importar**.

## Resumen de archivos disponibles

| Archivo | Propósito | Entradas principales | Resultado | 
| --- | --- | --- | --- |
| `add_tables.sql` | Migra/crea la estructura de `iso17025` incluyendo tablas multiempresa y columnas auxiliares. | Base vacía o legado. | Esquema actualizado sin perder datos existentes. |
| `add_seed_data.sql` | Población inicial de roles, permisos y catálogos multiempresa. | Esquema provisto por `add_tables.sql`. | Catálogos base listos para operar el panel interno. |
| `add_seed_data_demo.sql` | Usuarios, firmas y empresas ficticias para pruebas. | Datos base cargados por `add_seed_data.sql`. | Ambiente demostrativo listo para validaciones internas. |
| `insert_instrumentos.sql` | Inserta/actualiza el inventario de instrumentos utilizando sentencias `INSERT ... ON DUPLICATE KEY UPDATE`. | Script generado a partir de `instrumentos_normalizado.csv`. | Tabla `instrumentos` sincronizada con los catálogos correspondientes. |
| `insert_plan_riesgos.sql` | Inserta/actualiza el plan de riesgos asociado a cada instrumento. | Script generado a partir de `plan_riesgos_normalizado.csv`. | Tabla `plan_riesgos` alineada con los instrumentos existentes. |
| `insert_calibraciones_certificados.sql` | Inserta las calibraciones históricas detectadas en `CERT_instrumentos_original.csv`. | Script generado por `tools/scripts/generate_cert_calibrations.py`. | Tabla `calibraciones` con la programación histórica importada. |
| `add_quality_tables.sql` | Crea la estructura del módulo de calidad (documentos, capacitaciones, no conformidades). | Base con catálogos cargados. | Tablas de calidad disponibles. |
| `seed_quality_module.sql` | Registra permisos del módulo de calidad y carga ejemplos de referencia. | Estructura creada por `add_quality_tables.sql`. | Catálogos del módulo de calidad poblados. |
| `normalize_audit_trail.sql` | Normaliza el historial de cambios desde `audit_trail.csv` y lo inserta en `audit_trail`. | CSV `audit_trail.csv` (UTF-8). | Tablas `audit_trail` y `audit_trail_normalizado` actualizadas respetando claves foráneas. |
| `insert_historial_instrumentos.sql` | Genera movimientos en los historiales de instrumentos a partir de los datos normalizados. Este script se mantiene únicamente en `app/Modules/Internal/ArchivosSql/insert_historial_instrumentos.sql` para evitar duplicados. | Tabla `audit_trail_normalizado` creada por `normalize_audit_trail.sql`. | Tablas `historial_departamentos`, `historial_ubicaciones`, `historial_fecha_alta`, `historial_fecha_baja` y `historial_tipos_instrumento` sincronizadas. |
| `historial_instrumentos_prepare.sql` | Prepara variables, detecta el prefijo de estado y construye `tmp_historial_instrumentos` desde los datos normalizados. | `audit_trail_normalizado` con registros de la empresa deseada. | Variables de sesión configuradas y tabla temporal lista para los inserts. |
| `historial_departamentos.sql` | Inserta los departamentos históricos de cada instrumento evitando duplicados. | Scripts generados por `generate_historial_inserts.py` usando `normalize_instrumentos.csv`. | Registros nuevos en `historial_departamentos` respetando empresa y fecha. |
| `historial_ubicaciones.sql` | Inserta las ubicaciones históricas de cada instrumento evitando duplicados. | Scripts generados por `generate_historial_inserts.py` usando `normalize_instrumentos.csv`. | Registros nuevos en `historial_ubicaciones` respetando empresa y fecha. |
| `historial_fecha_alta.sql` | Inserta las fechas de alta históricas de cada instrumento evitando duplicados. | Scripts generados por `generate_historial_inserts.py` usando `normalize_instrumentos.csv`. | Registros nuevos en `historial_fecha_alta` respetando empresa y fecha. |
| `historial_fecha_baja.sql` | Inserta las fechas de baja históricas de cada instrumento evitando duplicados. | Scripts generados por `generate_historial_inserts.py` usando `normalize_instrumentos.csv`. | Registros nuevos en `historial_fecha_baja` respetando empresa y fecha. |
| `historial_tipos_instrumento.sql` | Inserta los estados o tipos históricos de cada instrumento evitando duplicados. | Scripts generados por `generate_historial_inserts.py` usando `normalize_instrumentos.csv`. | Registros nuevos en `historial_tipos_instrumento` respetando empresa y fecha. |
| `historial_instrumentos_cleanup.sql` | Restaura `lc_time_names` y elimina la tabla temporal utilizada por los inserts individuales. | Variables de sesión creadas por `historial_instrumentos_prepare.sql`. | Configuración regional restaurada y recursos temporales liberados. |
| `LM_instrumentos.csv` | Inventario canónico de instrumentos. | CSV con encabezados estándar. | Fuente para generar `instrumentos_normalizado.csv`. |
| `instrumentos_normalizado.csv` | Inventario normalizado con claves numéricas. | Generado por `convert_instrumentos_csv.py`. | Insumo de `insert_instrumentos.sql`. |
| `PR_instrumentos.csv` | Plan de riesgos canónico. | CSV con encabezados estándar. | Fuente para `plan_riesgos_normalizado.csv`. |
| `plan_riesgos_normalizado.csv` | Plan de riesgos normalizado con códigos validados contra el inventario. | Generado por `tools/scripts/generate_plan_riesgos.py`. | Insumo de `insert_plan_riesgos.sql`. |
| `audit_trail.csv` | Historial crudo de cambios. | CSV con encabezado `Fecha,Hoja,...`. | Insumo de `normalize_audit_trail.sql`. |

## Flujo recomendado en phpMyAdmin

1. **Preparar los archivos**
   - Descarga o ten a la mano los archivos `.sql` y `.csv` desde `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/`, `Archivos_CSV_originales/`, `Archivos_Normalize/` y `Normalize_Python/` según corresponda.
   - Asegúrate de que phpMyAdmin permita subir archivos locales (opción *Permitir la carga de archivos locales* al ejecutar scripts con `LOAD DATA`).

2. **Crear o migrar la base de datos**
   - Selecciona la base `iso17025` en phpMyAdmin.
   - Ve a la pestaña **Importar**, haz clic en **Seleccionar archivo**, elige `Archivos_BD_SBL/SBL_adds/add_tables.sql`, confirma que el formato sea **SQL** y pulsa **Continuar** para ejecutar la importación.

3. **Cargar catálogos base**
   - Desde la misma pestaña **Importar**, selecciona `Archivos_BD_SBL/SBL_adds/add_seed_data.sql` y ejecútalo. Este script es idempotente; puedes volver a importarlo en el futuro sin efectos adversos.
   - (Opcional) Si necesitas cuentas de práctica, importa `Archivos_BD_SBL/SBL_adds/add_seed_data_demo.sql`. Recuerda depurar estas cuentas antes de cualquier liberación productiva.

4. **Insertar/actualizar instrumentos**
   - Opción A (importación): en **Importar**, selecciona `Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql` y ejecútalo.
   - Opción B (copiar/pegar): abre la pestaña **SQL**, copia el contenido completo de `Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql`, pégalo en el editor y pulsa **Continuar**.
   - El script incluye `START TRANSACTION`/`COMMIT` y usa `ON DUPLICATE KEY UPDATE` para mantener sincronizados los catálogos (`catalogo_instrumentos`, `marcas`, `modelos`, `departamentos`).

5. **Insertar/actualizar plan de riesgos**
   - Importa `Archivos_BD_SBL/SBL_inserts/insert_plan_riesgos.sql` o copia/pega su contenido en la pestaña **SQL** y ejecútalo.
   - Si optas por copiar y pegar, abre la pestaña **SQL**, pega todo el contenido y pulsa **Continuar** para guardar los cambios.
   - El script mantiene la correspondencia por `instrumento_codigo`. Si algún código no existe en `instrumentos`, el `INSERT` fallará y podrás corregir el dato antes de volver a ejecutar la transacción completa.

6. **Crear tablas del módulo de calidad**
   - Importa `Archivos_BD_SBL/SBL_adds/add_quality_tables.sql` para crear las nuevas estructuras.
   - En **Importar**, selecciona el archivo, verifica que el formato sea **SQL** y pulsa **Continuar**.

7. **Sembrar datos del módulo de calidad**
   - Importa `Archivos_BD_SBL/SBL_adds/add_seed_quality_module.sql` o copia/pega su contenido. Este script es idempotente y actualiza permisos, roles y registros de ejemplo.
   - Si importas el archivo, usa la pestaña **Importar**; si copias y pegas, abre **SQL**, pega todo y pulsa **Continuar**.

8. **Cargar el historial de auditoría (opcional)**
   - Abre la pestaña **SQL** y copia el contenido de `normalize_audit_trail.sql`.
   - Antes de ejecutar, phpMyAdmin mostrará el campo **Archivo a cargar**. Adjunta `audit_trail.csv` y confirma la ejecución.
   - El script crea tablas temporales, normaliza fechas, rellena `audit_trail` y deja la información consolidada por instrumento en `audit_trail_normalizado`.
   - El flujo definitivo para cargar el historial de auditoría es usar `convert_audit_trail_csv.py` para generar `insert_audit_trail.sql` o, de forma más automatizada, `normalize_audit_trail.sql`. El archivo vacío `insert_audit_trail_creado.sql` se eliminó para evitar duplicidad, así que no es necesario importarlo.

### Cómo establecer `fecha_evento` después de la importación

- Si ya ejecutaste `insert_audit_trail.sql` y los registros quedaron con `fecha_evento = NULL`, ve a la pestaña **SQL** de phpMyAdmin y lanza actualizaciones puntuales con las fechas reales. Un ejemplo sería:

  ```sql
  UPDATE audit_trail
  SET fecha_evento = STR_TO_DATE('2024-04-19 12:55:00', '%Y-%m-%d %H:%i:%s')
  WHERE empresa_id = 1
    AND seccion = 'Instrumentos'
    AND usuario_correo = 'validacion5@sblpharma.com'
    AND fecha_evento IS NULL;
  ```

- Repite el `UPDATE` ajustando los filtros según el conjunto de filas que corresponda a cada sello de tiempo. Como alternativa masiva, regenera `insert_audit_trail.sql` ejecutando nuevamente `python app/Modules/Internal/ArchivosSql/Normalize_Python/convert_audit_trail_csv.py` (asegúrate de actualizar `audit_trail.csv` si hubo cambios) y vuelve a importarlo en phpMyAdmin.

9. **Generar historiales de instrumentos (opcional)**
   - Una vez que `insert_audit_trail.sql` haya poblado `audit_trail`, puedes elegir entre:
     - Ejecutar el script completo `Archivos_BD_SBL/SBL_inserts/insert_historial_instrumentos.sql`, o
     - Seguir el flujo modular importando cada archivo de `Archivos_BD_SBL/SBL_historiales/` (por ejemplo `historial_departamentos.sql`, `historial_ubicaciones.sql`, `historial_fecha_alta.sql`, `historial_fecha_baja.sql`, `historial_estado_instrumento.sql`).
   - En ambos casos puedes ajustar la empresa destino agregando `SET @empresa_id = <ID>;` al inicio del archivo antes de importarlo. El prefijo del estado se detecta automáticamente; si prefieres forzarlo, fija también `SET @estado_prefijo = 'K';` (o la letra correspondiente) antes de ejecutar.
   - Ejecuta el bloque o los archivos en el orden elegido. Se poblarán `historial_departamentos`, `historial_ubicaciones`, `historial_fecha_alta`, `historial_fecha_baja`, `historial_estado_instrumento` y tablas relacionadas evitando duplicados basados en instrumento, valor y fecha del evento.

10. **Verificaciones posteriores**
   - Ejecuta consultas de control como `SELECT COUNT(*) FROM instrumentos;` o revisa los catálogos (`catalogo_instrumentos`, `marcas`, `modelos`, `departamentos`) para validar los conteos esperados.
   - Repite cualquier paso importando nuevamente el archivo correspondiente si necesitas ajustar datos; todos los scripts están diseñados para ser idempotentes o manejar actualizaciones por clave natural.

## Notas adicionales

- `insert_instrumentos.sql` y `insert_plan_riesgos.sql` se regeneran desde los CSV normalizados. Si actualizas `Archivos_CSV_originales/LM_instrumentos_original.csv` o `Archivos_CSV_originales/PR_instrumentos_original.csv`, vuelve a ejecutar los convertidores (`convert_instrumentos_csv.py`, `generate_plan_riesgos.py`, etc.) para mantener sincronizados los scripts.
- `convert_audit_trail_csv.py` es ahora el responsable de generar `insert_audit_trail.sql`; asegúrate de ejecutar el script cada vez que `audit_trail.csv` cambie para conservar el historial actualizado.
- Mantén los CSV en formato UTF-8 (sin BOM) con los encabezados listados; los scripts convierten `NA`, `ND` y fechas en blanco a `NULL` automáticamente.
- Usa `python app/Modules/Internal/ArchivosSql/Normalize_Python/generate_historial_inserts.py` para regenerar los archivos `historial_*.sql` cuando se actualice `normalize_instrumentos.csv`. Si necesitas ajustar la empresa objetivo o guardar los resultados en otra carpeta agrega argumentos como `--empresa-id 5` o `--output-dir <ruta>`. Cada archivo resultante incluye comentarios para fijar `@empresa_id` antes de ejecutarlo en phpMyAdmin.
- Para reconstruir el calendario histórico de calibraciones, ejecuta `python tools/scripts/generate_cert_calibrations.py --empresa-id <ID>` y luego importa el archivo `insert_calibraciones_certificados.sql` resultante. Cada `INSERT` incluye un `WHERE NOT EXISTS` que evita duplicados cuando el script se vuelve a correr.

## Reporte de auditoría

- Ejecuta `python audit_trail_report.py` para obtener los totales del historial (`audit_trail_report_totals.json`) y el detalle por fila (`audit_trail_report_summary.csv`).
- El script limpia valores nulos equivalentes (`''`, `NA`, `ND`, `N/A`, `NULL`), agrupa por número de fila y valida las columnas A–H para confirmar altas completas.
- Los movimientos de departamento (F), ubicación (G) y cambios de fechas de alta/baja (H/I) solo se contabilizan cuando el valor normalizado difiere del anterior; las asignaciones y cambios de código se derivan de la columna E.
- Consulta `audit_trail_report.md` para la interpretación de métricas y ejemplos de validación.
