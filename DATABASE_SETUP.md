# Database Connection Configuration

> ⚠️ **Revisa primero los registros de MySQL:** si en herramientas como XAMPP aparece el mensaje “MySQL shutdown unexpectedly”, abre la pestaña **Logs** del panel y lee el archivo `mysql_error.log` para identificar el motivo (por ejemplo, tablas dañadas o falta de permisos). Corrige el problema y reinicia el servicio antes de volver a intentar la conexión.

This system now requires a real MySQL/phpMyAdmin database that must remain active while el backend está en ejecución; la modalidad de demostración fue eliminada.

## Servicio MySQL obligatorio

El bootstrap en `app/Core/db_config.php` intenta conectarse a MySQL durante el arranque. Si el servicio no está disponible lanza una excepción (`No se pudo conectar a la base de datos MySQL: ...`) para detener el backend hasta que el servidor vuelva a estar operativo. De esta manera evitas trabajar con datos obsoletos o inconsistentes.

## Configuring Real Database Connection

To connect to your real phpMyAdmin database, set the following environment variables:

```bash
export DB_HOST=localhost
export DB_USER=root
export DB_PASS=your_password
export DB_NAME=iso17025
```

Or modify the defaults in `app/Core/db_config.php`:

```php
$host = $_ENV['DB_HOST'] ?? 'localhost';
$user = $_ENV['DB_USER'] ?? 'root';
$pass = $_ENV['DB_PASS'] ?? '';
$db = $_ENV['DB_NAME'] ?? 'iso17025';
```

## Mensajes de log

`DatabaseManager` reporta el estado de la conexión mediante los siguientes mensajes:

- `Database Manager: Conectado a base de datos MySQL local` (conexión exitosa).
- `Database Manager: No se pudo establecer el conjunto de caracteres UTF-8.` (aparece sólo si MySQL rechaza `set_charset('utf8')`; revisa la configuración del servidor en ese caso).

## Database Structure

Ensure your MySQL database has the required tables:
- instrumentos
- catalogo_instrumentos
- marcas
- modelos
- departamentos
- calibraciones
- calibradores
- calibraciones_lecturas
- certificados
- audit_trail
- password_resets (tokens de restablecimiento de contraseña)

You can create these tables by importing `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_tables.sql` into your phpMyAdmin database.

> **Novedad:** el script crea los catálogos `calibradores` y `calibraciones_lecturas`, además de añadir las columnas `calibrador_id`, `origen_datos` y `payload_json` en `calibraciones`. Estas columnas permiten asociar lecturas ingestadas automáticamente con los registros finales y conservar la carga cruda recibida desde los dispositivos.

> El script `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_tables.sql` es idempotente: crea todas las tablas requeridas y ajusta esquemas legados sin
> eliminar datos históricos.

### Instrument inventory

The canonical inventory for instruments now lives in `app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/LM_instrumentos_original.csv`. It already contains every data point required by the `instrumentos` table:

| `instrumentos` column | Source in `LM_instrumentos_original.csv` | Notes |
| --- | --- | --- |
| `catalogo_id` | `instrumento` | El script de inserción resuelve el nombre del catálogo al ID correspondiente, creando el registro cuando es necesario. |
| `marca_id` | `marca` | Las marcas se crean al vuelo y se vinculan a cada registro. |
| `modelo_id` | `modelo` | Los modelos son opcionales; el script los vincula con la marca correspondiente cuando existen. |
| `serie` | `serie` | Copied verbatim. |
| `codigo` | `codigo` | Acts as the natural key to upsert each instrument. |
| `departamento_id` | `departamento_responsable` | Departments are created on demand and linked through their ID. |
| `ubicacion` | `ubicacion` | Copied verbatim. |
| `fecha_alta` | `fecha_alta` | Textual dates such as `16-Abr-19` are converted to real `DATE` values. `NA`, `ND` and blank values become `NULL`. |
| `fecha_baja` | `fecha_baja` | Same conversion rules as `fecha_alta`. |
| `empresa_id` | _Script constant_ | El proceso asigna la empresa mediante la variable `@empresa_id` (valor predeterminado `1`). |

Fields without a direct column in the CSV (`proxima_calibracion`, `estado`, `programado`) keep their database defaults, so no additional input is required.

Update the CSV when you need to add or edit equipment metadata and then ejecuta `insert_instrumentos.sql` para propagar los cambios. El script contiene sentencias `INSERT ... ON DUPLICATE KEY UPDATE` para mantener sincronizados catálogos e instrumentos en una única transacción y no requiere archivos auxiliares:

```sql
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql;
```

En phpMyAdmin puedes importar directamente el archivo (`Importar` → seleccionar `insert_instrumentos.sql`) o copiar y pegar su contenido en la pestaña **SQL**. Ninguna de las rutas internas depende de archivos locales; todo el dataset viaja dentro del script.

### Ubicaciones de archivos clave para phpMyAdmin

| Dataset | CSV original | CSV normalizado | Script SQL final |
| --- | --- | --- | --- |
| Inventario de instrumentos | `app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/LM_instrumentos_original.csv` | `app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_instrumentos.csv` | `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql` |
| Plan de riesgos | `app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/PR_instrumentos_original.csv` | `app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_plan_riesgos.csv` | `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_plan_riesgos.sql` |
| Historial (audit trail) | `app/Modules/Internal/ArchivosSql/Normalize_Python/audit_trail.csv` | _Se genera al vuelo con `convert_audit_trail_csv.py`_ | `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_audit_trail.sql` |
| Calibraciones históricas | `app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/CERT_instrumentos_original.csv` | _Procesado por `tools/scripts/generate_cert_calibrations.py`_ | `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_calibraciones_certificados.sql` |

## Testing Database Connection

Run the helper-based test to verify your connection and the portal domain
helpers:

```bash
php tests/php/portal_domains_helper_test.php
```

## Risk Plan Data Load

The canonical inventory for the risk plan now lives in `app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/PR_instrumentos_original.csv`. This CSV contains every record that used to ship in `lista_instrumentos_riesgos.sql`; update it whenever you need to add or edit risk metadata.

To refresh the `plan_riesgos` table:

1. Review `app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/PR_instrumentos_original.csv` and make sure it keeps the standard header and uses plain-text values (no merged cells or formulas).
2. Execute `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_plan_riesgos.sql` desde tu cliente MySQL o importándolo en phpMyAdmin. El script inserta o actualiza cada fila utilizando `instrumento_codigo` como clave y se ejecuta en una única transacción, por lo que puedes correrlo nuevamente sin producir duplicados.

> **Note:** `PR_instrumentos_original.csv` + `SBL_inserts/insert_plan_riesgos.sql` are the single source of truth for risk-plan data. The legacy `lista_instrumentos_riesgos.sql` dump has been removed from the repository.


## Quality module datasets

The quality workspace introduces dedicated tables for document control,
trainings and non-conformities. Run the helper scripts included in
`app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/` after loading the core schema:

- `SBL_adds/add_quality_tables.sql`: creates the `quality_documents`,
  `quality_trainings` and `quality_nonconformities` families with their related
  history/attendance/action tables.
- `SBL_adds/add_seed_quality_module.sql`: registers the permissions required to operate the
  module (`quality.*`), links them to the *Administrador general* and *Jefe de
  Calidad* roles, and seeds a reference dataset (un procedimiento publicado, una
  capacitación programada y una no conformidad cerrada).

`add_seed_data.sql` ahora sólo carga roles globales, catálogos y permisos base.
Para entornos de práctica puedes complementar con `add_seed_data_demo.sql`,
que crea cuentas y empresas ficticias; simplemente omítelo en producción o
elimina esos registros antes del arranque oficial. Los scripts son idempotentes,
por lo que volver a ejecutarlos únicamente actualizará la información existente.


## Execution order

Run the SQL scripts in the following order so every foreign key resolves correctly. Each file can be imported desde phpMyAdmin o ejecutado con `SOURCE` sin depender de rutas locales adicionales:

```sql
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_tables.sql;
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_seed_data.sql;
-- Opcional para entornos de prueba:
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_seed_data_demo.sql;
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql;
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_plan_riesgos.sql;
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_quality_tables.sql;
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_seed_quality_module.sql;
-- Opcional:
SOURCE app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_audit_trail.sql;
```

This sequence recreates the schema from scratch, loads the base catalog data, inserta ambos inventarios mediante scripts idempotentes y prepara el módulo de calidad (estructura + datos de referencia). Ejecutar `insert_audit_trail.sql` al final permite poblar el historial una vez que las tablas principales existen. Si necesitas regenerar `insert_audit_trail.sql`, usa `python app/Modules/Internal/ArchivosSql/Normalize_Python/convert_audit_trail_csv.py` junto con `audit_trail.csv` como se explica más adelante.

> `normalize_audit_trail.sql` ya no forma parte del repositorio. Para reconstruir el historial toma `audit_trail.csv`, corre `convert_audit_trail_csv.py` (o el script equivalente que se entregue en futuras versiones) y luego importa el SQL generado como se describe en el paso 7.

### phpMyAdmin step-by-step

1. Import `add_tables.sql` (pestaña **Importar** → seleccionar archivo → ejecutar).
   - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_tables.sql`.
2. Import `add_seed_data.sql` para cargar roles, catálogos y permisos base.
   - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_seed_data.sql`.
   - En phpMyAdmin abre la base `iso17025`, entra a **Importar**, pulsa **Seleccionar archivo**, elige el `.sql` y haz clic en **Continuar**.
2b. (Opcional) Importa `add_seed_data_demo.sql` si necesitas cuentas y empresas de prueba.
    - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_seed_data_demo.sql`.
    - Sigue el mismo procedimiento que en el paso anterior; recuerda eliminar o actualizar estos datos antes de pasar a producción.
3. Importa o copia/pega `insert_instrumentos.sql` para sincronizar el inventario de instrumentos.
   - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql`.
   - Si prefieres copiar y pegar, abre la pestaña **SQL**, pega todo el contenido y pulsa **Continuar**.
4. Importa o copia/pega `insert_plan_riesgos.sql` para actualizar el plan de riesgos.
   - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_plan_riesgos.sql`.
5. Import `add_quality_tables.sql` para crear las tablas del módulo de calidad.
   - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_quality_tables.sql`.
6. Importa o copia/pega `add_seed_quality_module.sql` para registrar permisos y datos de ejemplo.
   - Ruta del archivo: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_adds/add_seed_quality_module.sql`.
7. (Opcional) Genera `insert_audit_trail.sql` con el script Python y luego impórtalo para poblar el historial.
   - Ejecuta en la terminal: `python app/Modules/Internal/ArchivosSql/Normalize_Python/convert_audit_trail_csv.py`.
   - El comando lee `audit_trail.csv` (ubicado en `app/Modules/Internal/ArchivosSql/Normalize_Python/`) y produce `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_audit_trail.sql`.
   - En phpMyAdmin abre la base `iso17025`, ve a **Importar**, selecciona el archivo `insert_audit_trail.sql`, confirma que el formato sea **SQL** y pulsa **Continuar**.

Cada paso se realiza únicamente importando o copiando/pegando SQL, cumpliendo el requisito de “sólo copiar/pegar o importar directamente”.

## Integración de calibradores conectados

La funcionalidad de ingesta automática se activa con los mismos scripts anteriores:

1. `add_tables.sql` crea las tablas `calibradores` y `calibraciones_lecturas`, además de las columnas auxiliares en `calibraciones`.
2. `add_seed_data.sql` registra los permisos `integraciones_calibradores_*` para administradores/supervisores y da de alta un calibrador de demostración (`Calibrador Demo`) con el token `demo-calibrator-token`.

Tras ejecutar ambos scripts podrás administrar los dispositivos desde la nueva pantalla **Instrumentos → Calibradores** y asociarlos a instrumentos específicos. Las lecturas entrantes se almacenan en `calibraciones_lecturas` y pueden vincularse al momento de crear una calibración seleccionando el calibrador y la medición más reciente en el formulario.

Para ingresar lecturas desde un dispositivo físico o un simulador de datos ejecuta el servicio Node:

```bash
# Variables de ejemplo
export CALIBRATOR_ID=1
export CALIBRATOR_TOKEN=demo-calibrator-token
export CALIBRATOR_SOURCE="tcp://localhost:9000"   # o serial:///dev/ttyUSB0
export CALIBRATOR_ENDPOINT="http://localhost:8000/backend/integraciones/calibradores/store_measurement.php"

npm run calibrator:ingest
```

El script `tools/scripts/calibrator_ingest.js` admite fuentes TCP o serial (`serialport`) y firma cada carga con HMAC-SHA256 antes de publicarla en el endpoint PHP. Si no cuentas con un puerto serial disponible puedes enviar líneas JSON por TCP o por `stdin`; cada lectura debe incluir la información que el backend almacenará en `payload_json`.

## Connection Status

You can confirm that MySQL is active by checking the PHP error log. When the connection succeeds you will find the entry `Database Manager: Conectado a base de datos MySQL local`. If the service is down, PHP will raise an exception such as `No se pudo conectar a la base de datos MySQL` and the backend will remain offline until MySQL is restored.

## Gages Data Visualization

Once the real database is connected and contains data, the gages visualization at `/public/apps/internal/instrumentos/list_gages.html` will display the actual instrument data from your phpMyAdmin database.