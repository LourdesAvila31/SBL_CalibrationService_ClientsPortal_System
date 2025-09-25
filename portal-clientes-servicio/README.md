# Sistema Computarizado ISO 17025

Aplicación web para la gestión de calibraciones que combina un backend PHP modular y una
interfaz HTML5/Bootstrap. El proyecto ahora se organiza en torno a una carpeta `app/`
para el código del dominio y a `public/` como punto de entrada HTTP.

## Arquitectura de carpetas

- `app/`
  - `Core/`: utilidades compartidas (conexión a base de datos, protección de sesión,
    helpers y control de permisos).
  - `Modules/`: lógica de negocio segmentada por ámbito. Dentro encontrarás
    `Modules/Internal/` (auditoría, planeación, configuración, usuarios, etc.),
    `Modules/Tenant/` (operaciones disponibles para las empresas externas como
    instrumentos, calibraciones, reportes) y los recursos SQL en
    `Modules/Internal/ArchivosSql/`.
  - `index.php`: comprobación básica para el estado del backend.
- `public/`
  - `assets/`: recursos estáticos compartidos (CSS, JS, imágenes, incluyendo el
    núcleo JS `portal-base.js`, los bundles por portal (`scripts-internal.js`,
    `scripts-service.js`, `scripts-tenant.js`) y `topbar.js`).
  - `apps/internal/`: aplicación HTML para los usuarios internos (auditoría,
    planeación, catálogos, administración, etc.).
  - `apps/tenant/`: portal simplificado para empresas cliente con acceso a
    instrumentos, planeación, calibraciones y reportes filtrados por empresa.
    Los códigos QR generados para etiquetas y certificados redirigen a
    `apps/tenant/instrumentos/qr_details.html`; el flujo QR es exclusivo del
    portal tenant y no existe una vista compartida en `apps/shared`.
  - `backend/`: fachada pública que enruta las peticiones HTTP hacia los módulos en
    `app/Modules/` sin exponer el código fuente.
  - `index.php`: router contextual que expone rutas `/internal/...` y
    `/tenant/...` y redirige al paquete correcto según la sesión o el parámetro
    `?app=`.
- `tools/`: utilidades de desarrollo (`tools/scripts/test-server.js`, generadores
  de datos, etc.).
- `tests/`: pruebas automatizadas. Las suites en `tests/js/` ejercitan la capa de
  JavaScript mientras que los helpers en `tests/php/` cubren escenarios
  integrados que dependen de la base de datos (por ejemplo,
  `portal_domains_helper_test.php`).

## Backlog y planeación de nuevas funciones

Las siguientes iniciativas siguen en backlog. No existen controladores ni vistas en el repositorio actual; su desarrollo se programará en nuevas rutas para evitar confusiones mientras se define el alcance detallado.

| Funcionalidad | Estado | Nota de planeación |
| --- | --- | --- |
| SLA y semáforos | En planeación (Backlog) | Aún no hay scripts implementados. Se reservará un endpoint futuro como `public/backend/servicio/sla.php` y un módulo asociado (por ejemplo `app/Modules/Api/SLA/`) para concentrar los indicadores sin tocar los portales vigentes. |
| Plantillas por cliente | En planeación (Backlog) | No existen archivos actualmente. El desarrollo se documentará en nuevas rutas como `public/backend/tenant/plantillas.php` y un módulo dedicado bajo `app/Modules/Tenant/Plantillas/` para aislar la lógica por empresa. |
| App móvil / modo offline | En planeación (Backlog) | Sin implementación activa. El soporte se planeará en endpoints específicos (por ejemplo `public/backend/api/v1/offline_sync.php`) junto con un módulo en `app/Modules/Api/Offline/` encargado de la sincronización. |

## Puesta en marcha

1. Instala las dependencias de Node.js:
   ```bash
   npm install
   ```
2. Arranca el servidor embebido de PHP tomando `public/` como raíz del sitio:
   ```bash
   npm start
   # equivalente a: php -S localhost:8000 -t public
   ```
3. Abre <http://localhost:8000/apps/internal/index.html> (panel interno) o
   <http://localhost:8000/apps/tenant/index.html> (portal de clientes). El login
   se encuentra en `/apps/internal/usuarios/login.html` y el router redirige al
   contexto adecuado tras autenticarse.
4. Las peticiones AJAX de la interfaz apuntan a `/public/backend/...`, donde cada
   script incluye su homólogo en `app/Modules/`.

## Backend modular

Cada módulo PHP incluye sus propios controladores y vistas:

- `app/Modules/Internal/*`: funcionalidad exclusiva del personal de calidad
  (auditoría, planeación, usuarios, configuraciones, respaldos, módulo de
  calidad, etc.).
- `app/Modules/Tenant/*`: operaciones que pueden ejecutar las empresas externas
  (instrumentos, calibraciones, reportes y proveedores vinculados).
- `app/Modules/Database/`: utilidades puntuales de creación y actualización de
  tablas compartidas.

Las utilidades comunes residen en `app/Core/` (`db.php`, `SessionGuard.php`,
`permissions.php`, helpers de correo, etc.) y son requeridas por los módulos según
corresponda.

## Módulo de calidad

El módulo interno de calidad centraliza tres procesos clave del sistema:

- **Control documental**: el flujo de creación → revisión → publicación se
  modela en `app/Modules/Internal/Calidad/quality_module.php`, que conserva el
  historial de estados y valida que sólo los documentos en revisión puedan
  publicarse.
- **Capacitaciones**: el servicio permite programar sesiones, registrar
  asistencia y calcular indicadores de cumplimiento para cada curso.
- **No conformidades**: registra acciones correctivas, exige que todas se
  completen antes del cierre y documenta la evidencia de verificación.

Las tablas de soporte viven en `app/Modules/Internal/ArchivosSql/add_quality_tables.sql`
y se acompañan del dataset de referencia `seed_quality_module.sql` (permisos,
roles, documentos iniciales, capacitaciones y no conformidades de ejemplo). La
interfaz consume los helpers UMD de `public/assets/scripts/calidad/` para
renderizar cronologías, métricas de asistencia y avances de acciones.

## Contextos y límites por empresa

Los módulos internos y tenant consumen la misma base de datos, pero todos los
controladores usan `obtenerEmpresaId()` (en `app/Core/helpers/company.php`) para
filtrar la información según la empresa asociada a la sesión. El router y el login
almacenan `$_SESSION['app_context']` para distinguir entre panel interno y portal
de clientes, reforzando los permisos por módulo. De este modo se garantiza que los
clientes consultan únicamente sus instrumentos, calibraciones y reportes, mientras
que el personal interno mantiene visibilidad total de las operaciones.

### Dominios permitidos por portal

La tabla `portal_domains` centraliza la lista de dominios autorizados para cada
portal (`internal`, `tenant`, `service`). El script
`app/Modules/Database/create_tables.php` crea la tabla, asegura índices y carga un
semillado mínimo para instalaciones existentes. Si necesitas registrar un dominio
adicional basta con ejecutar una inserción o utilizar el helper PHP:

```bash
php -r "require 'app/Core/db.php'; require 'app/Core/portal_domains.php'; register_portal_domain(2, 'clientes.ejemplo.com', true);"
```

- `register_portal_domain(int $portalId, string $domain, bool $isPrimary = false, bool $isActive = true)`
  normaliza el dominio, lo marca como único y actualiza los flags si el registro ya
  existía.
- `resolve_portal_by_email(string $email): ?array` se utiliza durante el login para
  deducir el portal en función del correo. El resultado se cachea en memoria para
  evitar accesos repetidos a la base de datos durante la misma petición.
- `clear_portal_domain_cache(?string $domain = null)` está disponible para scripts
  de mantenimiento o pruebas automatizadas que necesiten invalidar la caché.

Las pruebas en `tests/php/portal_domains_helper_test.php` muestran cómo registrar
un dominio de manera temporal antes de la ejecución, aprovechar la caché del helper
durante las aserciones y limpiar tanto la tabla como la caché al finalizar.

## Dashboards de desempeño y tendencias

- El backend expone `public/backend/dashboard/kpi_trends.php`, que incluye a
  `app/Modules/Internal/Dashboard/kpi_trends.php`. El endpoint agrega las
  calibraciones, instrumentos y planes por mes para devolver porcentajes de
  cumplimiento/rechazo, días promedio hasta la siguiente calibración y la carga
  de vencimientos en ventanas de 30, 60 y 61+ días.
- La vista `public/apps/internal/reportes/reports.html` integra Chart.js vía CDN
  e incorpora un módulo JavaScript accesible que consume el endpoint, permite
  seleccionar el rango de fechas y actualiza una gráfica de líneas y una de barras
  apiladas con colores de alto contraste. Las figuras incluyen descripciones y
  etiquetas ARIA para lectores de pantalla.
- Para habilitar los dashboards arranca el servidor (`npm start`) y accede a
  `http://localhost:8000/apps/internal/reportes/reports.html`. El filtro se carga
  por defecto con los últimos 12 meses; al pulsar “Actualizar gráficas” se
  solicitarán los datos y se mostrará el resumen del último mes en las
  leyendas de cada figura.
- Verificación con datos de ejemplo: tras ejecutar los scripts de `DATABASE_SETUP`.
  crea registros de prueba (ajusta el `empresa_id` a la compañía activa):
  ```sql
  INSERT INTO instrumentos (id, serie, codigo, fecha_alta, proxima_calibracion, estado, programado, empresa_id)
  VALUES (9001, 'DEM-001', 'DEMO-01', '2024-01-05', '2024-07-05', 'activo', 1, 1);

  INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, fecha_proxima, resultado, estado_ejecucion)
  VALUES
    (9001, 1, 'Interna', '2024-01-10', '2024-07-05', 'Aprobado', 'Completada'),
    (9001, 1, 'Interna', '2024-03-15', '2024-09-15', 'Rechazado', 'Atrasada');

  INSERT INTO planes (instrumento_id, fecha_programada, estado, empresa_id)
  VALUES
    (9001, '2024-05-01', 'Programada', 1),
    (9001, '2024-08-15', 'Programada', 1);
  ```
  Con el servidor en marcha prueba el endpoint:
  ```bash
  curl "http://localhost:8000/backend/dashboard/kpi_trends.php?start=2024-01-01&end=2024-12-31" | jq
  ```
  y comprueba en la vista de reportes que las gráficas reflejan las tendencias y
  vencimientos de ejemplo.

## Recursos SQL y carga de datos

Todos los scripts están pensados para ejecutarse en MySQL (por consola o desde phpMyAdmin); no existe un modo alterno de base de datos. Los encontrarás en `app/Modules/ArchivosSql/`:

- `add_tables.sql`, `add_seed_data.sql` (y opcionalmente `add_seed_data_demo.sql` para cuentas ficticias): creación de tablas y datos base.
- `normalize_instrumentos.sql`, `normalize_plan_riesgos.sql`: sincronización con los
  CSV (`LM_instrumentos.csv`, `PR_instrumentos.csv`).
- `add_quality_tables.sql`, `seed_quality_module.sql`: estructura y datos base del
  módulo de calidad (permisos, documentos, capacitaciones y no conformidades).
- CSV de apoyo (`instrumentos.csv`, `plan_riesgos_normalizado.csv`, etc.).

Consulta [DATABASE_SETUP.md](DATABASE_SETUP.md) para instrucciones detalladas sobre
cómo ejecutar estos scripts y configurar la conexión a MySQL.

## Importación masiva de instrumentos

El panel interno y el portal tenant incorporan un botón **Importar inventario** en la
barra de acciones de la vista de instrumentos. Al hacer clic se abre un modal con:

1. Un enlace para descargar la plantilla CSV generada por
   `/public/backend/instrumentos/gages/download_inventory_template.php`.
2. Instrucciones básicas y un campo para subir el archivo (límite 5&nbsp;MB).
3. Resumen de los campos obligatorios. El backend rechaza archivos sin las columnas
   **Código**, **Estado** y **Programado**.

Cada importación llama a `/public/backend/instrumentos/gages/import_gages.php`, que:

- Normaliza el CSV (mismo criterio que `tools/scripts/generate_insert_instrumentos.py`).
- Crea o actualiza catálogos, marcas, modelos, departamentos e instrumentos dentro
  de una transacción por empresa.
- Devuelve un resumen con altas, actualizaciones y, si existen, filas con errores.

La plantilla acepta las siguientes columnas en UTF-8 (cabeceras exactas o sus
variantes en minúsculas):

- `Instrumento`
- `Marca`
- `Modelo`
- `Serie`
- `Código`
- `Departamento responsable`
- `Ubicación`
- `Fecha de alta`
- `Fecha de baja`
- `Próxima calibración`
- `Estado` (texto descriptivo, p. ej. *Activo*, *Stock*, *Inactivo*)
- `Programado` (`1`/`0`, `Sí`/`No`, `True`/`False`)

Al finalizar la importación el frontend muestra un modal con el resumen y, si es
necesario, la lista de filas que no se procesaron. Después de una importación
exitosa la tabla se recarga automáticamente.

## Impresión de etiquetas con código QR

La vista de instrumentos del panel interno y del portal de clientes incorpora un
botón **Imprimir etiqueta** que se habilita al seleccionar un único registro.
Al hacer clic se abre `print_label.html` en una pestaña nueva con la información
del instrumento y un código QR apuntando al certificado más reciente.

- El endpoint `public/backend/instrumentos/gages/get_label_payload.php` expone la
  identificación, ubicación y enlace compartible (`backend/calibraciones/share_certificate.php`)
  firmado para el instrumento seleccionado.
- `public/assets/scripts/labels.js` consume ese JSON, normaliza los campos y
  utiliza `assets/scripts/vendor/qrcode.min.js` para renderizar el QR dentro de
  la tarjeta lista para impresión.
- Durante la carga se muestran mensajes de estado y el botón **Imprimir** llama
  directamente a `window.print()` para aprovechar los estilos específicos de
  impresión (`@media print`).
- El mismo flujo está disponible para clientes en `apps/tenant/instrumentos/print_label.html`
  respetando los permisos de lectura existentes.
- En el portal de servicio se habilita la vista `apps/service/instrumentos/list_gages.html`.
  Desde la barra de acciones se puede usar **Imprimir etiqueta** (requiere
  seleccionar un único instrumento) para abrir `apps/service/instrumentos/print_label.html`
  en una pestaña nueva con el contenido formateado listo para impresión.

## Actualizar el tutorial de onboarding del portal tenant

El portal de clientes incluye un tutorial interactivo para guiar a los nuevos
usuarios (`public/apps/tenant/tutorial/index.html`). Para modificar su contenido o
adaptarlo a futuras iteraciones:

1. **Estructura y estilos**:
   - La maqueta HTML del tutorial vive en
     `public/apps/tenant/tutorial/index.html`.
   - Los estilos dedicados se encuentran en
     `public/apps/tenant/tutorial/tutorial.css`. Ajusta colores, tipografías o
     distribución desde ahí para mantener separado el diseño del resto del
     portal.
2. **Checklist y recorrido guiado**:
   - La lógica del checklist y del recorrido paso a paso se implementa en
     `public/apps/tenant/tutorial/tutorial.js`. Cada tarjeta marcada con
     `data-guide-step` participa en la guía interactiva. Modifica el orden o las
     descripciones actualizando los atributos `data-guide-title` y
     `data-guide-description`.
   - El progreso del checklist se guarda en `localStorage` con la llave
     `tenantTutorialChecklist`. Si se añaden elementos nuevos, bastará con crear
     un nuevo `<div class="checklist-item">` con un `input` y un `value` único
     para que queden registrados.
3. **Recursos externos**:
   - Los enlaces a documentación y videos viven en la sección "Recursos
     recomendados" dentro del propio HTML. Sustituye los `href` por la URL de tus
     manuales, videos o FAQs oficiales.
   - Si se publica material dentro del propio repositorio, colócalo en
     `public/assets/docs/` (o una ruta pública equivalente) y actualiza los
     enlaces para que el servidor embebido de PHP pueda servirlo sin ajustes
     adicionales.
4. **Enlaces rápidos del dashboard**:
   - El banner destacado y el acceso directo dentro de "Gestiones rápidas" se
     encuentran en `public/apps/tenant/index.html`. Cualquier cambio en el título
     del tutorial o en el ancla del checklist debe reflejarse en esos enlaces
     para mantener la navegación consistente.
5. **Permisos y accesos**:
   - `public/assets/scripts/portal-base.js` controla qué módulos ve cada rol. Si
     creas nuevas secciones relacionadas con el onboarding, añade su clave en el
     mapa `accessMap` y en `sectionMap` para que los roles de cliente la
     visualicen.

Tras editar los archivos anteriores reinicia el servidor (`npm start`) y prueba
el flujo completo en <http://localhost:8000/apps/tenant/tutorial/index.html> para
verificar que tooltips, checklist y recursos externos funcionan como se espera.

## Herramientas y pruebas
- `npm test`: ejecuta `node --test` contra los archivos en `tests/js/*.test.js`
  (incluye `calidad-workflows.test.js` para validar los helpers de calidad).

- `npm run test:server`: levanta el servidor de datos simulado utilizado en las
  pruebas de JavaScript.
- `npm run test:validation`: ejecuta únicamente la suite automatizada de validación IQ/OQ/PQ.
- `php tests/php/portal_domains_helper_test.php`: verifica la conexión disponible
  y comprueba los helpers de dominios por portal utilizando la base de datos
  activa.
- `php tools/scripts/run_validation.php --format=json --calibration-plan-id=123 --portal-scope=tenant`: corre el plan automatizado IQ/OQ (usar
  `--fixtures` en entornos sin BD). Ajusta el identificador a un plan de calibración válido de tu ambiente y define si el reporte impacta al panel interno (`internal`), al portal de clientes (`tenant`) o a ningún portal (`none`).
- `php tests/import_gages_test.php`: ejecuta una importación controlada con la
  fixture `tests/fixtures/instrumentos_import.csv` y valida la transacción.
- `php tests/php/quality_module_test.php`: comprueba el ciclo de vida de
  documentos, la asistencia de capacitaciones y el cierre de no conformidades.
- `php app/Modules/Calibraciones/alert_scheduler.php --daemon`: inicia el

  scheduler que envía alertas de calibraciones próximas.
- `php tools/scripts/calibration_workflow_test.php`: comprueba los escenarios de
  captura, aprobación, rechazo y filtrado de estados sin depender de la base de
  datos.

### Protocolo de alcance en reportes automatizados

Cada reporte o manual generado por `tools/scripts/run_validation.php` debe iniciar indicando explícitamente qué portal se ve impactado y repetir la aclaración antes de describir hallazgos o métricas. Utiliza `--portal-scope=internal`, `--portal-scope=tenant` o `--portal-scope=none` (valor por defecto) para alinear el mensaje con el módulo correspondiente. El script añadirá un encabezado del tipo `Impacto en: Panel interno (public/apps/internal)` y un recordatorio `Recordatorio de alcance: …` antes del detalle del IQ/OQ/PQ.

## API v1

La API HTTP expuesta en `public/backend/api/v1` permite integrar sistemas
externos con el inventario y la agenda de calibraciones. El acceso está protegido
por tokens Bearer emitidos mediante el endpoint `/backend/api/v1/tokens`.

### Autenticación y scopes

1. Solicita un token con un `POST` a `/backend/api/v1/tokens` enviando un JSON
   como el siguiente:
   ```json
   {
     "client_id": "laboratorio-externo",
     "scopes": ["instrumentos.read", "calibraciones.read"],
     "ttl": 3600,
     "rate_limit": { "limit": 60, "window": 60 }
   }
   ```
2. El servicio devuelve `access_token`, `expires_at`, los scopes asignados y el
   límite de peticiones configurado.
3. Incluye el token en el encabezado `Authorization: Bearer <token>` al consumir
   los recursos. Si lo prefieres puedes pasar el token en la cadena de consulta
   `?token=...`.

Los scopes disponibles actualmente son:

- `instrumentos.read`: lectura del catálogo de instrumentos.
- `calibraciones.read`: consulta del calendario y resultados de calibraciones.

Si el token no incluye el scope requerido la API devolverá `403`.

### Endpoints y capas técnicas

La interfaz HTML vive en `public/apps/*` (por ejemplo, el panel interno en
`public/apps/internal/` y el portal de clientes en `public/apps/tenant/`). Cada
vista interactúa con scripts PHP ubicados en `public/backend/*`, los cuales
incluyen a los módulos correspondientes en `app/Modules/*` y aprovechan las
utilidades compartidas de `app/Core/*` para sesiones, permisos y conexión a la
base de datos. El aislamiento multiempresa depende de `obtenerEmpresaId()`
dentro de estos módulos, asegurando que cada petición se filtre por la empresa
activa antes de consultar o modificar datos.

- `GET /backend/api/v1/instrumentos` (módulo `app/Modules/Api/V1`): devuelve el
  listado de instrumentos activos con datos básicos (código, nombre,
  ubicación). Requiere el scope `instrumentos.read` y filtra por empresa usando
  `obtenerEmpresaId()`.
- `GET /backend/api/v1/calibraciones` (módulo `app/Modules/Api/V1`): entrega las
  próximas calibraciones y resultados recientes. Requiere
  `calibraciones.read` y también aplica `obtenerEmpresaId()` para respetar el
  contexto de la sesión.

Todos los endpoints en `public/backend/*` respetan la cuota asignada al token.
Cuando se supera el límite configurado se responde con `429 Límite de
peticiones excedido`.

### Pruebas automatizadas

Puedes validar el flujo completo (emisión de tokens, scopes y rate limit) con:

- `php tests/php/api_tokens_helper_test.php`
- `php tests/php/api_guard_test.php`
- `npm test` (ejecuta `tests/js/api_v1.test.js` junto al resto de suites)

## Administración de usuarios

Después de actualizar el código del panel interno asegúrate de alinear los datos en la base con phpMyAdmin:

1. Entra a phpMyAdmin con tu usuario administrador y elige la base de datos del sistema ISO 17025.
2. Abre la pestaña **SQL**, pega la sentencia:

   ```sql
   UPDATE usuarios u
   JOIN roles r ON r.id = u.role_id
   SET u.portal_id = r.portal_id
   WHERE u.portal_id IS NULL
     AND r.portal_id IS NOT NULL;
   ```

3. Pulsa **Continuar** para ejecutar el ajuste y espera el mensaje de confirmación.
4. Para revisar el resultado, ejecuta inmediatamente `SELECT usuario, portal_id FROM usuarios WHERE portal_id IS NULL;`. Si la consulta no devuelve filas significa que todos los usuarios quedaron con su `portal_id` correcto.

Repite estos pasos cada vez que cargues cambios relacionados con cuentas para evitar inconsistencias entre roles y portales.

## Carga masiva de instrumentos

El portal de arrendatarios incorpora una vista dedicada para importar instrumentos
en bloque desde un archivo CSV o Excel (`apps/tenant/instrumentos/import_bulk.html`).
El flujo recomendado es el siguiente:

1. Descarga la plantilla oficial ubicada en
   `public/apps/tenant/instrumentos/templates/plantilla_instrumentos.csv` desde la
   propia vista de “Carga masiva”.
2. Completa, al menos, las columnas obligatorias con un registro por fila:
   - `Catalogo`
   - `Marca`
   - `Modelo`
   - `Serie`
   - `Codigo`
   - `Departamento`
   - `Ubicacion`
   - `Fecha Alta` (formatos admitidos: `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`).
   - De forma opcional puedes añadir `Fecha Baja` y `Observaciones`.
3. Guarda el archivo en formato CSV (delimitado por coma o punto y coma) o Excel
   (`.xlsx`).
4. Carga el archivo desde la vista de “Carga masiva”. El backend normaliza los
   catálogos (creando marcas, modelos o departamentos si no existen), valida
   duplicados por código, calcula el estado del instrumento mediante
   `derivarEstadoInstrumento()` y vincula cada registro con la empresa de la
   sesión activa.

El resumen de la importación incluye el detalle por fila (éxitos, fallos y sus
causas) y se persiste de forma opcional en la tabla `bulk_import_logs` para
rastrear historiales de carga.

## Flujo de recuperación de contraseña

El envío de enlaces de restablecimiento utiliza las utilidades de `app/Core/helpers`
con soporte para SMTP. Configura las variables `SMTP_*` y `APP_PUBLIC_URL` antes de
desplegar para que los correos apunten correctamente a la ruta pública
(`/apps/internal/usuarios/reset_password.html`).

## Administración de usuarios

Después de actualizar el código del portal interno, sincroniza los portales de los
usuarios desde phpMyAdmin para que cada cuenta quede ligada al portal correcto.

1. Entra a phpMyAdmin con la misma conexión que usa el sistema ISO 17025 y elige
   la base de datos del laboratorio desde el panel izquierdo.
2. Abre la pestaña **SQL**, copia y pega esta sentencia y presiona **Continuar**:

   ```sql
   UPDATE usuarios u
   JOIN roles r ON r.id = u.role_id
   SET u.portal_id = r.portal_id
   WHERE u.portal_id IS NULL
     AND r.portal_id IS NOT NULL;
   ```

3. phpMyAdmin mostrará cuántos registros se actualizaron. Anota el número y,
   enseguida, abre la tabla `usuarios`, ordena la columna `portal_id` y confirma
   que todas las filas tengan un número distinto de vacío.
4. Si alguna cuenta sigue con `portal_id` vacío, revisa su rol en la tabla
   `roles`, corrige el `portal_id` faltante y vuelve a ejecutar la consulta para
   completar la asignación.

## Carga manual del plan de riesgos

Cuando no sea posible ejecutar los scripts de normalización, utiliza
`app/Modules/ArchivosSql/plan_riesgos_normalizado.csv` como fuente para generar los
`INSERT`. Sigue las recomendaciones del archivo para validar integridad antes y
después de la importación.

## Más información

- `public/assets/scripts/portal-base.js`: lógica compartida de la interfaz
  (manejo de rutas, permisos, fetch con credenciales) reutilizada por los
  bundles por portal.
- `public/apps/internal/sidebar.html` y `topbar.html`: componentes reutilizables
  para la navegación.
- `app/validation_rules.md`: convenciones para validar entradas en nuevos módulos.

Mantén sincronizados los cambios entre `app/Modules/` y los stubs de `public/backend/`
cuando se agreguen nuevos endpoints HTTP.

## Pruebas manuales de navegación

1. **Portal interno**
   - Ingresa al panel y verifica que la opción “Servicio clientes” de la barra
     lateral abra `public/index.php?app=service` y que, al regresar, el menú
     interno conserve sus enlaces originales.
2. **Portal de servicio**
   - Inicia sesión con un usuario que posea el permiso `clientes_gestionar` y
     selecciona el portal de servicio mediante `?app=service`.
   - Confirma que la redirección aterrice en `apps/service/index.html`, que el
     topbar muestre el título “Portal de Servicio a Clientes” y que el menú
     lateral sólo liste clientes y calibraciones externas.
   - Navega a `clientes/panel.html`, `calibraciones/list_calibrations.html`,
     `calibraciones/alert_notifications.html` y
     `calibraciones/non_conformity_flow.html`, comprobando que cada vista
     reutiliza la misma barra lateral y topbar del portal.
3. **Portal de clientes (tenant)**
   - Con un usuario de rol “Cliente”, inicia sesión y verifica que el sistema
     conserve el contexto tenant (`apps/tenant/index.html`) sin exponer accesos
     al portal de servicio.
4. **Usuarios sin permiso de servicio**
   - Desde un usuario sin `clientes_gestionar`, intenta abrir cualquier URL bajo
     `/apps/service/` y valida que el sistema te redirija automáticamente al
     panel interno (`apps/internal/index.html`).

## Flujo de impresión de etiquetas (portal de servicio)

1. Ingresa al portal de servicio (`public/index.php?app=service`) y abre la
   vista de inventario en `apps/service/instrumentos/list_gages.html`.
2. Selecciona un solo registro de la tabla y pulsa **Imprimir etiqueta**. El
   botón habilitado consume `backend/instrumentos/gages/get_label_payload.php`
   para recuperar los datos normalizados del instrumento.
3. Se abrirá `apps/service/instrumentos/print_label.html` en una nueva pestaña.
   La vista carga `assets/scripts/labels.js`, genera el QR y usa `BASE_URL`
   para apuntar al backend del portal de servicio.
4. El código QR y el enlace “Ficha detallada” dirigen nuevamente a
   `print_label.html` con el `instrument_id` en la URL, de modo que cualquier
   usuario autorizado pueda escanearlo y consultar la información pública del
   instrumento antes de imprimir.
