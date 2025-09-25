# Portal Interno ISO 17025 – Requerimientos y Especificaciones

## 1. Alcance
- **Sistema objetivo:** Portal interno web para la gestión de instrumentos, plan de riesgos, auditorías y certificados de SBL Pharma.
- **Fuentes de datos:** CSV normalizados generados desde `Archivos_CSV_originales/*_v2.csv` y scripts Python en `app/Modules/Internal/ArchivosSql/Normalize_Python`.
- **Cobertura:** Usuarios autenticados mediante el módulo interno (`app/Modules/Internal/Usuarios`), operaciones CRUD sobre inventario/calendarios/certificados, auditoría y reporteo. Se excluyen portales de clientes externos.

## 2. Requerimientos de usuario (RU)
| Código | Clasificación | Descripción alineada al portal | Implementación / referencia | Estado |
| --- | --- | --- | --- | --- |
| RU-001 | Obligatorio | El portal debe contar con protección, integridad y respaldo de la información, con trazabilidad de auditoría. | Respaldos en `app/Modules/Internal/Backups/schedule_backups.php` y logging en `app/Modules/Internal/Auditoria/audit.php`. | ?? Requiere validar ejecución programada periódica. |
| RU-002 | Obligatorio | El acceso debe estar controlado por roles y credenciales únicas. | Autenticación en `app/Modules/Internal/Usuarios/login.php`, perfiles en `usuarios`/`roles`. | ? Implementado. |
| RU-003 | Obligatorio | Definir políticas de renovación de contraseña/códigos. | No existe política automática; solo hash con `password_hash`. | ? Pendiente de parametrizar rotación/verificación documental. |
| RU-004 | Obligatorio | Bloqueo después de intentos fallidos de ingreso. | No hay throttling en `login.php`. | ? Requerido. |
| RU-005 | Obligatorio | Protección con herramientas de seguridad TI alineadas a procedimientos. | Hash de contraseñas, variables de entorno (`config/app.php`), controles de sesión. | ?? Complementar con monitoreo y hardening del stack. |
| RU-006 | Obligatorio | Registrar y auditar toda acción sobre los módulos críticos. | Scripts `convert_audit_trail_csv.py`, `app/Modules/Internal/Auditoria` y `insert_audit_trail.sql` generan historial con firma interna. | ? Implementado. |
| RU-007 | Obligatorio | Prevenir eliminación no autorizada: conservar evidencia en audit trail. | Triggers lógicos en UI + almacenamiento de `valor_anterior/valor_nuevo` en `audit_trail`. | ? Siempre que se carguen los INSERT generados. |
| RU-008 | Obligatorio | Cambios al sistema sujetos a control documentado. | Procedimientos en `docs/registro_cambios_interno.md` y repositorio Git. | ?? Requiere aprobación formal al liberar. |
| RU-009 | Obligatorio | Operado solo por personal capacitado. | Manuales en `docs/internal_portal_user_import.md`, evidencia de capacitación en QA. | ?? Depende de capacitación externa. |
| RU-010 | Obligatorio | Funcionamiento en equipos con Google Chrome mínimo y hardware definido. | Portal responsive (`public/apps/internal/*`), probado en Chrome 114+. | ? Documentado (ver §4). |
| RU-011 | Obligatorio | Acceso restringido al personal de SBL. | Control por correos corporativos y roles (`insert_internal_portal_usuarios.sql`). | ? |
| RU-012 | Obligatorio | Permitir modificación del inventario por Ingenieros/Supervisor según rol. | Roles `Administrador`, `Developer` en `usuarios` + vistas `instrumentos`. | ? |
| RU-013 | Obligatorio | Visualizar/gestionar Inventario, Plan de riesgos y Certificados. | Módulos front `public/apps/internal/instrumentos`, `planeacion`, `calibraciones`. | ? |
| RU-014 | Obligatorio | Emisión de reportes impresos con firmas y paginación. | Vistas en `public/apps/internal/reportes/reports.html` y endpoints de exportación PDF. | ?? Pendiente consolidar plantillas con firmas.

## 3. Especificaciones funcionales (EF)
| Código | Relación | Descripción funcional (portal) | Implementación / código | Estado |
| --- | --- | --- | --- | --- |
| EF-001 | RU-001 | Mostrar alertas cuando un registro se encuentra protegido (solo lectura) o se intenta modificar sin permisos. | Componente `public/apps/internal/instrumentos/list_gages.html` (modales de validación) + validaciones en `app/Modules/Internal/Instrumentos/update.php`. | ? |
| EF-002 | RU-002 / RU-011 / RU-012 | Documentación controla la edición: roles Lectura/Edición administrados por Documentación y Validación. | Roles y seeds en `insert_internal_portal_usuarios.sql`; endpoints `app/Modules/Internal/Usuarios/*`. | ? |
| EF-003 | RU-004 | Bloquear sesión tras consecutivos fallos de autenticación. | No disponible. | ? |
| EF-004 | RU-006 | Registrar automáticamente usuario, fecha, hoja, campo, valor anterior y nuevo en el audit trail. | `convert_audit_trail_csv.py` + `insert_audit_trail.sql` generan registros con `instrumento_codigo`, coordenadas Excel y firma interna. | ? |
| EF-005 | RU-007 | Proteger el audit trail contra modificaciones no autorizadas. | Tabla `audit_trail` sin DELETE en UI; scripts solo realizan INSERT. | ? (se recomienda permisos MySQL restrictivos). |
| EF-006 | RU-007 / RU-014 | Permitir exportar el audit trail en PDF/CSV para evidencia. | UI `public/apps/internal/auditoria` consume API `app/Modules/Internal/Auditoria/list.php`. | ?? Exportación PDF parcial (CSV listo). |
| EF-007 | RU-012 / RU-013 | Registrar campos de inventario (Instrumento, Marca, Modelo, Serie, Código, Responsable, Ubicación, Fechas). | Normalización `convert_instrumentos_csv.py` y `insert_instrumentos.sql`. | ? |
| EF-008 | RU-013 | Sección de Calibración/Verificación replica datos del inventario salvo fechas de baja. | Vistas `public/apps/internal/planeacion` + script `generate_plan_riesgos.py`. | ? |
| EF-009 | RU-013 | Apartado de Calibración incluye análisis de riesgo, frecuencia y observaciones. | CSV `normalize_plan_riesgos.csv` y módulo Planeación. | ? |
| EF-010 | RU-013 | Apartado de Calibración determina clase de riesgo, tipo y fecha programada. | Campos calculados en `generate_plan_riesgos.py` (`tipo_calibracion`, `fecha_actualizacion`). | ? |
| EF-011 | RU-013 | Apartado de Certificados almacena historiales con vínculos a PDFs. | `generate_cert_calibrations.py` + vista `public/apps/internal/calibraciones`. | ? |
| EF-012 | RU-014 | Formato imprimible del inventario con logo, paginación y firmas. | Plantilla `public/apps/internal/reportes/reports.html` + scripts PDF. | ?? Ajustar espacio para firmas y numeración. |

## 4. Especificaciones de diseño (ED)
| Código | Relación | Descripción de diseño | Evidencia en código / docs | Estado |
| --- | --- | --- | --- | --- |
| ED-001 | RU-002 | El ingreso se realiza mediante cuentas corporativas y contraseña; se puede publicar un acceso federado si se integra SSO. | Login HTML `public/apps/internal/index.html`, backend `Usuarios/login.php`. | ? (SSO no habilitado). |
| ED-002 | RU-002 | Portal disponible en Google Chrome (montado como SPA con Bootstrap 5). | Recursos en `public/apps/internal/*` incluyen polyfills y responsive design. | ? |
| ED-003 | RU-006 | Evaluación de riesgos basada en PNO-SC-07 (parámetros definidos en plan de riesgos). | Scripts normalizadores + `docs/validation/*`. | ? |
| ED-004 | RU-003 | Política de rotación de contraseñas documentada en PNO-SC-07. | No automatizada en código. | ? |
| ED-005 | RU-004 | Bloqueo de usuario por intentos fallidos siguiendo PNO-SC-07. | Falta implementación en `login.php`. | ? |
| ED-006 | RU-005 | Información almacenada en unidad controlada y con copia de seguridad (Google Drive / servidores internos). | `app/Modules/Internal/Backups` y documentación `PNO-SC-02`. | ?? Confirmar integración Drive. |
| ED-007 | RU-008 | Cambios del sistema regulados por PNO-GC-13 (control cambios). | Registrados en `docs/registro_cambios_interno.md`. | ? (requiere seguimiento). |
| ED-008 | RU-009 | Capacitación del personal conforme PNO-RH-16. | Evidencia externa; portal soporta múltiples roles (`users`). | ?? Depende de RRHH. |
| ED-009 | RU-009 | Uso descrito en PNO-VA-23 (Alta/baja/movimiento de instrumentos). | Scripts de normalización garantizan integridad. | ? |
| ED-010 | RU-010 | Requerimientos de hardware (Intel Pentium 4+, Windows 10+). | Declarados en este documento; portal probado en Chrome. | ? (documental). |
| ED-011 | RU-013 | Formato de listas maestras con campos obligatorios según PNO-GC-44. | Vistas `public/apps/internal/reportes` y seeds `insert_instrumentos.sql`. | ? |

## 5. Supuestos y dependencias
- Los backups programados requieren cron externo o Task Scheduler apuntando a `schedule_backups.php`.
- Para consolidar bloqueo de sesión e higiene de contraseñas (RU-003, RU-004), será necesario extender `login.php` y la tabla `usuarios` con campos de control (intentos fallidos, fecha expiración).
- La exportación PDF del audit trail (EF-006, RU-014) depende de integrar la librería de generación (`dompdf`) o reutilizar los reportes existentes.
- La validez regulatoria presume que los INSERT generados (`SBL_inserts`) se ejecutarán en la base de datos productiva sin modificaciones manuales.

## 6. Próximos pasos sugeridos
1. Implementar contador de intentos y expiración de contraseñas en `app/Modules/Internal/Usuarios/login.php` + migraciones.
2. Completar plantilla PDF con campos de firma y paginación (`public/apps/internal/reportes`).
3. Automatizar la ejecución de respaldos y documentar evidencias.
4. Publicar procedimiento de capacitación y evidencia de aceptación de usuarios finales.

