# Plan de Validación IQ/OQ/PQ

La validación del sistema ISO 17025 se estructura en tres fases complementarias: Instalación/Configuración (IQ), Calificación Operacional (OQ) y Calificación de Desempeño (PQ). Cada fase contiene objetivos, prerrequisitos y listas de verificación que deben completarse para considerar la validación exitosa.

## Instalación/Configuración (IQ)

**Objetivo:** asegurar que la plataforma esté desplegada en un entorno controlado siguiendo las especificaciones técnicas y de seguridad.

**Prerrequisitos:**

- Infraestructura aprobada (servidores, red, certificados TLS si aplica).
- Acceso administrativo a la base de datos MySQL y al servidor web.
- Variables de entorno de conexión (`DB_HOST`, `DB_USER`, `DB_PASS`, `DB_NAME`) configuradas.
- Copia de los scripts de instalación (`app/Modules/Internal/ArchivosSql/add_tables.sql`, `app/Modules/Internal/ArchivosSql/add_seed_data.sql` y, si se requieren cuentas demo, `app/Modules/Internal/ArchivosSql/add_seed_data_demo.sql`) y del plan de respaldo.

**Checklist IQ:**

1. [ ] Confirmar versión de SO, PHP y servidor web frente a la matriz aprobada.
2. [ ] Verificar conectividad a la base de datos desde el servidor de la aplicación.
3. [ ] Ejecutar migraciones/tablas iniciales y comprobar ausencia de errores.
4. [ ] Crear roles críticos (`superadministrador`, `administrador`, `supervisor`, `operador`, `cliente`, `sistemas`).
5. [ ] Cargar permisos base y asociarlos a cada rol según política.
6. [ ] Configurar tareas programadas (alertas de calibración, respaldos) y documentar su habilitación.
7. [ ] Registrar evidencias (capturas/logs) del despliegue exitoso.

## Calificación Operacional (OQ)

**Objetivo:** comprobar que las funcionalidades clave operan según los requerimientos del laboratorio.

**Prerrequisitos:**

- Sistema desplegado con datos maestros (usuarios, clientes, instrumentos) cargados.
- Usuarios de prueba con roles diferenciados para evaluar permisos.
- Servidor de aplicaciones disponible y accesible vía HTTP/HTTPS.
- Script `tools/scripts/run_validation.php` actualizado y accesible.

**Checklist OQ:**

1. [ ] Autenticación y cierre de sesión correctos para usuario interno y cliente.
2. [ ] Captura de nueva calibración y almacenamiento de datos obligatorios.
3. [ ] Flujo de aprobación/rechazo con trazabilidad (supervisor/administrador).
4. [ ] Generación de reportes PDF/CSV y disponibilidad de descarga.
5. [ ] Restricciones de permisos respetadas (usuarios sin privilegios no modifican calibraciones).
6. [ ] Registro en bitácora/auditoría de operaciones críticas.
7. [ ] Ejecución de `php tools/scripts/run_validation.php --format=json --calibration-plan-id=<ID> --portal-scope=<alcance>` sin errores (ajustar `<ID>`
       a un plan válido) y archivo de resultados archivado. Define `<alcance>` como `internal`, `tenant` o `none` para que el reporte indique y repita el portal impactado.


## Calificación de Desempeño (PQ)

**Objetivo:** validar que el sistema mantiene el rendimiento y comportamiento esperado bajo cargas y escenarios reales.

**Prerrequisitos:**

- Ambiente que represente el volumen típico y pico de operaciones.
- Datos históricos o simulados que cubran la gama de instrumentos/calibraciones.
- Plan de casos de uso priorizados por el laboratorio.

**Checklist PQ:**

1. [ ] Procesar lote representativo de órdenes de calibración (volumen normal).
2. [ ] Simular carga pico (usuarios concurrentes o importaciones masivas) y monitorear tiempos de respuesta.
3. [ ] Verificar estabilidad de notificaciones/alertas durante la jornada simulada.
4. [ ] Asegurar integridad de reportes generados al finalizar el ciclo.
5. [ ] Documentar métricas de desempeño (tiempos, errores, utilización de recursos) y compararlas con los criterios de aceptación.
6. [ ] Revisar logs del sistema y base de datos para detectar anomalías.
7. [ ] Aprobar formalmente los resultados con responsables del laboratorio.

## Evidencia y almacenamiento

- Todos los resultados deben almacenarse en el repositorio de calidad siguiendo el control documental del laboratorio.
- Adjuntar bitácoras de ejecución, resultados del script de validación y firmas de aprobación.
- Registrar desviaciones y planes de acción en caso de encontrar fallos en IQ/OQ/PQ.

