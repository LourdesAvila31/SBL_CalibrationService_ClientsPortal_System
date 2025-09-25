# Carga rápida de usuarios reales para el portal interno

Este procedimiento se aplica **exclusivamente al portal interno** (`public/apps/internal`). Sigue cada paso desde phpMyAdmin para que las altas cumplan con ISO/IEC 17025 y la NOM-059.

## 1. Preparar los identificadores
1. Abre phpMyAdmin y selecciona la base `iso17025`.
2. En la tabla `empresas`, confirma el `id` de tu laboratorio. Si usas el dato de ejemplo "SBL Pharma" el identificador es `1`.
3. En la tabla `portals`, revisa que el portal interno tenga `slug = internal` y anota su `id`.
4. Valida que los roles "Superadministrador", "Administrador", "Lector", "Sistemas" y "Developer" estén presentes en la tabla `roles` y tengan asignado el portal interno (`portal_id` que encontraste en el paso anterior). Si hay versiones específicas por empresa, el script las prioriza automáticamente.

## 2. Ejecutar el script de inserción
1. En phpMyAdmin entra a la pestaña **SQL**.
2. Copia el contenido de `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_internal_portal_usuarios.sql` y pégalo en la consola.
3. Antes de ejecutar, revisa la línea que declara `SET @empresa_id := ...`. Si tu empresa tiene un nombre distinto a "SBL Pharma", reemplaza ese texto o asigna el identificador directo: `SET @empresa_id := 3;` (sustituye `3` por el valor real).
4. Presiona **Continuar** para ejecutar el script. Las cuentas se insertan o actualizan sin duplicar correos ni usuarios, y las firmas internas quedan registradas con vigencia abierta.

## 3. Validar el alta
1. Abre la tabla `usuarios` y filtra por los correos institucionales para confirmar que el `portal_id` sea el del portal interno y que cada persona tenga su rol correspondiente.
2. Repite la verificación en `usuario_firmas_internas`; cada registro debe mostrar la firma corta (ej. `lavila`) y `vigente_hasta` vacío.
3. Ingresa al portal interno (`public/apps/internal`) con la contraseña temporal `Temporal2024*` para cada cuenta. El backend compara los permisos con base en el rol asignado, por lo que podrás validar menús y módulos visibles para Superadministración, Administración, Sistemas y Lectura.
4. Cambia la contraseña durante el primer acceso y documenta la entrega conforme a tus políticas de seguridad.

## 4. ¿Qué hace el script?
- Asegura que todas las cuentas se relacionen con el portal interno y con la empresa que definas en `@empresa_id`.
- Reutiliza los roles ya cargados en la tabla `roles` en lugar de crear duplicados.
- Aplica el mismo hash seguro (`$2y$12$MZtPhyrZ5UquOUEx9imlHuw2m/dplfc7dLbjexx5OkgY3HocoF3N2`) para la contraseña temporal `Temporal2024*`.
- Inserta las firmas internas con su clave corta y las deja vinculadas al usuario que ejecutó el alta (`creado_por` utiliza el ID de `lavila`).
- Actualiza firmas previas (si existían) para mantener el correo corporativo correcto.

## 5. Referencias rápidas
- Script SQL: `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_internal_portal_usuarios.sql`
- Portal afectado: **portal interno** (`public/apps/internal`)
- Contraseña temporal establecida: `Temporal2024*`
- Campos clave en `usuarios`: `usuario`, `correo`, `role_id`, `portal_id`, `empresa_id`, `activo`, `sso`
- Campos clave en `usuario_firmas_internas`: `usuario_id`, `firma_interna`, `vigente_desde`, `vigente_hasta`

Con esto tendrás a todo el personal listo para realizar pruebas de permisos, seguridad y trazabilidad dentro del portal interno sin modificar otras áreas del sistema.
