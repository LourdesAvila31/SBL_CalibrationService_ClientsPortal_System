## INSTRUCCIONES DE LOGIN PARA ISO 17025

### USUARIOS POR DEFECTO

El sistema incluye usuarios de prueba con las siguientes credenciales:

| Usuario        | Email                    | Contraseña        | Rol                  |
|----------------|--------------------------|-------------------|----------------------|
| `superadmin`   | superadmin@ejemplo.com   | `SuperAdmin!2024` | Superadministrador   |
| `admin`        | admin@ejemplo.com        | `Admin!2024`      | Administrador        |
| `supervisor`   | supervisor@ejemplo.com   | `Supervisor!2024` | Supervisor           |
| `operador`     | operador@ejemplo.com     | `Operador!2024`   | Operador             |
| `lector`       | lector@ejemplo.com       | `Lector!2024`     | Lector               |
| `cliente`      | cliente@ejemplo.com      | `Cliente!2024`    | Cliente              |
| `sistemas`     | sistemas@ejemplo.com     | `Sistemas!2024`   | Sistemas             |
| `developer`    | developer@ejemplo.com    | `Developer#2024`  | Developer            |

### FORMAS DE ACCESO

Puede iniciar sesión usando:
- **Usuario**: Escriba el nombre de usuario (ej: `admin`)
- **Email**: Escriba el correo completo (ej: `admin@ejemplo.com`)

En ambos casos, use la contraseña correspondiente.

### ERRORES COMUNES

1. **"Usuario no encontrado"**: Verifique que esté escribiendo el usuario o email correctamente
2. **"Contraseña incorrecta"**: Verifique que la contraseña coincida con la indicada en esta guía
3. **"Usuario inactivo"**: El usuario existe pero está deshabilitado
4. **"Error al conectar con la base de datos"**: Problema de conexión a MySQL

### SOLUCIÓN DE PROBLEMAS

Si el login no funciona:

1. **Verifique la base de datos**: Asegúrese de que MySQL esté corriendo y la base de datos `iso17025` exista
2. **Ejecute el setup**: Use el script `backend/archivos_sql/add_tables.sql` seguido de `add_seed_data.sql` (y `add_seed_data_demo.sql` sólo si requiere usuarios de prueba)
3. **Revise permisos**: El usuario de MySQL debe tener permisos sobre la base de datos
4. **Configuración**: Verifique `backend/db_config.php` para la configuración de conexión

### CREDENCIALES RECOMENDADAS PARA PRUEBAS

**Para administración completa:**
- Usuario: `admin`
- Contraseña: `Admin!2024`

**Para operación diaria:**
- Usuario: `operador`
- Contraseña: `Operador!2024`

**Para seguimiento técnico en bitácoras (QA/Dev):**
- Usuario: `developer`
- Contraseña: `Developer#2024`

> Estas credenciales se crean únicamente al importar `add_seed_data_demo.sql`. En entornos productivos define usuarios reales desde el panel de administración.

### NOTAS IMPORTANTES

- Los usuarios deben estar **activos** (`activo = 1`) para poder iniciar sesión
- Las contraseñas están hasheadas con `password_hash()` de PHP
- El sistema soporta tanto nombre de usuario como email para login
- Cada usuario tiene un rol específico con permisos diferenciados