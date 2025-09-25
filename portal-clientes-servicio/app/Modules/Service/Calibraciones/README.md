# Controladores del portal de servicios

Esta carpeta replica los endpoints de calibraciones expuestos en los portales de cliente (tenant) e interno, pero aplicando las validaciones propias del portal de servicios.

* Todos los scripts aplican `ensure_portal_access('service')` para reforzar que solo se consuman desde el contexto correcto.
* Las consultas se aíslan por empresa con `obtenerEmpresaId()`, manteniendo el cumplimiento multiempresa requerido por ISO/IEC 17025.
* Si agregas nuevos endpoints compartidos, recuerda mantener esta separación y actualizar también los includes en `public/backend/calibraciones` para enrutar según `session_portal_slug()`.
