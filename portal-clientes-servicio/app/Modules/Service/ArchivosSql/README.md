# Archivos de soporte para importaciones reales

Este directorio conserva la estructura esperada por el **portal de servicios**
(`public/apps/service`), pero **ya no** incluye datos de demostración. Cada
archivo `.sql` funciona como plantilla para que el laboratorio cargue únicamente
información verificada desde los portales internos o de clientes.

## Procedimiento recomendado

1. Genera los paquetes `.zip` exportados desde el portal de clientes con datos
   reales de la empresa que desees validar.
2. Sustituye el contenido de los archivos ubicados en
   `app/Modules/Tenant/ArchivosSql/<Empresa>/Archivos_BD_SQL/` con la exportación
   resultante.
3. Comprime el directorio de la empresa y súbelo desde la sección **Importaciones**
   del portal de servicios.
4. Verifica los avisos del sistema antes de ejecutar los INSERT para asegurar que
   la empresa y los roles configurados sean los correctos.

## Mantenimiento

- Los archivos `insert_*.sql` se mantienen como referencias vacías para evitar
  contaminar ambientes con datos ficticios.
- Documenta en control de cambios cualquier ajuste que agregue datos reales a
  estas plantillas para preservar la trazabilidad ISO/IEC 17025 y NOM-059.
