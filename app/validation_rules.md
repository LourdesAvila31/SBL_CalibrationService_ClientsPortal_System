# Reglas de formato y validación

Para garantizar la integridad de los datos y prevenir inyecciones SQL todas las
entradas provenientes de formularios o parámetros de URL deben cumplir con las
siguientes normas:

- **Uso de `filter_input`/`filter_var`:** todo valor recibido por `$_GET` o
  `$_POST` debe validarse y sanitizarse antes de utilizarse.
- **Parámetros numéricos:** se deben validar con `FILTER_VALIDATE_INT` y
  rechazarse si no son enteros válidos.
- **Cadenas de texto:** aplicar `trim()` para eliminar espacios innecesarios y
  validar su contenido según contexto (longitud mínima, caracteres permitidos).
- **Consultas SQL:** está prohibido concatenar variables dentro de las
  sentencias. Utilizar siempre `prepare()` y `bind_param()`.
- **Listas controladas:** cuando un parámetro sólo admite ciertos valores (p.ej.
  `filtro` en la consulta de instrumentos) debe verificarse contra una lista
  blanca y descartarse cualquier valor no reconocido.
- **Subida de archivos:** comprobar el resultado de `$_FILES` y mover los
  archivos a ubicaciones seguras.

Estas pautas son obligatorias para futuras integraciones y deben revisarse en
cada nuevo módulo del backend.