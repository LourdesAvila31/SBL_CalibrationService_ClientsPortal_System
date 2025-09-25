# Integración de referencias de calidad en calibraciones

Este módulo permite vincular documentos de calidad, capacitaciones y acciones correctivas a los registros de calibración para mantener el contexto completo del ciclo de vida del instrumento.

## Tabla `calibracion_referencias`

* **Objetivo:** almacenar las asociaciones entre una calibración y cualquier elemento relevante de calidad.
* **Columnas principales:**
  * `calibracion_id` (FK a `calibraciones`).
  * `empresa_id` para garantizar el aislamiento multiempresa.
  * `tipo` (`documento_calidad`, `capacitacion`, `accion_correctiva`).
  * `referencia_id` (opcional, enlaza con el identificador numérico del módulo fuente).
  * `etiqueta` y `enlace` como metadatos flexibles (texto o URL interna/externa).
* **Reglas:** las filas se eliminan en cascada cuando se borra la calibración.

## Helper `calibration_references.php`

El helper centraliza la lógica de normalización y persistencia:

* `calibration_references_allowed_types()` expone los tipos disponibles.
* `calibration_references_parse_payload()` limpia la carga útil recibida desde formularios o APIs.
* `calibration_references_sync()` sustituye las referencias asociadas a una calibración.
* `calibration_references_fetch()` devuelve las referencias agrupadas por `calibracion_id`.

Utiliza estas funciones al extender APIs o crear nuevos flujos para mantener un único punto de validación.

## Hooks en formularios

Las vistas de captura (`apps/tenant/calibraciones/add_calibration.html` y `apps/internal/calibraciones/add_calibration.html`) incluyen un constructor visual de referencias. Este componente genera un JSON que se envía en el campo oculto `references_payload`.

* Los formularios existentes que no envían el campo mantienen intactas las referencias gracias a la validación condicional en `edit_calibration.php`.
* Los módulos externos pueden poblar `references_payload` con un arreglo JSON siguiendo la estructura `{ type, reference_id?, label?, link? }`.

## Listados

`list_calibrations.php` añade el arreglo `referencias` a cada elemento. Las vistas listan las referencias como insignias con acceso directo a los enlaces.

Cuando se añadan nuevos tipos, basta con ampliar el helper y actualizar las etiquetas de `referenceTypeLabels` en las vistas para reflejar los cambios sin tocar el backend.
