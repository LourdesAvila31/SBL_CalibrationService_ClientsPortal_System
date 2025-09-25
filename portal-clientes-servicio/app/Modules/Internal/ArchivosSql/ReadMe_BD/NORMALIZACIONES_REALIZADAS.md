# Diccionario de normalización de catálogos

Este anexo documenta las variantes de texto detectadas en las columnas `Instrumento`, `Marca`, `Departamento responsable` y `Ubicación` de los archivos `LM_instrumentos.csv` y `PR_instrumentos.csv`. Las coincidencias se obtuvieron revisando ambos archivos con un script en Python que agrupa valores crudos contra su versión recortada (`strip`).

| Campo | Variante detectada | Valor normalizado |
| --- | --- | --- |
| Instrumento | `Manómetro ` | `Manómetro` |
| Instrumento | `Medidor multiparamétrico ` | `Medidor multiparamétrico` |
| Instrumento | `Registrador de temperatura ` | `Registrador de temperatura` |
| Instrumento | `Termómetro ` | `Termómetro` |
| Instrumento | `Termostato programable ` | `Termostato programable` |
| Marca | `Thermo Eutech ` | `Thermo Eutech` |
| Departamento responsable | `Fabricación ` | `Fabricación` |
| Departamento responsable | `Validación ` | `Validación` |
| Ubicación | `Almacén de Material de Empaque ` | `Almacén de Material de Empaque` |
| Ubicación | `Análisis Fisicoquímicos ` | `Análisis Fisicoquímicos` |
| Ubicación | `Análisis Físicoquímicos ` | `Análisis Fisicoquímicos` |
| Ubicación | `Control en Proceso ` | `Control en Proceso` |
| Ubicación | `Encapsulado y Abrillantado #5  ` | `Encapsulado y Abrillantado #5` |
| Ubicación | `Microbiología ` | `Microbiología` |
| Ubicación | `Recepción de Material de Envase y Empaque ` | `Recepción de Material de Envase y Empaque` |
| Ubicación | `Recubrimiento ` | `Recubrimiento` |

Las rutinas `stage_*.sql` y `normalize_*.sql` aplican este diccionario antes de ejecutar las transformaciones de limpieza (`TRIM`, normalización de fechas, etc.). Esto garantiza que los catálogos auxiliares (`catalogo_instrumentos`, `marcas`, `departamentos`) reciban siempre los mismos valores canónicos sin importar la presencia de espacios extra o acentos inconsistentes en los CSV originales.
