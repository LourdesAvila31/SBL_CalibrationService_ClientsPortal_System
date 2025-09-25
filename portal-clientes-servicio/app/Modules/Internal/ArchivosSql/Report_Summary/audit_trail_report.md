# Resumen de hallazgos en `audit_trail.csv`

El script [`audit_trail_report.py`](./audit_trail_report.py) genera dos salidas:

- [`audit_trail_report_summary.csv`](./audit_trail_report_summary.csv): detalle
  por fila (número de renglón en la hoja *Instrumentos*) con los conteos de
  eventos detectados en las columnas A–I.
- [`audit_trail_report_totals.json`](./audit_trail_report_totals.json): totales
  agregados de las métricas clave y el número de coincidencias en la hoja
  *Calibración/Verificación*.

## Cómo reproducir el cálculo

```bash
python app/Modules/Internal/ArchivosSql/audit_trail_report.py
```

El script normaliza los valores equivalentes a vacío (`""`, `NA`, `ND`,
`N/A`, `NULL`) y agrupa los movimientos por fila. Una alta se confirma cuando
las columnas **D**, **E**, **F**, **G** y **H** tienen datos y, en algún momento
del historial, la columna **A** recibió un valor distinto de vacío. Los
movimientos de departamento (**F**) y ubicación (**G**) se contabilizan cada vez
que el nuevo texto difiere del anterior ignorando mayúsculas/minúsculas; los
`regresos_almacen` agrupan todas las actualizaciones de ubicación cuyo nuevo
valor contiene la cadena “almacen”.

Además del resumen por fila, el script revisa la hoja
**Calibración/Verificación** para detectar registros que incluyen las palabras
clave “rechazado”, “baja” o “calibración limitada” (sin distinguir tildes).

## Totales actuales

| Métrica                        | Total |
| ------------------------------ | ----- |
| Altas confirmadas              | 38    |
| Asignaciones de código         | 113   |
| Cambios de código              | 17    |
| Movimientos de departamento    | 81    |
| Movimientos de ubicación       | 355   |
| Regresos a almacén             | 100   |
| Actualizaciones de fecha de alta | 92  |
| Actualizaciones de fecha de baja | 62  |
| Coincidencias en Calibración/Verificación | 44 |

> Nota metodológica: para reproducir los valores es indispensable ejecutar el
> script con la versión actual de `audit_trail.csv` y respetar la normalización
> de textos descrita arriba. Las coincidencias en la hoja de calibración se
> contabilizan por fila, incluso cuando varias palabras clave aparecen en un
> mismo registro.
