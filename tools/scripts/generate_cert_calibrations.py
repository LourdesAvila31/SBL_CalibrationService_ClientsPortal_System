"""Generador de SQL para programaciones de calibración históricas.

Este script transforma el archivo `CERT_instrumentos_original_v2.csv` —ubicado en
`app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/`— en un conjunto de
sentencias SQL idempotentes listas para importar en la tabla `calibraciones`.
Cada fecha marcada en el CSV se convierte en un `INSERT` protegido con
`WHERE NOT EXISTS` para evitar duplicados, conservando el aislamiento por
empresa (`empresa_id`).

Uso típico:

```bash
python tools/scripts/generate_cert_calibrations.py \
    --empresa-id 1 \
    --input app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/CERT_instrumentos_original_v2.csv \
    --output app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_calibraciones_certificados.sql
```
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

BASE_DIR = Path(__file__).resolve().parents[2]
CSV_DIR = BASE_DIR / 'app/Modules/Internal/ArchivosSql/Archivos_CSV_originales'

SPANISH_MONTHS = {
    "ENE": 1,
    "FEB": 2,
    "MAR": 3,
    "ABR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SEP": 9,
    "SET": 9,
    "OCT": 10,
    "NOV": 11,
    "DIC": 12,
}

PERIOD_MAP = {
    "PERIODO 1": "P1",
    "PERIODO 2": "P2",
    "EXTRAORDINARIO": "EXTRA",
}

NA_VALUES = {"", "NA", "ND", "N/A", "null", "NULL"}

DATE_REGEX = re.compile(r"(\d{1,2})[\-/]([A-Za-zÁÉÍÓÚáéíóú\.]+)[\-/](\d{2,4})")


@dataclass(frozen=True)
class CalibrationEvent:
    """Representa una calibración programada proveniente del CSV CERT."""

    codigo: str
    fecha: dt.date
    periodo: str
    periodo_label: str
    year: int
    requerimiento: Optional[str]
    frecuencia_meses: Optional[int]


def _resolve_cert_path() -> Path:
    candidates = [
        CSV_DIR / 'CERT_instrumentos_original_v2.csv',
        CSV_DIR / 'CERT_instrumentos_original.csv',
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convierte CERT_instrumentos_original_v2.csv en INSERT idempotentes "
            "para la tabla calibraciones."
        )
    )
    parser.add_argument(
        "--empresa-id",
        type=int,
        default=1,
        help="Identificador de la empresa destino (coincide con empresa_id en la base).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=_resolve_cert_path(),
        help="Ruta al CSV original exportado de la hoja CERT.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/"
            "insert_calibraciones_certificados.sql"
        ),
        help="Archivo SQL de salida listo para importar en phpMyAdmin.",
    )
    return parser.parse_args(argv)


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    clean = value.strip()
    if clean in NA_VALUES:
        return None
    return clean


def extract_date(value: str) -> Optional[dt.date]:
    """Obtiene la primera fecha reconocible en una celda del CSV."""

    if not value:
        return None

    match = DATE_REGEX.search(value)
    if not match:
        return None

    day_raw, month_raw, year_raw = match.groups()
    day = int(day_raw)
    month_key = month_raw.strip().replace(".", "").upper()
    month_key = month_key.replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
    month = SPANISH_MONTHS.get(month_key)
    if not month:
        return None

    year_int = int(year_raw)
    if year_int < 100:
        year_int += 2000 if year_int <= 79 else 1900

    try:
        return dt.date(year_int, month, day)
    except ValueError:
        return None


def add_months(base_date: dt.date, months: int) -> dt.date:
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base_date.day, _days_in_month(year, month))
    return dt.date(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def sql_escape(value: str) -> str:
    return value.replace("'", "''")


def iter_events(
    csv_path: Path, empresa_id: int
) -> Iterable[CalibrationEvent]:  # pylint: disable=unused-argument
    """Lee el CSV y genera los eventos de calibración detectados."""

    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {csv_path}")

    events: List[CalibrationEvent] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            year_row = _next_relevant_row(reader)
            header_row = _next_relevant_row(reader)
        except StopIteration as exc:
            raise ValueError("El CSV no contiene encabezados suficientes.") from exc

        base_columns = 9
        period_columns = []
        for idx, label in enumerate(header_row):
            if idx < base_columns:
                continue
            year_value = normalize_text(year_row[idx] if idx < len(year_row) else None)
            period_label = normalize_text(label)
            if not year_value or not period_label:
                continue
            period_key = PERIOD_MAP.get(period_label.upper())
            if not period_key:
                continue
            try:
                year_int = int(year_value)
            except ValueError:
                continue
            period_columns.append((idx, period_key, period_label, year_int))

        for row in reader:
            if not any(normalize_text(cell) for cell in row):
                continue
            if len(row) < base_columns:
                continue

            codigo = normalize_text(row[4])
            if not codigo:
                continue

            requerimiento = normalize_text(row[7])
            frecuencia_raw = normalize_text(row[8])
            frecuencia_meses: Optional[int] = None
            if frecuencia_raw:
                try:
                    frecuencia_meses = int(float(frecuencia_raw))
                except ValueError:
                    frecuencia_meses = None

            for idx, periodo, periodo_label, year_int in period_columns:
                if idx >= len(row):
                    continue
                celda = normalize_text(row[idx])
                if not celda:
                    continue
                fecha = extract_date(celda)
                if not fecha:
                    continue
                events.append(
                    CalibrationEvent(
                        codigo=codigo,
                        fecha=fecha,
                        periodo=periodo,
                        periodo_label=periodo_label,
                        year=year_int,
                        requerimiento=requerimiento,
                        frecuencia_meses=frecuencia_meses,
                    )
                )

    return sorted(events, key=lambda e: (e.codigo, e.fecha, e.periodo))


def build_sql(events: Sequence[CalibrationEvent], empresa_id: int) -> str:
    if not events:
        return "-- No se detectaron eventos en el CSV proporcionado.\n"

    lines: List[str] = []
    lines.append("-- Calibraciones programadas generadas desde CERT_instrumentos_original_v2.csv")
    lines.append(f"-- Empresa destino: {empresa_id}")
    lines.append("START TRANSACTION;")

    for event in events:
        fecha_proxima: Optional[dt.date] = None
        if event.frecuencia_meses and event.frecuencia_meses > 0:
            try:
                fecha_proxima = add_months(event.fecha, event.frecuencia_meses)
            except ValueError:
                fecha_proxima = None

        observaciones_parts = [
            f"{event.periodo_label} {event.year}",
            "Fuente: CERT_instrumentos_original_v2.csv",
        ]
        if event.requerimiento:
            observaciones_parts.insert(0, f"Requerimiento: {event.requerimiento}")
        observaciones = sql_escape(". ".join(observaciones_parts))

        tipo = sql_escape(event.requerimiento) if event.requerimiento else "Calibración programada"

        lines.append(
            f"SET @instrumento_id = ("
            f"SELECT id FROM instrumentos WHERE codigo = '{sql_escape(event.codigo)}' "
            f"AND empresa_id = {empresa_id} LIMIT 1);")
        lines.append(
            "INSERT INTO calibraciones (" +
            "instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones" +
            ")"
        )
        values_line = (
            "SELECT @instrumento_id, {empresa_id}, '{tipo}', '{fecha}', '{periodo}', {fecha_proxima}, NULL, '{observaciones}' "
            "FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS ("
            "SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id "
            "AND empresa_id = {empresa_id} AND fecha_calibracion = '{fecha}' AND periodo = '{periodo}'"
            ");"
        )
        fecha_sql = event.fecha.isoformat()
        fecha_proxima_sql = f"'{fecha_proxima.isoformat()}'" if fecha_proxima else "NULL"
        lines.append(
            values_line.format(
                empresa_id=empresa_id,
                tipo=tipo,
                fecha=fecha_sql,
                periodo=event.periodo,
                fecha_proxima=fecha_proxima_sql,
                observaciones=observaciones,
            )
        )
        lines.append("")

    lines.append("COMMIT;")
    lines.append("")
    return "\n".join(lines)


def _next_relevant_row(reader: Iterable[List[str]]) -> List[str]:
    for row in reader:
        if any(cell.strip() for cell in row):
            return row
    raise StopIteration


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    events = list(iter_events(args.input, args.empresa_id))
    sql_output = build_sql(events, args.empresa_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(sql_output, encoding="utf-8")
    print(
        f"Se generaron {len(events)} eventos en {args.output}. "
        "Ejecuta este archivo en phpMyAdmin después de validar los datos."
    )


if __name__ == "__main__":
    main()

