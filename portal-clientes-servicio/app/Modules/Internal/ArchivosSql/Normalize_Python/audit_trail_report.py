#!/usr/bin/env python3
"""Genera los archivos de resumen para `audit_trail.csv`.

El script agrupa los movimientos por fila (número) y por columna (A–I) de la
hoja *Instrumentos* para calcular las métricas utilizadas en los reportes
operativos. Los resultados se escriben en:

- ``audit_trail_report_summary.csv``
- ``audit_trail_report_totals.json``

Ambos archivos se ubican en el mismo directorio de este script.
"""
from __future__ import annotations

import csv
import json
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, MutableMapping, Optional, Tuple

ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app.Modules.Internal.ArchivosSql.Normalize_Python.convert_audit_trail_csv import (  # noqa: E402
    CSV_PATH,
    normalize_section_name,
    normalize_text,
    parse_datetime,
    resolve_cell_reference,
    resolve_section,
)
SUMMARY_PATH = ROOT / "audit_trail_report_summary.csv"
TOTALS_PATH = ROOT / "audit_trail_report_totals.json"

TARGET_SECTION = "Instrumentos"
KEYWORD_SECTION = "Calibración/Verificación"
ALLOWED_COLUMNS = tuple("ABCDEFGHI")
REQUIRED_FOR_ALTA = tuple("DEFGH")
CODE_COLUMN = "E"
DEPARTMENT_COLUMN = "F"
LOCATION_COLUMN = "G"
FECHA_ALTA_COLUMN = "H"
FECHA_BAJA_COLUMN = "I"

KEYWORD_PATTERNS = {
    "rechazado": ("rechazad",),
    "baja": ("baja",),
    "calibración limitada": ("calibracion limitada",),
}


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def canonicalize(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.casefold()


def is_almacen(value: Optional[str]) -> bool:
    if value is None:
        return False
    return "almacen" in strip_accents(value).casefold()


def values_differ(previous: Optional[str], new: Optional[str]) -> bool:
    if new is None:
        return False
    if previous is None:
        return True
    return new.casefold() != previous.casefold()


def normalize_for_keywords(value: Optional[str]) -> str:
    if value is None:
        return ""
    return strip_accents(value).casefold()


@dataclass(order=True)
class CellChange:
    """Representa un cambio en una celda individual."""

    sort_index: Tuple[int, int]
    column: str
    row_number: int
    timestamp: Optional[int]
    timestamp_str: str
    previous_value: Optional[str]
    new_value: Optional[str]


@dataclass
class RowMetrics:
    """Acumula los movimientos relevantes de una fila."""

    row_number: int
    asignaciones_codigo: int = 0
    cambios_codigo: int = 0
    movimientos_departamento: int = 0
    movimientos_ubicacion: int = 0
    regresos_almacen: int = 0
    cambios_fecha_alta: int = 0
    cambios_fecha_baja: int = 0
    alta_confirmada: bool = False
    fecha_alta_completa: Optional[str] = None
    _current_values: MutableMapping[str, Tuple[Optional[str], Optional[str]]] = field(
        default_factory=lambda: {col: (None, None) for col in ALLOWED_COLUMNS}
    )
    _columns_with_data: MutableMapping[str, bool] = field(
        default_factory=lambda: {col: False for col in ALLOWED_COLUMNS}
    )

    def _set_value(self, column: str, value: Optional[str]) -> None:
        self._current_values[column] = (value, canonicalize(value))
        if value is not None:
            self._columns_with_data[column] = True

    def _get_value(self, column: str) -> Tuple[Optional[str], Optional[str]]:
        return self._current_values[column]

    def process_change(self, change: CellChange) -> None:
        new_value = change.new_value
        new_canonical = canonicalize(new_value)
        previous_value = change.previous_value
        previous_canonical = canonicalize(previous_value)
        old_value, old_canonical = self._get_value(change.column)

        if change.column == CODE_COLUMN:
            if new_value is not None:
                had_history = self._columns_with_data.get(CODE_COLUMN, False)
                if previous_value is None:
                    self.asignaciones_codigo += 1
                    if had_history:
                        self.cambios_codigo += 1
                elif values_differ(previous_value, new_value):
                    self.cambios_codigo += 1
            # Si se elimina el código no se contabiliza como movimiento.

        elif change.column == DEPARTMENT_COLUMN:
            if values_differ(previous_value, new_value):
                self.movimientos_departamento += 1
        elif change.column == LOCATION_COLUMN:
            if values_differ(previous_value, new_value):
                self.movimientos_ubicacion += 1
                if is_almacen(new_value):
                    self.regresos_almacen += 1

        elif change.column == FECHA_ALTA_COLUMN:
            if values_differ(previous_value, new_value):
                self.cambios_fecha_alta += 1

        elif change.column == FECHA_BAJA_COLUMN:
            if values_differ(previous_value, new_value):
                self.cambios_fecha_baja += 1

        self._set_value(change.column, new_value)

        if not self.alta_confirmada:
            if all(self._current_values[col][1] is not None for col in REQUIRED_FOR_ALTA) and self._columns_with_data.get("A", False):
                self.alta_confirmada = True
                if change.timestamp_str:
                    self.fecha_alta_completa = change.timestamp_str


def parse_cell(cell: Optional[str]) -> Optional[Tuple[str, int]]:
    if not cell:
        return None
    if ":" in cell:
        return None
    prefix = ""
    number_part = ""
    for char in cell:
        if char.isalpha():
            prefix += char.upper()
        elif char.isdigit():
            number_part += char
        else:
            return None
    if not prefix or not number_part:
        return None
    if prefix not in ALLOWED_COLUMNS:
        return None
    return prefix, int(number_part)


def load_changes() -> Dict[int, List[CellChange]]:
    grouped: Dict[int, List[CellChange]] = {}
    with CSV_PATH.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for index, row in enumerate(reader):
            if resolve_section(row) != TARGET_SECTION:
                continue

            parsed_cell = parse_cell(resolve_cell_reference(row))
            if parsed_cell is None:
                continue
            column, row_number = parsed_cell

            timestamp_dt = parse_datetime(row.get("Fecha"))
            timestamp_key: Optional[int]
            timestamp_str = ""
            if timestamp_dt is not None:
                timestamp_key = int(timestamp_dt.timestamp())
                timestamp_str = timestamp_dt.strftime("%Y-%m-%d %H:%M")
            else:
                timestamp_key = None

            change = CellChange(
                sort_index=(timestamp_key or 0, index),
                column=column,
                row_number=row_number,
                timestamp=timestamp_key,
                timestamp_str=timestamp_str,
                previous_value=normalize_text(row.get("Valor anterior")),
                new_value=normalize_text(row.get("Nuevo valor")),
            )
            grouped.setdefault(row_number, []).append(change)

    for changes in grouped.values():
        changes.sort(key=lambda item: item.sort_index)
    return grouped


def count_keyword_matches() -> int:
    matches = 0
    with CSV_PATH.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if normalize_section_name(row.get("Sección")) != KEYWORD_SECTION and resolve_section(row) != KEYWORD_SECTION:
                continue
            valor_anterior = normalize_for_keywords(normalize_text(row.get("Valor anterior")))
            valor_nuevo = normalize_for_keywords(normalize_text(row.get("Nuevo valor")))
            for patterns in KEYWORD_PATTERNS.values():
                if any(pattern in valor_anterior for pattern in patterns if pattern):
                    matches += 1
                    break
                if any(pattern in valor_nuevo for pattern in patterns if pattern):
                    matches += 1
                    break
    return matches


def build_metrics(changes_by_row: Dict[int, List[CellChange]]) -> List[RowMetrics]:
    metrics: List[RowMetrics] = []
    for row_number in sorted(changes_by_row):
        accumulator = RowMetrics(row_number=row_number)
        for change in changes_by_row[row_number]:
            accumulator.process_change(change)
        metrics.append(accumulator)
    return metrics


def write_summary(rows: Iterable[RowMetrics]) -> None:
    header = [
        "fila",
        "alta_confirmada",
        "fecha_alta_completa",
        "asignaciones_codigo",
        "cambios_codigo",
        "movimientos_departamento",
        "movimientos_ubicacion",
        "regresos_almacen",
        "cambios_fecha_alta",
        "cambios_fecha_baja",
    ]

    with SUMMARY_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header)
        for row in rows:
            writer.writerow(
                [
                    row.row_number,
                    "SI" if row.alta_confirmada else "NO",
                    row.fecha_alta_completa or "",
                    row.asignaciones_codigo,
                    row.cambios_codigo,
                    row.movimientos_departamento,
                    row.movimientos_ubicacion,
                    row.regresos_almacen,
                    row.cambios_fecha_alta,
                    row.cambios_fecha_baja,
                ]
            )


def write_totals(rows: Iterable[RowMetrics], keyword_matches: int) -> None:
    rows_list = list(rows)
    totals = {
        "instrumentos_registrados": len(rows_list),
        "altas_confirmadas": sum(1 for row in rows_list if row.alta_confirmada),
        "asignaciones_codigo": sum(row.asignaciones_codigo for row in rows_list),
        "cambios_codigo": sum(row.cambios_codigo for row in rows_list),
        "movimientos_departamento": sum(
            row.movimientos_departamento for row in rows_list
        ),
        "movimientos_ubicacion": sum(row.movimientos_ubicacion for row in rows_list),
        "regresos_almacen": sum(row.regresos_almacen for row in rows_list),
        "cambios_fecha_alta": sum(row.cambios_fecha_alta for row in rows_list),
        "cambios_fecha_baja": sum(row.cambios_fecha_baja for row in rows_list),
        "coincidencias_calibracion": keyword_matches,
    }

    with TOTALS_PATH.open("w", encoding="utf-8") as totals_file:
        json.dump(totals, totals_file, ensure_ascii=False, indent=2)


def main() -> None:
    changes = load_changes()
    metrics = build_metrics(changes)
    write_summary(metrics)
    keyword_matches = count_keyword_matches()
    write_totals(metrics, keyword_matches)


if __name__ == "__main__":
    main()
