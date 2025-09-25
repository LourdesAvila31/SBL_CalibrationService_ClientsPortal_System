#!/usr/bin/env python3
"""Genera los scripts de inserción para historiales a partir de ``audit_trail.csv``.

El objetivo de este conversor es permitir que phpMyAdmin importe los
movimientos históricos sin depender de ``LOAD DATA LOCAL INFILE``. El script
lee el mismo CSV utilizado por ``convert_audit_trail_csv.py`` y replica la
lógica del bloque ``tmp_historial_instrumentos`` contenido en el SQL legado.
"""

from __future__ import annotations

import csv
import datetime as dt
import pathlib
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from convert_audit_trail_csv import (  # type: ignore[import-not-found]
    CSV_PATH,
    EMPRESA_ID,
    MONTH_MAP,
    normalize_cell_reference,
    normalize_text,
    parse_datetime,
    resolve_cell_reference,
    resolve_section,
    sql_quote,
)

ROOT = pathlib.Path(__file__).resolve().parent
OUTPUT_DIR = ROOT.parent / "Archivos_BD_SQL"

DEPARTAMENTOS_SQL = OUTPUT_DIR / "insert_historial_departamentos.sql"
UBICACIONES_SQL = OUTPUT_DIR / "insert_historial_ubicaciones.sql"
FECHA_ALTA_SQL = OUTPUT_DIR / "insert_historial_fecha_alta.sql"
FECHA_BAJA_SQL = OUTPUT_DIR / "insert_historial_fecha_baja.sql"
TIPOS_SQL = OUTPUT_DIR / "insert_historial_tipos_instrumento.sql"

TARGET_SECTION = "Instrumentos"
CODE_PREFIX = "E"
DEPARTAMENTO_PREFIX = "F"
UBICACION_PREFIX = "G"
FECHA_ALTA_PREFIX = "H"
FECHA_BAJA_PREFIX = "I"

CELL_PATTERN = re.compile(r"([A-Za-z]+)(\d+)")
MONTH_NAME_PATTERN = re.compile(r"^(\d{1,2})-([A-Za-zñÑ\.]+)-(\d{2,4})$")
ISO_DATE_PATTERNS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y")


@dataclass
class CsvEntry:
    """Representa una fila relevante del historial crudo."""

    row_position: int
    row_number: int
    prefix: str
    timestamp: Optional[dt.datetime]
    valor_anterior: Optional[str]
    valor_nuevo: Optional[str]

    @property
    def valor_normalizado(self) -> Optional[str]:
        if self.valor_nuevo is not None and self.valor_nuevo != "":
            return self.valor_nuevo
        return self.valor_anterior


@dataclass(frozen=True)
class HistorialTexto:
    instrumento_codigo: str
    valor: str
    fecha_evento: Optional[dt.datetime]


@dataclass(frozen=True)
class HistorialFecha:
    instrumento_codigo: str
    fecha_valor: dt.date
    fecha_evento: Optional[dt.datetime]


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def parse_cell_reference(raw_value: Optional[str]) -> Optional[Tuple[str, int]]:
    if not raw_value:
        return None
    cleaned = normalize_cell_reference(raw_value)
    if cleaned is None:
        return None
    match = CELL_PATTERN.search(cleaned)
    if not match:
        return None
    prefix = match.group(1).upper()
    index = int(match.group(2))
    return prefix, index


def normalize_month(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    without_accents = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )
    return without_accents.lower().strip(".")


def parse_historial_date(value: Optional[str]) -> Optional[dt.date]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None

    normalized = (
        cleaned.replace("a.m.", "AM")
        .replace("p.m.", "PM")
        .replace("a. m.", "AM")
        .replace("p. m.", "PM")
        .replace(". ", ".")
    )

    for pattern in ISO_DATE_PATTERNS:
        try:
            return dt.datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue

    match = MONTH_NAME_PATTERN.match(normalized)
    if not match:
        return None

    day = int(match.group(1))
    month_key = normalize_month(match.group(2))
    month = MONTH_MAP.get(month_key)
    if month is None and len(month_key) > 3:
        month = MONTH_MAP.get(month_key[:3])
    if month is None:
        return None

    year_raw = int(match.group(3))
    year = year_raw + 2000 if year_raw < 100 else year_raw
    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def load_entries() -> List[CsvEntry]:
    entries: List[CsvEntry] = []
    if not CSV_PATH.exists():
        return entries

    with CSV_PATH.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for index, row in enumerate(reader, start=1):
            if resolve_section(row) != TARGET_SECTION:
                continue

            reference = parse_cell_reference(resolve_cell_reference(row))
            if reference is None:
                continue

            prefix, row_number = reference
            fecha_evento = parse_datetime(row.get("Fecha"))
            valor_anterior = normalize_text(row.get("Valor anterior"))
            valor_nuevo = normalize_text(row.get("Nuevo valor"))

            entries.append(
                CsvEntry(
                    row_position=index,
                    row_number=row_number,
                    prefix=prefix,
                    timestamp=fecha_evento,
                    valor_anterior=valor_anterior,
                    valor_nuevo=valor_nuevo,
                )
            )

    return entries


def build_code_fallback(entries: Iterable[CsvEntry]) -> Dict[int, str]:
    fallback: Dict[int, str] = {}
    for entry in entries:
        if entry.prefix != CODE_PREFIX:
            continue
        candidate = entry.valor_nuevo or entry.valor_anterior
        if candidate:
            fallback[entry.row_number] = candidate
    return fallback


def sort_key(entry: CsvEntry) -> Tuple[dt.datetime, int]:
    timestamp = entry.timestamp or dt.datetime(1970, 1, 1)
    return (timestamp, entry.row_position)


def deduplicate_text(records: Iterable[HistorialTexto]) -> List[HistorialTexto]:
    seen = set()
    unique: List[HistorialTexto] = []
    for record in records:
        key = (
            record.instrumento_codigo,
            record.valor,
            record.fecha_evento.strftime("%Y-%m-%d %H:%M:%S")
            if record.fecha_evento
            else None,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def deduplicate_date(records: Iterable[HistorialFecha]) -> List[HistorialFecha]:
    seen = set()
    unique: List[HistorialFecha] = []
    for record in records:
        key = (
            record.instrumento_codigo,
            record.fecha_valor.isoformat(),
            record.fecha_evento.strftime("%Y-%m-%d %H:%M:%S")
            if record.fecha_evento
            else None,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def build_historiales(entries: Sequence[CsvEntry]) -> Tuple[
    List[HistorialTexto],
    List[HistorialTexto],
    List[HistorialFecha],
    List[HistorialFecha],
    List[HistorialTexto],
    str,
]:
    fallback_codes = build_code_fallback(entries)
    current_codes: Dict[int, str] = {}
    departamentos: List[HistorialTexto] = []
    ubicaciones: List[HistorialTexto] = []
    fechas_alta: List[HistorialFecha] = []
    fechas_baja: List[HistorialFecha] = []
    estado_por_prefijo: Dict[str, List[HistorialTexto]] = defaultdict(list)

    for entry in sorted(entries, key=sort_key):
        if entry.prefix == CODE_PREFIX:
            candidate = entry.valor_normalizado
            if candidate:
                current_codes[entry.row_number] = candidate
            continue

        instrumento_codigo = current_codes.get(entry.row_number)
        if not instrumento_codigo:
            instrumento_codigo = fallback_codes.get(entry.row_number)
        if not instrumento_codigo:
            continue

        valor = entry.valor_normalizado
        if entry.prefix == DEPARTAMENTO_PREFIX:
            if valor:
                departamentos.append(
                    HistorialTexto(
                        instrumento_codigo=instrumento_codigo,
                        valor=valor,
                        fecha_evento=entry.timestamp,
                    )
                )
        elif entry.prefix == UBICACION_PREFIX:
            if valor:
                ubicaciones.append(
                    HistorialTexto(
                        instrumento_codigo=instrumento_codigo,
                        valor=valor,
                        fecha_evento=entry.timestamp,
                    )
                )
        elif entry.prefix == FECHA_ALTA_PREFIX:
            fecha_valor = parse_historial_date(valor)
            if fecha_valor:
                fechas_alta.append(
                    HistorialFecha(
                        instrumento_codigo=instrumento_codigo,
                        fecha_valor=fecha_valor,
                        fecha_evento=entry.timestamp,
                    )
                )
        elif entry.prefix == FECHA_BAJA_PREFIX:
            fecha_valor = parse_historial_date(valor)
            if fecha_valor:
                fechas_baja.append(
                    HistorialFecha(
                        instrumento_codigo=instrumento_codigo,
                        fecha_valor=fecha_valor,
                        fecha_evento=entry.timestamp,
                    )
                )
        else:
            if valor:
                estado_por_prefijo[entry.prefix].append(
                    HistorialTexto(
                        instrumento_codigo=instrumento_codigo,
                        valor=valor,
                        fecha_evento=entry.timestamp,
                    )
                )

    estado_prefijo = ""
    prefijo_counts = Counter(
        {prefix: len(items) for prefix, items in estado_por_prefijo.items()}
    )
    if prefijo_counts:
        estado_prefijo = max(prefijo_counts.items(), key=lambda item: (item[1], item[0]))[0]
    else:
        estado_prefijo = "K"

    estado_registros = estado_por_prefijo.get(estado_prefijo, [])

    return (
        deduplicate_text(departamentos),
        deduplicate_text(ubicaciones),
        deduplicate_date(fechas_alta),
        deduplicate_date(fechas_baja),
        deduplicate_text(estado_registros),
        estado_prefijo,
    )


def format_datetime(value: Optional[dt.datetime]) -> str:
    if value is None:
        return "NULL"
    return sql_quote(value.strftime("%Y-%m-%d %H:%M:%S"))


def format_date(value: dt.date) -> str:
    return sql_quote(value.strftime("%Y-%m-%d"))


def build_union_select(
    registros: Sequence[Sequence[str]],
    column_names: Sequence[str],
) -> List[str]:
    lines: List[str] = []
    for index, record in enumerate(registros):
        select_parts = [
            f"{value} AS {column}"
            for value, column in zip(record, column_names, strict=True)
        ]
        line = "        SELECT " + ", ".join(select_parts)
        if index < len(registros) - 1:
            line += "\n        UNION ALL"
        lines.append(line)
    return lines


def build_departamentos_sql(registros: Sequence[HistorialTexto]) -> List[str]:
    header = [
        "-- Archivo generado automáticamente por convert_historiales_csv.py",
        "",
        "START TRANSACTION;",
        "SET @now_historial := NOW();",
        "",
    ]
    if not registros:
        header.append("-- No se detectaron movimientos de departamentos.")
        header.append("COMMIT;")
        header.append("")
        return header

    payload = [
        (
            str(EMPRESA_ID),
            sql_quote(item.instrumento_codigo),
            sql_quote(item.valor),
            format_datetime(item.fecha_evento),
        )
        for item in registros
    ]

    lines = header.copy()
    lines.extend(
        [
            "INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)",
            "SELECT",
            "    datos.instrumento_id,",
            "    datos.departamento_id,",
            "    datos.empresa_id,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS fecha,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`",
            "FROM (",
            "    SELECT",
            "        base.empresa_id,",
            "        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,",
            "        (SELECT id FROM departamentos WHERE empresa_id = base.empresa_id AND LOWER(nombre) = LOWER(base.valor_texto) LIMIT 1) AS departamento_id,",
            "        base.valor_texto,",
            "        base.fecha_evento",
            "    FROM (",
        ]
    )

    lines.extend(
        build_union_select(
            payload,
            ("empresa_id", "instrumento_codigo", "valor_texto", "fecha_evento"),
        )
    )

    lines.extend(
        [
            "    ) AS base",
            ") AS datos",
            "WHERE datos.instrumento_id IS NOT NULL",
            "  AND datos.departamento_id IS NOT NULL",
            "  AND NOT EXISTS (",
            "      SELECT 1",
            "      FROM historial_departamentos hd",
            "      WHERE hd.instrumento_id = datos.instrumento_id",
            "        AND hd.departamento_id = datos.departamento_id",
            "        AND hd.empresa_id = datos.empresa_id",
            "        AND hd.fecha <=> COALESCE(datos.fecha_evento, @now_historial)",
            "  );",
            "",
            "COMMIT;",
            "",
        ]
    )

    return lines


def build_ubicaciones_sql(registros: Sequence[HistorialTexto]) -> List[str]:
    header = [
        "-- Archivo generado automáticamente por convert_historiales_csv.py",
        "",
        "START TRANSACTION;",
        "SET @now_historial := NOW();",
        "",
    ]
    if not registros:
        header.append("-- No se detectaron movimientos de ubicaciones.")
        header.append("COMMIT;")
        header.append("")
        return header

    payload = [
        (
            str(EMPRESA_ID),
            sql_quote(item.instrumento_codigo),
            sql_quote(item.valor),
            format_datetime(item.fecha_evento),
        )
        for item in registros
    ]

    lines = header.copy()
    lines.extend(
        [
            "INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)",
            "SELECT",
            "    datos.instrumento_id,",
            "    datos.valor_texto,",
            "    datos.empresa_id,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS fecha,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`",
            "FROM (",
            "    SELECT",
            "        base.empresa_id,",
            "        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,",
            "        base.valor_texto,",
            "        base.fecha_evento",
            "    FROM (",
        ]
    )

    lines.extend(
        build_union_select(
            payload,
            ("empresa_id", "instrumento_codigo", "valor_texto", "fecha_evento"),
        )
    )

    lines.extend(
        [
            "    ) AS base",
            ") AS datos",
            "WHERE datos.instrumento_id IS NOT NULL",
            "  AND datos.valor_texto IS NOT NULL",
            "  AND NOT EXISTS (",
            "      SELECT 1",
            "      FROM historial_ubicaciones hu",
            "      WHERE hu.instrumento_id = datos.instrumento_id",
            "        AND hu.ubicacion = datos.valor_texto",
            "        AND hu.empresa_id = datos.empresa_id",
            "        AND hu.fecha <=> COALESCE(datos.fecha_evento, @now_historial)",
            "  );",
            "",
            "COMMIT;",
            "",
        ]
    )

    return lines


def build_fecha_sql(
    registros: Sequence[HistorialFecha],
    table_name: str,
) -> List[str]:
    header = [
        "-- Archivo generado automáticamente por convert_historiales_csv.py",
        "",
        "START TRANSACTION;",
        "SET @now_historial := NOW();",
        "",
    ]
    if not registros:
        header.append(f"-- No se detectaron movimientos para {table_name}.")
        header.append("COMMIT;")
        header.append("")
        return header

    payload = [
        (
            str(EMPRESA_ID),
            sql_quote(item.instrumento_codigo),
            format_date(item.fecha_valor),
            format_datetime(item.fecha_evento),
        )
        for item in registros
    ]

    lines = header.copy()
    lines.extend(
        [
            f"INSERT INTO {table_name} (instrumento_id, fecha, empresa_id, `timestamp`)",
            "SELECT",
            "    datos.instrumento_id,",
            "    datos.valor_fecha,",
            "    datos.empresa_id,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`",
            "FROM (",
            "    SELECT",
            "        base.empresa_id,",
            "        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,",
            "        base.valor_fecha,",
            "        base.fecha_evento",
            "    FROM (",
        ]
    )

    lines.extend(
        build_union_select(
            payload,
            ("empresa_id", "instrumento_codigo", "valor_fecha", "fecha_evento"),
        )
    )

    lines.extend(
        [
            "    ) AS base",
            ") AS datos",
            "WHERE datos.instrumento_id IS NOT NULL",
            "  AND datos.valor_fecha IS NOT NULL",
            "  AND NOT EXISTS (",
            f"      SELECT 1 FROM {table_name} ht",
            "      WHERE ht.instrumento_id = datos.instrumento_id",
            "        AND ht.fecha = datos.valor_fecha",
            "        AND ht.empresa_id = datos.empresa_id",
            "  );",
            "",
            "COMMIT;",
            "",
        ]
    )

    return lines


def build_tipos_sql(
    registros: Sequence[HistorialTexto],
    estado_prefijo: str,
) -> List[str]:
    header = [
        "-- Archivo generado automáticamente por convert_historiales_csv.py",
        f"-- Prefijo de estado detectado: {estado_prefijo}",
        "",
        "START TRANSACTION;",
        "SET @now_historial := NOW();",
        "",
    ]
    if not registros:
        header.append("-- No se detectaron movimientos de estado.")
        header.append("COMMIT;")
        header.append("")
        return header

    payload = [
        (
            str(EMPRESA_ID),
            sql_quote(item.instrumento_codigo),
            sql_quote(item.valor),
            format_datetime(item.fecha_evento),
        )
        for item in registros
    ]

    lines = header.copy()
    lines.extend(
        [
            "INSERT INTO historial_tipos_instrumento (instrumento_id, estado, empresa_id, fecha, `timestamp`)",
            "SELECT",
            "    datos.instrumento_id,",
            "    datos.valor_texto,",
            "    datos.empresa_id,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS fecha,",
            "    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`",
            "FROM (",
            "    SELECT",
            "        base.empresa_id,",
            "        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,",
            "        base.valor_texto,",
            "        base.fecha_evento",
            "    FROM (",
        ]
    )

    lines.extend(
        build_union_select(
            payload,
            ("empresa_id", "instrumento_codigo", "valor_texto", "fecha_evento"),
        )
    )

    lines.extend(
        [
            "    ) AS base",
            ") AS datos",
            "WHERE datos.instrumento_id IS NOT NULL",
            "  AND datos.valor_texto IS NOT NULL",
            "  AND NOT EXISTS (",
            "      SELECT 1",
            "      FROM historial_tipos_instrumento hti",
            "      WHERE hti.instrumento_id = datos.instrumento_id",
            "        AND hti.estado = datos.valor_texto",
            "        AND hti.empresa_id = datos.empresa_id",
            "        AND hti.fecha <=> COALESCE(datos.fecha_evento, @now_historial)",
            "  );",
            "",
            "COMMIT;",
            "",
        ]
    )

    return lines


def write_sql_file(path: pathlib.Path, lines: Iterable[str]) -> None:
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ensure_output_dir()
    entries = load_entries()

    (
        departamentos,
        ubicaciones,
        fechas_alta,
        fechas_baja,
        tipos,
        estado_prefijo,
    ) = build_historiales(entries)

    write_sql_file(DEPARTAMENTOS_SQL, build_departamentos_sql(departamentos))
    write_sql_file(UBICACIONES_SQL, build_ubicaciones_sql(ubicaciones))
    write_sql_file(FECHA_ALTA_SQL, build_fecha_sql(fechas_alta, "historial_fecha_alta"))
    write_sql_file(FECHA_BAJA_SQL, build_fecha_sql(fechas_baja, "historial_fecha_baja"))
    write_sql_file(TIPOS_SQL, build_tipos_sql(tipos, estado_prefijo))


if __name__ == "__main__":
    main()

