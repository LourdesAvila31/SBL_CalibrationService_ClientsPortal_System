#!/usr/bin/env python3
"""Genera archivos normalizados y SQL idempotente a partir del audit trail.

Esta versión trabaja con los nuevos CSV terminados en `_v2`, expande rangos de
celdas copiadas masivamente, deriva el código del instrumento a partir de las
hojas originales y agrega la firma interna de cada usuario. Además genera un
CSV intermedio (`normalize_audit_trail.csv`) y registra un resumen de las
transformaciones programáticas aplicadas.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence

BASE_DIR = Path(__file__).resolve().parent
ARCHIVOS_SQL_DIR = BASE_DIR.parent
CSV_ORIGINAL_DIR = ARCHIVOS_SQL_DIR / "Archivos_CSV_originales"
NORMALIZE_DIR = ARCHIVOS_SQL_DIR / "Archivos_Normalize"
REPORT_DIR = ARCHIVOS_SQL_DIR / "Report_Summary"

INPUT_CANDIDATES = [
    CSV_ORIGINAL_DIR / "AT_instrumentos_original_v2.csv",
    CSV_ORIGINAL_DIR / "AT_instrumentos_original.csv",
]
DEFAULT_INPUT_CSV = next((path for path in INPUT_CANDIDATES if path.exists()), INPUT_CANDIDATES[0])
CSV_PATH = DEFAULT_INPUT_CSV
DEFAULT_OUTPUT_SQL = ARCHIVOS_SQL_DIR / "Archivos_BD_SBL" / "SBL_inserts" / "insert_audit_trail.sql"
DEFAULT_NORMALIZED_CSV = NORMALIZE_DIR / "normalize_audit_trail.csv"
DEFAULT_CODE_LOG = REPORT_DIR / "audit_trail_code_log.md"

EMPRESA_ID = 1
MONTH_MAP = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "set": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}
NA_VALUES = {"", "NA", "ND", "N/A", "NULL", "null"}
SECTION_FIELD_CANDIDATES = ("Sección", "Hoja")
ID_FIELD_CANDIDATES = ("ID", "Rango")
CELL_PATTERN = re.compile(r"^([A-Za-z]+)(\d+)$")
DATE_PATTERN = re.compile(r"(\d{1,2})-([A-Za-z\.]+)-(\d{2,4})")

EMAIL_DIRECTORY = {
    "lourdesmarienavila@gmail.com": "Lourdes Marien Avila Lopez",
    "practicas.validacion@sblpharma.com": "Lourdes Marien Avila Lopez",
    "validacion7@sblpharma.com": "Kenia N. Vazquez Perez",
    "validacion5@sblpharma.com": "Vivian Denise Rodriguez Martinez",
    "validacion@sblpharma.com": "Laura Clarisa Martinez Malagon",
    "validacion8@sblpharma.com": "Israel Castillo Paco",
    "validacion3@sblpharma.com": "Alexis Orlando Munoz Lara",
    "validacion6@sblpharma.com": "Equipo Validacion",
    "documentacion3@sblpharma.com": "Equipo Documentacion",
    "sistemas@sblpharma.com": "Luis David Guzman Flores",
    "sistemas2@sblpharma.com": "Gilberto Eduardo Garcia Marquez",
}

SHEET_NAME_ALIASES = {
    "instrumentos": {"instrumentos", "sbllm08", "sbllm-08", "sbl-lm-08", "sbl lm 08", "sbl_lm_08"},
    "plan_riesgos": {"calibracionverificacion", "calibracion/verificacion", "calibracion_verificacion"},
    "certificados": {"certificados"},
}

SEGMENT_LABELS = {
    "instrumentos": "Instrumentos",
    "plan_riesgos": "Plan de riesgos",
    "certificados": "Certificados",
    "otros": "General",
}

SHEET_SOURCES = {
    "instrumentos": ("LM_instrumentos_original_v2.csv", "LM_instrumentos_original.csv"),
    "plan_riesgos": ("PR_instrumentos_original_v2.csv", "PR_instrumentos_original.csv"),
    "certificados": ("CERT_instrumentos_original_v2.csv", "CERT_instrumentos_original.csv"),
}

HEADER_ROWS = {
    "instrumentos": 1,
    "plan_riesgos": 1,
    "certificados": 2,
}

CODE_COLUMN = {
    "instrumentos": "E",
    "plan_riesgos": "E",
    "certificados": "E",
}


@dataclass
class SheetSnapshot:
    rows: List[List[str]]
    header_rows: int

    @classmethod
    def from_csv(cls, path: Path, header_rows: int) -> "SheetSnapshot":
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            rows = [list(row) for row in reader]
        return cls(rows=rows, header_rows=header_rows)

    def get_cell(self, column_letter: str, row_number: int) -> Optional[str]:
        row_index = row_number - 1
        if row_index < 0 or row_index >= len(self.rows):
            return None
        col_index = column_to_index(column_letter) - 1
        if col_index < 0 or col_index >= len(self.rows[row_index]):
            return None
        value = self.rows[row_index][col_index]
        return value if value != "" else None

    def build_column_labels(self) -> Mapping[str, str]:
        labels: dict[str, str] = {}
        if not self.rows:
            return labels
        if self.header_rows == 1:
            header = self.rows[0]
            for idx, raw in enumerate(header, start=1):
                column_letter = index_to_column(idx)
                labels[column_letter] = sanitize_header(raw)
        elif len(self.rows) >= 2:
            top = self.rows[0]
            bottom = self.rows[1]
            size = max(len(top), len(bottom))
            for idx in range(1, size + 1):
                column_letter = index_to_column(idx)
                upper = sanitize_header(top[idx - 1]) if idx - 1 < len(top) else ""
                lower = sanitize_header(bottom[idx - 1]) if idx - 1 < len(bottom) else ""
                if lower and upper:
                    labels[column_letter] = f"{lower} ({upper})"
                elif lower:
                    labels[column_letter] = lower
                elif upper:
                    labels[column_letter] = upper
                else:
                    labels[column_letter] = f"Columna {column_letter}"
        return labels


@dataclass
class RawAuditRow:
    fecha_evento: Optional[dt.datetime]
    hoja: str
    rango: str
    valor_anterior: Optional[str]
    valor_nuevo: Optional[str]
    usuario: str
    row_position: int


@dataclass
class NormalizedChange:
    row_position: int
    segmento_actor: str
    hoja: str
    seccion: str
    campo: str
    columna_excel: str
    fila_excel: int
    instrumento_codigo: Optional[str]
    fecha_evento: Optional[dt.datetime]
    valor_anterior: Optional[str]
    valor_nuevo: Optional[str]
    usuario_correo: str
    usuario_nombre: str
    usuario_firma_interna: Optional[str]


@dataclass
class NormalizationStats:
    total_rows: int = 0
    total_cells: int = 0
    fallback_new_values: int = 0
    missing_codes: int = 0
    unknown_sheets: int = 0


_snapshot_cache: dict[str, SheetSnapshot] = {}
_column_labels_cache: dict[str, Mapping[str, str]] = {}


def column_to_index(column: str) -> int:
    result = 0
    for char in column.upper():
        if not char.isalpha():
            return 0
        result = result * 26 + (ord(char) - 64)
    return result


def index_to_column(index: int) -> str:
    column = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        column = chr(65 + remainder) + column
    return column or "A"


def sanitize_header(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = value.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def normalize_cell_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return None
    lines: List[str] = []
    for line in normalized.split("\n"):
        piece = normalize_text(line)
        if piece:
            lines.append(piece)
    if not lines:
        return None
    return "\n".join(lines)

def sanitize_sheet_key(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    normalized = strip_accents(raw)
    normalized = re.sub(r"[^a-z0-9]", "", normalized.lower())
    if not normalized:
        return None
    for canonical, aliases in SHEET_NAME_ALIASES.items():
        if normalized in aliases:
            return canonical
    return None


def resolve_sheet_config(canonical: str) -> SheetSnapshot:
    if canonical not in SHEET_SOURCES:
        raise KeyError(canonical)
    if canonical not in _snapshot_cache:
        candidates = SHEET_SOURCES[canonical]
        header_rows = HEADER_ROWS.get(canonical, 1)
        path = next(
            (CSV_ORIGINAL_DIR / name for name in candidates if (CSV_ORIGINAL_DIR / name).exists()),
            CSV_ORIGINAL_DIR / candidates[0],
        )
        _snapshot_cache[canonical] = SheetSnapshot.from_csv(path, header_rows)
    return _snapshot_cache[canonical]


def get_column_labels(canonical: str) -> Mapping[str, str]:
    if canonical not in _column_labels_cache:
        snapshot = resolve_sheet_config(canonical)
        _column_labels_cache[canonical] = snapshot.build_column_labels()
    return _column_labels_cache[canonical]


def resolve_field_label(canonical: str, column_letter: str) -> str:
    labels = get_column_labels(canonical)
    return labels.get(column_letter, f"Columna {column_letter}")


def resolve_instrument_code(canonical: str, row_number: int) -> Optional[str]:
    snapshot = resolve_sheet_config(canonical)
    code_column = CODE_COLUMN.get(canonical, "E")
    value = snapshot.get_cell(code_column, row_number)
    return normalize_text(value)


def resolve_sheet_label(canonical: Optional[str]) -> str:
    if canonical is None:
        return SEGMENT_LABELS["otros"]
    return SEGMENT_LABELS.get(canonical, SEGMENT_LABELS["otros"])


def expand_range(reference: str) -> List[tuple[str, int]]:
    if not reference:
        return []
    cleaned = reference.replace("$", "").strip()
    if not cleaned:
        return []
    cleaned = cleaned.upper()
    if ":" in cleaned:
        start_raw, end_raw = cleaned.split(":", 1)
    else:
        start_raw = end_raw = cleaned
    start_match = CELL_PATTERN.match(start_raw.strip())
    end_match = CELL_PATTERN.match(end_raw.strip())
    if not start_match or not end_match:
        return []
    start_col, start_row = start_match.group(1), int(start_match.group(2))
    end_col, end_row = end_match.group(1), int(end_match.group(2))
    start_idx = column_to_index(start_col)
    end_idx = column_to_index(end_col)
    if start_idx <= 0 or end_idx <= 0:
        return []
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
    if start_row > end_row:
        start_row, end_row = end_row, start_row
    cells: List[tuple[str, int]] = []
    for col_idx in range(start_idx, end_idx + 1):
        column_letter = index_to_column(col_idx)
        for row in range(start_row, end_row + 1):
            cells.append((column_letter, row))
    return cells


def resolve_user_name(email: str) -> str:
    lookup = email.lower()
    if lookup in EMAIL_DIRECTORY:
        return EMAIL_DIRECTORY[lookup]
    local_part = lookup.split("@", 1)[0]
    if "." in local_part:
        pieces = [piece for piece in local_part.split(".") if piece]
        if pieces:
            return " ".join(piece.capitalize() for piece in pieces)
    return local_part.capitalize() or "Usuario Desconocido"


def derive_signature(full_name: str) -> str:
    tokens = [token for token in strip_accents(full_name).lower().split() if token]
    if not tokens:
        return ""
    first = tokens[0]
    surnames = tokens[1:] or [tokens[0]]
    signature = first[0] + surnames[0]
    signature = re.sub(r"[^a-z0-9]", "", signature)
    return signature


def parse_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if value is None:
        return None
    cleaned = normalize_text(value.replace("a.m.", "AM").replace("p.m.", "PM"))
    if cleaned is None:
        return None
    match = DATE_PATTERN.search(cleaned)
    if match:
        day = int(match.group(1))
        month_key = match.group(2).replace(".", "").lower()
        month = MONTH_MAP.get(month_key)
        if month is None:
            return None
        year_raw = match.group(3)
        year = int(year_raw)
        if year < 100:
            year += 2000
        time_part = cleaned[match.end():].strip()
        if time_part:
            try:
                time_obj = dt.datetime.strptime(time_part, "%I:%M %p")
                hour = time_obj.hour
                minute = time_obj.minute
            except ValueError:
                hour = 0
                minute = 0
        else:
            hour = 0
            minute = 0
        return dt.datetime(year, month, day, hour, minute)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return dt.datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def normalize_cell_reference(value: Optional[str]) -> Optional[str]:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    normalized = normalized.replace("$", "")
    if "!" in normalized:
        normalized = normalized.split("!", 1)[1]
    if ":" in normalized:
        return None
    match = CELL_PATTERN.match(normalized.upper())
    if not match:
        return None
    return match.group(0)


def resolve_cell_reference(row: Mapping[str, str | None]) -> Optional[str]:
    raw_value = _first_available(row, ID_FIELD_CANDIDATES)
    return normalize_cell_reference(raw_value)


def resolve_section(row: Mapping[str, str | None]) -> Optional[str]:
    raw_value = _first_available(row, SECTION_FIELD_CANDIDATES)
    normalized = normalize_text(raw_value)
    key = sanitize_sheet_key(normalized)
    return resolve_sheet_label(key)


def _first_available(row: Mapping[str, str | None], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        if key in row and row[key]:
            return row[key]
    return None


def sql_quote(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace("'", "''").replace("\r", "").replace("\n", "\\n")
    return f"'{escaped}'"


def sql_value(value: object | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, dt.datetime):
        return sql_quote(value.strftime("%Y-%m-%d %H:%M:%S"))
    return sql_quote(str(value))


def chunked(seq: Sequence[Sequence[object]], size: int) -> Iterable[List[Sequence[object]]]:
    for start in range(0, len(seq), size):
        yield list(seq[start : start + size])


def _parse_placeholder(value: str) -> dt.datetime:
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "El valor de --fecha-lote debe estar en formato ISO (YYYY-MM-DDTHH:MM:SS)"
        ) from exc


def load_raw_rows(csv_path: Path) -> List[RawAuditRow]:
    rows: List[RawAuditRow] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for idx, record in enumerate(reader, start=2):
            fecha_evento = parse_datetime(record.get("Fecha"))
            hoja = record.get("Hoja") or record.get("Sección") or ""
            rango = record.get("Rango") or record.get("ID") or ""
            valor_anterior = normalize_cell_value(record.get("Valor anterior"))
            valor_nuevo = normalize_cell_value(record.get("Nuevo valor"))
            usuario = (record.get("Usuario") or "").strip()
            rows.append(
                RawAuditRow(
                    fecha_evento=fecha_evento,
                    hoja=hoja,
                    rango=rango,
                    valor_anterior=valor_anterior,
                    valor_nuevo=valor_nuevo,
                    usuario=usuario,
                    row_position=idx,
                )
            )
    return rows


def expand_changes(rows: List[RawAuditRow], placeholder: Optional[dt.datetime]) -> tuple[List[NormalizedChange], NormalizationStats]:
    stats = NormalizationStats(total_rows=len(rows))
    changes: List[NormalizedChange] = []
    for raw in rows:
        canonical = sanitize_sheet_key(raw.hoja)
        segmento = resolve_sheet_label(canonical)
        if canonical is None:
            stats.unknown_sheets += 1
        cells = expand_range(raw.rango)
        if not cells:
            reference = normalize_cell_reference(raw.rango)
            if reference:
                match = CELL_PATTERN.match(reference)
                if match:
                    cells = [(match.group(1), int(match.group(2)))]
        if not cells:
            cells = [("", 0)]
        for column_letter, row_number in cells:
            stats.total_cells += 1
            campo = resolve_field_label(canonical or "instrumentos", column_letter or "A") if canonical else f"Columna {column_letter or '?'}"
            instrumento_codigo = resolve_instrument_code(canonical or "instrumentos", row_number) if canonical else None
            if instrumento_codigo is None:
                stats.missing_codes += 1
            valor_nuevo = raw.valor_nuevo
            if not valor_nuevo and canonical:
                snapshot_value = resolve_sheet_config(canonical).get_cell(column_letter, row_number)
                valor_nuevo = normalize_cell_value(snapshot_value)
                if valor_nuevo is not None:
                    stats.fallback_new_values += 1
            valor_anterior = raw.valor_anterior
            fecha_evento = raw.fecha_evento or placeholder
            correo = raw.usuario or "auditoria@sistema.local"
            correo = correo.strip() or "auditoria@sistema.local"
            usuario_nombre = resolve_user_name(correo)
            usuario_firma = derive_signature(usuario_nombre)
            changes.append(
                NormalizedChange(
                    row_position=raw.row_position,
                    segmento_actor=segmento,
                    hoja=normalize_text(raw.hoja) or raw.hoja,
                    seccion=campo,
                    campo=campo,
                    columna_excel=column_letter,
                    fila_excel=row_number,
                    instrumento_codigo=instrumento_codigo,
                    fecha_evento=fecha_evento,
                    valor_anterior=valor_anterior,
                    valor_nuevo=valor_nuevo,
                    usuario_correo=correo,
                    usuario_nombre=usuario_nombre,
                    usuario_firma_interna=usuario_firma,
                )
            )
    return changes, stats


def write_normalized_csv(changes: List[NormalizedChange], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "row_position",
        "empresa_id",
        "segmento_actor",
        "hoja",
        "seccion",
        "campo",
        "columna_excel",
        "fila_excel",
        "instrumento_codigo",
        "fecha_evento",
        "valor_anterior",
        "valor_nuevo",
        "usuario_correo",
        "usuario_nombre",
        "usuario_firma_interna",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for change in changes:
            writer.writerow(
                [
                    change.row_position,
                    EMPRESA_ID,
                    change.segmento_actor,
                    change.hoja,
                    change.seccion,
                    change.campo,
                    change.columna_excel,
                    change.fila_excel,
                    change.instrumento_codigo or "",
                    change.fecha_evento.strftime("%Y-%m-%d %H:%M:%S") if change.fecha_evento else "",
                    change.valor_anterior or "",
                    change.valor_nuevo or "",
                    change.usuario_correo,
                    change.usuario_nombre,
                    change.usuario_firma_interna or "",
                ]
            )


def write_sql(changes: List[NormalizedChange], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = (
        "empresa_id",
        "segmento_actor",
        "instrumento_codigo",
        "columna_excel",
        "fila_excel",
        "fecha_evento",
        "seccion",
        "valor_anterior",
        "valor_nuevo",
        "usuario_correo",
        "usuario_nombre",
        "usuario_firma_interna",
    )
    lines: List[str] = [
        "-- Archivo generado automaticamente por convert_audit_trail_csv.py",
        f"-- Empresa destino: {EMPRESA_ID}",
        "",
        "START TRANSACTION;",
    ]
    rows = [
        [
            EMPRESA_ID,
            change.segmento_actor,
            change.instrumento_codigo,
            change.columna_excel or None,
            change.fila_excel if change.fila_excel else None,
            change.fecha_evento,
            change.campo,
            change.valor_anterior,
            change.valor_nuevo,
            change.usuario_correo,
            change.usuario_nombre,
            change.usuario_firma_interna,
        ]
        for change in changes
    ]
    for chunk in chunked(rows, 200):
        if not chunk:
            continue
        lines.append("")
        lines.append("INSERT INTO audit_trail (" + ", ".join(columns) + ")")
        lines.append("VALUES")
        value_lines: List[str] = []
        for record in chunk:
            formatted = ", ".join(sql_value(value) for value in record)
            value_lines.append(f"    ({formatted})")
        lines.append(",\n".join(value_lines) + ";")
    lines.append("")
    lines.append("COMMIT;")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_code_log(stats: NormalizationStats, total_changes: int, csv_path: Path, log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    if log_path.exists():
        existing = log_path.read_text(encoding="utf-8").rstrip("\n")
        if existing:
            lines.append(existing)
    lines.append("# Registro de transformaciones del audit trail")
    lines.append(f"- Fuente analizada: {csv_path.name}")
    lines.append(f"- Filas originales: {stats.total_rows}")
    lines.append(f"- Cambios normalizados: {total_changes}")
    lines.append(f"- Celdas derivadas de rangos: {stats.total_cells}")
    lines.append(f"- Valores completados desde hojas base: {stats.fallback_new_values}")
    lines.append(f"- Registros sin código detectable: {stats.missing_codes}")
    lines.append(f"- Hojas sin mapeo específico: {stats.unknown_sheets}")
    lines.append("")
    log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normaliza el audit trail y genera el SQL listo para importar en phpMyAdmin."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=CSV_PATH,
        help="Ruta del archivo AT_instrumentos_original_v2.csv (o su versión previa).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_SQL,
        help="Archivo SQL final listo para importar en phpMyAdmin.",
    )
    parser.add_argument(
        "--normalized-output",
        type=Path,
        default=DEFAULT_NORMALIZED_CSV,
        help="Ruta del CSV intermedio con los cambios normalizados.",
    )
    parser.add_argument(
        "--code-log",
        type=Path,
        default=DEFAULT_CODE_LOG,
        help="Archivo Markdown donde se registrarán las transformaciones automáticas.",
    )
    parser.add_argument(
        "--fecha-lote",
        type=str,
        default=None,
        help=(
            "Marca de tiempo (formato ISO) para filas sin fecha explícita. "
            "Si se omite, permanecerán en NULL."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    if not args.csv.exists():
        raise FileNotFoundError(f"No se encontró el archivo de auditoría en: {args.csv}")
    placeholder = _parse_placeholder(args.fecha_lote) if args.fecha_lote else None
    raw_rows = load_raw_rows(args.csv)
    changes, stats = expand_changes(raw_rows, placeholder)
    write_normalized_csv(changes, args.normalized_output)
    write_sql(changes, args.output)
    write_code_log(stats, len(changes), args.csv, args.code_log)

if __name__ == "__main__":
    main()
