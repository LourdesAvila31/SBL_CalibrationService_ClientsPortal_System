#!/usr/bin/env python3
"""Genera el CSV normalizado y el SQL idempotente del plan de riesgos corporativo.

El script conserva textos multilínea, normaliza valores `NA`/`ND`, valida que
cada código de instrumento exista en el inventario y produce un script SQL
idempotente basado en claves naturales (código + empresa).

Ejemplo desde la raíz del repositorio:

```bash
python tools/scripts/generate_plan_riesgos.py
```

Resultados:

- `app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_plan_riesgos.csv`
- `app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_plan_riesgos.sql`
"""
from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
ARCHIVOS_SQL_DIR = REPO_ROOT / "app/Modules/Internal/ArchivosSql"
PLAN_SOURCE_CANDIDATES = [
    ARCHIVOS_SQL_DIR / "PR_instrumentos.csv",
    ARCHIVOS_SQL_DIR / "Archivos_CSV_originales" / "PR_instrumentos_original_v2.csv",
    ARCHIVOS_SQL_DIR / "Archivos_CSV_originales" / "PR_instrumentos_original_v2.csv",
]
PLAN_NORMALIZED = (
    ARCHIVOS_SQL_DIR / "Archivos_Normalize" / "normalize_plan_riesgos.csv"
)
PLAN_SQL = (
    ARCHIVOS_SQL_DIR
    / "Archivos_BD_SBL"
    / "SBL_inserts"
    / "insert_plan_riesgos.sql"
)
INVENTORY_SOURCE_CANDIDATES = [
    ARCHIVOS_SQL_DIR / "instrumentos_normalizado.csv",
    ARCHIVOS_SQL_DIR / "Archivos_Normalize" / "normalize_instrumentos.csv",
]

EMPRESA_ID = 1

PLAN_MIN_YEAR = 2015
PLAN_MAX_YEAR = date.today().year + 10

PLACEHOLDERS = {"", "NA", "ND", "N/A", "N.D.", "N-D", "#VALUE!"}
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
    "SEPT": 9,
    "OCT": 10,
    "NOV": 11,
    "DIC": 12,
}
DATE_PATTERNS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
)

MISSING_CERTIFICATE_MESSAGE = "No se ha añadido su primer certificado"

CSV_HEADERS = [
    "instrumento_codigo",
    "empresa_id",
    "requerimiento",
    "impacto_falla",
    "consideraciones_falla",
    "clase_riesgo",
    "capacidad_deteccion",
    "frecuencia",
    "fecha_actualizacion",
    "observaciones",
    "tipo_calibracion",
    "especificaciones",
]


def _resolve_source_path(candidates: list[Path], description: str) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    joined = "\n  - ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        f"No se encontró {description}. Se buscaron las rutas:\n  - {joined}"
    )


PLAN_SOURCE = _resolve_source_path(PLAN_SOURCE_CANDIDATES, "PR_instrumentos.csv")
INVENTORY_NORMALIZED = _resolve_source_path(
    INVENTORY_SOURCE_CANDIDATES, "instrumentos_normalizado.csv"
)


@dataclass
class RiskPlanRow:
    codigo: str
    empresa_id: int
    requerimiento: str
    impacto_falla: str
    consideraciones_falla: str
    clase_riesgo: str
    capacidad_deteccion: str
    frecuencia: str
    fecha_actualizacion: str
    observaciones: str
    tipo_calibracion: str
    especificaciones: str

    def as_csv_row(self) -> List[str]:
        return [
            self.codigo,
            str(self.empresa_id),
            self.requerimiento,
            self.impacto_falla,
            self.consideraciones_falla,
            self.clase_riesgo,
            self.capacidad_deteccion,
            self.frecuencia,
            self.fecha_actualizacion,
            self.observaciones,
            self.tipo_calibracion,
            self.especificaciones,
        ]


def _normalize_placeholder(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = value.strip()
    if not cleaned:
        return ""
    ascii_upper = (
        unicodedata.normalize("NFKD", cleaned)
        .encode("ascii", "ignore")
        .decode()
        .upper()
    )
    if ascii_upper in PLACEHOLDERS:
        return ""
    return cleaned


def _normalize_required(value: Optional[str], *, default: str = "NA") -> str:
    cleaned = _normalize_placeholder(value)
    return cleaned if cleaned else default


def _normalize_optional(value: Optional[str]) -> str:
    cleaned = _normalize_placeholder(value)
    return cleaned


def _normalize_code(value: Optional[str], line_number: int) -> str:
    cleaned = _normalize_placeholder(value)
    if not cleaned:
        raise ValueError(f"La columna 'Código' está vacía en la línea {line_number}")
    return cleaned.upper()


def _parse_month_name(name: str, line_number: int) -> int:
    normalized = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode()
        .upper()
        .replace(".", "")
    )
    if normalized in SPANISH_MONTHS:
        return SPANISH_MONTHS[normalized]
    try:
        return datetime.strptime(normalized[:3], "%b").month
    except ValueError as exc:
        raise ValueError(
            f"Mes desconocido '{name}' en la columna 'Fecha programada' (línea {line_number})"
        ) from exc


def _parse_fecha_programada(value: Optional[str], line_number: int) -> str:
    cleaned = _normalize_placeholder(value)
    if not cleaned:
        return ""

    normalized = cleaned.replace(".", "-")

    for pattern in DATE_PATTERNS:
        try:
            parsed = datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
        if not _year_is_valid(parsed.year):
            return ""
        return parsed.isoformat()

    match = re.match(r"^(\d{1,2})-([A-Za-zñÑáéíóúÁÉÍÓÚ]{3,})-(\d{2,4})$", normalized)
    if match:
        day_str, month_name, year_str = match.groups()
        month = _parse_month_name(month_name, line_number)
        year = _normalize_year_component(year_str)
        if year is None:
            return ""
        return date(year, month, int(day_str)).isoformat()

    match = re.match(r"^([A-Za-zñÑáéíóúÁÉÍÓÚ]{3,})-(\d{2,4})$", normalized)
    if match:
        month_name, year_str = match.groups()
        month = _parse_month_name(month_name, line_number)
        year = _normalize_year_component(year_str)
        if year is None:
            return ""
        return date(year, month, 1).isoformat()

    raise ValueError(
        f"Formato de fecha no soportado en 'Fecha programada' (línea {line_number}): {cleaned!r}"
    )


def _year_is_valid(year: int) -> bool:
    return PLAN_MIN_YEAR <= year <= PLAN_MAX_YEAR


def _normalize_year_component(year_str: str) -> Optional[int]:
    digits = year_str.strip()
    if not digits.isdigit():
        return None
    year = int(digits)
    if len(digits) == 2:
        year += 2000 if year < 70 else 1900
    elif len(digits) != 4:
        return None
    if not _year_is_valid(year):
        return None
    return year


def _load_inventory_codes() -> set[str]:
    codes: set[str] = set()
    with INVENTORY_NORMALIZED.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = _normalize_placeholder(row.get("codigo"))
            if code:
                codes.add(code.upper())
    if not codes:
        raise RuntimeError("No se pudieron cargar códigos desde instrumentos_normalizado.csv")
    return codes


def _read_plan_rows(valid_codes: set[str]) -> List[RiskPlanRow]:
    rows: List[RiskPlanRow] = []
    seen_codes: set[str] = set()
    with PLAN_SOURCE.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for line_number, raw in enumerate(reader, start=2):
            instrumento_raw = (raw.get("Instrumento") or "").strip()
            if instrumento_raw.startswith("ND=No disponible") or instrumento_raw.startswith("Rechazado, Dado de baja") or not (raw.get("Código") or "").strip():
                continue
            codigo = _normalize_code(raw.get("Código"), line_number)
            if codigo in seen_codes:
                raise ValueError(
                    f"El código {codigo!r} está duplicado en la línea {line_number}"
                )
            if codigo not in valid_codes:
                raise ValueError(
                    f"El código {codigo!r} (línea {line_number}) no existe en el inventario normalizado"
                )
            seen_codes.add(codigo)

            raw_fecha = raw.get("Fecha programada")
            fecha_programada = (
                _parse_fecha_programada(raw_fecha, line_number)
                if _normalize_placeholder(raw_fecha)
                else ""
            )
            observaciones = _normalize_optional(raw.get("Observaciones"))
            if not fecha_programada:
                if observaciones:
                    if MISSING_CERTIFICATE_MESSAGE not in observaciones:
                        observaciones = f"{observaciones}. {MISSING_CERTIFICATE_MESSAGE}"
                else:
                    observaciones = MISSING_CERTIFICATE_MESSAGE

            row = RiskPlanRow(
                codigo=codigo,
                empresa_id=EMPRESA_ID,
                requerimiento=_normalize_required(raw.get("Requerimiento")),
                impacto_falla=_normalize_required(raw.get("Impacto de la falla")),
                consideraciones_falla=_normalize_required(raw.get("Consideraciones de falla")),
                clase_riesgo=_normalize_required(raw.get("Clase de riesgo")),
                capacidad_deteccion=_normalize_required(raw.get("Capacidad de detección ")),
                frecuencia=_normalize_required(raw.get("Frecuencia")),
                fecha_actualizacion=fecha_programada,
                observaciones=observaciones,
                tipo_calibracion=_normalize_required(raw.get("Tipo de calibración")),
                especificaciones=_normalize_optional(raw.get("Especificaciones")),
            )
            rows.append(row)
    rows.sort(key=lambda r: r.codigo)
    return rows


def _write_csv(rows: Iterable[RiskPlanRow]) -> None:
    PLAN_NORMALIZED.parent.mkdir(parents=True, exist_ok=True)
    with PLAN_NORMALIZED.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADERS)
        for row in rows:
            writer.writerow(row.as_csv_row())


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _sql_value(text: str, *, allow_null: bool = False) -> str:
    if allow_null and text == "":
        return "NULL"
    return f"'{_sql_escape(text)}'"


def _render_sql(rows: List[RiskPlanRow]) -> str:
    buffer = StringIO()
    print("-- Archivo generado automáticamente por generate_plan_riesgos.py", file=buffer)
    print(file=buffer)
    print("START TRANSACTION;", file=buffer)
    print(file=buffer)
    print("INSERT INTO plan_riesgos (", file=buffer)
    print("    instrumento_id,", file=buffer)
    print("    empresa_id,", file=buffer)
    print("    requerimiento,", file=buffer)
    print("    impacto_falla,", file=buffer)
    print("    consideraciones_falla,", file=buffer)
    print("    clase_riesgo,", file=buffer)
    print("    capacidad_deteccion,", file=buffer)
    print("    frecuencia,", file=buffer)
    print("    fecha_actualizacion,", file=buffer)
    print("    observaciones,", file=buffer)
    print("    tipo_calibracion,", file=buffer)
    print("    especificaciones", file=buffer)
    print(")", file=buffer)
    print("VALUES", file=buffer)

    total = len(rows)
    for idx, row in enumerate(rows):
        select_instrumento = (
            "(SELECT id FROM instrumentos WHERE codigo = "
            f"'{_sql_escape(row.codigo)}' AND empresa_id = {row.empresa_id} LIMIT 1)"
        )
        select_empresa = (
            f"(SELECT id FROM empresas WHERE id = {row.empresa_id} LIMIT 1)"
        )
        fecha_value = (
            _sql_value(row.fecha_actualizacion)
            if row.fecha_actualizacion
            else "NULL"
        )
        observaciones_value = _sql_value(row.observaciones, allow_null=True)
        especificaciones_value = _sql_value(row.especificaciones, allow_null=True)

        values = ", ".join(
            [
                select_instrumento,
                select_empresa,
                _sql_value(row.requerimiento),
                _sql_value(row.impacto_falla),
                _sql_value(row.consideraciones_falla),
                _sql_value(row.clase_riesgo),
                _sql_value(row.capacidad_deteccion),
                _sql_value(row.frecuencia),
                fecha_value,
                observaciones_value,
                _sql_value(row.tipo_calibracion),
                especificaciones_value,
            ]
        )
        suffix = "," if idx < total - 1 else ""
        print(f"    ({values}){suffix}", file=buffer)

    print("ON DUPLICATE KEY UPDATE", file=buffer)
    print("    empresa_id = VALUES(empresa_id),", file=buffer)
    print("    requerimiento = VALUES(requerimiento),", file=buffer)
    print("    impacto_falla = VALUES(impacto_falla),", file=buffer)
    print("    consideraciones_falla = VALUES(consideraciones_falla),", file=buffer)
    print("    clase_riesgo = VALUES(clase_riesgo),", file=buffer)
    print("    capacidad_deteccion = VALUES(capacidad_deteccion),", file=buffer)
    print("    frecuencia = VALUES(frecuencia),", file=buffer)
    print("    fecha_actualizacion = VALUES(fecha_actualizacion),", file=buffer)
    print("    observaciones = VALUES(observaciones),", file=buffer)
    print("    tipo_calibracion = VALUES(tipo_calibracion),", file=buffer)
    print("    especificaciones = VALUES(especificaciones);", file=buffer)
    print(file=buffer)
    print("COMMIT;", file=buffer)
    print(file=buffer)
    return buffer.getvalue()


def _write_sql(content: str) -> None:
    PLAN_SQL.parent.mkdir(parents=True, exist_ok=True)
    PLAN_SQL.write_text(content, encoding="utf-8")


def generate() -> None:
    valid_codes = _load_inventory_codes()
    rows = _read_plan_rows(valid_codes)
    _write_csv(rows)
    sql = _render_sql(rows)
    _write_sql(sql)


if __name__ == "__main__":
    generate()
