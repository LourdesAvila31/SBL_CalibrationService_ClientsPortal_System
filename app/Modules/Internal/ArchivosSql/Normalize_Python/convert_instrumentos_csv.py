#!/usr/bin/env python3
"""Convierte el inventario maestro a IDs numéricos usando el repositorio oficial.

El archivo de salida (`normalize_instrumentos.csv`) conserva las demás columnas
pero reemplaza `catalogo_id`, `marca_id`, `modelo_id` y `departamento_id` por sus
identificadores enteros y agrega `empresa_id`. Durante la conversión se limpian
los valores `NA`/`ND`, se normalizan las fechas (`fecha_alta`,
`fecha_baja`, `proxima_calibracion`) y se recalculan los campos `estado` y
`programado` aplicando las reglas corporativas. A partir de esta versión el
script toma como fuente primaria `LM_instrumentos_original_v2.csv` (el listado
maestro ubicado en `Archivos_CSV_originales/`) para eliminar desfaces con el
inventario y aplica la "Fecha programada" del plan de riesgos como
`proxima_calibracion`, redondeada al primer día del mes para respetar la
visualización mes-año del panel interno.

Ejemplo de uso desde la raíz del proyecto:

```bash
python app/Modules/Internal/ArchivosSql/Normalize_Python/convert_instrumentos_csv.py \
    --output app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_instrumentos.csv
```
"""
from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence

BASE_DIR = Path(__file__).resolve().parent
ARCHIVOS_SQL_DIR = BASE_DIR.parent
CSV_ORIGINAL_DIR = ARCHIVOS_SQL_DIR / "Archivos_CSV_originales"
NORMALIZE_DIR = ARCHIVOS_SQL_DIR / "Archivos_Normalize"
README_DIR = ARCHIVOS_SQL_DIR / "ReadMe_BD"


def _resolve_csv_path(*relative_names: str) -> Path:
    """Devuelve la primera ruta existente dentro de Archivos_CSV_originales."""
    for name in relative_names:
        candidate = CSV_ORIGINAL_DIR / name
        if candidate.exists():
            return candidate
    return CSV_ORIGINAL_DIR / relative_names[0]


DEFAULT_NORMALIZADO = README_DIR / "README_NORMALIZADO.md"
DEFAULT_LEGACY_INPUT = _resolve_csv_path("instrumentos_original.csv")
DEFAULT_MASTER_INPUT = _resolve_csv_path("LM_instrumentos_original_v2.csv", "LM_instrumentos_original.csv")
DEFAULT_OUTPUT_CSV = NORMALIZE_DIR / "normalize_instrumentos.csv"
DEFAULT_PLAN_RIESGOS = _resolve_csv_path("PR_instrumentos_original_v2.csv", "PR_instrumentos_original.csv")
EMPRESA_ID = 1

MISSING_CERTIFICATE_MESSAGE = "No se ha añadido su primer certificado"
MIN_OPERATIONAL_YEAR = 2015
MAX_OPERATIONAL_YEAR = date.today().year + 10
DUE_DATE_COLUMNS = {"proxima_calibracion", "fecha programada"}

CANONICAL_FIELDNAMES = [
    "catalogo_id",
    "marca_id",
    "modelo_id",
    "serie",
    "codigo",
    "departamento_id",
    "ubicacion",
    "fecha_alta",
    "fecha_baja",
    "proxima_calibracion",
    "estado",
    "programado",
]

SECTION_PATTERNS = {
    "departamentos": r"## Departamentos.*?```text(.*?)```",
    "catalogo": r"## Catálogo de instrumentos.*?```text(.*?)```",
    "marcas": r"## Marcas.*?```text(.*?)```",
    "modelos": r"## Modelos.*?```text(.*?)```",
}

COLUMN_DEFAULTS = {
    "marca_id": "NA",
    "modelo_id": "NA",
    "departamento_id": "NA",
}

ADDITIONAL_ALIASES = {
    "catalogo_id": {
        "#REF!": "Báscula digital",
        "Parilla con agitador": "Parrilla con agitador",
        "Sistema de filtros de UMA´s": "Sistema de filtros de UMAs",
        "ND=No disponible NA=No aplica Rechazado, Dado de baja= Rechazado en su calibración Dado de baja=Pérdida, avería sin reparación, fuera de uso": "Termostato programable",
    },
    "marca_id": {
        "Extech": "Extech Instruments / Extech Instruments",
        "Extech Instrument": "Extech Instruments / Extech Instruments",
        "Chimney ballon": "Chimney Balloon",
        "Tube&Socket": "Tube & Socket",
    },
    "modelo_id": {
        "0.5 - 5 mL": "0.5-5 mL",
        "100 - 1000 μL": "100-1000 μL",
        "Research plus 100 - 1000 μL": "Research plus 100-1000 μL",
        "Pocket Pro + Multi 2": "Pocket Pro+Multi2",
        "HET5SAML1C/HET9SAML1C": "HET5SAML1C",
        "MS9-LWS-G-U-V/MS6-LF-1/2-CRM/MS6-LFM-1/2-BUV-HF-DA/MS6-LFM-1/2-AUV-HF-DA/MS6-LFM-1/2-AR-HF-DA:*W": "MS9-LWS-G-U-V",
        "AW-20/AM250C/AMD250C": "AW-20",
        "AFR 2000/AL 2000": "AFR 2000",
        "MS6-LF-1/2-CRV/MS6-LFM-1/2-BRM-DA/MS6-LFM-1/2-ARV-DA": "MS6-LF-1/2-CRV",
        "76 mm": "76mm",
    },
}

PLACEHOLDER_VALUES = {"NA", "ND", "N/A", "N.D.", "N-D", "#VALUE!"}

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
    "%Y/%m/%d",
)

OBS_CORPORATIVAS = {
    "rechazado": "Rechazado en calibración",
    "baja": "Instrumento dado de baja",
}


@dataclass(frozen=True)
class PlanMetadata:
    """Información relevante del plan de riesgos para un instrumento."""

    observacion: str
    has_baja: bool
    has_rechazo: bool
    fecha_programada_str: str
    fecha_programada_dt: Optional[date]


def _lookup_key(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    ascii_value = re.sub(r"\s+", " ", ascii_value)
    return ascii_value.strip().lower()


def _clean_placeholder(value: Optional[str]) -> str:
    if value is None:
        return ""
    trimmed = value.strip()
    if not trimmed:
        return ""
    ascii_upper = unicodedata.normalize("NFKD", trimmed).encode("ascii", "ignore").decode().upper()
    if ascii_upper in PLACEHOLDER_VALUES:
        return ""
    return trimmed


def _is_due_date_column(column: str) -> bool:
    normalized = unicodedata.normalize("NFKD", column).encode("ascii", "ignore").decode().strip().lower()
    return normalized in DUE_DATE_COLUMNS


def _year_within_operational_range(year: int) -> bool:
    return MIN_OPERATIONAL_YEAR <= year <= MAX_OPERATIONAL_YEAR


def _parse_date_value(
    raw_value: Optional[str],
    column: str,
    line_number: int,
) -> tuple[str, Optional[date]]:
    cleaned = _clean_placeholder(raw_value)
    if not cleaned:
        return "", None

    due_date_column = _is_due_date_column(column)

    def _build_date(year: int, month: int, day: int) -> tuple[str, Optional[date]]:
        if due_date_column and not _year_within_operational_range(year):
            return "", None
        parsed = date(year, month, day)
        return parsed.isoformat(), parsed

    normalized = cleaned.replace(".", "-")
    # Detect formatos con nombre de mes (es/en) como 14-Abr-22
    match = re.match(r"^(\d{1,2})-([A-Za-zñÑáéíóúÁÉÍÓÚ]{3,})-(\d{2,4})$", normalized)
    if match:
        day_str, month_name, year_str = match.groups()
        month_key = (
            unicodedata.normalize("NFKD", month_name)
            .encode("ascii", "ignore")
            .decode()
            .upper()
            .replace(".", "")
        )
        month_key = month_key[:4] if month_key.startswith("SEPT") else month_key[:3]
        month_number = SPANISH_MONTHS.get(month_key)
        if month_number is None:
            try:
                month_number = datetime.strptime(month_key, "%b").month
            except ValueError as exc:  # pragma: no cover - rutas inesperadas
                raise ValueError(
                    f"Mes desconocido '{month_name}' en la columna '{column}' (línea {line_number})"
                ) from exc
        year = int(year_str)
        if year < 100:
            year += 2000 if year < 70 else 1900
        result_str, result_dt = _build_date(year, month_number, int(day_str))
        if due_date_column and result_dt is None:
            return "", None
        return result_str, result_dt

    match_month_year = re.match(r"^([A-Za-zñÑáéíóúÁÉÍÓÚ]{3,})-(\d{2,4})$", normalized)
    if match_month_year:
        if due_date_column:
            return "", None
        month_name, year_str = match_month_year.groups()
        month_key = (
            unicodedata.normalize("NFKD", month_name)
            .encode("ascii", "ignore")
            .decode()
            .upper()
            .replace(".", "")
        )
        month_key = month_key[:4] if month_key.startswith("SEPT") else month_key[:3]
        month_number = SPANISH_MONTHS.get(month_key)
        if month_number is None:
            try:
                month_number = datetime.strptime(month_key, "%b").month
            except ValueError as exc:  # pragma: no cover - rutas inesperadas
                raise ValueError(
                    f"Mes desconocido '{month_name}' en la columna '{column}' (línea {line_number})"
                ) from exc
        year = int(year_str)
        if year < 100:
            year += 2000 if year < 70 else 1900
        result_str, result_dt = _build_date(year, month_number, 1)
        if due_date_column and result_dt is None:
            return "", None
        return result_str, result_dt

    for pattern in DATE_PATTERNS:
        try:
            parsed_dt = datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
        if due_date_column and not _year_within_operational_range(parsed_dt.year):
            return "", None
        return parsed_dt.isoformat(), parsed_dt

    raise ValueError(
        f"Formato de fecha no soportado en la columna '{column}' (línea {line_number}): '{cleaned}'"
    )


def _normalize_observacion(value: Optional[str]) -> tuple[str, bool, bool]:
    cleaned = _clean_placeholder(value)
    if not cleaned:
        return "", False, False

    normalized = (
        unicodedata.normalize("NFKD", cleaned)
        .encode("ascii", "ignore")
        .decode()
        .lower()
    )
    has_rechazo = "rechaz" in normalized
    has_baja = "baja" in normalized

    if has_rechazo:
        mapped = OBS_CORPORATIVAS["rechazado"]
    elif has_baja:
        mapped = OBS_CORPORATIVAS["baja"]
    else:
        mapped = cleaned

    return mapped, has_baja, has_rechazo


def _load_plan_metadata(plan_csv: Path) -> dict[str, PlanMetadata]:
    if not plan_csv.exists():
        print(
            f"[INFO] No se encontró el plan de riesgos en {plan_csv}. "
            "Se continuará sin observaciones adicionales."
        )
        return {}

    mapping: dict[str, PlanMetadata] = {}
    with plan_csv.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for line_number, row in enumerate(reader, start=2):
            codigo = _clean_placeholder(row.get("Código"))
            if not codigo:
                continue

            observacion = row.get("Observaciones")
            corporativa, has_baja, has_rechazo = _normalize_observacion(observacion)

            fecha_prog_raw = row.get("Fecha programada")
            fecha_prog_cleaned = _clean_placeholder(fecha_prog_raw)
            fecha_prog_str, fecha_prog_dt = _parse_date_value(
                fecha_prog_raw, "Fecha programada", line_number
            )
            if fecha_prog_dt is not None:
                fecha_prog_dt = date(fecha_prog_dt.year, fecha_prog_dt.month, 1)
                fecha_prog_str = fecha_prog_dt.isoformat()
            elif fecha_prog_cleaned:
                corporativa = corporativa or MISSING_CERTIFICATE_MESSAGE
                fecha_prog_str = ""

            if not (corporativa or has_baja or has_rechazo or fecha_prog_dt):
                continue

            mapping[codigo.strip().upper()] = PlanMetadata(
                observacion=corporativa,
                has_baja=has_baja,
                has_rechazo=has_rechazo,
                fecha_programada_str=fecha_prog_str,
                fecha_programada_dt=fecha_prog_dt,
            )
    return mapping


def _calibracion_vigente(fecha: Optional[date]) -> bool:
    if fecha is None:
        return False
    return fecha >= date.today()


def _derivar_estado(
    fecha_alta: Optional[date],
    proxima_calibracion: Optional[date],
    fecha_baja: Optional[date],
    observacion_info: Optional[tuple[str, bool, bool]],
) -> str:
    tiene_baja = fecha_baja is not None
    if observacion_info:
        _, obs_baja, obs_rechazo = observacion_info
        tiene_baja = tiene_baja or obs_baja or obs_rechazo

    if tiene_baja:
        return "Inactivo"
    if fecha_alta is None:
        return "Stock"
    if _calibracion_vigente(proxima_calibracion):
        return "Activo"
    if proxima_calibracion is None:
        return "Stock"
    # Si hay fecha de calibración pero ya no es vigente, mantenerlo como activo para
    # permitir programaciones pendientes sin marcarlo como baja automática.
    return "Activo"


def _parse_table(section: str, normalizado_path: Path) -> dict[str, int]:
    pattern = SECTION_PATTERNS[section]
    content = normalizado_path.read_text(encoding="utf-8")
    match = re.search(pattern, content, flags=re.S)
    if not match:
        raise RuntimeError(
            f"No se encontró la sección '{section}' en {normalizado_path}"
        )

    table_text = match.group(1)
    mapping: dict[str, int] = {}
    for raw_line in table_text.splitlines():
        line = raw_line.strip()
        if not line or line.lower().startswith("id"):
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) < 2:
            continue
        try:
            identifier = int(parts[0])
        except ValueError as exc:
            raise ValueError(f"No se pudo parsear el ID en la línea: '{raw_line}'") from exc
        nombre = parts[1].strip()
        key_full = _lookup_key(nombre)
        mapping[key_full] = identifier
        # Si el nombre contiene variantes separadas por '/', registrar cada una.
        for alias in nombre.split("/"):
            alias_clean = alias.strip()
            if not alias_clean:
                continue
            alias_key = _lookup_key(alias_clean)
            mapping.setdefault(alias_key, identifier)
    return mapping


def _build_mappings(normalizado_path: Path) -> dict[str, dict[str, int]]:
    mappings = {
        "catalogo_id": _parse_table("catalogo", normalizado_path),
        "marca_id": _parse_table("marcas", normalizado_path),
        "modelo_id": _parse_table("modelos", normalizado_path),
        "departamento_id": _parse_table("departamentos", normalizado_path),
    }

    for column, aliases in ADDITIONAL_ALIASES.items():
        target_mapping = mappings[column]
        for alias, canonical in aliases.items():
            alias_key = _lookup_key(alias)
            canonical_key = _lookup_key(canonical)
            if canonical_key not in target_mapping:
                raise KeyError(
                    f"La variante canonica '{canonical}' no se encontró en {column}."
                )
            target_mapping[alias_key] = target_mapping[canonical_key]

    return mappings


def _normalize_value(column: str, value: str) -> str:
    normalized = value.replace("\r", "").strip()
    if "\n" in normalized:
        if column == "modelo_id":
            normalized = normalized.replace("\n", "/")
        else:
            normalized = re.sub(r"\s+", " ", normalized)
    normalized_upper = normalized.upper()
    if normalized_upper in {"N/A", "N.A."}:
        normalized = "NA"
    elif normalized_upper == "N-D":
        normalized = "ND"

    if not normalized and column in COLUMN_DEFAULTS:
        normalized = COLUMN_DEFAULTS[column]

    return normalized


def _load_input_rows(
    master_candidates: list[Path], legacy_candidates: list[Path]
) -> tuple[list[dict[str, str]], list[str]]:
    for master_path in master_candidates:
        if not master_path.exists():
            continue
        rows: list[dict[str, str]] = []
        with master_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                rows.append(
                    {
                        "catalogo_id": raw.get("Instrumento", ""),
                        "marca_id": raw.get("Marca", ""),
                        "modelo_id": raw.get("Modelo", ""),
                        "serie": raw.get("Serie", ""),
                        "codigo": raw.get("Código", ""),
                        "departamento_id": raw.get(
                            "Departamento responsable", ""
                        ),
                        "ubicacion": raw.get("Ubicación", ""),
                        "fecha_alta": raw.get("Fecha de alta", ""),
                        "fecha_baja": raw.get("Fecha de baja", ""),
                        "proxima_calibracion": "",
                        "estado": "Activo",
                        "programado": "1",
                    }
                )
        return rows, list(CANONICAL_FIELDNAMES)

    for legacy_path in legacy_candidates:
        if not legacy_path.exists():
            continue
        with legacy_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            fieldnames = reader.fieldnames
            if not fieldnames:
                raise RuntimeError("El CSV de entrada no tiene encabezados")
        return rows, list(fieldnames)

    tried = [str(path) for path in master_candidates + legacy_candidates]
    joined = "\n  - ".join(tried)
    raise FileNotFoundError(
        "No se encontró ni el listado maestro ni el CSV legado. Se buscaron las"
        f" rutas:\n  - {joined}"
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--normalizado",
        type=Path,
        default=DEFAULT_NORMALIZADO,
        help="Ruta del README con los catálogos normalizados.",
    )
    parser.add_argument(
        "--master",
        type=Path,
        default=DEFAULT_MASTER_INPUT,
        help="Listado maestro exportado desde LM_instrumentos.",
    )
    parser.add_argument(
        "--legacy",
        type=Path,
        default=DEFAULT_LEGACY_INPUT,
        help="CSV legado de instrumentos (si aún se usa).",
    )
    parser.add_argument(
        "--plan",
        type=Path,
        default=DEFAULT_PLAN_RIESGOS,
        help="Plan de riesgos original para complementar observaciones.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help="Archivo CSV normalizado que se generará.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    if not args.normalizado.exists():
        raise FileNotFoundError(
            f"No se encontró el catálogo normalizado en: {args.normalizado}"
        )

    master_candidates = [args.master, BASE_DIR / "LM_instrumentos.csv"]
    legacy_candidates = [args.legacy, BASE_DIR / "instrumentos.csv"]

    mappings = _build_mappings(args.normalizado)
    plan_metadata = _load_plan_metadata(args.plan)

    input_rows, original_fieldnames = _load_input_rows(
        master_candidates, legacy_candidates
    )

    fieldnames = list(original_fieldnames)
    if "empresa_id" not in fieldnames:
        insert_pos = fieldnames.index("modelo_id") + 1
        fieldnames.insert(insert_pos, "empresa_id")

    rows = []
    for line_number, row in enumerate(input_rows, start=2):
        raw_catalogo = (row.get("catalogo_id") or "").strip()
        if raw_catalogo.startswith("ND=No disponible") or raw_catalogo.startswith("Rechazado, Dado de baja"):
            continue
        transformed = dict(row)
        transformed["codigo"] = _clean_placeholder(transformed.get("codigo"))
        codigo = transformed["codigo"]
        if not codigo:
            raise ValueError(f"La columna 'codigo' está vacía en la línea {line_number}")
        for column in ("catalogo_id", "marca_id", "modelo_id", "departamento_id"):
            raw_value = transformed.get(column)
            if raw_value is None or raw_value == "":
                if column in COLUMN_DEFAULTS:
                    normalized = COLUMN_DEFAULTS[column]
                else:
                    raise ValueError(
                        f"La columna '{column}' está vacía en la línea {line_number}: {row}"
                    )
            else:
                normalized = _normalize_value(column, raw_value)
            lookup = _lookup_key(normalized)
            try:
                mapped_id = mappings[column][lookup]
            except KeyError as exc:
                raise KeyError(
                    f"Valor '{normalized}' de la columna '{column}' no se encontró en "
                    f"las tablas de referencia (línea {line_number})"
                ) from exc
            transformed[column] = str(mapped_id)

        fecha_alta_str, fecha_alta_dt = _parse_date_value(
            row.get("fecha_alta"), "fecha_alta", line_number
        )
        fecha_baja_str, fecha_baja_dt = _parse_date_value(
            row.get("fecha_baja"), "fecha_baja", line_number
        )
        proxima_calibracion_str, proxima_calibracion_dt = _parse_date_value(
            row.get("proxima_calibracion"), "proxima_calibracion", line_number
        )

        plan_info = plan_metadata.get(codigo.upper())
        observacion_info: Optional[tuple[str, bool, bool]] = None
        if plan_info:
            if plan_info.observacion or plan_info.has_baja or plan_info.has_rechazo:
                observacion_info = (
                    plan_info.observacion,
                    plan_info.has_baja,
                    plan_info.has_rechazo,
                )
            if plan_info.fecha_programada_dt is not None:
                proxima_calibracion_dt = plan_info.fecha_programada_dt
                proxima_calibracion_str = plan_info.fecha_programada_str

        transformed["fecha_alta"] = fecha_alta_str
        transformed["fecha_baja"] = fecha_baja_str
        transformed["proxima_calibracion"] = proxima_calibracion_str

        # El estado corporativo depende de las fechas normalizadas y de los
        # hallazgos del plan de riesgos (bajas o rechazos documentados).
        estado = _derivar_estado(
            fecha_alta_dt, proxima_calibracion_dt, fecha_baja_dt, observacion_info
        )
        transformed["estado"] = estado
        if "programado" in transformed:
            # Los instrumentos activos quedan programados automáticamente; el
            # resto se marca como "no programado" para evitar que aparezcan
            # en listados operativos.
            transformed["programado"] = "1" if estado == "Activo" else "0"
        transformed["empresa_id"] = str(EMPRESA_ID)
        rows.append(transformed)

    output_path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # Validación: asegurar que los campos *_id sean numéricos antes de escribir.
            for column in ("catalogo_id", "marca_id", "modelo_id", "departamento_id", "empresa_id"):
                value = row[column]
                if not value.isdigit():
                    raise ValueError(
                        f"El valor '{value}' en la columna '{column}' no es numérico (fila {row})"
                    )
            writer.writerow(row)


if __name__ == "__main__":
    main()
