#!/usr/bin/env python3
"""Genera archivos SQL para poblar historiales de instrumentos.

El script toma como insumo principal ``normalize_instrumentos.csv`` y construye
``INSERT`` individuales para las tablas ``historial_departamentos``,
``historial_ubicaciones``, ``historial_fecha_alta``, ``historial_fecha_baja``,
``historial_tipos_instrumento``, ``historial_calibraciones``,
``historial_especificaciones`` y ``historial_estado_instrumento``. Cada archivo
se escribe dentro de ``Archivos_BD_SBL/SBL_historiales/`` con el formato
requerido por phpMyAdmin.

Adem?s consume los archivos normalizados ``normalize_plan_riesgos.csv`` y
``normalize_certificates.csv`` para poblar los historiales complementarios con
datos reales de la empresa obtenidos desde ``Archivos_CSV_originales``.

Puedes sobreescribir el archivo de entrada, el directorio de salida y el
``empresa_id`` objetivo mediante argumentos de l?nea de comandos:

```bash
python app/Modules/Internal/ArchivosSql/Normalize_Python/generate_historial_inserts.py     --input app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_instrumentos.csv     --output-dir app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_historiales     --empresa-id 1
```

Si en el futuro se dispone de un CSV con los datos normalizados del audit trail,
este script puede ampliarse para incluir movimientos hist?ricos adicionales
leyendo ``audit_trail_normalizado.csv``.
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib
import argparse
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence, Optional

ROOT = pathlib.Path(__file__).resolve().parent
DEFAULT_EMPRESA_ID = 1
DEFAULT_INPUT = ROOT.parent / "Archivos_Normalize" / "normalize_instrumentos.csv"
DEFAULT_OUTPUT_DIR = ROOT.parent / "Archivos_BD_SBL" / "SBL_historiales"
DEFAULT_PLAN_PATH = ROOT.parent / "Archivos_Normalize" / "normalize_plan_riesgos.csv"
DEFAULT_CERTIFICATES_PATH = ROOT.parent / "Archivos_Normalize" / "normalize_certificates.csv"
SQL_DATE_FORMAT = "%Y-%m-%d"

NA_VALUES = {"", "NA", "N/A", "ND", "NULL", "null", None}
NA_VALUES_UPPER = {str(value).upper() for value in NA_VALUES if isinstance(value, str)}


@dataclass(frozen=True)
class InstrumentRecord:
    """Valores relevantes del inventario normalizado."""

    codigo: str
    departamento_id: int | None
    ubicacion: str | None
    fecha_alta: dt.date | None
    fecha_baja: dt.date | None
    estado: str | None


@dataclass(frozen=True)
class PlanRiskEntry:
    """Informaci?n relevante del plan de riesgos normalizado."""

    codigo: str
    empresa_id: int
    fecha_actualizacion: dt.date | None
    requerimiento: Optional[str]
    observaciones: Optional[str]
    tipo_calibracion: Optional[str]
    especificaciones: Optional[str]


@dataclass(frozen=True)
class CalibrationEvent:
    """Evento de calibraci?n detectado en los certificados normalizados."""

    codigo: str
    empresa_id: int
    fecha_evento: dt.date
    tipo_evento: str
    descripcion: str
    certificado_codigo: Optional[str]


@dataclass(frozen=True)
class InsertStatement:
    """Representa un bloque `INSERT ... SELECT` listo para imprimirse."""

    table: str
    sql: str


def read_instruments(path: pathlib.Path) -> Iterable[InstrumentRecord]:
    """Carga el CSV normalizado en memoria."""

    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo requerido: {path.as_posix()}"
        )

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            codigo = (row.get("codigo") or "").strip()
            if not codigo:
                continue

            departamento_raw = (row.get("departamento_id") or "").strip()
            departamento_id = int(departamento_raw) if departamento_raw else None

            ubicacion_raw = normalize_text(row.get("ubicacion"))
            if ubicacion_raw and ubicacion_raw.upper() not in NA_VALUES_UPPER:
                ubicacion = ubicacion_raw
            else:
                ubicacion = None

            fecha_alta = parse_date(row.get("fecha_alta"))
            fecha_baja = parse_date(row.get("fecha_baja"))

            estado_raw = normalize_text(row.get("estado"))
            if estado_raw and estado_raw.upper() not in NA_VALUES_UPPER:
                estado = estado_raw
            else:
                estado = None

            yield InstrumentRecord(
                codigo=codigo,
                departamento_id=departamento_id,
                ubicacion=ubicacion,
                fecha_alta=fecha_alta,
                fecha_baja=fecha_baja,
                estado=estado,
            )


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned.strip()


def sanitize_text(value: str | None, *, preserve_newlines: bool = False) -> Optional[str]:
    if value is None:
        return None
    if preserve_newlines:
        normalized = value.replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.strip() for line in normalized.split("\n")]
        cleaned = "\n".join(line for line in lines if line)
    else:
        cleaned = normalize_text(value)
    if not cleaned:
        return None
    if cleaned.upper() in NA_VALUES_UPPER:
        return None
    return cleaned


def parse_empresa_id(raw: str | None) -> int:
    if raw is None:
        return DEFAULT_EMPRESA_ID
    cleaned = raw.strip()
    if not cleaned:
        return DEFAULT_EMPRESA_ID
    try:
        return int(cleaned)
    except ValueError:
        return DEFAULT_EMPRESA_ID


def parse_date(value: str | None) -> dt.date | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.upper() in NA_VALUES:
        return None
    return dt.datetime.strptime(cleaned, SQL_DATE_FORMAT).date()


def sql_quote(text: str) -> str:
    escaped = (
        text.replace("\\", "\\\\")
        .replace("'", "''")
        .replace("\r", "")
        .replace("\n", "\\n")
    )
    return f"'{escaped}'"


def date_expr(date_value: dt.date | None) -> str:
    if date_value is None:
        return "NULL"
    return (
        "STR_TO_DATE(" + sql_quote(date_value.strftime(SQL_DATE_FORMAT)) + ", '%Y-%m-%d')"
    )


def coalesce_timestamp(expr: str) -> str:
    if expr == "NULL":
        return "NOW()"
    return f"COALESCE({expr}, NOW())"


def coalesce_date(expr: str) -> str:
    if expr == "NULL":
        return "CURDATE()"
    return f"COALESCE({expr}, CURDATE())"


def build_department_inserts(record: InstrumentRecord) -> list[InsertStatement]:
    if record.departamento_id is None:
        return []

    fecha_expr = date_expr(record.fecha_alta)
    timestamp_expr = coalesce_timestamp(fecha_expr)

    sql = f"""
INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, {record.departamento_id}, @empresa_id, {timestamp_expr}, {timestamp_expr}
FROM instrumentos i
WHERE i.codigo = {sql_quote(record.codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = {record.departamento_id}
        AND hd.empresa_id = @empresa_id
  );
""".strip()

    return [InsertStatement(table="historial_departamentos", sql=sql)]


def build_location_inserts(record: InstrumentRecord) -> list[InsertStatement]:
    if record.ubicacion is None:
        return []

    fecha_expr = date_expr(record.fecha_alta)
    timestamp_expr = coalesce_timestamp(fecha_expr)

    sql = f"""
INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, {sql_quote(record.ubicacion)}, @empresa_id, {timestamp_expr}, {timestamp_expr}
FROM instrumentos i
WHERE i.codigo = {sql_quote(record.codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = {sql_quote(record.ubicacion)}
        AND hu.empresa_id = @empresa_id
  );
""".strip()

    return [InsertStatement(table="historial_ubicaciones", sql=sql)]


def build_fecha_alta_inserts(record: InstrumentRecord) -> list[InsertStatement]:
    if record.fecha_alta is None:
        return []

    fecha_expr = date_expr(record.fecha_alta)
    timestamp_expr = coalesce_timestamp(fecha_expr)

    sql = f"""
INSERT INTO historial_fecha_alta (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, {fecha_expr}, @empresa_id, {timestamp_expr}
FROM instrumentos i
WHERE i.codigo = {sql_quote(record.codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_alta hfa
      WHERE hfa.instrumento_id = i.id
        AND hfa.fecha = {fecha_expr}
        AND hfa.empresa_id = @empresa_id
  );
""".strip()

    return [InsertStatement(table="historial_fecha_alta", sql=sql)]


def build_fecha_baja_inserts(record: InstrumentRecord) -> list[InsertStatement]:
    if record.fecha_baja is None:
        return []

    fecha_expr = date_expr(record.fecha_baja)
    timestamp_expr = coalesce_timestamp(fecha_expr)

    sql = f"""
INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, {fecha_expr}, @empresa_id, {timestamp_expr}
FROM instrumentos i
WHERE i.codigo = {sql_quote(record.codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = {fecha_expr}
        AND hfb.empresa_id = @empresa_id
  );
""".strip()

    return [InsertStatement(table="historial_fecha_baja", sql=sql)]


def build_estado_inserts(record: InstrumentRecord) -> list[InsertStatement]:
    if record.estado is None:
        return []

    if record.estado.upper() in {"NA", "ND", "N/A", "NULL", "SIN DATO"}:
        return []

    fecha_base = record.fecha_baja if record.estado.lower().startswith("ina") else record.fecha_alta
    fecha_expr = date_expr(fecha_base)
    timestamp_expr = coalesce_timestamp(fecha_expr)

    sql = f"""
INSERT INTO historial_tipos_instrumento (instrumento_id, estado, empresa_id, fecha, `timestamp`)
SELECT i.id, {sql_quote(record.estado)}, @empresa_id, {timestamp_expr}, {timestamp_expr}
FROM instrumentos i
WHERE i.codigo = {sql_quote(record.codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_tipos_instrumento hti
      WHERE hti.instrumento_id = i.id
        AND hti.estado = {sql_quote(record.estado)}
        AND hti.empresa_id = @empresa_id
  );
""".strip()

    return [InsertStatement(table="historial_tipos_instrumento", sql=sql)]


def load_plan_riesgos(path: pathlib.Path | None) -> dict[str, list[PlanRiskEntry]]:
    if path is None or not path.exists():
        return {}
    plan_records: dict[str, list[PlanRiskEntry]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            codigo_raw = sanitize_text(row.get("instrumento_codigo"))
            if not codigo_raw:
                continue
            codigo = codigo_raw.upper()
            empresa_id = parse_empresa_id(row.get("empresa_id"))
            fecha_actualizacion = parse_date(row.get("fecha_actualizacion"))
            plan_records.setdefault(codigo, []).append(
                PlanRiskEntry(
                    codigo=codigo,
                    empresa_id=empresa_id,
                    fecha_actualizacion=fecha_actualizacion,
                    requerimiento=sanitize_text(row.get("requerimiento")),
                    observaciones=sanitize_text(row.get("observaciones"), preserve_newlines=True),
                    tipo_calibracion=sanitize_text(row.get("tipo_calibracion")),
                    especificaciones=sanitize_text(row.get("especificaciones"), preserve_newlines=True),
                )
            )
    return plan_records


def select_plan_entry(
    entries: Sequence[PlanRiskEntry] | None,
    empresa_id: int,
) -> Optional[PlanRiskEntry]:
    if not entries:
        return None
    filtered = [entry for entry in entries if entry.empresa_id == empresa_id]
    candidates = filtered or list(entries)
    return max(
        candidates,
        key=lambda entry: entry.fecha_actualizacion or dt.date.min,
    )


def load_calibration_events(path: pathlib.Path | None) -> list[CalibrationEvent]:
    if path is None or not path.exists():
        return []
    events: list[CalibrationEvent] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            codigo_raw = sanitize_text(row.get("instrumento_codigo"))
            if not codigo_raw:
                continue
            codigo = codigo_raw.upper()
            fecha_evento = parse_date(row.get("fecha_calibracion"))
            if fecha_evento is None:
                continue
            empresa_id = parse_empresa_id(row.get("empresa_id"))
            tipo_evento = sanitize_text(row.get("tipo")) or "Calibraci?n"
            descripcion = sanitize_text(row.get("observaciones"), preserve_newlines=True)
            if not descripcion:
                descripcion = sanitize_text(row.get("requerimiento"))
            if not descripcion:
                periodo_label = sanitize_text(row.get("periodo_label"))
                periodo_year = sanitize_text(row.get("periodo_year"))
                periodo = sanitize_text(row.get("periodo"))
                detalles: list[str] = []
                if periodo_label and periodo_year:
                    detalles.append(f"{periodo_label} {periodo_year}")
                elif periodo_label:
                    detalles.append(periodo_label)
                if periodo and periodo not in detalles:
                    detalles.append(periodo)
                if detalles:
                    descripcion = ". ".join(detalles)
            if not descripcion:
                descripcion = f"Calibraci?n registrada en {path.name}"
            certificado_codigo = sanitize_text(row.get("certificado_codigo"))
            events.append(
                CalibrationEvent(
                    codigo=codigo,
                    empresa_id=empresa_id,
                    fecha_evento=fecha_evento,
                    tipo_evento=tipo_evento,
                    descripcion=descripcion,
                    certificado_codigo=certificado_codigo,
                )
            )
    events.sort(key=lambda event: (event.codigo, event.fecha_evento, event.tipo_evento))
    return events


def build_calibration_inserts(
    events: Sequence[CalibrationEvent],
    empresa_id: int,
    known_codes: set[str],
) -> list[InsertStatement]:
    statements: list[InsertStatement] = []
    seen: set[tuple[str, dt.date, str]] = set()
    for event in events:
        if event.empresa_id != empresa_id:
            continue
        if event.codigo not in known_codes:
            continue
        key = (event.codigo, event.fecha_evento, event.tipo_evento)
        if key in seen:
            continue
        seen.add(key)
        fecha_expr = date_expr(event.fecha_evento)
        tipo_expr = sql_quote(event.tipo_evento)
        descripcion_expr = sql_quote(event.descripcion)
        certificado_expr = sql_quote(event.certificado_codigo) if event.certificado_codigo else "NULL"
        sql = f"""
INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, {fecha_expr}, {tipo_expr}, {descripcion_expr}, {certificado_expr}, NULL
FROM instrumentos i
WHERE i.codigo = {sql_quote(event.codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = {fecha_expr}
        AND hc.tipo_evento = {tipo_expr}
  );
""".strip()
        statements.append(InsertStatement(table="historial_calibraciones", sql=sql))
    return statements


def build_specification_inserts(
    plan_records: Mapping[str, Sequence[PlanRiskEntry]],
    empresa_id: int,
    known_codes: set[str],
) -> list[InsertStatement]:
    statements: list[InsertStatement] = []
    seen: set[tuple[str, dt.date, str]] = set()
    for codigo in sorted(plan_records.keys()):
        if codigo not in known_codes:
            continue
        for entry in plan_records[codigo]:
            if entry.empresa_id != empresa_id:
                continue
            if entry.especificaciones is None:
                continue
            if entry.fecha_actualizacion is None:
                continue
            version = entry.tipo_calibracion or "General"
            key = (codigo, entry.fecha_actualizacion, version, entry.especificaciones)
            if key in seen:
                continue
            seen.add(key)
            fecha_expr = date_expr(entry.fecha_actualizacion)
            version_expr = sql_quote(version)
            descripcion_expr = sql_quote(entry.especificaciones)
            sql = f"""
INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, {fecha_expr}, {version_expr}, {descripcion_expr}, NULL
FROM instrumentos i
WHERE i.codigo = {sql_quote(codigo)}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = {fecha_expr}
        AND he.version = {version_expr}
  );
""".strip()
            statements.append(InsertStatement(table="historial_especificaciones", sql=sql))
    return statements


def build_estado_historial_inserts(
    record: InstrumentRecord,
    plan_entry: Optional[PlanRiskEntry],
) -> list[InsertStatement]:
    if record.estado is None:
        return []
    estado = record.estado.strip()
    if not estado or estado.upper() in NA_VALUES_UPPER:
        return []
    if estado.lower().startswith("ina"):
        fecha_base = record.fecha_baja
    else:
        fecha_base = record.fecha_alta
    if fecha_base is None and plan_entry and plan_entry.fecha_actualizacion:
        fecha_base = plan_entry.fecha_actualizacion
    fecha_expr = date_expr(fecha_base)
    fecha_evento_expr = coalesce_date(fecha_expr)
    motivo_candidates: list[str] = []
    if plan_entry:
        if plan_entry.observaciones:
            motivo_candidates.append(plan_entry.observaciones)
        if plan_entry.requerimiento and not motivo_candidates:
            motivo_candidates.append(plan_entry.requerimiento)
        if plan_entry.tipo_calibracion:
            motivo_candidates.append(f"Tipo de calibraci?n: {plan_entry.tipo_calibracion}")
    motivo = motivo_candidates[0] if motivo_candidates else "Estado importado desde normalize_instrumentos.csv"
    motivo_sanitizado = sanitize_text(motivo, preserve_newlines=True) or "Estado importado desde normalize_instrumentos.csv"
    estado_expr = sql_quote(estado)
    motivo_expr = sql_quote(motivo_sanitizado)
    codigo_expr = sql_quote(record.codigo)
    sql = f"""
INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, {fecha_evento_expr}, {estado_expr}, {motivo_expr}, NULL
FROM instrumentos i
WHERE i.codigo = {codigo_expr}
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = {estado_expr}
        AND hei.fecha_evento = {fecha_evento_expr}
  );
""".strip()
    return [InsertStatement(table="historial_estado_instrumento", sql=sql)]

EXPLANATORY_NOTES = {
    "historial_calibraciones": (
        "-- El historial de calibraciones se gestiona desde la tabla `calibraciones`. "
        "No se detectaron movimientos adicionales en el inventario normalizado."
    ),
    "historial_especificaciones": (
        "-- No se encontraron especificaciones históricas en el inventario normalizado; "
        "se conserva el archivo como plantilla."
    ),
    "historial_estado_instrumento": (
        "-- El estado operativo se registra en `historial_tipos_instrumento`. "
        "No se generaron inserciones adicionales para SBL."
    ),
}


def render_file_header(table: str, empresa_id: int) -> str:
    return (
        "-- Archivo generado automáticamente por generate_historial_inserts.py\n"
        f"-- Empresa destino: {empresa_id}\n"
        "-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:\n"
        f"-- SET @empresa_id = {empresa_id};\n\n"
        f"SET @empresa_id := IFNULL(@empresa_id, {empresa_id});\n\n"
        "START TRANSACTION;"
    )


def render_file_footer() -> str:
    return "COMMIT;\n"


def write_statements(
    table: str,
    statements: list[InsertStatement],
    output_dir: pathlib.Path,
    empresa_id: int,
) -> None:
    output_path = output_dir / f"{table}.sql"
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(render_file_header(table, empresa_id))
        handle.write("\n\n")
        if not statements:
            handle.write("-- No se encontraron registros para este historial.\n")
            note = EXPLANATORY_NOTES.get(table)
            if note:
                handle.write(note)
                handle.write("\n")
        else:
            for statement in statements:
                handle.write(statement.sql)
                handle.write("\n\n")
        handle.write(render_file_footer())


def generate_historial_files(
    input_path: pathlib.Path,
    output_dir: pathlib.Path,
    empresa_id: int = DEFAULT_EMPRESA_ID,
    plan_path: pathlib.Path | None = DEFAULT_PLAN_PATH,
    certificates_path: pathlib.Path | None = DEFAULT_CERTIFICATES_PATH,
) -> Mapping[str, int]:
    """Genera los archivos SQL y devuelve el total de inserciones por tabla."""

    records = sorted(read_instruments(input_path), key=lambda r: r.codigo)
    known_codes = {record.codigo.upper() for record in records}

    plan_records = load_plan_riesgos(plan_path)
    calibration_events = load_calibration_events(certificates_path)

    grouped: dict[str, list[InsertStatement]] = {
        "historial_departamentos": [],
        "historial_ubicaciones": [],
        "historial_fecha_alta": [],
        "historial_fecha_baja": [],
        "historial_tipos_instrumento": [],
        "historial_calibraciones": [],
        "historial_especificaciones": [],
        "historial_estado_instrumento": [],
    }

    for record in records:
        grouped["historial_departamentos"].extend(build_department_inserts(record))
        grouped["historial_ubicaciones"].extend(build_location_inserts(record))
        grouped["historial_fecha_alta"].extend(build_fecha_alta_inserts(record))
        grouped["historial_fecha_baja"].extend(build_fecha_baja_inserts(record))
        grouped["historial_tipos_instrumento"].extend(build_estado_inserts(record))
        plan_entry = select_plan_entry(plan_records.get(record.codigo.upper()), empresa_id)
        grouped["historial_estado_instrumento"].extend(
            build_estado_historial_inserts(record, plan_entry)
        )

    grouped["historial_calibraciones"].extend(
        build_calibration_inserts(calibration_events, empresa_id, known_codes)
    )
    grouped["historial_especificaciones"].extend(
        build_specification_inserts(plan_records, empresa_id, known_codes)
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, int] = {}
    for table_name, statements in grouped.items():
        write_statements(table_name, statements, output_dir, empresa_id)
        summary[table_name] = len(statements)

    return summary


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Genera archivos SQL con INSERT para poblar los historiales del "
            "portal interno a partir de normalize_instrumentos.csv."
        )
    )
    parser.add_argument(
        "--input",
        dest="input_path",
        type=pathlib.Path,
        default=DEFAULT_INPUT,
        help=(
            "Ruta al CSV normalizado de instrumentos. "
            "Por defecto usa Archivos_Normalize/normalize_instrumentos.csv."
        ),
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Directorio donde se guardarán los SQL generados. "
            "Por defecto usa Archivos_BD_SBL/SBL_historiales."
        ),
    )
    parser.add_argument(
        "--plan-path",
        dest="plan_path",
        type=pathlib.Path,
        default=DEFAULT_PLAN_PATH,
        help=(
            "Ruta al CSV normalizado del plan de riesgos. "
            "Por defecto usa Archivos_Normalize/normalize_plan_riesgos.csv."
        ),
    )
    parser.add_argument(
        "--certificates-path",
        dest="certificates_path",
        type=pathlib.Path,
        default=DEFAULT_CERTIFICATES_PATH,
        help=(
            "Ruta al CSV normalizado de certificados/calibraciones. "
            "Por defecto usa Archivos_Normalize/normalize_certificates.csv."
        ),
    )

    parser.add_argument(
        "--empresa-id",
        dest="empresa_id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Identificador de empresa que se documentará en los comentarios.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_argument_parser().parse_args(argv)

    summary = generate_historial_files(
        input_path=args.input_path,
        output_dir=args.output_dir,
        empresa_id=args.empresa_id,
        plan_path=args.plan_path,
        certificates_path=args.certificates_path,
    )

    for table_name, total in summary.items():
        print(f"Generado {table_name} con {total} inserts")


if __name__ == "__main__":
    main()
