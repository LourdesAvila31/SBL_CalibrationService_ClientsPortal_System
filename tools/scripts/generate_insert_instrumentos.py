"""Herramienta de preprocesamiento para generar inserciones idempotentes.

Este script lee el archivo CSV `LM_instrumentos.csv`, aplica las reglas de
normalización utilizadas en `normalize_instrumentos.sql` y produce un archivo
SQL con sentencias `INSERT ... ON DUPLICATE KEY UPDATE` segmentadas en
transacciones de tamaño configurable (por defecto, 100 filas). El resultado es
compatible con phpMyAdmin y otros entornos donde la ejecución de scripts
largos puede provocar "timeouts".

Ejemplo rápido desde la raíz del repositorio:

```bash
python tools/scripts/generate_insert_instrumentos.py \
    --input app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_instrumentos.csv \
    --output app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_instrumentos.sql
```
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import sys
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
ARCHIVOS_SQL_DIR = REPO_ROOT / "app/Modules/Internal/ArchivosSql"
README_NORMALIZADO = ARCHIVOS_SQL_DIR / "ReadMe_BD" / "README_NORMALIZADO.md"
REFERENCE_PATTERNS = {
    "catalogo": r"## Catálogo de instrumentos.*?```text(.*?)```",
    "marcas": r"## Marcas.*?```text(.*?)```",
    "modelos": r"## Modelos.*?```text(.*?)```",
    "departamentos": r"## Departamentos.*?```text(.*?)```",
}


NA_DATE_VALUES = {"NA", "ND", "N/A"}

EMPRESA_ID = 1

MONTH_MAP = {
    "ENE": 1,
    "FEB": 2,
    "MAR": 3,
    "ABR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SEP": 9,
    "SET": 9,  # Variante frecuente en español latinoamericano.
    "OCT": 10,
    "NOV": 11,
    "DIC": 12,
}


@dataclass(frozen=True)
class InstrumentoRegistro:
    """Representa un registro normalizado de instrumento."""

    instrumento: Optional[str]
    marca: Optional[str]
    modelo: Optional[str]
    serie: Optional[str]
    codigo: Optional[str]
    departamento: Optional[str]
    ubicacion: Optional[str]
    fecha_alta: Optional[str]
    fecha_baja: Optional[str]
    proxima_calibracion: Optional[str]
    estado: Optional[str]
    programado: Optional[int]
    catalogo_id_val: Optional[int] = None
    marca_id_val: Optional[int] = None
    modelo_id_val: Optional[int] = None
    departamento_id_val: Optional[int] = None


def _cargar_referencias_desde_readme() -> Dict[str, Dict[int, str]]:
    if not README_NORMALIZADO.exists():
        print(
            "[INFO] No se encontró README_NORMALIZADO.md; se utilizarán únicamente "
            "los identificadores numéricos disponibles."
        )
        return {}

    content = README_NORMALIZADO.read_text(encoding="utf-8")
    referencias: Dict[str, Dict[int, str]] = {}

    for key, pattern in REFERENCE_PATTERNS.items():
        match = re.search(pattern, content, flags=re.S)
        if not match:
            continue
        table_text = match.group(1)
        mapping: Dict[int, str] = {}
        for raw_line in table_text.splitlines():
            line = raw_line.strip()
            if not line or line.lower().startswith("id"):
                continue
            parts = re.split(r"\s{2,}", line)
            if len(parts) < 2:
                continue
            try:
                identifier = int(parts[0])
            except ValueError:
                continue
            mapping[identifier] = parts[1].strip()
        referencias[key] = mapping

    return referencias


def _lookup_referencia(mapping: Dict[int, str], identifier: Optional[int]) -> Optional[str]:
    if identifier is None:
        return None
    return mapping.get(identifier)


def leer_csv_normalizado(
    csv_path: Path,
    estado_programado: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
) -> List[InstrumentoRegistro]:
    """Lee el CSV y aplica la normalización definida en `normalize_instrumentos.sql`."""

    registros: List[InstrumentoRegistro] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        normalized_headers = {name.strip().lower() for name in fieldnames}
        numeric_mode = (
            "catalogo_id" in normalized_headers
            and "instrumento" not in normalized_headers
        )

        referencias = (
            _cargar_referencias_desde_readme()
            if numeric_mode
            else {}
        )

        for raw in reader:
            codigo_normalizado = _normalizar_codigo(
                _obtener_columna(raw, ["Código", "codigo"])
            )

            if numeric_mode:
                catalogo_id_val = _normalizar_entero(raw.get("catalogo_id"))
                marca_id_val = _normalizar_entero(raw.get("marca_id"))
                modelo_id_val = _normalizar_entero(raw.get("modelo_id"))
                departamento_id_val = _normalizar_entero(raw.get("departamento_id"))

                estado_valor = _obtener_columna(raw, ["estado"])
                programado_valor = _obtener_columna(raw, ["programado"])

                if (
                    estado_valor is None
                    and estado_programado
                    and codigo_normalizado
                    and codigo_normalizado in estado_programado
                ):
                    estado_valor = estado_programado[codigo_normalizado][0]

                if (
                    programado_valor is None
                    and estado_programado
                    and codigo_normalizado
                    and codigo_normalizado in estado_programado
                ):
                    programado_valor = estado_programado[codigo_normalizado][1]

                instrumento_nombre = _lookup_referencia(
                    referencias.get("catalogo", {}), catalogo_id_val
                )
                marca_nombre = _lookup_referencia(
                    referencias.get("marcas", {}), marca_id_val
                )
                modelo_nombre = _lookup_referencia(
                    referencias.get("modelos", {}), modelo_id_val
                )
                departamento_nombre = _lookup_referencia(
                    referencias.get("departamentos", {}), departamento_id_val
                )

                if instrumento_nombre:
                    catalogo_id_val = None
                else:
                    instrumento_nombre = _normalizar_texto(
                        raw.get("instrumento"), allow_empty=True
                    )

                if marca_nombre:
                    marca_id_val = None
                else:
                    marca_nombre = _normalizar_texto(
                        raw.get("marca"), allow_empty=True
                    )

                if modelo_nombre:
                    modelo_id_val = None
                else:
                    modelo_nombre = _normalizar_texto(
                        raw.get("modelo"), allow_empty=True
                    )

                if departamento_nombre:
                    departamento_id_val = None
                else:
                    departamento_nombre = _normalizar_texto(
                        raw.get("departamento"), allow_empty=True
                    )

                registro = InstrumentoRegistro(
                    instrumento=instrumento_nombre,
                    marca=marca_nombre,
                    modelo=modelo_nombre,
                    serie=_normalizar_texto(raw.get("serie"), allow_empty=True),
                    codigo=codigo_normalizado,
                    departamento=departamento_nombre,
                    ubicacion=_normalizar_texto(raw.get("ubicacion"), allow_empty=True),
                    fecha_alta=_normalizar_fecha(raw.get("fecha_alta")),
                    fecha_baja=_normalizar_fecha(raw.get("fecha_baja")),
                    proxima_calibracion=_normalizar_fecha(raw.get("proxima_calibracion")),
                    estado=_normalizar_estado(estado_valor),
                    programado=_normalizar_programado(programado_valor),
                    catalogo_id_val=catalogo_id_val,
                    marca_id_val=marca_id_val,
                    modelo_id_val=modelo_id_val,
                    departamento_id_val=departamento_id_val,
                )
            else:
                estado_valor = _obtener_columna(
                    raw,
                    [
                        "estado",
                        "Estado",
                    ],
                )
                programado_valor = _obtener_columna(
                    raw,
                    [
                        "programado",
                        "Programado",
                    ],
                )

                if (
                    estado_valor is None
                    and estado_programado
                    and codigo_normalizado
                    and codigo_normalizado in estado_programado
                ):
                    estado_valor = estado_programado[codigo_normalizado][0]

                if (
                    programado_valor is None
                    and estado_programado
                    and codigo_normalizado
                    and codigo_normalizado in estado_programado
                ):
                    programado_valor = estado_programado[codigo_normalizado][1]

                registro = InstrumentoRegistro(
                    instrumento=_normalizar_texto(raw.get("Instrumento")),
                    marca=_normalizar_texto(raw.get("Marca")),
                    modelo=_normalizar_texto(raw.get("Modelo")),
                    serie=_normalizar_texto(raw.get("Serie"), allow_empty=True),
                    codigo=codigo_normalizado,
                    departamento=_normalizar_texto(raw.get("Departamento responsable")),
                    ubicacion=_normalizar_texto(raw.get("Ubicación"), allow_empty=True),
                    fecha_alta=_normalizar_fecha(raw.get("Fecha de alta")),
                    fecha_baja=_normalizar_fecha(raw.get("Fecha de baja")),
                    proxima_calibracion=_normalizar_fecha(
                        _obtener_columna(raw, [
                            "próxima calibración",
                            "proxima calibracion",
                            "Próxima calibración",
                            "Proxima calibracion",
                            "proxima_calibracion",
                        ])
                    ),
                    estado=_normalizar_estado(estado_valor),
                    programado=_normalizar_programado(programado_valor),
                )

            if registro.estado is None:
                raise ValueError(
                    "No se pudo determinar el estado del instrumento con código "
                    f"{registro.codigo!r}"
                )
            if registro.programado is None:
                raise ValueError(
                    "No se pudo determinar el indicador 'programado' del instrumento con "
                    f"código {registro.codigo!r}"
                )
            registros.append(registro)

    return registros


def _normalizar_entero(valor: Optional[str]) -> Optional[int]:
    if valor is None:
        return None
    texto = valor.strip()
    if not texto:
        return None
    try:
        return int(texto)
    except ValueError:
        return None


def preparar_entidades(
    registros: Sequence[InstrumentoRegistro],
) -> Dict[str, Sequence]:
    """Agrupa los registros normalizados en entidades únicas."""

    catalogo = sorted({r.instrumento for r in registros if r.instrumento})
    marcas = sorted({r.marca for r in registros if r.marca})

    modelos = sorted(
        {
            (r.marca, r.modelo)
            for r in registros
            if r.modelo and r.marca
        }
    )

    departamentos = sorted({r.departamento for r in registros if r.departamento})

    instrumentos: Dict[str, InstrumentoRegistro] = {}
    for registro in registros:
        if registro.codigo:
            instrumentos[registro.codigo] = registro

    return {
        "catalogo": catalogo,
        "marcas": marcas,
        "modelos": modelos,
        "departamentos": departamentos,
        "instrumentos": [instrumentos[codigo] for codigo in sorted(instrumentos)],
    }


def generar_script_sql(
    entidades: Dict[str, Sequence],
    batch_size: int,
) -> str:
    """Crea el script SQL con bloques transaccionales."""

    buffer = StringIO()
    print("-- Archivo generado automáticamente por generate_insert_instrumentos.py", file=buffer)
    print("USE iso17025;", file=buffer)
    print(file=buffer)

    _render_inserciones_catalogo(entidades["catalogo"], batch_size, buffer)
    _render_inserciones_marcas(entidades["marcas"], batch_size, buffer)
    _render_inserciones_modelos(entidades["modelos"], batch_size, buffer)
    _render_inserciones_departamentos(entidades["departamentos"], batch_size, buffer)
    _render_inserciones_instrumentos(entidades["instrumentos"], batch_size, buffer)

    return buffer.getvalue()


def _normalizar_texto(valor: Optional[str], *, allow_empty: bool = False) -> Optional[str]:
    if valor is None:
        return None
    limpio = valor.strip()
    if not limpio and not allow_empty:
        return None
    return limpio if limpio else None


def _normalizar_codigo(valor: Optional[str]) -> Optional[str]:
    if valor is None:
        return None
    return valor.strip() or None


def _normalizar_fecha(valor: Optional[str]) -> Optional[str]:
    if valor is None:
        return None

    texto = valor.strip()
    if not texto:
        return None

    if texto.upper() in NA_DATE_VALUES:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            parsed = dt.datetime.strptime(texto, fmt).date()
            return parsed.isoformat()
        except ValueError:
            continue

    partes = texto.replace("\u00a0", " ").split("-")
    if len(partes) == 3:
        dia, mes, anio = partes
        try:
            dia_int = int(dia)
            mes_int = _traducir_mes(mes)
            anio_int = _normalizar_anio(int(anio))
            return dt.date(anio_int, mes_int, dia_int).isoformat()
        except (ValueError, KeyError):
            pass

    raise ValueError(f"Formato de fecha no reconocido: {valor}")


def _normalizar_estado(valor: Optional[str]) -> Optional[str]:
    texto = _normalizar_texto(valor, allow_empty=True)
    if texto is None:
        return None
    if texto.upper() in NA_DATE_VALUES:
        return None
    return texto


def _normalizar_programado(valor: Optional[str]) -> Optional[int]:
    if valor is None:
        return None

    texto = valor.strip()
    if not texto:
        return None

    mayusculas = texto.upper()
    if mayusculas in NA_DATE_VALUES:
        return None

    equivalencias_true = {"SI", "SÍ", "YES", "TRUE", "VERDADERO"}
    equivalencias_false = {"NO", "FALSE", "FALSO"}

    if mayusculas in equivalencias_true:
        return 1
    if mayusculas in equivalencias_false:
        return 0

    try:
        numero = int(texto)
    except (TypeError, ValueError):
        raise ValueError(f"Valor de programado no válido: {valor}") from None

    if numero not in (0, 1):
        raise ValueError(f"Valor de programado fuera de rango (esperado 0 o 1): {valor}")

    return numero


def _traducir_mes(mes: str) -> int:
    clave = mes.strip().upper()
    if len(clave) > 3:
        clave = clave[:3]
    if clave not in MONTH_MAP:
        raise KeyError(mes)
    return MONTH_MAP[clave]


def _normalizar_anio(anio_dos_digitos: int) -> int:
    if anio_dos_digitos < 100:
        return 2000 + anio_dos_digitos
    return anio_dos_digitos


def _render_inserciones_catalogo(
    catalogo: Sequence[str], batch_size: int, buffer: StringIO
) -> None:
    if not catalogo:
        return

    for chunk in _chunk(catalogo, batch_size):
        print("START TRANSACTION;", file=buffer)
        print("INSERT INTO catalogo_instrumentos (nombre, empresa_id)", file=buffer)
        _render_values([
            f"({sql_quote(nombre)}, {EMPRESA_ID})" for nombre in chunk
        ], buffer)
        print("ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);", file=buffer)
        print("COMMIT;", file=buffer)
        print(file=buffer)


def _render_inserciones_marcas(
    marcas: Sequence[str], batch_size: int, buffer: StringIO
) -> None:
    if not marcas:
        return

    for chunk in _chunk(marcas, batch_size):
        print("START TRANSACTION;", file=buffer)
        print("INSERT INTO marcas (nombre, empresa_id)", file=buffer)
        _render_values([
            f"({sql_quote(nombre)}, {EMPRESA_ID})" for nombre in chunk
        ], buffer)
        print("ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);", file=buffer)
        print("COMMIT;", file=buffer)
        print(file=buffer)


def _render_inserciones_modelos(
    modelos: Sequence[Tuple[str, str]], batch_size: int, buffer: StringIO
) -> None:
    if not modelos:
        return

    for chunk in _chunk(modelos, batch_size):
        print("START TRANSACTION;", file=buffer)
        print("INSERT INTO modelos (nombre, marca_id, empresa_id)", file=buffer)
        values = []
        for marca, modelo in chunk:
            marca_id = _subselect_marca_id(marca)
            values.append(
                "(" + ", ".join(
                    [
                        sql_quote(modelo),
                        marca_id,
                        sql_number(EMPRESA_ID),
                    ]
                ) + ")"
            )
        _render_values(values, buffer)
        print(
            "ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), marca_id = VALUES(marca_id);",
            file=buffer,
        )
        print("COMMIT;", file=buffer)
        print(file=buffer)


def _render_inserciones_departamentos(
    departamentos: Sequence[str], batch_size: int, buffer: StringIO
) -> None:
    if not departamentos:
        return

    for chunk in _chunk(departamentos, batch_size):
        print("START TRANSACTION;", file=buffer)
        print("INSERT INTO departamentos (nombre, empresa_id)", file=buffer)
        _render_values([
            f"({sql_quote(nombre)}, {EMPRESA_ID})" for nombre in chunk
        ], buffer)
        print("ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);", file=buffer)
        print("COMMIT;", file=buffer)
        print(file=buffer)


def _render_inserciones_instrumentos(
    instrumentos: Sequence[InstrumentoRegistro], batch_size: int, buffer: StringIO
) -> None:
    if not instrumentos:
        return

    for chunk in _chunk(instrumentos, batch_size):
        print("START TRANSACTION;", file=buffer)
        print(
            "INSERT INTO instrumentos (catalogo_id, marca_id, modelo_id, serie, codigo, "
            "departamento_id, ubicacion, fecha_alta, fecha_baja, proxima_calibracion, "
            "estado, programado, empresa_id)",
            file=buffer,
        )

        values: List[str] = []
        for registro in chunk:
            if registro.catalogo_id_val is not None:
                catalogo_id = sql_number(registro.catalogo_id_val)
            else:
                catalogo_id = _subselect_catalogo_id(registro.instrumento)

            if registro.marca_id_val is not None:
                marca_id = sql_number(registro.marca_id_val)
            else:
                marca_id = _subselect_marca_id(registro.marca)

            if registro.modelo_id_val is not None:
                modelo_id = sql_number(registro.modelo_id_val)
            else:
                modelo_id = _subselect_modelo_id(registro.marca, registro.modelo)

            if registro.departamento_id_val is not None:
                departamento_id = sql_number(registro.departamento_id_val)
            else:
                departamento_id = _subselect_departamento_id(registro.departamento)

            fila = "(" + ", ".join(
                [
                    catalogo_id,
                    marca_id,
                    modelo_id,
                    sql_quote(registro.serie),
                    sql_quote(registro.codigo),
                    departamento_id,
                    sql_quote(registro.ubicacion),
                    sql_quote(registro.fecha_alta),
                    sql_quote(registro.fecha_baja),
                    sql_quote(registro.proxima_calibracion),
                    sql_quote(registro.estado),
                    sql_number(registro.programado),
                    sql_number(EMPRESA_ID),
                ]
            ) + ")"
            values.append(fila)

        _render_values(values, buffer)
        print(
            "ON DUPLICATE KEY UPDATE catalogo_id = VALUES(catalogo_id), "
            "marca_id = VALUES(marca_id), modelo_id = VALUES(modelo_id), serie = VALUES(serie), "
            "departamento_id = VALUES(departamento_id), ubicacion = VALUES(ubicacion), "
            "fecha_alta = VALUES(fecha_alta), fecha_baja = VALUES(fecha_baja), "
            "proxima_calibracion = VALUES(proxima_calibracion), estado = VALUES(estado), "
            "programado = VALUES(programado);",
            file=buffer,
        )
        print("COMMIT;", file=buffer)
        print(file=buffer)


def _subselect_catalogo_id(instrumento: Optional[str]) -> str:
    if not instrumento:
        return "NULL"
    return (
        "(SELECT id FROM catalogo_instrumentos WHERE nombre = {nombre} "
        "AND empresa_id = {empresa_id} LIMIT 1)"
    ).format(
        nombre=sql_quote(instrumento),
        empresa_id=sql_number(EMPRESA_ID),
    )


def _subselect_marca_id(marca: Optional[str]) -> str:
    if not marca:
        return "NULL"
    return (
        "(SELECT id FROM marcas WHERE nombre = {nombre} "
        "AND empresa_id = {empresa_id} LIMIT 1)"
    ).format(
        nombre=sql_quote(marca),
        empresa_id=sql_number(EMPRESA_ID),
    )


def _subselect_modelo_id(marca: Optional[str], modelo: Optional[str]) -> str:
    if not (marca and modelo):
        return "NULL"
    return (
        "(SELECT mo.id FROM modelos mo WHERE mo.nombre = {modelo} "
        "AND mo.empresa_id = {empresa_id} AND mo.marca_id = (SELECT id FROM marcas "
        "WHERE nombre = {marca} AND empresa_id = {empresa_id} LIMIT 1) LIMIT 1)"
    ).format(
        modelo=sql_quote(modelo),
        marca=sql_quote(marca),
        empresa_id=sql_number(EMPRESA_ID),
    )


def _subselect_departamento_id(departamento: Optional[str]) -> str:
    if not departamento:
        return "NULL"
    return (
        "(SELECT id FROM departamentos WHERE nombre = {nombre} "
        "AND empresa_id = {empresa_id} LIMIT 1)"
    ).format(
        nombre=sql_quote(departamento),
        empresa_id=sql_number(EMPRESA_ID),
    )


def _chunk(sequence: Sequence, size: int) -> Iterator[Sequence]:
    for idx in range(0, len(sequence), size):
        yield sequence[idx : idx + size]


def _render_values(values: Iterable[str], buffer: StringIO) -> None:
    values = list(values)
    print("VALUES", file=buffer)
    for index, value in enumerate(values):
        suffix = "," if index < len(values) - 1 else ""
        print(f"    {value}{suffix}", file=buffer)


def sql_quote(valor: Optional[str]) -> str:
    if valor is None:
        return "NULL"
    escapado = valor.replace("'", "''")
    return f"'{escapado}'"


def sql_number(valor: Optional[int]) -> str:
    if valor is None:
        return "NULL"
    return str(int(valor))


def _obtener_columna(raw: Dict[str, Optional[str]], nombres: Sequence[str]) -> Optional[str]:
    for nombre in nombres:
        if nombre in raw and raw[nombre] is not None:
            return raw[nombre]
    return None


def _cargar_estado_programado(
    csv_path: Path,
) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    mapping: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    if not csv_path.exists():
        return mapping

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            codigo = _normalizar_codigo(row.get("codigo"))
            if not codigo:
                continue
            estado = row.get("estado")
            programado = row.get("programado")
            mapping[codigo] = (estado, programado)

    return mapping


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(
            "app/Modules/Internal/ArchivosSql/Archivos_Normalize/"
            "normalize_instrumentos.csv"
        ),
        help="Ruta del archivo CSV fuente.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/"
            "insert_instrumentos.sql"
        ),
        help="Ruta del archivo SQL resultante.",
    )
    parser.add_argument(
        "--estado-programado",
        type=Path,
        default=Path(
            "app/Modules/Internal/ArchivosSql/Archivos_Normalize/"
            "normalize_instrumentos.csv"
        ),
        help=(
            "Ruta del CSV normalizado que provee los valores de 'estado' y 'programado'."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Número máximo de filas por transacción.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    if not args.input.exists():
        print(
            "[ERROR] No se encontró el CSV normalizado. Verifica la ruta con --input:\n"
            f"        {args.input}"
        )
        return 1

    if not args.estado_programado.exists():
        print(
            "[INFO] No se localizó el archivo de referencia para 'estado/programado'. "
            "Se generará el SQL usando únicamente el CSV principal."
        )

    estado_programado = _cargar_estado_programado(args.estado_programado)
    registros = leer_csv_normalizado(args.input, estado_programado)
    entidades = preparar_entidades(registros)
    script = generar_script_sql(entidades, batch_size=args.batch_size)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(script, encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())

