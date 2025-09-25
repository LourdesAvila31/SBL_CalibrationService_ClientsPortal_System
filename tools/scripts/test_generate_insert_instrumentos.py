"""Pruebas de verificación para el preprocesador de instrumentos."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_insert_instrumentos import (
    generar_script_sql,
    leer_csv_normalizado,
    preparar_entidades,
)


CSV_PATH = Path("backend/archivos_sql/LM_instrumentos.csv")


def _replicar_normalize_sql():
    registros = leer_csv_normalizado(CSV_PATH)

    catalogo = sorted({r.instrumento for r in registros if r.instrumento})
    marcas = sorted({r.marca for r in registros if r.marca})
    modelos = sorted({(r.marca, r.modelo) for r in registros if r.marca and r.modelo})
    departamentos = sorted({r.departamento for r in registros if r.departamento})

    instrumentos = {}
    for registro in registros:
        if registro.instrumento and registro.codigo:
            instrumentos[registro.codigo] = {
                "instrumento": registro.instrumento,
                "marca": registro.marca,
                "modelo": registro.modelo,
                "departamento": registro.departamento,
                "fecha_alta": registro.fecha_alta,
                "fecha_baja": registro.fecha_baja,
            }

    return {
        "catalogo": catalogo,
        "marcas": marcas,
        "modelos": modelos,
        "departamentos": departamentos,
        "instrumentos": instrumentos,
        "registros": registros,
    }


def main() -> int:
    datos_sql = _replicar_normalize_sql()

    registros = datos_sql["registros"]
    entidades = preparar_entidades(registros)

    assert list(entidades["catalogo"]) == datos_sql["catalogo"], "Catálogo distinto"
    assert list(entidades["marcas"]) == datos_sql["marcas"], "Marcas distintas"
    assert list(entidades["modelos"]) == datos_sql["modelos"], "Modelos distintos"
    assert list(entidades["departamentos"]) == datos_sql["departamentos"], "Departamentos distintos"

    instrumentos_generados = {
        registro.codigo: {
            "instrumento": registro.instrumento,
            "marca": registro.marca,
            "modelo": registro.modelo,
            "departamento": registro.departamento,
            "fecha_alta": registro.fecha_alta,
            "fecha_baja": registro.fecha_baja,
        }
        for registro in entidades["instrumentos"]
    }

    assert instrumentos_generados == datos_sql["instrumentos"], "Instrumentos diferentes"

    script = generar_script_sql(entidades, batch_size=100)
    output_path = Path("backend/archivos_sql/insert_instrumentos.sql")
    assert output_path.read_text(encoding="utf-8") == script, "El archivo SQL no coincide"

    bloques = script.split("START TRANSACTION;")
    for bloque in bloques[1:]:
        valores = bloque.split("VALUES", 1)[-1].split("ON DUPLICATE KEY", 1)[0]
        filas = [line for line in valores.splitlines() if line.strip().startswith("(")]
        assert len(filas) <= 100, "Bloque excede las 100 filas"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

