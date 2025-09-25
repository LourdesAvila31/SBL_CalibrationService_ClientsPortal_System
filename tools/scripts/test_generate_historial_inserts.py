from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = (
    REPO_ROOT
    / "app/Modules/Internal/ArchivosSql/Normalize_Python/generate_historial_inserts.py"
)
INPUT_PATH = (
    REPO_ROOT
    / "app/Modules/Internal/ArchivosSql/Archivos_Normalize/normalize_instrumentos.csv"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("generate_historial_inserts", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise RuntimeError("No se pudo cargar generate_historial_inserts.py")
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def main() -> int:
    module = _load_module()

    with TemporaryDirectory() as tmp_dir:
        output_dir = Path(tmp_dir)
        summary = module.generate_historial_files(
            input_path=INPUT_PATH,
            output_dir=output_dir,
            empresa_id=99,
        )

        expected_tables = [
            "historial_departamentos",
            "historial_ubicaciones",
            "historial_fecha_alta",
        ]

        for table in expected_tables:
            assert summary[table] > 0, f"Se esperaban inserciones en {table}"

            file_path = output_dir / f"{table}.sql"
            assert file_path.exists(), f"No se generó el archivo {file_path.name}"

            content = file_path.read_text(encoding="utf-8")
            lines = content.splitlines()

            assert "-- Empresa destino: 99" in lines[1], "Encabezado sin empresa_id personalizado"
            assert (
                "SET @empresa_id := IFNULL(@empresa_id, 99);" in content
            ), "No se fijó la variable @empresa_id"
            assert "UPDATE" not in content, "Se detectó una sentencia UPDATE"

        # Los historiales sin movimientos siguen estando disponibles como plantillas.
        optional_file = output_dir / "historial_calibraciones.sql"
        assert optional_file.exists(), "Falta historial_calibraciones.sql"
        assert summary["historial_calibraciones"] == 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
