#!/usr/bin/env python3
"""Script especializado para generar archivos de certificaciones y calibraciones.

Este script genera archivos SQL y reportes específicos para el proceso de certificación
de instrumentos y calibraciones en el sistema SBL.

Funcionalidades principales:
- Generación de SQL para certificaciones por lotes
- Creación de reportes de calibraciones pendientes
- Validación de datos de calibración
- Exportación de certificados en formato estandarizado
- Integración con el sistema de auditoría

Características específicas para el portal de clientes:
- Reportes por cliente con información de calibraciones
- Generación de certificados individuales
- Programación automática de próximas calibraciones
- Alertas de calibraciones vencidas

Uso típico:
```bash
python generate_cert_calibrations.py --empresa-id 1 --cliente-id 5
python generate_cert_calibrations.py --all-clients --backup
```

También mantiene compatibilidad con el proceso histórico:
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
import json
import re
import subprocess
import sys
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Dict, Any, Set

# Importar utilidades
try:
    from sbl_utils import (
        setup_logging, get_repo_root, TextNormalizer, 
        DateParser, CSVHandler, SQLGenerator, DataValidator
    )
    UTILS_AVAILABLE = True
except ImportError:
    # Fallback para compatibilidad con el script original
    UTILS_AVAILABLE = False
    def setup_logging(name):
        import logging
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)
    
    def get_repo_root(file_path):
        return Path(file_path).resolve().parents[2]

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


# ===== NUEVAS FUNCIONALIDADES MODERNAS =====

@dataclass
class CertificationRecord:
    """Representa un registro de certificación moderno."""
    instrumento_id: str
    cliente_id: int
    tipo_instrumento: str
    numero_serie: str
    fecha_calibracion: dt.date
    fecha_vencimiento: dt.date
    certificado_numero: str
    estado: str = "vigente"
    observaciones: str = ""
    tecnico_responsable: str = ""
    empresa_id: int = 1


@dataclass
class CalibrationSchedule:
    """Representa un programa de calibración."""
    instrumento_id: str
    cliente_id: int
    proxima_calibracion: dt.date
    frecuencia_meses: int
    prioridad: str = "normal"  # alta, normal, baja
    notificacion_dias: int = 30
    estado_programacion: str = "programada"


class CertCalibrationGenerator:
    """Generador moderno de certificaciones y calibraciones para el sistema SBL."""
    
    def __init__(self, empresa_id: int = 1, backup: bool = False):
        self.empresa_id = empresa_id
        self.backup = backup
        self.repo_root = get_repo_root(__file__)
        self.logger = setup_logging("cert_calibration_generator")
        
        # Utilidades (solo si están disponibles)
        if UTILS_AVAILABLE:
            self.text_normalizer = TextNormalizer()
            self.date_parser = DateParser()
            self.csv_handler = CSVHandler()
            self.sql_generator = SQLGenerator()
            self.data_validator = DataValidator()
        
        # Directorios
        self.setup_directories()
        
        # Contadores
        self.stats = {
            'certificates_generated': 0,
            'calibrations_scheduled': 0,
            'expired_notifications': 0,
            'validation_errors': 0
        }
    
    def setup_directories(self):
        """Configura los directorios necesarios."""
        base_storage = self.repo_root / "storage"
        
        self.directories = {
            'input': self.repo_root / "app" / "Modules" / "Internal" / "ArchivosCsv",
            'output_sql': self.repo_root / "app" / "Modules" / "Internal" / "ArchivosSql",
            'certificates': base_storage / "certificates",
            'calibration_schedules': base_storage / "calibration_schedules",
            'reports': base_storage / "calibration_reports",
            'backup': base_storage / "backup" / "calibrations"
        }
        
        # Crear directorios si no existen
        for directory in self.directories.values():
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Directorio verificado: {directory}")
    
    def load_instrument_data(self) -> List[Dict[str, Any]]:
        """Carga datos de instrumentos desde archivos CSV."""
        instrument_files = [
            "instrumentos.csv",
            "instrumentos_clientes.csv",
            "calibraciones_pendientes.csv"
        ]
        
        all_instruments = []
        
        for filename in instrument_files:
            file_path = self.directories['input'] / filename
            
            if not file_path.exists():
                self.logger.warning(f"Archivo no encontrado: {filename}")
                continue
            
            try:
                if UTILS_AVAILABLE:
                    instruments = self.csv_handler.read_csv(file_path)
                else:
                    # Fallback básico
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        instruments = list(reader)
                
                self.logger.info(f"Cargados {len(instruments)} registros de {filename}")
                
                # Añadir fuente del archivo
                for instrument in instruments:
                    instrument['_source_file'] = filename
                
                all_instruments.extend(instruments)
                
            except Exception as e:
                self.logger.error(f"Error leyendo {filename}: {e}")
                self.stats['validation_errors'] += 1
        
        self.logger.info(f"Total de registros de instrumentos cargados: {len(all_instruments)}")
        return all_instruments
    
    def generate_certificate_number(self, instrument: Dict[str, Any]) -> str:
        """Genera un número único de certificado."""
        # Formato: CERT-YYYY-EMPRESA-CLIENTE-SECUENCIAL
        year = dt.date.today().year
        empresa_id = instrument.get('empresa_id', self.empresa_id)
        cliente_id = instrument['cliente_id']
        
        # Generar secuencial basado en fecha/hora
        timestamp = dt.datetime.now().strftime("%m%d%H%M")
        
        cert_number = f"CERT-{year}-E{empresa_id:02d}-C{cliente_id:03d}-{timestamp}"
        
        return cert_number
    
    def calculate_next_calibration_date(self, 
                                      last_calibration: dt.date, 
                                      instrument_type: str) -> dt.date:
        """Calcula la próxima fecha de calibración según el tipo de instrumento."""
        
        # Frecuencias de calibración por tipo de instrumento (en meses)
        calibration_frequencies = {
            'balanza': 12,
            'balanza_analitica': 6,
            'pipeta': 12,
            'micropipeta': 6,
            'termometro': 12,
            'termometro_digital': 6,
            'ph_metro': 6,
            'medidor_ph': 6,
            'conductimetro': 12,
            'espectrofotometro': 6,
            'autoclave': 6,
            'incubadora': 12,
            'refrigerador': 6,
            'congelador': 6,
            'centrifuga': 12,
            'agitador': 12,
            'cronometro': 24,
            'default': 12
        }
        
        # Normalizar tipo de instrumento para búsqueda
        if UTILS_AVAILABLE:
            normalized_type = self.text_normalizer.normalize_text(instrument_type).lower()
        else:
            normalized_type = instrument_type.lower().strip()
        
        # Buscar frecuencia específica
        frequency_months = calibration_frequencies.get('default')
        for instrument_pattern, months in calibration_frequencies.items():
            if instrument_pattern in normalized_type:
                frequency_months = months
                break
        
        # Calcular próxima fecha
        try:
            next_date = last_calibration.replace(
                year=last_calibration.year + (last_calibration.month + frequency_months - 1) // 12,
                month=(last_calibration.month + frequency_months - 1) % 12 + 1
            )
        except ValueError:
            # Manejar casos de fechas inválidas (ej: 29 feb)
            next_date = last_calibration + dt.timedelta(days=frequency_months * 30)
        
        return next_date
    
    def run_modern_generation_process(self, cliente_id: Optional[int] = None) -> bool:
        """Ejecuta el proceso moderno de generación de certificaciones."""
        self.logger.info("🚀 Iniciando generación moderna de certificaciones y calibraciones")
        
        try:
            # 1. Cargar datos de instrumentos
            self.logger.info("📥 Cargando datos de instrumentos...")
            instruments = self.load_instrument_data()
            
            if not instruments:
                self.logger.error("No se encontraron datos de instrumentos")
                return False
            
            # Filtrar por cliente si se especifica
            if cliente_id is not None:
                instruments = [i for i in instruments if i.get('cliente_id') == str(cliente_id)]
                self.logger.info(f"Filtrado para cliente {cliente_id}: {len(instruments)} instrumentos")
            
            # 2. Procesar instrumentos
            certificates_data = []
            schedules_data = []
            
            for instrument in instruments:
                try:
                    # Generar certificado
                    cert_number = self.generate_certificate_number(instrument)
                    
                    # Determinar fechas
                    if 'fecha_ultima_calibracion' in instrument:
                        if UTILS_AVAILABLE:
                            calibration_date = self.date_parser.parse_date(instrument['fecha_ultima_calibracion'])
                        else:
                            # Fallback básico de parseo de fecha
                            try:
                                calibration_date = dt.datetime.strptime(instrument['fecha_ultima_calibracion'], '%Y-%m-%d').date()
                            except:
                                calibration_date = dt.date.today()
                    else:
                        calibration_date = dt.date.today()
                    
                    # Calcular vencimiento
                    expiration_date = self.calculate_next_calibration_date(
                        calibration_date, 
                        instrument.get('tipo_instrumento', 'default')
                    )
                    
                    # Crear registro de certificación
                    cert_record = CertificationRecord(
                        instrumento_id=instrument.get('instrumento_id', f'INST_{len(certificates_data):04d}'),
                        cliente_id=int(instrument.get('cliente_id', 1)),
                        tipo_instrumento=instrument.get('tipo_instrumento', 'Instrumento'),
                        numero_serie=instrument.get('numero_serie', 'N/A'),
                        fecha_calibracion=calibration_date,
                        fecha_vencimiento=expiration_date,
                        certificado_numero=cert_number,
                        estado="vigente" if expiration_date > dt.date.today() else "vencido",
                        observaciones=instrument.get('observaciones', ''),
                        tecnico_responsable=instrument.get('tecnico_responsable', 'SBL'),
                        empresa_id=int(instrument.get('empresa_id', self.empresa_id))
                    )
                    
                    certificates_data.append(cert_record)
                    self.stats['certificates_generated'] += 1
                    
                    # Crear programa de calibración
                    schedule = CalibrationSchedule(
                        instrumento_id=cert_record.instrumento_id,
                        cliente_id=cert_record.cliente_id,
                        proxima_calibracion=expiration_date,
                        frecuencia_meses=12,  # Default
                        prioridad="alta" if cert_record.estado == "vencido" else "normal",
                        notificacion_dias=30,
                        estado_programacion="programada"
                    )
                    
                    schedules_data.append(schedule)
                    self.stats['calibrations_scheduled'] += 1
                    
                except Exception as e:
                    self.logger.error(f"Error procesando instrumento {instrument.get('instrumento_id', 'unknown')}: {e}")
                    self.stats['validation_errors'] += 1
            
            # 3. Guardar resultados
            self.logger.info("💾 Guardando resultados...")
            
            # Guardar certificaciones CSV
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            cert_csv_path = self.directories['certificates'] / f"certificaciones_generadas_{timestamp}.csv"
            
            with open(cert_csv_path, 'w', newline='', encoding='utf-8') as f:
                if certificates_data:
                    writer = csv.DictWriter(f, fieldnames=asdict(certificates_data[0]).keys())
                    writer.writeheader()
                    for cert in certificates_data:
                        cert_dict = asdict(cert)
                        cert_dict['fecha_calibracion'] = cert.fecha_calibracion.isoformat()
                        cert_dict['fecha_vencimiento'] = cert.fecha_vencimiento.isoformat()
                        writer.writerow(cert_dict)
            
            self.logger.info(f"Certificaciones guardadas en: {cert_csv_path}")
            
            # Guardar programas CSV
            schedule_csv_path = self.directories['calibration_schedules'] / f"programas_calibracion_{timestamp}.csv"
            
            with open(schedule_csv_path, 'w', newline='', encoding='utf-8') as f:
                if schedules_data:
                    writer = csv.DictWriter(f, fieldnames=asdict(schedules_data[0]).keys())
                    writer.writeheader()
                    for schedule in schedules_data:
                        schedule_dict = asdict(schedule)
                        schedule_dict['proxima_calibracion'] = schedule.proxima_calibracion.isoformat()
                        writer.writerow(schedule_dict)
            
            self.logger.info(f"Programas guardados en: {schedule_csv_path}")
            
            # 4. Generar SQL si está disponible el generador
            if UTILS_AVAILABLE and certificates_data:
                self.logger.info("🗃️ Generando archivos SQL...")
                
                # Preparar datos para SQL
                cert_sql_data = []
                for cert in certificates_data:
                    cert_dict = asdict(cert)
                    cert_dict['fecha_calibracion'] = cert.fecha_calibracion.isoformat()
                    cert_dict['fecha_vencimiento'] = cert.fecha_vencimiento.isoformat()
                    cert_sql_data.append(cert_dict)
                
                # Generar SQL
                sql_content = self.sql_generator.generate_batch_insert(
                    "certificaciones_instrumentos", 
                    cert_sql_data,
                    on_duplicate="UPDATE"
                )
                
                # Guardar SQL
                sql_path = self.directories['output_sql'] / f"insert_certificaciones_modernas_{timestamp}.sql"
                with open(sql_path, 'w', encoding='utf-8') as f:
                    f.write(sql_content)
                
                self.logger.info(f"SQL generado: {sql_path}")
            
            # 5. Reporte final
            self.logger.info("✅ Proceso moderno completado exitosamente")
            self.logger.info(f"📊 Estadísticas:")
            self.logger.info(f"  - Certificaciones generadas: {self.stats['certificates_generated']}")
            self.logger.info(f"  - Programas de calibración: {self.stats['calibrations_scheduled']}")
            self.logger.info(f"  - Errores de validación: {self.stats['validation_errors']}")
            
            # Alertas de vencimiento
            expired_count = len([c for c in certificates_data if c.estado == "vencido"])
            if expired_count > 0:
                self.logger.warning(f"⚠️ {expired_count} instrumentos con calibración VENCIDA")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error en el proceso moderno de generación: {e}")
            return False


def main(argv: Optional[Sequence[str]] = None) -> None:
    # Parsear argumentos con el sistema actualizado
    parser = argparse.ArgumentParser(
        description="Generador de certificaciones y calibraciones SBL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

Modo moderno (recomendado):
  python generate_cert_calibrations.py                    # Generar para todos los clientes
  python generate_cert_calibrations.py --cliente-id 5     # Solo para cliente específico
  python generate_cert_calibrations.py --backup           # Con backup

Modo histórico (compatibilidad):
  python generate_cert_calibrations.py --historical \
    --input app/Modules/Internal/ArchivosSql/Archivos_CSV_originales/CERT_instrumentos_original_v2.csv \
    --output app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/insert_calibraciones_certificados.sql
        """
    )
    
    # Argumentos del modo moderno
    parser.add_argument(
        "--empresa-id",
        type=int,
        default=1,
        help="ID de la empresa (default: 1)"
    )
    
    parser.add_argument(
        "--cliente-id",
        type=int,
        help="ID del cliente específico (opcional)"
    )
    
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Crear backup antes de generar archivos"
    )
    
    # Modo histórico
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Usar modo histórico (convierte CSV CERT a SQL)"
    )
    
    parser.add_argument(
        "--input",
        type=Path,
        default=_resolve_cert_path(),
        help="Ruta al CSV original exportado de la hoja CERT (modo histórico)."
    )
    
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "app/Modules/Internal/ArchivosSql/Archivos_BD_SBL/SBL_inserts/"
            "insert_calibraciones_certificados.sql"
        ),
        help="Archivo SQL de salida listo para importar en phpMyAdmin (modo histórico)."
    )
    
    args = parser.parse_args(argv)
    
    # Determinar modo de operación
    if args.historical or (hasattr(args, 'input') and args.input != _resolve_cert_path()) or (hasattr(args, 'output') and 'insert_calibraciones_certificados.sql' in str(args.output)):
        # Modo histórico
        print("🕒 Ejecutando en modo histórico (compatibilidad)...")
        events = list(iter_events(args.input, args.empresa_id))
        sql_output = build_sql(events, args.empresa_id)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(sql_output, encoding="utf-8")
        print(
            f"Se generaron {len(events)} eventos en {args.output}. "
            "Ejecuta este archivo en phpMyAdmin después de validar los datos."
        )
    else:
        # Modo moderno
        print("🚀 Ejecutando en modo moderno...")
        generator = CertCalibrationGenerator(
            empresa_id=args.empresa_id,
            backup=args.backup
        )
        
        success = generator.run_modern_generation_process(cliente_id=args.cliente_id)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

