"""Utilidades comunes para scripts Python del Sistema SBL - Portal Clientes.

Este módulo contiene funciones y clases de utilidad que son compartidas
entre todos los scripts de procesamiento de datos del sistema SBL.

Funcionalidades:
- Normalización de texto y datos
- Manejo de fechas en español
- Validación de datos
- Logging configurado
- Manejo de archivos CSV con diferentes encodings
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

# Configuración de logging
def setup_logging(script_name: str, log_level: int = logging.INFO) -> logging.Logger:
    """Configura el sistema de logging para un script."""
    logger = logging.getLogger(script_name)
    logger.setLevel(log_level)
    
    # Evitar duplicar handlers
    if logger.handlers:
        return logger
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Formato de los mensajes
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    return logger

# Constantes para normalización
SPANISH_MONTHS = {
    "ENE": 1, "ENERO": 1,
    "FEB": 2, "FEBRERO": 2,
    "MAR": 3, "MARZO": 3,
    "ABR": 4, "ABRIL": 4,
    "MAY": 5, "MAYO": 5,
    "JUN": 6, "JUNIO": 6,
    "JUL": 7, "JULIO": 7,
    "AGO": 8, "AGOSTO": 8,
    "SEP": 9, "SET": 9, "SEPTIEMBRE": 9, "SETIEMBRE": 9,
    "OCT": 10, "OCTUBRE": 10,
    "NOV": 11, "NOVIEMBRE": 11,
    "DIC": 12, "DICIEMBRE": 12,
}

NA_VALUES = {"", "NA", "ND", "N/A", "null", "NULL", "None", "-", "--"}

# Expresiones regulares comunes
DATE_PATTERNS = [
    re.compile(r"(\d{1,2})[\-/]([A-Za-zÁÉÍÓÚáéíóú\.]+)[\-/](\d{2,4})"),  # 15/ENE/2023
    re.compile(r"(\d{1,2})[\-/](\d{1,2})[\-/](\d{2,4})"),  # 15/01/2023
    re.compile(r"(\d{4})[\-/](\d{1,2})[\-/](\d{1,2})"),  # 2023/01/15
]

CODIGO_PATTERN = re.compile(r"^[A-Z]{2,4}[-_]?\d{3,6}[A-Z]?$", re.IGNORECASE)


class TextNormalizer:
    """Normalizador de texto para datos del sistema SBL."""
    
    @staticmethod
    def normalize_text(text: Optional[str]) -> Optional[str]:
        """Normaliza texto removiendo acentos y caracteres especiales."""
        if text is None or str(text).strip() in NA_VALUES:
            return None
        
        text = str(text).strip()
        if not text:
            return None
        
        # Remover acentos
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        
        return text
    
    @staticmethod
    def normalize_codigo(codigo: Optional[str]) -> Optional[str]:
        """Normaliza códigos de instrumentos."""
        if not codigo or str(codigo).strip() in NA_VALUES:
            return None
        
        codigo = str(codigo).strip().upper()
        
        # Remover caracteres especiales excepto guiones
        codigo = re.sub(r'[^\w\-]', '', codigo)
        
        # Validar formato básico
        if not CODIGO_PATTERN.match(codigo):
            return None
        
        return codigo
    
    @staticmethod
    def normalize_multiline_text(text: Optional[str]) -> Optional[str]:
        """Normaliza texto multilínea conservando saltos de línea."""
        if not text or str(text).strip() in NA_VALUES:
            return None
        
        text = str(text).strip()
        if not text:
            return None
        
        # Normalizar cada línea por separado
        lines = text.split('\n')
        normalized_lines = []
        
        for line in lines:
            line = line.strip()
            if line and line not in NA_VALUES:
                # Remover acentos pero conservar estructura
                line = unicodedata.normalize('NFD', line)
                line = ''.join(c for c in line if unicodedata.category(c) != 'Mn')
                normalized_lines.append(line)
        
        return '\n'.join(normalized_lines) if normalized_lines else None


class DateParser:
    """Parser de fechas en formato español."""
    
    @staticmethod
    def parse_spanish_date(date_str: Optional[str]) -> Optional[dt.date]:
        """Parsea fechas en formato español."""
        if not date_str or str(date_str).strip() in NA_VALUES:
            return None
        
        date_str = str(date_str).strip().upper()
        
        for pattern in DATE_PATTERNS:
            match = pattern.search(date_str)
            if match:
                try:
                    if len(match.groups()) == 3:
                        day, month, year = match.groups()
                        
                        # Convertir mes si es texto
                        if month.isalpha():
                            month_num = SPANISH_MONTHS.get(month)
                            if not month_num:
                                continue
                            month = str(month_num)
                        
                        # Normalizar año
                        year = int(year)
                        if year < 100:
                            year += 2000 if year < 50 else 1900
                        
                        return dt.date(year, int(month), int(day))
                
                except (ValueError, KeyError):
                    continue
        
        return None
    
    @staticmethod
    def add_months(base_date: dt.date, months: int) -> dt.date:
        """Añade meses a una fecha base."""
        import calendar
        
        month_index = base_date.month - 1 + months
        year = base_date.year + month_index // 12
        month = month_index % 12 + 1
        day = min(base_date.day, calendar.monthrange(year, month)[1])
        
        return dt.date(year, month, day)


class CSVHandler:
    """Manejador de archivos CSV con detección automática de encoding."""
    
    @staticmethod
    def detect_encoding(file_path: Path) -> str:
        """Detecta el encoding de un archivo."""
        try:
            import chardet
            with open(file_path, 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                return result['encoding'] or 'utf-8'
        except ImportError:
            # Fallback si chardet no está disponible
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        f.read()
                    return encoding
                except UnicodeDecodeError:
                    continue
            return 'utf-8'
    
    @staticmethod
    def read_csv(
        file_path: Path,
        encoding: Optional[str] = None,
        delimiter: str = ',',
        **kwargs
    ) -> List[Dict[str, str]]:
        """Lee un archivo CSV con detección automática de encoding."""
        if encoding is None:
            encoding = CSVHandler.detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                # Detectar delimitador si no se especifica
                if delimiter == ',':
                    sample = f.read(1024)
                    f.seek(0)
                    sniffer = csv.Sniffer()
                    try:
                        delimiter = sniffer.sniff(sample).delimiter
                    except csv.Error:
                        delimiter = ','
                
                reader = csv.DictReader(f, delimiter=delimiter, **kwargs)
                return list(reader)
        
        except UnicodeDecodeError as e:
            raise ValueError(f"Error de encoding en {file_path}: {e}")
    
    @staticmethod
    def write_csv(
        file_path: Path,
        data: List[Dict[str, Any]],
        encoding: str = 'utf-8',
        delimiter: str = ',',
        **kwargs
    ) -> None:
        """Escribe datos a un archivo CSV."""
        if not data:
            return
        
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding=encoding, newline='') as f:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter, **kwargs)
            writer.writeheader()
            writer.writerows(data)


class SQLGenerator:
    """Generador de SQL para el sistema SBL."""
    
    @staticmethod
    def escape_sql_string(value: Optional[str]) -> str:
        """Escapa una cadena para uso en SQL."""
        if value is None:
            return "NULL"
        return f"'{str(value).replace('\'', '\'\'')}'"
    
    @staticmethod
    def generate_insert_on_duplicate(
        table: str,
        data: Dict[str, Any],
        unique_keys: List[str],
        empresa_id: Optional[int] = None
    ) -> str:
        """Genera INSERT ... ON DUPLICATE KEY UPDATE."""
        if empresa_id is not None:
            data['empresa_id'] = empresa_id
        
        columns = list(data.keys())
        values = [SQLGenerator.escape_sql_string(data[col]) for col in columns]
        
        # Construir la parte de UPDATE
        update_parts = []
        for col in columns:
            if col not in unique_keys:
                update_parts.append(f"{col} = VALUES({col})")
        
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        if update_parts:
            sql += f" ON DUPLICATE KEY UPDATE {', '.join(update_parts)}"
        
        return sql + ";"


class ValidationError(Exception):
    """Excepción para errores de validación de datos."""
    pass


class DataValidator:
    """Validador de datos para el sistema SBL."""
    
    @staticmethod
    def validate_codigo(codigo: Optional[str]) -> str:
        """Valida un código de instrumento."""
        normalized = TextNormalizer.normalize_codigo(codigo)
        if not normalized:
            raise ValidationError(f"Código inválido: {codigo}")
        return normalized
    
    @staticmethod
    def validate_date(date_str: Optional[str]) -> Optional[dt.date]:
        """Valida una fecha."""
        if not date_str or str(date_str).strip() in NA_VALUES:
            return None
        
        parsed_date = DateParser.parse_spanish_date(date_str)
        if parsed_date is None:
            raise ValidationError(f"Fecha inválida: {date_str}")
        
        return parsed_date
    
    @staticmethod
    def validate_required_field(value: Optional[str], field_name: str) -> str:
        """Valida que un campo requerido no esté vacío."""
        if not value or str(value).strip() in NA_VALUES:
            raise ValidationError(f"Campo requerido vacío: {field_name}")
        return str(value).strip()


# Funciones de utilidad para paths
def get_repo_root(script_file: str) -> Path:
    """Obtiene la raíz del repositorio desde cualquier script."""
    script_path = Path(script_file).resolve()
    
    # Buscar hacia arriba hasta encontrar package.json o README.md
    current = script_path.parent
    while current != current.parent:
        if (current / "package.json").exists() or (current / "README.md").exists():
            return current
        current = current.parent
    
    # Fallback: asumir que está 2 niveles arriba de tools/scripts
    if "tools" in script_path.parts and "scripts" in script_path.parts:
        return script_path.parents[2]
    
    return script_path.parent


def get_archivos_sql_dir(repo_root: Path) -> Path:
    """Obtiene el directorio de archivos SQL."""
    return repo_root / "app" / "Modules" / "Internal" / "ArchivosSql"


def get_csv_originales_dir(repo_root: Path) -> Path:
    """Obtiene el directorio de CSVs originales."""
    return get_archivos_sql_dir(repo_root) / "Archivos_CSV_originales"


def get_normalize_dir(repo_root: Path) -> Path:
    """Obtiene el directorio de archivos normalizados."""
    return get_archivos_sql_dir(repo_root) / "Archivos_Normalize"


def get_sql_inserts_dir(repo_root: Path) -> Path:
    """Obtiene el directorio de inserts SQL."""
    return get_archivos_sql_dir(repo_root) / "Archivos_BD_SBL" / "SBL_inserts"