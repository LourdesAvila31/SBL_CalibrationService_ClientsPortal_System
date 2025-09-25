#!/usr/bin/env python3
"""Validador de integridad de datos para el Portal de Servicios a Clientes SBL.

Este script valida la integridad y consistencia de todos los archivos CSV
del portal de servicios, detectando errores comunes, datos faltantes y inconsistencias.

Características:
- Validación de formatos de fecha
- Verificación de códigos de instrumentos
- Detección de duplicados
- Validación de referencias cruzadas por cliente
- Reportes detallados de errores
- Sugerencias de corrección automática

Uso:
```bash
python tools/scripts/data_validator.py --fix-auto --backup
```
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import re

from sbl_utils import (
    setup_logging, get_repo_root, get_csv_originales_dir, 
    get_normalize_dir, CSVHandler, DateParser, TextNormalizer,
    DataValidator, ValidationError
)

@dataclass
class ValidationIssue:
    """Representa un problema de validación encontrado."""
    file_path: Path
    row_number: int
    column: str
    issue_type: str
    description: str
    current_value: str
    suggested_fix: Optional[str] = None
    severity: str = "WARNING"  # ERROR, WARNING, INFO

@dataclass
class ValidationReport:
    """Reporte de validación de un archivo."""
    file_path: Path
    total_rows: int
    issues: List[ValidationIssue] = field(default_factory=list)
    duplicates: List[Tuple[int, int]] = field(default_factory=list)
    missing_references: List[str] = field(default_factory=list)
    
    @property
    def errors_count(self) -> int:
        return len([i for i in self.issues if i.severity == "ERROR"])
    
    @property
    def warnings_count(self) -> int:
        return len([i for i in self.issues if i.severity == "WARNING"])
    
    @property
    def is_valid(self) -> bool:
        return self.errors_count == 0

class ClientDataValidator:
    """Validador de datos específico para el portal de clientes."""
    
    def __init__(self):
        self.repo_root = get_repo_root(__file__)
        self.logger = setup_logging("client_data_validator")
        
        # Directorios
        self.csv_dir = get_csv_originales_dir(self.repo_root)
        self.normalize_dir = get_normalize_dir(self.repo_root)
        
        # Patrones de validación
        self.codigo_pattern = re.compile(r"^[A-Z]{2,4}[-_]?\d{3,6}[A-Z]?$", re.IGNORECASE)
        self.email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        self.telefono_pattern = re.compile(r"^[\d\s\-\+\(\)]{7,15}$")
        
        # Conjuntos de referencia para validación cruzada
        self.valid_codes: Set[str] = set()
        self.valid_clients: Set[str] = set()
        self.client_codes: Dict[str, str] = {}  # codigo -> cliente
        
        # Reportes de validación
        self.reports: List[ValidationReport] = []
    
    def load_reference_data(self) -> None:
        """Carga datos de referencia para validación cruzada."""
        self.logger.info("Cargando datos de referencia para clientes...")
        
        # Cargar códigos válidos desde instrumentos
        instrumentos_files = [
            self.normalize_dir / "normalize_instrumentos.csv",
            self.csv_dir / "LM_instrumentos.csv"
        ]
        
        for file_path in instrumentos_files:
            if file_path.exists():
                try:
                    data = CSVHandler.read_csv(file_path)
                    for row in data:
                        codigo = row.get('codigo', '').strip().upper()
                        if codigo and self.codigo_pattern.match(codigo):
                            self.valid_codes.add(codigo)
                            # Intentar extraer cliente del código
                            cliente = self._extract_client_from_code(codigo)
                            if cliente:
                                self.client_codes[codigo] = cliente
                    self.logger.info(f"Cargados {len(self.valid_codes)} códigos desde {file_path.name}")
                    break
                except Exception as e:
                    self.logger.warning(f"Error cargando códigos desde {file_path}: {e}")
        
        # Cargar clientes válidos
        clientes_files = [
            self.csv_dir / "clientes.csv",
            self.csv_dir / "empresas_clientes.csv",
            self.normalize_dir / "normalize_clientes.csv"
        ]
        
        for file_path in clientes_files:
            if file_path.exists():
                try:
                    data = CSVHandler.read_csv(file_path)
                    for row in data:
                        nombre = row.get('nombre', '').strip()
                        codigo = row.get('codigo', '').strip().upper()
                        if nombre:
                            self.valid_clients.add(nombre.upper())
                        if codigo:
                            self.valid_clients.add(codigo)
                    self.logger.info(f"Cargados {len(self.valid_clients)} clientes desde {file_path.name}")
                    break
                except Exception as e:
                    self.logger.warning(f"Error cargando clientes desde {file_path}: {e}")
    
    def _extract_client_from_code(self, codigo: str) -> Optional[str]:
        """Extrae el código de cliente de un código de instrumento."""
        if '-' in codigo:
            return codigo.split('-')[0]
        elif '_' in codigo:
            return codigo.split('_')[0]
        else:
            # Extraer primeros 2-4 caracteres alfabéticos
            match = re.match(r'^([A-Z]{2,4})', codigo)
            return match.group(1) if match else None
    
    def validate_client_codigo(self, value: str, row_num: int, file_path: Path) -> List[ValidationIssue]:
        """Valida un código de instrumento considerando cliente."""
        issues = []
        
        if not value or value.strip() in {"", "NA", "ND", "N/A"}:
            issues.append(ValidationIssue(
                file_path=file_path,
                row_number=row_num,
                column="codigo",
                issue_type="MISSING_VALUE",
                description="Código de instrumento vacío",
                current_value=value,
                severity="ERROR"
            ))
            return issues
        
        cleaned_code = value.strip().upper()
        
        # Validar formato
        if not self.codigo_pattern.match(cleaned_code):
            suggested_fix = self._suggest_codigo_fix(cleaned_code)
            issues.append(ValidationIssue(
                file_path=file_path,
                row_number=row_num,
                column="codigo",
                issue_type="INVALID_FORMAT",
                description=f"Formato de código inválido: {value}",
                current_value=value,
                suggested_fix=suggested_fix,
                severity="ERROR"
            ))
        
        # Validar consistencia de cliente
        if cleaned_code in self.client_codes:
            expected_client = self.client_codes[cleaned_code]
            extracted_client = self._extract_client_from_code(cleaned_code)
            
            if extracted_client and extracted_client != expected_client:
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column="codigo",
                    issue_type="CLIENT_MISMATCH",
                    description=f"Inconsistencia de cliente en código: {cleaned_code}",
                    current_value=value,
                    severity="WARNING"
                ))
        
        return issues
    
    def validate_client_consistency(self, data: List[Dict[str, Any]], file_path: Path) -> List[ValidationIssue]:
        """Valida la consistencia entre códigos de instrumentos y clientes."""
        issues = []
        
        for row_num, row in enumerate(data, start=2):
            codigo = row.get('codigo', '').strip().upper()
            cliente_declarado = row.get('cliente', '').strip().upper()
            
            if not codigo or not cliente_declarado:
                continue
            
            # Extraer cliente del código
            cliente_codigo = self._extract_client_from_code(codigo)
            
            if cliente_codigo and cliente_codigo != cliente_declarado:
                # Verificar si es una abreviación conocida
                if not self._is_valid_client_abbreviation(cliente_codigo, cliente_declarado):
                    issues.append(ValidationIssue(
                        file_path=file_path,
                        row_number=row_num,
                        column="cliente",
                        issue_type="CLIENT_CODE_MISMATCH",
                        description=f"Cliente en código ({cliente_codigo}) no coincide con cliente declarado ({cliente_declarado})",
                        current_value=f"{codigo} / {cliente_declarado}",
                        severity="WARNING"
                    ))
        
        return issues
    
    def _is_valid_client_abbreviation(self, code_client: str, declared_client: str) -> bool:
        """Verifica si code_client es una abreviación válida de declared_client."""
        # Mapeo de abreviaciones conocidas
        abbreviations = {
            'SBL': ['SBL LABORATORIOS', 'SBL LAB', 'SBL'],
            'LAB': ['LABORATORIO', 'LAB'],
            'CLI': ['CLIENTE', 'CLIENT'],
            'EXT': ['EXTERNO', 'EXTERNAL'],
        }
        
        if code_client in abbreviations:
            return any(abbrev in declared_client for abbrev in abbreviations[code_client])
        
        return False
    
    def validate_service_data(self, data: List[Dict[str, Any]], file_path: Path) -> List[ValidationIssue]:
        """Valida datos específicos de servicios a clientes."""
        issues = []
        
        for row_num, row in enumerate(data, start=2):
            # Validar estado de servicio
            estado = row.get('estado_servicio', '').strip().upper()
            estados_validos = ['PENDIENTE', 'EN_PROCESO', 'COMPLETADO', 'CANCELADO', 'REPROGRAMADO']
            
            if estado and estado not in estados_validos:
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column="estado_servicio",
                    issue_type="INVALID_SERVICE_STATUS",
                    description=f"Estado de servicio inválido: {estado}",
                    current_value=estado,
                    suggested_fix="Usar: " + ", ".join(estados_validos),
                    severity="ERROR"
                ))
            
            # Validar tipo de servicio
            tipo_servicio = row.get('tipo_servicio', '').strip().upper()
            tipos_validos = ['CALIBRACION', 'MANTENIMIENTO', 'REPARACION', 'VALIDACION', 'VERIFICACION']
            
            if tipo_servicio and tipo_servicio not in tipos_validos:
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column="tipo_servicio",
                    issue_type="INVALID_SERVICE_TYPE",
                    description=f"Tipo de servicio inválido: {tipo_servicio}",
                    current_value=tipo_servicio,
                    suggested_fix="Usar: " + ", ".join(tipos_validos),
                    severity="ERROR"
                ))
            
            # Validar fechas de servicio
            fecha_programada = row.get('fecha_programada', '')
            fecha_completada = row.get('fecha_completada', '')
            
            if fecha_programada and fecha_completada:
                try:
                    date_prog = DateParser.parse_spanish_date(fecha_programada)
                    date_comp = DateParser.parse_spanish_date(fecha_completada)
                    
                    if date_prog and date_comp and date_comp < date_prog:
                        issues.append(ValidationIssue(
                            file_path=file_path,
                            row_number=row_num,
                            column="fecha_completada",
                            issue_type="INVALID_DATE_SEQUENCE",
                            description="Fecha de completado anterior a fecha programada",
                            current_value=f"{fecha_programada} -> {fecha_completada}",
                            severity="ERROR"
                        ))
                except Exception:
                    pass  # Los errores de fecha se capturan en otra validación
        
        return issues
    
    def validate_telefono(self, value: str, column: str, row_num: int, file_path: Path) -> List[ValidationIssue]:
        """Valida un número de teléfono."""
        issues = []
        
        if not value or value.strip() in {"", "NA", "ND", "N/A"}:
            return issues  # Teléfono puede ser opcional
        
        if not self.telefono_pattern.match(value.strip()):
            issues.append(ValidationIssue(
                file_path=file_path,
                row_number=row_num,
                column=column,
                issue_type="INVALID_PHONE",
                description=f"Formato de teléfono inválido: {value}",
                current_value=value,
                severity="WARNING"
            ))
        
        return issues
    
    def validate_file(self, file_path: Path, validation_config: Dict[str, Any]) -> ValidationReport:
        """Valida un archivo CSV completo con validaciones específicas para clientes."""
        self.logger.info(f"Validando archivo de cliente: {file_path}")
        
        try:
            data = CSVHandler.read_csv(file_path)
        except Exception as e:
            report = ValidationReport(file_path=file_path, total_rows=0)
            report.issues.append(ValidationIssue(
                file_path=file_path,
                row_number=0,
                column="FILE",
                issue_type="READ_ERROR",
                description=f"Error leyendo archivo: {e}",
                current_value="",
                severity="ERROR"
            ))
            return report
        
        report = ValidationReport(file_path=file_path, total_rows=len(data))
        
        # Validar columnas requeridas
        required_columns = validation_config.get('required_columns', [])
        available_columns = set(data[0].keys() if data else [])
        
        for req_col in required_columns:
            if req_col not in available_columns:
                report.issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=0,
                    column=req_col,
                    issue_type="MISSING_COLUMN",
                    description=f"Columna requerida faltante: {req_col}",
                    current_value="",
                    severity="ERROR"
                ))
        
        # Validaciones específicas por tipo de archivo
        if 'servicios' in validation_config:
            report.issues.extend(self.validate_service_data(data, file_path))
        
        if 'client_consistency' in validation_config:
            report.issues.extend(self.validate_client_consistency(data, file_path))
        
        # Validar cada fila
        for row_num, row in enumerate(data, start=2):
            # Validar códigos con contexto de cliente
            if 'codigo' in row:
                report.issues.extend(self.validate_client_codigo(row['codigo'], row_num, file_path))
            
            # Validar fechas
            date_columns = validation_config.get('date_columns', [])
            for col in date_columns:
                if col in row:
                    report.issues.extend(self.validate_date(row[col], col, row_num, file_path))
            
            # Validar emails
            email_columns = validation_config.get('email_columns', [])
            for col in email_columns:
                if col in row:
                    report.issues.extend(self.validate_email(row[col], col, row_num, file_path))
            
            # Validar teléfonos
            phone_columns = validation_config.get('phone_columns', [])
            for col in phone_columns:
                if col in row:
                    report.issues.extend(self.validate_telefono(row[col], col, row_num, file_path))
            
            # Validar valores numéricos
            numeric_columns = validation_config.get('numeric_columns', {})
            for col, constraints in numeric_columns.items():
                if col in row:
                    report.issues.extend(self.validate_numeric(
                        row[col], col, row_num, file_path,
                        constraints.get('min'), constraints.get('max')
                    ))
        
        # Buscar duplicados
        duplicate_keys = validation_config.get('duplicate_keys', [])
        if duplicate_keys:
            report.duplicates = self.find_duplicates(data, duplicate_keys, file_path)
        
        return report
    
    def validate_date(self, value: str, column: str, row_num: int, file_path: Path) -> List[ValidationIssue]:
        """Valida una fecha (copiado del validador base)."""
        issues = []
        
        if not value or value.strip() in {"", "NA", "ND", "N/A"}:
            return issues
        
        try:
            parsed_date = DateParser.parse_spanish_date(value)
            if parsed_date is None:
                raise ValueError("Fecha no reconocible")
            
            # Validar rango razonable
            today = dt.date.today()
            if parsed_date < dt.date(1990, 1, 1):
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column=column,
                    issue_type="DATE_TOO_OLD",
                    description=f"Fecha demasiado antigua: {value}",
                    current_value=value,
                    severity="WARNING"
                ))
            elif parsed_date > today + dt.timedelta(days=3650):
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column=column,
                    issue_type="DATE_TOO_FUTURE",
                    description=f"Fecha muy lejana en el futuro: {value}",
                    current_value=value,
                    severity="WARNING"
                ))
        
        except (ValueError, TypeError):
            suggested_fix = self._suggest_date_fix(value)
            issues.append(ValidationIssue(
                file_path=file_path,
                row_number=row_num,
                column=column,
                issue_type="INVALID_DATE",
                description=f"Formato de fecha inválido: {value}",
                current_value=value,
                suggested_fix=suggested_fix,
                severity="ERROR"
            ))
        
        return issues
    
    def validate_email(self, value: str, column: str, row_num: int, file_path: Path) -> List[ValidationIssue]:
        """Valida una dirección de email."""
        issues = []
        
        if not value or value.strip() in {"", "NA", "ND", "N/A"}:
            return issues
        
        if not self.email_pattern.match(value.strip()):
            issues.append(ValidationIssue(
                file_path=file_path,
                row_number=row_num,
                column=column,
                issue_type="INVALID_EMAIL",
                description=f"Formato de email inválido: {value}",
                current_value=value,
                severity="ERROR"
            ))
        
        return issues
    
    def validate_numeric(self, value: str, column: str, row_num: int, file_path: Path, 
                        min_val: Optional[float] = None, max_val: Optional[float] = None) -> List[ValidationIssue]:
        """Valida un valor numérico."""
        issues = []
        
        if not value or value.strip() in {"", "NA", "ND", "N/A"}:
            return issues
        
        try:
            num_value = float(value.replace(',', '.'))
            
            if min_val is not None and num_value < min_val:
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column=column,
                    issue_type="VALUE_TOO_LOW",
                    description=f"Valor demasiado bajo: {value} (mínimo: {min_val})",
                    current_value=value,
                    severity="WARNING"
                ))
            
            if max_val is not None and num_value > max_val:
                issues.append(ValidationIssue(
                    file_path=file_path,
                    row_number=row_num,
                    column=column,
                    issue_type="VALUE_TOO_HIGH",
                    description=f"Valor demasiado alto: {value} (máximo: {max_val})",
                    current_value=value,
                    severity="WARNING"
                ))
        
        except (ValueError, TypeError):
            issues.append(ValidationIssue(
                file_path=file_path,
                row_number=row_num,
                column=column,
                issue_type="INVALID_NUMBER",
                description=f"Valor numérico inválido: {value}",
                current_value=value,
                severity="ERROR"
            ))
        
        return issues
    
    def find_duplicates(self, data: List[Dict[str, Any]], key_columns: List[str], file_path: Path) -> List[Tuple[int, int]]:
        """Encuentra filas duplicadas basadas en columnas clave."""
        duplicates = []
        seen = {}
        
        for i, row in enumerate(data):
            key_parts = []
            for col in key_columns:
                value = row.get(col, '').strip().upper()
                key_parts.append(value)
            
            key = tuple(key_parts)
            
            if all(part == '' for part in key_parts):
                continue
            
            if key in seen:
                duplicates.append((seen[key] + 2, i + 2))
            else:
                seen[key] = i
        
        return duplicates
    
    def get_validation_configs(self) -> Dict[str, Dict[str, Any]]:
        """Define configuraciones de validación para el portal de clientes."""
        return {
            'instrumentos_clientes': {
                'required_columns': ['codigo', 'cliente', 'descripcion'],
                'date_columns': ['fecha_adquisicion', 'ultima_calibracion', 'proxima_calibracion'],
                'email_columns': ['contacto_email'],
                'phone_columns': ['telefono_contacto'],
                'numeric_columns': {
                    'frecuencia_calibracion': {'min': 1, 'max': 120},
                    'costo_servicio': {'min': 0}
                },
                'duplicate_keys': ['codigo'],
                'client_consistency': True
            },
            'servicios': {
                'required_columns': ['codigo', 'cliente', 'tipo_servicio'],
                'date_columns': ['fecha_programada', 'fecha_completada'],
                'email_columns': ['contacto_email'],
                'phone_columns': ['telefono'],
                'duplicate_keys': ['codigo', 'fecha_programada'],
                'client_consistency': True,
                'servicios': True
            },
            'clientes': {
                'required_columns': ['nombre'],
                'email_columns': ['email', 'contacto_email'],
                'phone_columns': ['telefono', 'telefono_contacto'],
                'duplicate_keys': ['nombre', 'email']
            }
        }
    
    def validate_all_files(self) -> None:
        """Valida todos los archivos CSV del portal de clientes."""
        self.logger.info("Iniciando validación de archivos del portal de clientes")
        
        # Cargar datos de referencia
        self.load_reference_data()
        
        # Configuraciones de validación
        configs = self.get_validation_configs()
        
        # Archivos a validar
        files_to_validate = [
            # Archivos de clientes
            (self.csv_dir / "clientes.csv", "clientes"),
            (self.csv_dir / "servicios_clientes.csv", "servicios"),
            (self.csv_dir / "instrumentos_clientes.csv", "instrumentos_clientes"),
            
            # Archivos normalizados
            (self.normalize_dir / "normalize_clientes.csv", "clientes"),
            (self.normalize_dir / "normalize_servicios.csv", "servicios"),
        ]
        
        for file_path, config_type in files_to_validate:
            if file_path.exists():
                config = configs.get(config_type, {})
                report = self.validate_file(file_path, config)
                self.reports.append(report)
            else:
                self.logger.info(f"Archivo no encontrado: {file_path}")
    
    def generate_validation_report(self, output_file: Path) -> None:
        """Genera un reporte detallado de validación para clientes."""
        self.logger.info(f"Generando reporte de validación de clientes en {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# REPORTE DE VALIDACIÓN - PORTAL DE SERVICIOS A CLIENTES SBL\n")
            f.write(f"Fecha de generación: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Resumen general
            total_files = len(self.reports)
            valid_files = len([r for r in self.reports if r.is_valid])
            total_errors = sum(r.errors_count for r in self.reports)
            total_warnings = sum(r.warnings_count for r in self.reports)
            
            f.write("## RESUMEN GENERAL\n\n")
            f.write(f"- **Archivos validados:** {total_files}\n")
            f.write(f"- **Archivos válidos:** {valid_files}\n")
            f.write(f"- **Errores totales:** {total_errors}\n")
            f.write(f"- **Advertencias totales:** {total_warnings}\n\n")
            
            # Estado específico para portal de clientes
            client_specific_issues = [
                i for report in self.reports for i in report.issues
                if i.issue_type in ['CLIENT_MISMATCH', 'CLIENT_CODE_MISMATCH', 'INVALID_SERVICE_STATUS']
            ]
            
            f.write(f"- **Issues específicos de clientes:** {len(client_specific_issues)}\n\n")
            
            # Estado general
            if total_errors == 0:
                f.write("🟢 **ESTADO: VÁLIDO PARA SERVICIOS** - Portal listo para operación\n\n")
            elif total_errors < 5:
                f.write("🟡 **ESTADO: REVISAR** - Algunos errores menores en datos de clientes\n\n")
            else:
                f.write("🔴 **ESTADO: CRÍTICO** - Errores importantes que afectan servicios\n\n")
            
            # Análisis por tipo de problema
            f.write("## ANÁLISIS POR TIPO DE PROBLEMA\n\n")
            
            issue_types = {}
            for report in self.reports:
                for issue in report.issues:
                    issue_types[issue.issue_type] = issue_types.get(issue.issue_type, 0) + 1
            
            for issue_type, count in sorted(issue_types.items(), key=lambda x: x[1], reverse=True):
                f.write(f"- **{issue_type}:** {count} ocurrencias\n")
            
            f.write("\n")
            
            # Detalle por archivo
            f.write("## DETALLE POR ARCHIVO\n\n")
            
            for report in sorted(self.reports, key=lambda r: r.errors_count, reverse=True):
                f.write(f"### {report.file_path.name}\n\n")
                f.write(f"- **Filas procesadas:** {report.total_rows}\n")
                f.write(f"- **Errores:** {report.errors_count}\n")
                f.write(f"- **Advertencias:** {report.warnings_count}\n")
                f.write(f"- **Duplicados:** {len(report.duplicates)}\n")
                
                if report.is_valid:
                    f.write("- **Estado:** ✅ VÁLIDO PARA SERVICIOS\n\n")
                else:
                    f.write("- **Estado:** ❌ REQUIERE CORRECCIÓN\n\n")
                
                # Mostrar errores críticos específicos
                critical_issues = [i for i in report.issues if i.severity == "ERROR"][:5]
                if critical_issues:
                    f.write("**Errores principales:**\n")
                    for issue in critical_issues:
                        f.write(f"- Fila {issue.row_number}, columna '{issue.column}': {issue.description}\n")
                        if issue.suggested_fix:
                            f.write(f"  - Sugerencia: {issue.suggested_fix}\n")
                    f.write("\n")
            
            # Recomendaciones específicas para servicios
            f.write("## RECOMENDACIONES PARA PORTAL DE SERVICIOS\n\n")
            
            if total_errors > 0:
                f.write("1. **URGENTE**: Corregir errores de códigos de instrumentos antes de programar servicios\n")
                f.write("2. **Validar coherencia cliente-código** para evitar errores en facturación\n")
                f.write("3. **Revisar estados de servicios** para mantener flujo de trabajo correcto\n")
            
            if len(client_specific_issues) > 0:
                f.write("4. **Reconciliar datos de clientes** con sistema de facturación\n")
            
            f.write("5. **Implementar validación automática** en formularios de entrada de datos\n")
            f.write("6. **Establecer proceso de revisión** antes de confirmar servicios con clientes\n")
    
    def _suggest_codigo_fix(self, codigo: str) -> Optional[str]:
        """Sugiere una corrección para un código inválido."""
        clean_code = re.sub(r'[^\w\-]', '', codigo).upper()
        
        if re.match(r'^[A-Z]{2,4}\d{3,6}[A-Z]?$', clean_code):
            return clean_code
        
        match = re.match(r'^([A-Z]{2,4})(\d{3,6}[A-Z]?)$', clean_code)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        
        return None
    
    def _suggest_date_fix(self, date_str: str) -> Optional[str]:
        """Sugiere una corrección para una fecha inválida."""
        patterns = [
            (r'(\d{1,2})[\./-](\d{1,2})[\./-](\d{2,4})', r'\1/\2/\3'),
            (r'(\d{4})[\./-](\d{1,2})[\./-](\d{1,2})', r'\3/\2/\1'),
        ]
        
        for pattern, replacement in patterns:
            if re.match(pattern, date_str):
                suggestion = re.sub(pattern, replacement, date_str)
                if DateParser.parse_spanish_date(suggestion):
                    return suggestion
        
        return None
    
    def run_validation(self, output_dir: Optional[Path] = None) -> bool:
        """Ejecuta el proceso completo de validación para el portal de clientes."""
        self.logger.info("Iniciando proceso de validación del portal de clientes")
        
        # Validar todos los archivos
        self.validate_all_files()
        
        # Generar reporte
        if output_dir is None:
            output_dir = self.repo_root / "storage" / "client_validation_reports"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = output_dir / f"client_validation_report_{timestamp}.md"
        
        self.generate_validation_report(report_file)
        
        # Resumen en consola
        total_errors = sum(r.errors_count for r in self.reports)
        total_warnings = sum(r.warnings_count for r in self.reports)
        
        self.logger.info(f"Validación de portal de clientes completada:")
        self.logger.info(f"  - Archivos validados: {len(self.reports)}")
        self.logger.info(f"  - Errores encontrados: {total_errors}")
        self.logger.info(f"  - Advertencias: {total_warnings}")
        self.logger.info(f"  - Reporte generado: {report_file}")
        
        return total_errors == 0


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description="Valida la integridad de los datos del Portal de Servicios a Clientes SBL"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Directorio de salida para reportes"
    )
    
    args = parser.parse_args()
    
    validator = ClientDataValidator()
    success = validator.run_validation(output_dir=args.output)
    
    if not success:
        exit(1)


if __name__ == "__main__":
    main()