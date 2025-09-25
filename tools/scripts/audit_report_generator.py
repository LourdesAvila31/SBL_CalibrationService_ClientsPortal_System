#!/usr/bin/env python3
"""Generador automático de reportes de auditoría para el Portal de Servicios a Clientes SBL.

Este script genera reportes detallados de auditoría basados en los datos
históricos de calibraciones, instrumentos y actividades del portal de clientes.

Características:
- Reportes de cumplimiento de calibraciones por cliente
- Análisis de tendencias temporales
- Detección de instrumentos críticos
- Exportación a múltiples formatos (CSV, Excel)
- Análisis estadístico básico

Uso:
```bash
python tools/scripts/audit_report_generator.py --empresa-id 1 --output storage/audit_reports/
```
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import json

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

from sbl_utils import (
    setup_logging, get_repo_root, get_csv_originales_dir, 
    get_normalize_dir, CSVHandler, DateParser, TextNormalizer
)

@dataclass
class ClientInstrumentStatus:
    """Estado de un instrumento de cliente en el sistema."""
    codigo: str
    cliente: str
    descripcion: str
    ubicacion: str
    ultima_calibracion: Optional[dt.date]
    proxima_calibracion: Optional[dt.date]
    estado_cumplimiento: str
    dias_vencimiento: Optional[int]
    frecuencia_meses: Optional[int]
    criticidad: str
    servicio_tipo: str

@dataclass
class ClientAuditMetrics:
    """Métricas de auditoría por cliente del sistema."""
    cliente: str
    total_instrumentos: int
    instrumentos_al_dia: int
    instrumentos_vencidos: int
    instrumentos_proximos_vencer: int
    porcentaje_cumplimiento: float
    promedio_dias_vencimiento: float
    instrumentos_criticos: int
    servicios_pendientes: int

class ClientAuditReportGenerator:
    """Generador de reportes de auditoría para clientes."""
    
    def __init__(self, empresa_id: int = 1):
        self.empresa_id = empresa_id
        self.repo_root = get_repo_root(__file__)
        self.logger = setup_logging("client_audit_report_generator")
        
        # Directorios
        self.csv_dir = get_csv_originales_dir(self.repo_root)
        self.normalize_dir = get_normalize_dir(self.repo_root)
        self.output_dir = self.repo_root / "storage" / "client_audit_reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Datos cargados
        self.instrumentos: List[Dict[str, Any]] = []
        self.calibraciones: List[Dict[str, Any]] = []
        self.clientes: List[Dict[str, Any]] = []
        self.client_instrument_status: List[ClientInstrumentStatus] = []
    
    def load_data(self) -> None:
        """Carga los datos necesarios desde los archivos CSV."""
        self.logger.info("Cargando datos para el reporte de auditoría de clientes...")
        
        # Cargar instrumentos normalizados
        instrumentos_file = self.normalize_dir / "normalize_instrumentos.csv"
        if instrumentos_file.exists():
            self.instrumentos = CSVHandler.read_csv(instrumentos_file)
            self.logger.info(f"Cargados {len(self.instrumentos)} instrumentos")
        else:
            self.logger.warning(f"No se encontró {instrumentos_file}")
        
        # Cargar datos de clientes
        clientes_files = [
            self.csv_dir / "clientes.csv",
            self.csv_dir / "empresas_clientes.csv",
            self.normalize_dir / "normalize_clientes.csv"
        ]
        
        for cliente_file in clientes_files:
            if cliente_file.exists():
                try:
                    cliente_data = CSVHandler.read_csv(cliente_file)
                    self.clientes.extend(cliente_data)
                    self.logger.info(f"Cargados {len(cliente_data)} clientes desde {cliente_file.name}")
                    break
                except Exception as e:
                    self.logger.warning(f"Error cargando {cliente_file}: {e}")
        
        # Cargar calibraciones (buscar en varios posibles archivos)
        calibraciones_files = [
            self.normalize_dir / "normalize_calibraciones.csv",
            self.csv_dir / "CERT_instrumentos_original_v2.csv",
            self.repo_root / "storage" / "calibraciones_export.csv"
        ]
        
        for cal_file in calibraciones_files:
            if cal_file.exists():
                try:
                    cal_data = CSVHandler.read_csv(cal_file)
                    self.calibraciones.extend(cal_data)
                    self.logger.info(f"Cargadas {len(cal_data)} calibraciones desde {cal_file.name}")
                except Exception as e:
                    self.logger.warning(f"Error cargando {cal_file}: {e}")
    
    def get_client_name(self, codigo_instrumento: str) -> str:
        """Determina el cliente basado en el código del instrumento."""
        # Lógica para extraer cliente del código o buscar en datos
        codigo_upper = codigo_instrumento.upper()
        
        # Buscar en datos de clientes si están disponibles
        for cliente in self.clientes:
            cliente_codigo = cliente.get('codigo', '').upper()
            if cliente_codigo and codigo_upper.startswith(cliente_codigo):
                return cliente.get('nombre', cliente_codigo)
        
        # Fallback: extraer prefijo del código
        if '-' in codigo_upper:
            prefix = codigo_upper.split('-')[0]
        else:
            prefix = codigo_upper[:3]
        
        # Mapeo de prefijos conocidos (puede expandirse)
        client_mapping = {
            'SBL': 'SBL Laboratorios',
            'LAB': 'Laboratorio Externo',
            'CLI': 'Cliente General',
            'EXT': 'Servicio Externo',
            'INT': 'Interno'
        }
        
        return client_mapping.get(prefix, f'Cliente_{prefix}')
    
    def analyze_client_instrument_status(self) -> None:
        """Analiza el estado de cumplimiento de cada instrumento por cliente."""
        self.logger.info("Analizando estado de instrumentos por cliente...")
        
        today = dt.date.today()
        
        for instrumento in self.instrumentos:
            codigo = instrumento.get('codigo', '')
            if not codigo:
                continue
            
            cliente = self.get_client_name(codigo)
            
            # Buscar calibraciones para este instrumento
            calibraciones_instrumento = [
                cal for cal in self.calibraciones 
                if cal.get('codigo', '').upper() == codigo.upper()
            ]
            
            # Encontrar última calibración
            ultima_calibracion = None
            proxima_calibracion = None
            frecuencia_meses = None
            
            if calibraciones_instrumento:
                fechas_calibracion = []
                for cal in calibraciones_instrumento:
                    fecha_str = cal.get('fecha_calibracion') or cal.get('fecha')
                    if fecha_str:
                        fecha = DateParser.parse_spanish_date(fecha_str)
                        if fecha:
                            fechas_calibracion.append(fecha)
                
                if fechas_calibracion:
                    ultima_calibracion = max(fechas_calibracion)
                    
                    # Intentar determinar frecuencia
                    freq_str = instrumento.get('frecuencia_calibracion') or '12'
                    try:
                        frecuencia_meses = int(freq_str) if freq_str.isdigit() else 12
                    except (ValueError, AttributeError):
                        frecuencia_meses = 12
                    
                    # Calcular próxima calibración
                    if frecuencia_meses:
                        proxima_calibracion = DateParser.add_months(
                            ultima_calibracion, frecuencia_meses
                        )
            
            # Determinar estado de cumplimiento
            estado_cumplimiento = "SIN_DATOS"
            dias_vencimiento = None
            
            if proxima_calibracion:
                dias_vencimiento = (proxima_calibracion - today).days
                
                if dias_vencimiento > 30:
                    estado_cumplimiento = "AL_DIA"
                elif dias_vencimiento > 0:
                    estado_cumplimiento = "PROXIMO_VENCER"
                else:
                    estado_cumplimiento = "VENCIDO"
            
            # Determinar criticidad y tipo de servicio
            descripcion = instrumento.get('descripcion', '').lower()
            ubicacion = instrumento.get('ubicacion', '').lower()
            
            criticidad = "NORMAL"
            if any(term in descripcion for term in ['critico', 'vital', 'principal']):
                criticidad = "CRITICA"
            elif any(term in ubicacion for term in ['produccion', 'proceso', 'linea']):
                criticidad = "ALTA"
            
            # Determinar tipo de servicio
            servicio_tipo = "CALIBRACION"
            if any(term in descripcion for term in ['mantenimiento', 'reparacion']):
                servicio_tipo = "MANTENIMIENTO"
            elif any(term in descripcion for term in ['validacion', 'verificacion']):
                servicio_tipo = "VALIDACION"
            
            status = ClientInstrumentStatus(
                codigo=codigo,
                cliente=cliente,
                descripcion=instrumento.get('descripcion', ''),
                ubicacion=instrumento.get('ubicacion', ''),
                ultima_calibracion=ultima_calibracion,
                proxima_calibracion=proxima_calibracion,
                estado_cumplimiento=estado_cumplimiento,
                dias_vencimiento=dias_vencimiento,
                frecuencia_meses=frecuencia_meses,
                criticidad=criticidad,
                servicio_tipo=servicio_tipo
            )
            
            self.client_instrument_status.append(status)
        
        self.logger.info(f"Analizados {len(self.client_instrument_status)} instrumentos de clientes")
    
    def calculate_client_metrics(self) -> Dict[str, ClientAuditMetrics]:
        """Calcula métricas de auditoría por cliente."""
        client_metrics = {}
        
        # Agrupar por cliente
        clients = {}
        for status in self.client_instrument_status:
            cliente = status.cliente
            if cliente not in clients:
                clients[cliente] = []
            clients[cliente].append(status)
        
        # Calcular métricas para cada cliente
        for cliente, instruments in clients.items():
            total = len(instruments)
            
            if total == 0:
                continue
            
            al_dia = len([s for s in instruments if s.estado_cumplimiento == "AL_DIA"])
            vencidos = len([s for s in instruments if s.estado_cumplimiento == "VENCIDO"])
            proximos = len([s for s in instruments if s.estado_cumplimiento == "PROXIMO_VENCER"])
            criticos = len([s for s in instruments if s.criticidad == "CRITICA"])
            
            # Servicios pendientes (vencidos + próximos a vencer)
            servicios_pendientes = vencidos + proximos
            
            porcentaje_cumplimiento = (al_dia / total) * 100
            
            # Calcular promedio de días de vencimiento
            dias_vencimiento = [s.dias_vencimiento for s in instruments if s.dias_vencimiento is not None]
            promedio_dias = sum(dias_vencimiento) / len(dias_vencimiento) if dias_vencimiento else 0.0
            
            client_metrics[cliente] = ClientAuditMetrics(
                cliente=cliente,
                total_instrumentos=total,
                instrumentos_al_dia=al_dia,
                instrumentos_vencidos=vencidos,
                instrumentos_proximos_vencer=proximos,
                porcentaje_cumplimiento=porcentaje_cumplimiento,
                promedio_dias_vencimiento=promedio_dias,
                instrumentos_criticos=criticos,
                servicios_pendientes=servicios_pendientes
            )
        
        return client_metrics
    
    def generate_client_summary_report(self, client_metrics: Dict[str, ClientAuditMetrics], output_file: Path) -> None:
        """Genera reporte resumen por cliente."""
        self.logger.info(f"Generando reporte resumen de clientes en {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# REPORTE DE AUDITORÍA POR CLIENTE - SISTEMA SBL\n")
            f.write(f"Fecha de generación: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Empresa ID: {self.empresa_id}\n\n")
            
            # Resumen general
            total_clientes = len(client_metrics)
            total_instrumentos = sum(m.total_instrumentos for m in client_metrics.values())
            total_vencidos = sum(m.instrumentos_vencidos for m in client_metrics.values())
            
            f.write("## RESUMEN GENERAL\n\n")
            f.write(f"- **Total de clientes:** {total_clientes}\n")
            f.write(f"- **Total de instrumentos:** {total_instrumentos}\n")
            f.write(f"- **Total instrumentos vencidos:** {total_vencidos}\n")
            f.write(f"- **Promedio cumplimiento:** {sum(m.porcentaje_cumplimiento for m in client_metrics.values()) / total_clientes:.1f}%\n\n")
            
            # Detalle por cliente
            f.write("## DETALLE POR CLIENTE\n\n")
            
            # Ordenar clientes por porcentaje de cumplimiento (menor primero)
            sorted_clients = sorted(client_metrics.items(), key=lambda x: x[1].porcentaje_cumplimiento)
            
            for cliente, metrics in sorted_clients:
                f.write(f"### {cliente}\n\n")
                f.write(f"- **Instrumentos totales:** {metrics.total_instrumentos}\n")
                f.write(f"- **Cumplimiento:** {metrics.porcentaje_cumplimiento:.1f}%\n")
                f.write(f"- **Instrumentos vencidos:** {metrics.instrumentos_vencidos}\n")
                f.write(f"- **Próximos a vencer:** {metrics.instrumentos_proximos_vencer}\n")
                f.write(f"- **Servicios pendientes:** {metrics.servicios_pendientes}\n")
                f.write(f"- **Instrumentos críticos:** {metrics.instrumentos_criticos}\n")
                
                # Indicador de prioridad
                if metrics.porcentaje_cumplimiento < 70:
                    f.write("- **🔴 PRIORIDAD ALTA** - Requiere atención inmediata\n")
                elif metrics.porcentaje_cumplimiento < 85:
                    f.write("- **🟡 PRIORIDAD MEDIA** - Necesita seguimiento\n")
                else:
                    f.write("- **🟢 ESTADO SATISFACTORIO**\n")
                
                f.write("\n")
            
            # Clientes con mayor necesidad de servicios
            f.write("## CLIENTES PRIORITARIOS\n\n")
            
            priority_clients = [
                (cliente, metrics) for cliente, metrics in client_metrics.items()
                if metrics.instrumentos_vencidos > 0 or metrics.porcentaje_cumplimiento < 80
            ]
            
            priority_clients.sort(key=lambda x: (x[1].instrumentos_vencidos, -x[1].porcentaje_cumplimiento), reverse=True)
            
            for cliente, metrics in priority_clients[:10]:  # Top 10
                f.write(f"**{cliente}:**\n")
                f.write(f"  - Vencidos: {metrics.instrumentos_vencidos}\n")
                f.write(f"  - Cumplimiento: {metrics.porcentaje_cumplimiento:.1f}%\n")
                f.write(f"  - Servicios pendientes: {metrics.servicios_pendientes}\n\n")
    
    def generate_client_detailed_csv(self, output_file: Path) -> None:
        """Genera reporte detallado por cliente en formato CSV."""
        self.logger.info(f"Generando reporte detallado CSV por cliente en {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'cliente', 'codigo', 'descripcion', 'ubicacion', 'servicio_tipo', 'criticidad',
                'ultima_calibracion', 'proxima_calibracion', 'estado_cumplimiento',
                'dias_vencimiento', 'frecuencia_meses'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for status in sorted(self.client_instrument_status, key=lambda x: (x.cliente, x.codigo)):
                writer.writerow({
                    'cliente': status.cliente,
                    'codigo': status.codigo,
                    'descripcion': status.descripcion,
                    'ubicacion': status.ubicacion,
                    'servicio_tipo': status.servicio_tipo,
                    'criticidad': status.criticidad,
                    'ultima_calibracion': status.ultima_calibracion.isoformat() if status.ultima_calibracion else '',
                    'proxima_calibracion': status.proxima_calibracion.isoformat() if status.proxima_calibracion else '',
                    'estado_cumplimiento': status.estado_cumplimiento,
                    'dias_vencimiento': status.dias_vencimiento or '',
                    'frecuencia_meses': status.frecuencia_meses or ''
                })
    
    def generate_client_excel_report(self, client_metrics: Dict[str, ClientAuditMetrics], output_file: Path) -> None:
        """Genera reporte en formato Excel por cliente."""
        if not HAS_PANDAS:
            self.logger.warning("pandas no disponible, omitiendo reporte Excel")
            return
        
        self.logger.info(f"Generando reporte Excel por cliente en {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Hoja de resumen por cliente
            client_summary_data = []
            for cliente, metrics in client_metrics.items():
                client_summary_data.append({
                    'Cliente': cliente,
                    'Total Instrumentos': metrics.total_instrumentos,
                    'Al Día': metrics.instrumentos_al_dia,
                    'Vencidos': metrics.instrumentos_vencidos,
                    'Próximos a Vencer': metrics.instrumentos_proximos_vencer,
                    'Cumplimiento (%)': round(metrics.porcentaje_cumplimiento, 1),
                    'Servicios Pendientes': metrics.servicios_pendientes,
                    'Instrumentos Críticos': metrics.instrumentos_criticos
                })
            
            df_summary = pd.DataFrame(client_summary_data)
            df_summary = df_summary.sort_values('Cumplimiento (%)')
            df_summary.to_excel(writer, sheet_name='Resumen por Cliente', index=False)
            
            # Hoja con todos los instrumentos
            instruments_data = []
            for status in self.client_instrument_status:
                instruments_data.append({
                    'Cliente': status.cliente,
                    'Código': status.codigo,
                    'Descripción': status.descripcion,
                    'Ubicación': status.ubicacion,
                    'Tipo Servicio': status.servicio_tipo,
                    'Criticidad': status.criticidad,
                    'Última Calibración': status.ultima_calibracion,
                    'Próxima Calibración': status.proxima_calibracion,
                    'Estado': status.estado_cumplimiento,
                    'Días hasta Vencimiento': status.dias_vencimiento,
                    'Frecuencia (meses)': status.frecuencia_meses
                })
            
            df_instruments = pd.DataFrame(instruments_data)
            df_instruments.to_excel(writer, sheet_name='Todos los Instrumentos', index=False)
            
            # Hoja de servicios pendientes
            pending_df = df_instruments[df_instruments['Estado'].isin(['VENCIDO', 'PROXIMO_VENCER'])]
            if not pending_df.empty:
                pending_df = pending_df.sort_values(['Cliente', 'Estado'])
                pending_df.to_excel(writer, sheet_name='Servicios Pendientes', index=False)
    
    def generate_reports(self, output_dir: Optional[Path] = None) -> None:
        """Genera todos los reportes de auditoría por cliente."""
        if output_dir is None:
            output_dir = self.output_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generar timestamp para los archivos
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Calcular métricas por cliente
        client_metrics = self.calculate_client_metrics()
        
        # Generar diferentes tipos de reportes
        self.generate_client_summary_report(
            client_metrics, 
            output_dir / f"client_audit_summary_{timestamp}.md"
        )
        
        self.generate_client_detailed_csv(
            output_dir / f"client_audit_detailed_{timestamp}.csv"
        )
        
        # Excel solo si pandas está disponible
        if HAS_PANDAS:
            try:
                self.generate_client_excel_report(
                    client_metrics,
                    output_dir / f"client_audit_report_{timestamp}.xlsx"
                )
            except Exception as e:
                self.logger.warning(f"Error generando Excel: {e}")
        
        self.logger.info(f"Reportes de clientes generados en {output_dir}")
        self.logger.info(f"Total clientes analizados: {len(client_metrics)}")
        
        # Log de clientes con mayor prioridad
        priority_clients = [
            (cliente, metrics) for cliente, metrics in client_metrics.items()
            if metrics.instrumentos_vencidos > 0
        ]
        if priority_clients:
            self.logger.info(f"Clientes con instrumentos vencidos: {len(priority_clients)}")
    
    def run(self, output_dir: Optional[Path] = None) -> None:
        """Ejecuta el proceso completo de generación de reportes por cliente."""
        self.logger.info("Iniciando generación de reportes de auditoría por cliente")
        
        self.load_data()
        self.analyze_client_instrument_status()
        self.generate_reports(output_dir)
        
        self.logger.info("Proceso de auditoría por cliente completado")


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description="Genera reportes de auditoría por cliente para el Portal de Servicios SBL"
    )
    parser.add_argument(
        "--empresa-id",
        type=int,
        default=1,
        help="ID de la empresa para el reporte"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Directorio de salida para los reportes"
    )
    
    args = parser.parse_args()
    
    generator = ClientAuditReportGenerator(empresa_id=args.empresa_id)
    generator.run(output_dir=args.output)


if __name__ == "__main__":
    main()