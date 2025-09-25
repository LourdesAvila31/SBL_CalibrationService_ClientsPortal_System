#!/usr/bin/env python3
"""Script maestro para ejecutar todos los procesos del Portal de Servicios a Clientes SBL.

Este script orquesta la ejecución de todos los componentes específicos del portal:
- Validación de datos de clientes
- Normalización de archivos CSV de servicios
- Generación de SQL de inserción para clientes
- Generación de reportes de auditoría por cliente
- Limpieza y mantenimiento del portal

Características:
- Ejecución secuencial con manejo de errores
- Logging detallado de todo el proceso
- Opciones para ejecutar solo partes específicas
- Generación de reportes de resumen
- Verificación de prerrequisitos del portal

Uso:
```bash
python tools/scripts/run_all_processes.py --full --backup
```
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any
import json

from sbl_utils import setup_logging, get_repo_root

class SBLClientPortalOrchestrator:
    """Orquestador principal del portal de servicios a clientes SBL."""
    
    def __init__(self, empresa_id: int = 1, backup: bool = False):
        self.empresa_id = empresa_id
        self.backup = backup
        self.repo_root = get_repo_root(__file__)
        self.logger = setup_logging("sbl_client_portal_orchestrator")
        
        # Directorios
        self.tools_dir = self.repo_root / "tools" / "scripts"
        self.output_dir = self.repo_root / "storage" / "client_process_runs"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Estado del proceso
        self.process_results: Dict[str, Any] = {}
        self.start_time = dt.datetime.now()
        
        # Scripts disponibles específicos para el portal de clientes
        self.available_scripts = {
            'setup_environment': 'setup_python_environment.py',
            'validate_client_data': 'data_validator.py',
            'client_audit_report': 'audit_report_generator.py',
            'generate_cert_calibrations': 'generate_cert_calibrations.py',
            'generate_insert_instrumentos': 'generate_insert_instrumentos.py',
            'generate_plan_riesgos': 'generate_plan_riesgos.py',
        }
    
    def check_prerequisites(self) -> bool:
        """Verifica que todos los prerrequisitos del portal estén instalados."""
        self.logger.info("Verificando prerrequisitos del portal de servicios...")
        
        # Verificar Python
        if sys.version_info < (3, 8):
            self.logger.error("Python 3.8+ requerido")
            return False
        
        # Verificar que los scripts existan
        missing_scripts = []
        for script_name, script_file in self.available_scripts.items():
            script_path = self.tools_dir / script_file
            if not script_path.exists():
                missing_scripts.append(script_file)
        
        if missing_scripts:
            self.logger.error(f"Scripts faltantes: {missing_scripts}")
            return False
        
        # Verificar directorios específicos del portal
        required_dirs = [
            self.repo_root / "app" / "Modules" / "Internal" / "ArchivosSql",
            self.repo_root / "storage" / "client_audit_reports",
            self.repo_root / "storage" / "client_validation_reports",
            self.repo_root / "portal-clientes-servicio" / "storage",
        ]
        
        for dir_path in required_dirs:
            if not dir_path.exists():
                self.logger.warning(f"Directorio faltante: {dir_path}")
                dir_path.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Directorio creado: {dir_path}")
        
        # Verificar archivos de configuración del portal
        config_files = [
            self.repo_root / "portal-clientes-servicio" / "package.json",
            self.repo_root / "README.md",
        ]
        
        for config_file in config_files:
            if not config_file.exists():
                self.logger.warning(f"Archivo de configuración faltante: {config_file}")
        
        self.logger.info("✅ Prerrequisitos del portal verificados")
        return True
    
    def run_script(self, script_key: str, args: List[str] = None, required: bool = True) -> bool:
        """Ejecuta un script específico del portal."""
        if script_key not in self.available_scripts:
            self.logger.error(f"Script desconocido: {script_key}")
            return False
        
        script_file = self.available_scripts[script_key]
        script_path = self.tools_dir / script_file
        
        if not script_path.exists():
            message = f"Script no encontrado: {script_path}"
            if required:
                self.logger.error(message)
                return False
            else:
                self.logger.warning(message)
                return True
        
        self.logger.info(f"Ejecutando script del portal: {script_file}")
        
        # Construir comando
        cmd = [sys.executable, str(script_path)]
        if args:
            cmd.extend(args)
        
        # Añadir argumentos específicos del portal
        if script_key != 'setup_environment':
            if '--empresa-id' not in (args or []):
                cmd.extend(['--empresa-id', str(self.empresa_id)])
        
        try:
            # Ejecutar script
            start_time = dt.datetime.now()
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                check=True,
                cwd=self.repo_root
            )
            
            end_time = dt.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Registrar resultado
            self.process_results[script_key] = {
                'status': 'success',
                'duration': duration,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'timestamp': end_time.isoformat()
            }
            
            self.logger.info(f"✅ {script_file} completado en {duration:.1f}s")
            
            # Mostrar salida informativa
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n')[:5]:
                    self.logger.info(f"  {line}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            end_time = dt.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Registrar error
            self.process_results[script_key] = {
                'status': 'error',
                'duration': duration,
                'stdout': e.stdout,
                'stderr': e.stderr,
                'return_code': e.returncode,
                'timestamp': end_time.isoformat()
            }
            
            message = f"❌ Error en {script_file} (código: {e.returncode})"
            if required:
                self.logger.error(message)
                if e.stderr:
                    self.logger.error(f"Error: {e.stderr}")
                return False
            else:
                self.logger.warning(message)
                return True
                
        except Exception as e:
            self.process_results[script_key] = {
                'status': 'exception',
                'duration': 0,
                'error': str(e),
                'timestamp': dt.datetime.now().isoformat()
            }
            
            message = f"❌ Excepción en {script_file}: {e}"
            if required:
                self.logger.error(message)
                return False
            else:
                self.logger.warning(message)
                return True
    
    def run_setup_process(self) -> bool:
        """Ejecuta el proceso de configuración inicial del portal."""
        self.logger.info("🚀 Iniciando configuración del portal de servicios...")
        
        # Instalar dependencias específicas del portal
        setup_args = ['--dev'] if self.backup else []
        if not self.run_script('setup_environment', setup_args, required=False):
            self.logger.warning("Setup del entorno falló, continuando...")
        
        return True
    
    def run_client_validation_process(self) -> bool:
        """Ejecuta el proceso de validación de datos de clientes."""
        self.logger.info("🔍 Iniciando validación de datos de clientes...")
        
        args = []
        if self.backup:
            args.append('--backup')
        
        # Validar con enfoque en datos de clientes
        return self.run_script('validate_client_data', args, required=True)
    
    def run_client_generation_process(self) -> bool:
        """Ejecuta los procesos de generación específicos para clientes."""
        self.logger.info("⚙️ Iniciando generación de archivos para clientes...")
        
        success = True
        
        # Generar calibraciones de certificados (importante para clientes)
        if not self.run_script('generate_cert_calibrations', [], required=True):
            self.logger.error("Generación de calibraciones crítica para servicios a clientes")
            success = False
        
        # Generar inserts de instrumentos (para inventario de clientes)
        if not self.run_script('generate_insert_instrumentos', [], required=False):
            self.logger.warning("Generación de instrumentos falló")
            success = False
        
        # Generar plan de riesgos (para clientes que lo requieran)
        if not self.run_script('generate_plan_riesgos', [], required=False):
            self.logger.warning("Generación de plan de riesgos falló")
        
        return success
    
    def run_client_reporting_process(self) -> bool:
        """Ejecuta el proceso de generación de reportes para clientes."""
        self.logger.info("📊 Iniciando generación de reportes de clientes...")
        
        # Generar reportes específicos por cliente
        return self.run_script('client_audit_report', [], required=True)
    
    def analyze_client_service_readiness(self) -> Dict[str, Any]:
        """Analiza si el portal está listo para ofrecer servicios."""
        readiness = {
            'overall_status': 'unknown',
            'critical_processes': 0,
            'warning_processes': 0,
            'successful_processes': 0,
            'blocking_issues': [],
            'recommendations': []
        }
        
        # Contar estados de procesos
        for script_key, result in self.process_results.items():
            if result['status'] == 'success':
                readiness['successful_processes'] += 1
            elif result['status'] in ['error', 'exception']:
                # Determinar si es crítico para servicios
                if script_key in ['validate_client_data', 'generate_cert_calibrations', 'client_audit_report']:
                    readiness['critical_processes'] += 1
                    readiness['blocking_issues'].append(f"Proceso crítico fallido: {script_key}")
                else:
                    readiness['warning_processes'] += 1
        
        # Determinar estado general
        if readiness['critical_processes'] == 0:
            if readiness['warning_processes'] == 0:
                readiness['overall_status'] = 'ready'
                readiness['recommendations'].append("Portal listo para ofrecer servicios a clientes")
            else:
                readiness['overall_status'] = 'ready_with_warnings'
                readiness['recommendations'].append("Portal operativo con algunas limitaciones menores")
        else:
            readiness['overall_status'] = 'not_ready'
            readiness['recommendations'].append("Resolver problemas críticos antes de ofrecer servicios")
        
        # Recomendaciones específicas
        if 'validate_client_data' in [k for k, v in self.process_results.items() if v['status'] != 'success']:
            readiness['recommendations'].append("URGENTE: Validar y corregir datos de clientes")
        
        if 'generate_cert_calibrations' in [k for k, v in self.process_results.items() if v['status'] != 'success']:
            readiness['recommendations'].append("CRÍTICO: Regenerar calibraciones para servicios")
        
        return readiness
    
    def generate_client_portal_summary(self) -> Path:
        """Genera un reporte resumen específico del portal de clientes."""
        timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"client_portal_summary_{timestamp}.md"
        
        self.logger.info(f"Generando reporte del portal de clientes: {report_file}")
        
        total_duration = (dt.datetime.now() - self.start_time).total_seconds()
        readiness = self.analyze_client_service_readiness()
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# REPORTE DEL PORTAL DE SERVICIOS A CLIENTES SBL\n\n")
            f.write(f"**Fecha de ejecución:** {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Duración total:** {total_duration:.1f} segundos\n")
            f.write(f"**Empresa ID:** {self.empresa_id}\n\n")
            
            # Estado de preparación para servicios
            f.write("## ESTADO DE PREPARACIÓN PARA SERVICIOS\n\n")
            
            status_icons = {
                'ready': '🟢 **LISTO**',
                'ready_with_warnings': '🟡 **OPERATIVO CON ADVERTENCIAS**',
                'not_ready': '🔴 **NO LISTO**',
                'unknown': '⚪ **DESCONOCIDO**'
            }
            
            f.write(f"{status_icons.get(readiness['overall_status'], '⚪ DESCONOCIDO')}\n\n")
            
            f.write(f"- **Procesos exitosos:** {readiness['successful_processes']}\n")
            f.write(f"- **Procesos críticos fallidos:** {readiness['critical_processes']}\n")
            f.write(f"- **Advertencias:** {readiness['warning_processes']}\n\n")
            
            # Problemas bloqueantes
            if readiness['blocking_issues']:
                f.write("### ⚠️ PROBLEMAS BLOQUEANTES\n\n")
                for issue in readiness['blocking_issues']:
                    f.write(f"- {issue}\n")
                f.write("\n")
            
            # Resumen de procesos específicos del portal
            f.write("## PROCESOS ESPECÍFICOS DEL PORTAL\n\n")
            
            critical_processes = {
                'validate_client_data': 'Validación de Datos de Clientes',
                'generate_cert_calibrations': 'Generación de Calibraciones',
                'client_audit_report': 'Reportes de Auditoría por Cliente'
            }
            
            for script_key, description in critical_processes.items():
                if script_key in self.process_results:
                    result = self.process_results[script_key]
                    status_icon = "✅" if result['status'] == 'success' else "❌"
                    f.write(f"### {status_icon} {description}\n\n")
                    f.write(f"- **Estado:** {result['status'].upper()}\n")
                    f.write(f"- **Duración:** {result.get('duration', 0):.1f} segundos\n")
                    
                    if result['status'] != 'success':
                        f.write(f"- **⚠️ IMPACTO:** Afecta la capacidad de ofrecer servicios\n")
                    
                    f.write("\n")
            
            # Detalle de otros procesos
            f.write("## OTROS PROCESOS\n\n")
            
            for script_key, result in self.process_results.items():
                if script_key not in critical_processes:
                    script_name = self.available_scripts[script_key]
                    status_icon = "✅" if result['status'] == 'success' else "❌"
                    
                    f.write(f"### {status_icon} {script_name}\n\n")
                    f.write(f"- **Estado:** {result['status'].upper()}\n")
                    f.write(f"- **Duración:** {result.get('duration', 0):.1f} segundos\n\n")
            
            # Recomendaciones específicas
            f.write("## RECOMENDACIONES PARA EL PORTAL\n\n")
            
            for i, recommendation in enumerate(readiness['recommendations'], 1):
                f.write(f"{i}. {recommendation}\n")
            
            f.write("\n")
            
            # Pasos siguientes
            f.write("## PRÓXIMOS PASOS\n\n")
            
            if readiness['overall_status'] == 'ready':
                f.write("1. ✅ **Portal listo** - Puede comenzar a ofrecer servicios\n")
                f.write("2. 📋 **Monitorear** reportes de auditoría regularmente\n")
                f.write("3. 🔄 **Ejecutar** este proceso semanalmente\n")
            elif readiness['overall_status'] == 'ready_with_warnings':
                f.write("1. ⚠️ **Revisar** advertencias antes de ofrecer servicios críticos\n")
                f.write("2. 🔧 **Corregir** problemas menores cuando sea posible\n")
                f.write("3. 📊 **Verificar** reportes de clientes antes de entregarlos\n")
            else:
                f.write("1. 🚨 **URGENTE** - Corregir problemas críticos identificados\n")
                f.write("2. 🔍 **No ofrecer servicios** hasta resolver problemas\n")
                f.write("3. 🛠️ **Ejecutar nuevamente** después de correcciones\n")
            
            # Información de contacto y soporte
            f.write("\n## SOPORTE TÉCNICO\n\n")
            f.write("Para problemas con el portal de servicios:\n")
            f.write("1. Revisar logs detallados en `storage/client_process_runs/`\n")
            f.write("2. Ejecutar scripts individuales para diagnóstico específico\n")
            f.write("3. Verificar configuración de base de datos y permisos\n")
        
        return report_file
    
    def run_full_client_portal_process(self) -> bool:
        """Ejecuta todo el proceso completo del portal de clientes."""
        self.logger.info("🎯 Iniciando proceso completo del Portal de Servicios a Clientes SBL")
        
        # Verificar prerrequisitos
        if not self.check_prerequisites():
            self.logger.error("Prerrequisitos del portal no cumplidos, abortando")
            return False
        
        success = True
        
        # 1. Configuración inicial del portal
        if not self.run_setup_process():
            self.logger.warning("Configuración inicial del portal falló, continuando...")
        
        # 2. Validación de datos de clientes (CRÍTICO)
        if not self.run_client_validation_process():
            self.logger.error("❌ CRÍTICO: Validación de datos de clientes falló")
            success = False
            # Continuar para generar reporte de estado
        
        # 3. Generación de archivos para servicios (CRÍTICO)
        if not self.run_client_generation_process():
            self.logger.error("❌ CRÍTICO: Generación de archivos para servicios falló")
            success = False
        
        # 4. Generación de reportes por cliente (CRÍTICO)
        if not self.run_client_reporting_process():
            self.logger.error("❌ CRÍTICO: Generación de reportes de clientes falló")
            success = False
        
        # 5. Generar reportes finales del portal
        summary_report = self.generate_client_portal_summary()
        process_log = self.save_process_log()
        
        # Análisis final de preparación
        readiness = self.analyze_client_service_readiness()
        
        # Resumen final
        total_duration = (dt.datetime.now() - self.start_time).total_seconds()
        successful_count = len([r for r in self.process_results.values() if r['status'] == 'success'])
        total_count = len(self.process_results)
        
        self.logger.info(f"🏁 Proceso del portal completado en {total_duration:.1f} segundos")
        self.logger.info(f"📈 Resultados: {successful_count}/{total_count} procesos exitosos")
        self.logger.info(f"🎯 Estado del portal: {readiness['overall_status'].upper()}")
        self.logger.info(f"📄 Reporte del portal: {summary_report}")
        self.logger.info(f"📋 Log detallado: {process_log}")
        
        # Mensaje final específico
        if readiness['overall_status'] == 'ready':
            self.logger.info("🟢 Portal LISTO para ofrecer servicios a clientes")
        elif readiness['overall_status'] == 'ready_with_warnings':
            self.logger.info("🟡 Portal OPERATIVO con algunas limitaciones")
        else:
            self.logger.error("🔴 Portal NO LISTO - Revisar problemas críticos")
        
        return success and readiness['overall_status'] in ['ready', 'ready_with_warnings']
    
    def save_process_log(self) -> Path:
        """Guarda un log detallado específico del portal."""
        timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        log_file = self.output_dir / f"client_portal_log_{timestamp}.json"
        
        readiness = self.analyze_client_service_readiness()
        
        log_data = {
            'portal_info': {
                'name': 'Portal de Servicios a Clientes SBL',
                'start_time': self.start_time.isoformat(),
                'end_time': dt.datetime.now().isoformat(),
                'total_duration': (dt.datetime.now() - self.start_time).total_seconds(),
                'empresa_id': self.empresa_id,
                'backup_enabled': self.backup,
                'service_readiness': readiness
            },
            'execution_info': {
                'python_version': f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                'working_directory': str(self.repo_root),
                'scripts_executed': len(self.process_results)
            },
            'process_results': self.process_results
        }
        
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        
        return log_file


def main():
    """Función principal del portal de clientes."""
    parser = argparse.ArgumentParser(
        description="Ejecuta todos los procesos del Portal de Servicios a Clientes SBL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso para Portal de Clientes:
  python run_all_processes.py --full                    # Proceso completo del portal
  python run_all_processes.py --processes validate generate  # Solo validar y generar para clientes
  python run_all_processes.py --backup --empresa-id 2        # Con backup para empresa 2
        """
    )
    
    parser.add_argument(
        "--full",
        action="store_true",
        help="Ejecutar proceso completo del portal (configuración, validación, generación, reportes)"
    )
    
    parser.add_argument(
        "--processes",
        nargs="+",
        help="Procesos específicos: setup, validate, generate, report, o nombres de scripts"
    )
    
    parser.add_argument(
        "--empresa-id",
        type=int,
        default=1,
        help="ID de la empresa (default: 1)"
    )
    
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Crear copias de seguridad antes de procesar"
    )
    
    args = parser.parse_args()
    
    # Validar argumentos
    if not args.full and not args.processes:
        parser.error("Debe especificar --full o --processes")
    
    # Crear orquestador del portal
    orchestrator = SBLClientPortalOrchestrator(
        empresa_id=args.empresa_id,
        backup=args.backup
    )
    
    # Ejecutar proceso del portal
    if args.full:
        success = orchestrator.run_full_client_portal_process()
    else:
        # Para procesos parciales, usar la misma lógica pero con contexto de portal
        success = True
        for process in args.processes:
            if process == 'setup':
                success &= orchestrator.run_setup_process()
            elif process == 'validate':
                success &= orchestrator.run_client_validation_process()
            elif process == 'generate':
                success &= orchestrator.run_client_generation_process()
            elif process == 'report':
                success &= orchestrator.run_client_reporting_process()
            elif process in orchestrator.available_scripts:
                success &= orchestrator.run_script(process)
            else:
                orchestrator.logger.error(f"Proceso desconocido: {process}")
                success = False
        
        # Generar reportes finales
        orchestrator.generate_client_portal_summary()
        orchestrator.save_process_log()
    
    # Salir con código apropiado
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()