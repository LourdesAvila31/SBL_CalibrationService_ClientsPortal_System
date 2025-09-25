#!/usr/bin/env python3
"""Configuración de entorno Python para el Sistema de Servicios a Clientes SBL.

Este script configura el entorno Python necesario para ejecutar todos los
scripts de automatización y procesamiento de datos del sistema SBL.

Características:
- Detecta automáticamente la versión de Python disponible
- Instala dependencias necesarias usando pip
- Configura las rutas de trabajo
- Valida la instalación de herramientas

Uso:
```bash
python setup_python_environment.py
```
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# Dependencias requeridas para el sistema
REQUIRED_PACKAGES = [
    "pandas>=1.5.0",
    "openpyxl>=3.0.0",  # Para manejo de archivos Excel
    "python-dateutil>=2.8.0",  # Para parseo de fechas
    "chardet>=4.0.0",  # Para detección de encoding
    "setuptools>=65.0.0",
    "wheel>=0.37.0",
]

# Dependencias opcionales para desarrollo
DEV_PACKAGES = [
    "pytest>=7.0.0",
    "black>=22.0.0",
    "flake8>=5.0.0",
    "mypy>=0.991",
    "pylint>=2.15.0",
]

class PythonEnvironmentSetup:
    """Configurador del entorno Python para SBL."""
    
    def __init__(self):
        self.python_exe = sys.executable
        self.repo_root = Path(__file__).resolve().parents[0]
        self.requirements_file = self.repo_root / "requirements.txt"
    
    def check_python_version(self) -> bool:
        """Verifica que la versión de Python sea compatible."""
        version = sys.version_info
        print(f"Python detectado: {version.major}.{version.minor}.{version.micro}")
        
        if version.major != 3 or version.minor < 8:
            print("❌ Error: Se requiere Python 3.8 o superior")
            return False
        
        print("✅ Versión de Python compatible")
        return True
    
    def install_packages(self, packages: List[str], dev: bool = False) -> bool:
        """Instala paquetes usando pip."""
        package_type = "desarrollo" if dev else "producción"
        print(f"\n📦 Instalando paquetes de {package_type}...")
        
        try:
            cmd = [self.python_exe, "-m", "pip", "install", "--upgrade"] + packages
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print("✅ Paquetes instalados correctamente")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Error instalando paquetes: {e}")
            print(f"Salida del error: {e.stderr}")
            return False
    
    def create_requirements_file(self) -> None:
        """Crea el archivo requirements.txt si no existe."""
        if not self.requirements_file.exists():
            print(f"\n📝 Creando {self.requirements_file}")
            content = "# Dependencias Python para Sistema SBL - Portal Clientes\n"
            content += "# Instalación: pip install -r requirements.txt\n\n"
            content += "\n".join(REQUIRED_PACKAGES)
            content += "\n\n# Dependencias de desarrollo (opcional)\n"
            content += "\n".join(f"# {pkg}" for pkg in DEV_PACKAGES)
            
            self.requirements_file.write_text(content, encoding="utf-8")
            print("✅ Archivo requirements.txt creado")
    
    def validate_installation(self) -> bool:
        """Valida que las dependencias estén correctamente instaladas."""
        print("\n🔍 Validando instalación...")
        
        test_imports = [
            ("pandas", "pd"),
            ("openpyxl", "openpyxl"),
            ("dateutil", "dateutil"),
            ("chardet", "chardet"),
        ]
        
        all_good = True
        for module, alias in test_imports:
            try:
                __import__(module)
                print(f"✅ {module}")
            except ImportError:
                print(f"❌ {module} no encontrado")
                all_good = False
        
        return all_good
    
    def setup_directories(self) -> None:
        """Crea directorios necesarios para el sistema."""
        print("\n📁 Configurando directorios...")
        
        directories = [
            self.repo_root / "storage" / "python_temp",
            self.repo_root / "storage" / "python_logs",
            self.repo_root / "tests" / "python",
            self.repo_root / "tools" / "scripts" / "__pycache__",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"✅ {directory.relative_to(self.repo_root)}")
    
    def run_setup(self, install_dev: bool = False) -> bool:
        """Ejecuta la configuración completa del entorno."""
        print("🚀 Configurando entorno Python para Sistema SBL - Portal Clientes\n")
        
        # Verificar versión de Python
        if not self.check_python_version():
            return False
        
        # Actualizar pip
        print("\n⬆️ Actualizando pip...")
        try:
            subprocess.run([
                self.python_exe, "-m", "pip", "install", "--upgrade", "pip"
            ], check=True, capture_output=True)
            print("✅ pip actualizado")
        except subprocess.CalledProcessError:
            print("⚠️ No se pudo actualizar pip, continuando...")
        
        # Instalar paquetes de producción
        if not self.install_packages(REQUIRED_PACKAGES):
            return False
        
        # Instalar paquetes de desarrollo si se solicita
        if install_dev:
            if not self.install_packages(DEV_PACKAGES, dev=True):
                print("⚠️ Error instalando paquetes de desarrollo, continuando...")
        
        # Crear archivos de configuración
        self.create_requirements_file()
        
        # Configurar directorios
        self.setup_directories()
        
        # Validar instalación
        if not self.validate_installation():
            print("\n❌ Algunos paquetes no se instalaron correctamente")
            return False
        
        print("\n🎉 ¡Entorno Python configurado exitosamente!")
        print(f"📍 Directorio raíz: {self.repo_root}")
        print(f"🐍 Python: {self.python_exe}")
        
        return True


def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Configura el entorno Python para el Sistema SBL - Portal Clientes"
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Instala también paquetes de desarrollo"
    )
    
    args = parser.parse_args()
    
    setup = PythonEnvironmentSetup()
    success = setup.run_setup(install_dev=args.dev)
    
    if not success:
        sys.exit(1)
    
    print("\n📚 Scripts disponibles:")
    print("  • tools/scripts/generate_cert_calibrations.py")
    print("  • tools/scripts/generate_insert_instrumentos.py")
    print("  • tools/scripts/generate_plan_riesgos.py")
    print("  • app/Modules/Internal/ArchivosSql/Normalize_Python/convert_*.py")


if __name__ == "__main__":
    main()