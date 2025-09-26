#!/usr/bin/env php
<?php
/**
 * Script de Verificación de Homologación de Diseño
 * Sistema SBL Pharma - ISO 17025/NOM-059
 * 
 * Este script verifica que todos los archivos HTML del sistema
 * cumplan con los estándares de diseño homologado.
 */

class DesignHomologationChecker {
    
    private $baseDir;
    private $errors = [];
    private $warnings = [];
    private $checkedFiles = 0;
    
    // Patrones requeridos para homologación
    private $requiredPatterns = [
        'bootstrap' => 'bootstrap@5\.3\.0',
        'fontawesome' => 'font-awesome.*6\.4\.',
        'montserrat' => 'Montserrat',
        'base_url_script' => 'BASE_URL.*=',
        'master_theme' => 'assets/styles/.*\.css'
    ];
    
    // Colores deprecados que deben ser reemplazados
    private $deprecatedColors = [
        '#667eea' => '--sbl-primary',
        '#764ba2' => '--sbl-secondary', 
        '#1a1a2e' => '--sbl-primary',
        '#16213e' => '--sbl-secondary',
        '#dc3545.*#6f42c1' => '--sbl-primary.*--sbl-secondary'
    ];
    
    public function __construct($baseDir) {
        $this->baseDir = rtrim($baseDir, '/');
    }
    
    public function checkDirectory($dir = '') {
        $fullDir = $this->baseDir . ($dir ? '/' . $dir : '');
        
        if (!is_dir($fullDir)) {
            $this->errors[] = "Directorio no encontrado: $fullDir";
            return;
        }
        
        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($fullDir)
        );
        
        foreach ($iterator as $file) {
            if ($file->getExtension() === 'html') {
                $this->checkFile($file->getPathname());
            }
        }
    }
    
    private function checkFile($filePath) {
        $this->checkedFiles++;
        $content = file_get_contents($filePath);
        $relativePath = str_replace($this->baseDir . '/', '', $filePath);
        
        // Verificar patrones requeridos
        $this->checkRequiredPatterns($content, $relativePath);
        
        // Verificar colores deprecados
        $this->checkDeprecatedColors($content, $relativePath);
        
        // Verificar estructura de sidebar
        $this->checkSidebarStructure($content, $relativePath);
        
        // Verificar responsive design
        $this->checkResponsiveDesign($content, $relativePath);
    }
    
    private function checkRequiredPatterns($content, $filePath) {
        foreach ($this->requiredPatterns as $name => $pattern) {
            if (!preg_match("/$pattern/i", $content)) {
                $this->warnings[] = "[$filePath] Falta patrón requerido: $name";
            }
        }
    }
    
    private function checkDeprecatedColors($content, $filePath) {
        foreach ($this->deprecatedColors as $deprecated => $replacement) {
            if (preg_match("/$deprecated/i", $content)) {
                $this->errors[] = "[$filePath] Color deprecado encontrado: $deprecated, usar: $replacement";
            }
        }
    }
    
    private function checkSidebarStructure($content, $filePath) {
        // Verificar que el sidebar tenga la estructura correcta
        if (strpos($content, 'sidebar') !== false) {
            if (!preg_match('/width:\s*200px|--sbl-sidebar-width/', $content)) {
                $this->warnings[] = "[$filePath] Sidebar no usa ancho estándar (200px)";
            }
            
            if (!preg_match('/#0d575a|--sbl-primary/', $content)) {
                $this->errors[] = "[$filePath] Sidebar no usa color primario estándar";
            }
        }
    }
    
    private function checkResponsiveDesign($content, $filePath) {
        if (strpos($content, 'sidebar') !== false) {
            if (!preg_match('/@media.*768px/', $content)) {
                $this->warnings[] = "[$filePath] Falta breakpoint responsive para móviles";
            }
        }
    }
    
    public function generateReport() {
        echo "\n=== REPORTE DE HOMOLOGACIÓN DE DISEÑO ===\n";
        echo "Sistema SBL Pharma - ISO 17025/NOM-059\n";
        echo "Archivos verificados: {$this->checkedFiles}\n\n";
        
        if (count($this->errors) > 0) {
            echo "🔴 ERRORES CRÍTICOS (" . count($this->errors) . "):\n";
            foreach ($this->errors as $error) {
                echo "  - $error\n";
            }
            echo "\n";
        }
        
        if (count($this->warnings) > 0) {
            echo "🟡 ADVERTENCIAS (" . count($this->warnings) . "):\n";
            foreach ($this->warnings as $warning) {
                echo "  - $warning\n";
            }
            echo "\n";
        }
        
        if (count($this->errors) === 0 && count($this->warnings) === 0) {
            echo "✅ TODOS LOS ARCHIVOS ESTÁN HOMOLOGADOS CORRECTAMENTE\n\n";
        }
        
        $this->generateRecommendations();
    }
    
    private function generateRecommendations() {
        echo "📋 RECOMENDACIONES:\n";
        echo "  1. Usar variables CSS (--sbl-*) en lugar de colores hardcodeados\n";
        echo "  2. Incluir master-theme.css en todos los archivos nuevos\n";
        echo "  3. Verificar responsive design en dispositivos móviles\n";
        echo "  4. Mantener consistencia en componentes (sidebar, cards, botones)\n";
        echo "  5. Seguir la guía en docs/GUIA_HOMOLOGACION_DISEÑO.md\n\n";
    }
    
    public function getScore() {
        $totalIssues = count($this->errors) + count($this->warnings);
        if ($totalIssues === 0) return 100;
        
        $score = max(0, 100 - ($totalIssues * 5));
        return $score;
    }
}

// Verificar argumentos de línea de comandos
$baseDir = $argv[1] ?? getcwd();

if (!is_dir($baseDir)) {
    echo "Error: Directorio no válido: $baseDir\n";
    echo "Uso: php verify_design_homologation.php [directorio]\n";
    exit(1);
}

echo "Iniciando verificación de homologación en: $baseDir\n";

$checker = new DesignHomologationChecker($baseDir);

// Verificar directorios principales
$checker->checkDirectory('public/apps');
$checker->checkDirectory('public');

// Si existe portal-clientes-servicio, verificarlo también
if (is_dir($baseDir . '/portal-clientes-servicio/public')) {
    $checker->checkDirectory('portal-clientes-servicio/public');
}

$checker->generateReport();

$score = $checker->getScore();
echo "📊 PUNTUACIÓN DE HOMOLOGACIÓN: $score/100\n";

if ($score >= 90) {
    echo "🌟 EXCELENTE - Sistema completamente homologado\n";
} elseif ($score >= 70) {
    echo "👍 BUENO - Pocas mejoras necesarias\n"; 
} elseif ($score >= 50) {
    echo "⚠️  REGULAR - Requiere atención\n";
} else {
    echo "❌ CRÍTICO - Requiere homologación urgente\n";
}

echo "\nVerificación completada.\n";
?>