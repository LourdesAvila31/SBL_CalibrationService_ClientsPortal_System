# Glosario del sistema

Este documento explica vocablos en inglés o de significado técnico usados en el código del sistema para facilitar su comprensión al equipo de desarrollo en español.

| Término | Dónde aparece con mayor frecuencia | Significado dentro del proyecto |
| --- | --- | --- |
| **public** | Carpeta raíz `public/` y rutas como `/public/apps/...` | Directorio expuesto por el servidor web; contiene los recursos accesibles desde el navegador (HTML, CSS, JS, imágenes). |
| **app** | Carpeta raíz `app/` | Código fuente PHP del backend (núcleo, módulos, controladores, helpers, validaciones). |
| **Core** | `app/Core/` | Conjunto de clases base compartidas por los módulos (por ejemplo `Controller`, `Model`, `Router`). |
| **Modules** | `app/Modules/` | Módulos funcionales del backend (empresas, usuarios, catálogos, etc.). Cada módulo agrupa sus propios controladores, modelos y vistas. |
| **Controller** | Clases bajo `app/Core/Controllers/` y `app/Modules/*/Controllers/` | Controlador HTTP: recibe peticiones, valida datos y orquesta la lógica del módulo. |
| **Model** | Clases bajo `app/Core/Models/` y `app/Modules/*/Models/` | Representación PHP de tablas de base de datos; encapsula consultas y reglas de negocio. |
| **Helper** | Archivos en `app/Core/Helpers/` y `app/Modules/*/Helpers/` | Funciones auxiliares reutilizables (formato de datos, generación de URLs, etc.). |
| **Middleware** | `app/Core/Middleware/` | Filtros que se ejecutan antes o después de un controlador para validar autenticación, permisos o transformar peticiones. |
| **Seed / Seeder** | Scripts en `app/Modules/*/Seeds/` | Datos precargados que inicializan catálogos y roles (por ejemplo, roles "Responsable" y "Consulta" para clientes). |
| **Tenant** | Tablas `tenant_*` en base de datos y módulos relacionados | Concepto de "empresa" cliente que usa el sistema; agrupa usuarios, roles y configuraciones particulares. |
| **Portal interno (`internal`)** | Rutas bajo `public/apps/internal/` | Interfaz usada por el personal de SBL para operar y configurar el sistema. |
| **Portal de clientes (`tenant`)** | Rutas bajo `public/apps/tenant/` | Interfaz que usan las empresas clientes para gestionar sus propios datos y usuarios. |
| **Portal de servicios (`service`)** | Rutas bajo `public/apps/service/` | Interfaz dedicada a operaciones de campo y seguimiento de calibraciones. |
| **Assets** | Subcarpeta `public/assets/` | Recursos estáticos compartidos (hojas de estilo, scripts comunes, imágenes). |
| **Storage** | Carpeta `storage/` | Archivos generados por la aplicación (logs, adjuntos, temporales). No es público para el navegador. |
| **Validation rules** | Archivo `app/validation_rules.md` y clases relacionadas | Documentación y clases que definen las reglas de validación de formularios. |
| **Tests** | Carpeta `tests/` | Suites automatizadas para validar funcionalidades del backend y del portal. |
| **Tools** | Carpeta `tools/` | Scripts auxiliares de mantenimiento, migraciones y utilidades para desarrollo. |

> Para sugerir nuevas entradas o correcciones, edita este archivo y describe brevemente el contexto en el que aparece la palabra a documentar.
