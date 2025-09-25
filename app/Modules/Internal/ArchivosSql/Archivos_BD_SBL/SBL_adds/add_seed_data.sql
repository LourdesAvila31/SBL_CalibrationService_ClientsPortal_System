USE iso17025;
SET FOREIGN_KEY_CHECKS = 0;

-- Datos base para el sistema ISO 17025 - SBL Pharma
-- Este archivo contiene los datos esenciales para el funcionamiento del sistema
-- Basado en la estructura definida en add_tables.sql con SBL Pharma como empresa principal (ID=1)

START TRANSACTION;

-- =============================================================================
-- EMPRESA PRINCIPAL - SBL PHARMA (ID=1)
-- =============================================================================
INSERT INTO empresas (id, nombre, contacto, direccion, telefono, email, activo, fecha_creacion) VALUES
(1, 'SBL Pharma', 'Administrador General', 'Dirección Corporativa SBL Pharma', '555-0123', 'contacto@sblpharma.com', 1, NOW())
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    contacto = VALUES(contacto),
    direccion = VALUES(direccion),
    telefono = VALUES(telefono),
    email = VALUES(email),
    activo = VALUES(activo);

-- =============================================================================
-- ROLES BASE DEL SISTEMA
-- =============================================================================
-- Roles globales (sin empresa_id, para el portal interno)
INSERT INTO roles (id, nombre, empresa_id, delegated, portal_id) VALUES
(1, 'Superadministrador', NULL, 0, 1),
(2, 'Administrador', NULL, 0, 1),
(3, 'Supervisor', NULL, 0, 1),
(4, 'Analista', NULL, 0, 1),
(5, 'Lector', NULL, 0, 1),
(6, 'Developer', NULL, 0, 1),
(7, 'Sistemas', NULL, 0, 1)
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    portal_id = VALUES(portal_id);

-- Roles específicos de SBL Pharma (empresa_id = 1)
INSERT INTO roles (nombre, empresa_id, delegated, portal_id) VALUES
('Administrador SBL', 1, 0, 1),
('Supervisor SBL', 1, 0, 1),
('Analista SBL', 1, 0, 1),
('Técnico SBL', 1, 0, 1)
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre);

-- =============================================================================
-- PERMISOS BASE DEL SISTEMA
-- =============================================================================
INSERT INTO permissions (id, nombre, descripcion) VALUES
(1, 'admin_usuarios', 'Administrar usuarios del sistema'),
(2, 'admin_roles', 'Administrar roles y permisos'),
(3, 'admin_empresas', 'Administrar empresas'),
(4, 'admin_instrumentos', 'Administrar instrumentos'),
(5, 'admin_calibraciones', 'Administrar calibraciones'),
(6, 'admin_certificados', 'Administrar certificados'),
(7, 'admin_reportes', 'Administrar reportes'),
(8, 'admin_configuracion', 'Administrar configuración del sistema'),
(9, 'ver_dashboard', 'Ver panel de control'),
(10, 'ver_instrumentos', 'Ver listado de instrumentos'),
(11, 'ver_calibraciones', 'Ver listado de calibraciones'),
(12, 'ver_certificados', 'Ver certificados'),
(13, 'ver_reportes', 'Ver reportes'),
(14, 'crear_instrumentos', 'Crear nuevos instrumentos'),
(15, 'editar_instrumentos', 'Editar instrumentos existentes'),
(16, 'eliminar_instrumentos', 'Eliminar instrumentos'),
(17, 'crear_calibraciones', 'Crear nuevas calibraciones'),
(18, 'editar_calibraciones', 'Editar calibraciones'),
(19, 'eliminar_calibraciones', 'Eliminar calibraciones'),
(20, 'exportar_datos', 'Exportar datos del sistema'),
(21, 'admin_departamentos', 'Administrar departamentos'),
(22, 'admin_marcas', 'Administrar marcas'),
(23, 'admin_modelos', 'Administrar modelos'),
(24, 'admin_catalogos', 'Administrar catálogos')
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    descripcion = VALUES(descripcion);

-- =============================================================================
-- ASIGNACIÓN DE PERMISOS A ROLES GLOBALES
-- =============================================================================
-- Superadministrador - Todos los permisos
INSERT INTO role_permissions (role_id, permission_id) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (1, 10),
(1, 11), (1, 12), (1, 13), (1, 14), (1, 15), (1, 16), (1, 17), (1, 18), (1, 19), (1, 20),
(1, 21), (1, 22), (1, 23), (1, 24)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Administrador - Permisos operativos completos
INSERT INTO role_permissions (role_id, permission_id) VALUES
(2, 1), (2, 4), (2, 5), (2, 6), (2, 7), (2, 9), (2, 10), (2, 11), (2, 12), (2, 13),
(2, 14), (2, 15), (2, 16), (2, 17), (2, 18), (2, 19), (2, 20), (2, 21), (2, 22), (2, 23)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Supervisor - Permisos de supervisión
INSERT INTO role_permissions (role_id, permission_id) VALUES
(3, 5), (3, 6), (3, 7), (3, 9), (3, 10), (3, 11), (3, 12), (3, 13),
(3, 15), (3, 18), (3, 20)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Analista - Permisos operativos básicos
INSERT INTO role_permissions (role_id, permission_id) VALUES
(4, 9), (4, 10), (4, 11), (4, 12), (4, 13), (4, 14), (4, 17)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Lector - Solo permisos de lectura
INSERT INTO role_permissions (role_id, permission_id) VALUES
(5, 9), (5, 10), (5, 11), (5, 12), (5, 13)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- =============================================================================
-- DEPARTAMENTOS DE SBL PHARMA
-- =============================================================================
INSERT INTO departamentos (id, nombre, empresa_id) VALUES
(1, 'Almacén', 1),
(2, 'Ambiental', 1),
(3, 'Control de Calidad', 1),
(4, 'Desarrollo Analítico', 1),
(5, 'Desarrollo Farmacéutico', 1),
(6, 'Fabricación', 1),
(7, 'Gestión de la Calidad', 1),
(8, 'Logística', 1),
(9, 'Mantenimiento', 1),
(10, 'Microbiología', 1),
(11, 'Validación', 1),
(12, 'NA', 1)
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    empresa_id = VALUES(empresa_id);

-- =============================================================================
-- CATÁLOGO DE INSTRUMENTOS DE SBL PHARMA
-- =============================================================================
INSERT INTO catalogo_instrumentos (id, nombre, empresa_id) VALUES
(1, 'Aerotest', 1),
(2, 'Agitador magnético', 1),
(3, 'Agitador propela', 1),
(4, 'Agitador Vórtex', 1),
(5, 'Amperímetro', 1),
(6, 'Anemómetro', 1),
(7, 'Balanza analítica', 1),
(8, 'Balanza electrónica', 1),
(9, 'Balanza semi-microanalítica', 1),
(10, 'Balómetro', 1),
(11, 'Baño de agua', 1),
(12, 'Báscula digital', 1),
(13, 'Bomba peristáltica', 1),
(14, 'Calefactor', 1),
(15, 'Contador de partículas', 1),
(16, 'Controlador de temperatura de formado', 1),
(17, 'Controlador de temperatura de sellado', 1),
(18, 'Criotermostato de recirculación', 1),
(19, 'Cronómetro', 1),
(20, 'Densímetro', 1),
(21, 'Durómetro', 1),
(22, 'Electrodo', 1),
(23, 'Filtro HEPA 24*26*5 7/8', 1),
(24, 'Filtro HEPA 510 x 510 x 305 MM', 1),
(25, 'Filtro HEPA 24" x 24" x 11.5" inyección', 1),
(26, 'Filtro HEPA 24" x 24" x 11.5" extracción', 1),
(27, 'Fusiómetro', 1),
(28, 'Humidostato', 1),
(29, 'Lámpara UV', 1),
(30, 'Lápiz de humo', 1),
(31, 'Luxómetro', 1),
(32, 'Manómetro', 1),
(33, 'Manómetro de presión', 1),
(34, 'Manómetro de vacío', 1),
(35, 'Manómetro diferencial', 1),
(36, 'Manómetro Diferencial Digital', 1),
(37, 'Marco de masas', 1),
(38, 'Masa', 1),
(39, 'Masa 1 mg', 1),
(40, 'Medidor multiparamétrico', 1),
(41, 'Micrómetro', 1),
(42, 'Micropipeta', 1),
(43, 'Microscopio', 1),
(44, 'Muestreador de aire', 1),
(45, 'Multímetro', 1),
(46, 'Multiparamétrico', 1),
(47, 'Nivel', 1),
(48, 'Parrilla con agitador', 1),
(49, 'Parrilla eléctrica', 1),
(50, 'Pesa 5Kg', 1),
(51, 'Pesa 10Kg', 1),
(52, 'Pesa 25Kg', 1),
(53, 'Pesa 50Kg', 1),
(54, 'Pistola de recubrimiento', 1),
(55, 'Punzón 6 mm', 1),
(56, 'Punzón 8 mm', 1),
(57, 'Punzón 10 mm', 1),
(58, 'Punzón 11.1 mm x 5.5 mm', 1),
(59, 'Purificador de agua', 1),
(60, 'Registrador de temperatura', 1),
(61, 'Registrador de temperatura y humedad', 1),
(62, 'Inductor de calor', 1),
(63, 'Sensor de presión', 1),
(64, 'Sensor de punto de rocío', 1),
(65, 'Sensor de temperatura', 1),
(66, 'Sistema de filtros de agua', 1),
(67, 'Sistema de filtros de UMAs', 1),
(68, 'Sistema de filtros', 1),
(69, 'Sistema de filtros de aire comprimido', 1),
(70, 'Sonicador', 1),
(71, 'Tacómetro', 1),
(72, 'Tambor', 1),
(73, 'Temporizador', 1),
(74, 'Termobalanza', 1),
(75, 'Termohigrómetro', 1),
(76, 'Termómetro', 1),
(77, 'Termómetro digital', 1),
(78, 'Termómetro IR', 1),
(79, 'Termopar', 1),
(80, 'Termostato programable', 1),
(81, 'Transmisor de temperatura y humedad', 1),
(82, 'Vernier', 1),
(83, 'Viscosímetro', 1)
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    empresa_id = VALUES(empresa_id);

-- =============================================================================
-- MARCAS DE INSTRUMENTOS DE SBL PHARMA
-- =============================================================================
INSERT INTO marcas (id, nombre, empresa_id) VALUES
(1, 'Dräger', 1),
(2, 'Corning', 1),
(3, 'VWR', 1),
(4, 'Heidolph', 1),
(5, 'ND', 1),
(6, 'Thomas Scientific', 1),
(7, 'Scientific Industries', 1),
(8, 'Fluke', 1),
(9, 'Extech Instruments / Extech Instruments', 1),
(10, 'CAS', 1),
(11, 'Ohaus', 1),
(12, 'AND', 1),
(13, 'ADAM', 1),
(14, 'Alnor TSI Incorporated / Alnor', 1),
(15, 'Watson Marlow', 1),
(16, 'NA', 1),
(17, 'Lighthouse', 1),
(18, 'OMRON', 1),
(19, 'Julabo', 1),
(20, 'Control Company', 1),
(21, 'Fisher Scientific', 1),
(22, 'General Tools', 1),
(23, 'Hanhart', 1),
(24, 'Pharma Alliance', 1),
(25, 'Dillon', 1),
(26, 'ELECTROLAB', 1),
(27, 'HARTEK', 1),
(28, 'HANNA Instruments', 1),
(29, 'Mettler Toledo', 1),
(30, 'OAKTON', 1),
(31, 'Thermo Eutech', 1),
(32, 'VECOFLOW', 1),
(33, 'Freudenberg', 1),
(34, 'Greenfilt', 1),
(35, 'Thermo Scientific', 1),
(36, 'Honeywell', 1),
(37, 'Analytik Jena US', 1),
(38, 'Entela', 1),
(39, 'Chimney Balloon', 1),
(40, 'HIOKI', 1),
(41, 'USG', 1),
(42, 'C.A. Norgreen Co.', 1),
(43, 'Ashcroft', 1),
(44, 'MC', 1),
(45, 'Winters', 1),
(46, 'SMC / SMC Pneumatics', 1),
(47, 'TRU-FLATE', 1),
(48, 'Tube & Socket', 1),
(49, 'Wika', 1),
(50, 'PRO-BLOCK', 1),
(51, 'Puroflo', 1),
(52, 'Dwyer Instruments / Dwyer', 1),
(53, 'SENSOCON', 1),
(54, 'Dewit / DEWIT', 1),
(55, 'aquex', 1),
(56, 'ENERPAC', 1),
(57, 'MASS', 1),
(58, 'SNS', 1),
(59, 'Power Team', 1),
(60, 'Instrutek', 1),
(61, 'Manometer', 1),
(62, 'Huayi', 1),
(63, 'TROEMNER', 1),
(64, 'SUMIMET', 1),
(65, 'Mitutoyo', 1),
(66, 'Eppendorf', 1),
(67, 'Brand', 1),
(68, 'LABOMED', 1),
(69, 'TRIO.BAS', 1),
(70, 'Truper', 1),
(71, 'HACH / HATCH', 1),
(72, 'SPI-TRONIC', 1),
(73, 'DLAB', 1),
(74, 'Rice Lake', 1),
(75, 'Binks', 1),
(76, 'ELGA Purelab', 1),
(77, 'Graphtec', 1),
(78, 'Omega', 1),
(79, 'Amprobe', 1),
(80, 'CEM', 1),
(81, 'Ampere', 1),
(82, 'Lascar Electronics', 1),
(83, 'JORESTECH', 1),
(84, 'CS Instruments', 1),
(85, 'Sensewell', 1),
(86, 'Novus', 1),
(87, 'ASTROCEL I', 1),
(88, 'Festo', 1),
(89, 'Tianyu', 1),
(90, 'JAJA', 1),
(91, 'Branson', 1),
(92, 'Autonics', 1),
(93, 'Sper Scientific', 1),
(94, 'HUATO', 1),
(95, 'Alla France', 1),
(96, 'STB-22', 1),
(97, 'Nachtman', 1),
(98, 'Fungilab', 1),
(99, 'Neiko', 1),
(100, 'Carrier', 1)
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    empresa_id = VALUES(empresa_id);

-- =============================================================================
-- MODELOS PRINCIPALES DE SBL PHARMA
-- =============================================================================
INSERT INTO modelos (id, nombre, marca_id, empresa_id) VALUES
(1, 'NA', 16, 1),
(2, 'ND', 5, 1),
(3, 'Alpha', NULL, 1),
(4, 'PC-410', NULL, 1),
(5, 'RZR2020', NULL, 1),
(6, 'Digital Genie2 120V', NULL, 1),
(7, 'LSE', NULL, 1),
(8, 'Digital Vortex-Genie 2', 7, 1),
(9, '323', 8, 1),
(10, '325', 8, 1),
(11, 'Traceable Hot Wire', 20, 1),
(12, 'Field Master', 14, 1),
(13, 'XB420HW', 9, 1),
(14, 'Adventurer PRO', 11, 1),
(15, 'AX124', 11, 1),
(16, 'PA224', 11, 1),
(17, 'PX224/E', 11, 1),
(18, 'HR-120', 12, 1),
(19, 'ABL 225', 12, 1),
(20, 'AX223', 11, 1),
(21, 'Scout Pro SP601', 11, 1),
(22, 'SPX222', 11, 1),
(23, 'GH-202', 10, 1),
(24, 'PX225D', 11, 1),
(25, 'Loflo 6200D', 15, 1),
(26, '6200', 15, 1),
(27, 'SOLAIR 3100+', 17, 1),
(28, 'S3100', 17, 1),
(29, 'CORIO CD-201F', 19, 1),
(30, 'Halo HI11102', 28, 1),
(31, 'Research plus 100-1000 μL', 66, 1),
(32, '0.5-5 mL', 66, 1),
(33, '100-1000 μL', 66, 1),
(34, 'Pocket Pro+Multi2', 30, 1),
(35, '76mm', NULL, 1),
(36, 'Traceable', 20, 1),
(37, 'AW-20', NULL, 1),
(38, 'AFR 2000', NULL, 1),
(39, 'MS9-LWS-G-U-V', NULL, 1),
(40, 'MS6-LF-1/2-CRV', NULL, 1)
ON DUPLICATE KEY UPDATE 
    nombre = VALUES(nombre),
    marca_id = VALUES(marca_id),
    empresa_id = VALUES(empresa_id);

COMMIT;

SET FOREIGN_KEY_CHECKS = 1;

-- =============================================================================
-- DEPARTAMENTOS
-- =============================================================================
INSERT INTO departamentos (id, nombre) VALUES
(1, 'Almacén'),
(2, 'Ambiental'),
(3, 'Control de Calidad'),
(4, 'Desarrollo Analítico'),
(5, 'Desarrollo Farmacéutico'),
(6, 'Fabricación'),
(7, 'Gestión de la Calidad'),
(8, 'Logística'),
(9, 'Mantenimiento'),
(10, 'Microbiología'),
(11, 'Validación'),
(12, 'NA')
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);

-- =============================================================================
-- CATÁLOGO DE INSTRUMENTOS
-- =============================================================================
INSERT INTO catalogo_instrumentos (id, nombre) VALUES
(1, 'Aerotest'),
(2, 'Agitador magnético'),
(3, 'Agitador propela'),
(4, 'Agitador Vórtex'),
(5, 'Amperímetro'),
(6, 'Anemómetro'),
(7, 'Balanza analítica'),
(8, 'Balanza electrónica'),
(9, 'Balanza semi-microanalítica'),
(10, 'Balómetro'),
(11, 'Baño de agua'),
(12, 'Báscula digital'),
(13, 'Bomba peristáltica'),
(14, 'Calefactor'),
(15, 'Contador de partículas'),
(16, 'Controlador de temperatura de formado'),
(17, 'Controlador de temperatura de sellado'),
(18, 'Criotermostato de recirculación'),
(19, 'Cronómetro'),
(20, 'Densímetro'),
(21, 'Durómetro'),
(22, 'Electrodo'),
(23, 'Filtro HEPA 24*26*5 7/8'),
(24, 'Filtro HEPA 510 x 510 x 305 MM'),
(25, 'Filtro HEPA 24" x 24" x 11.5" inyección'),
(26, 'Filtro HEPA 24" x 24" x 11.5" extracción'),
(27, 'Fusiómetro'),
(28, 'Humidostato'),
(29, 'Lámpara UV'),
(30, 'Lápiz de humo'),
(31, 'Luxómetro'),
(32, 'Manómetro'),
(33, 'Manómetro de presión'),
(34, 'Manómetro de vacío'),
(35, 'Manómetro diferencial'),
(36, 'Manómetro Diferencial Digital'),
(37, 'Marco de masas'),
(38, 'Masa'),
(39, 'Masa 1 mg'),
(40, 'Medidor multiparamétrico'),
(41, 'Micrómetro'),
(42, 'Micropipeta'),
(43, 'Microscopio'),
(44, 'Muestreador de aire'),
(45, 'Multímetro'),
(46, 'Multiparamétrico'),
(47, 'Nivel'),
(48, 'Parrilla con agitador'),
(49, 'Parrilla eléctrica'),
(50, 'Pesa 5Kg'),
(51, 'Pesa 10Kg'),
(52, 'Pesa 25Kg'),
(53, 'Pesa 50Kg'),
(54, 'Pistola de recubrimiento'),
(55, 'Punzón 6 mm'),
(56, 'Punzón 8 mm'),
(57, 'Punzón 10 mm'),
(58, 'Punzón 11.1 mm x 5.5 mm'),
(59, 'Purificador de agua'),
(60, 'Registrador de temperatura'),
(61, 'Registrador de temperatura y humedad'),
(62, 'Inductor de calor'),
(63, 'Sensor de presión'),
(64, 'Sensor de punto de rocío'),
(65, 'Sensor de temperatura'),
(66, 'Sistema de filtros de agua'),
(67, 'Sistema de filtros de UMAs'),
(68, 'Sistema de filtros'),
(69, 'Sistema de filtros de aire comprimido'),
(70, 'Sonicador'),
(71, 'Tacómetro'),
(72, 'Tambor'),
(73, 'Temporizador'),
(74, 'Termobalanza'),
(75, 'Termohigrómetro'),
(76, 'Termómetro'),
(77, 'Termómetro digital'),
(78, 'Termómetro IR'),
(79, 'Termopar'),
(80, 'Termostato programable'),
(81, 'Transmisor de temperatura y humedad'),
(82, 'Vernier'),
(83, 'Viscosímetro')
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);

-- =============================================================================
-- MARCAS DE INSTRUMENTOS
-- =============================================================================
INSERT INTO marcas (id, nombre) VALUES
(1, 'Dräger'),
(2, 'Corning'),
(3, 'VWR'),
(4, 'Heidolph'),
(5, 'ND'),
(6, 'Thomas Scientific'),
(7, 'Scientific Industries'),
(8, 'Fluke'),
(9, 'Extech Instruments / Extech Instruments'),
(10, 'CAS'),
(11, 'Ohaus'),
(12, 'AND'),
(13, 'ADAM'),
(14, 'Alnor TSI Incorporated / Alnor'),
(15, 'Watson Marlow'),
(16, 'NA'),
(17, 'Lighthouse'),
(18, 'OMRON'),
(19, 'Julabo'),
(20, 'Control Company'),
(21, 'Fisher Scientific'),
(22, 'General Tools'),
(23, 'Hanhart'),
(24, 'Pharma Alliance'),
(25, 'Dillon'),
(26, 'ELECTROLAB'),
(27, 'HARTEK'),
(28, 'HANNA Instruments'),
(29, 'Mettler Toledo'),
(30, 'OAKTON'),
(31, 'Thermo Eutech'),
(32, 'VECOFLOW'),
(33, 'Freudenberg'),
(34, 'Greenfilt'),
(35, 'Thermo Scientific'),
(36, 'Honeywell'),
(37, 'Analytik Jena US'),
(38, 'Entela'),
(39, 'Chimney Balloon'),
(40, 'HIOKI'),
(41, 'USG'),
(42, 'C.A. Norgreen Co.'),
(43, 'Ashcroft'),
(44, 'MC'),
(45, 'Winters'),
(46, 'SMC / SMC Pneumatics'),
(47, 'TRU-FLATE'),
(48, 'Tube & Socket'),
(49, 'Wika'),
(50, 'PRO-BLOCK'),
(51, 'Puroflo'),
(52, 'Dwyer Instruments / Dwyer'),
(53, 'SENSOCON'),
(54, 'Dewit / DEWIT'),
(55, 'aquex'),
(56, 'ENERPAC'),
(57, 'MASS'),
(58, 'SNS'),
(59, 'Power Team'),
(60, 'Instrutek'),
(61, 'Manometer'),
(62, 'Huayi'),
(63, 'TROEMNER'),
(64, 'SUMIMET'),
(65, 'Mitutoyo'),
(66, 'Eppendorf'),
(67, 'Brand'),
(68, 'LABOMED'),
(69, 'TRIO.BAS'),
(70, 'Truper'),
(71, 'HACH / HATCH'),
(72, 'SPI-TRONIC'),
(73, 'DLAB'),
(74, 'Rice Lake'),
(75, 'Binks'),
(76, 'ELGA Purelab'),
(77, 'Graphtec'),
(78, 'Omega'),
(79, 'Amprobe'),
(80, 'CEM'),
(81, 'Ampere'),
(82, 'Lascar Electronics'),
(83, 'JORESTECH'),
(84, 'CS Instruments'),
(85, 'Sensewell'),
(86, 'Novus'),
(87, 'ASTROCEL I'),
(88, 'Festo'),
(89, 'Tianyu'),
(90, 'JAJA'),
(91, 'Branson'),
(92, 'Autonics'),
(93, 'Sper Scientific'),
(94, 'HUATO'),
(95, 'Alla France'),
(96, 'STB-22'),
(97, 'Nachtman'),
(98, 'Fungilab'),
(99, 'Neiko'),
(100, 'Carrier')
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);

-- =============================================================================
-- MODELOS DE INSTRUMENTOS (SELECCIÓN PRINCIPAL)
-- =============================================================================
INSERT INTO modelos (id, nombre, marca_id) VALUES
(1, 'NA', 16),
(2, 'ND', 5),
(3, 'Alpha', NULL),
(4, 'PC-410', NULL),
(5, 'RZR2020', NULL),
(6, 'Digital Genie2 120V', NULL),
(7, 'LSE', NULL),
(8, 'Digital Vortex-Genie 2', NULL),
(9, '323', NULL),
(10, '325', NULL),
(11, 'Traceable Hot Wire', NULL),
(12, 'Field Master', NULL),
(13, 'XB420HW', NULL),
(14, 'Adventurer PRO', 11),
(15, 'AX124', 11),
(16, 'PA224', 11),
(17, 'PX224/E', 11),
(18, 'HR-120', 12),
(19, 'ABL 225', 12),
(20, 'AX223', 11),
(21, 'Scout Pro SP601', 11),
(22, 'SPX222', 11),
(23, 'GH-202', 10),
(24, 'PX225D', 11),
(25, 'Loflo 6200D', 15),
(26, '6200', 15),
(27, '5L', NULL),
(28, 'EC-II', NULL),
(29, 'DBB Jr.', NULL),
(30, 'RC301P30', NULL),
(31, 'HD', NULL),
(32, 'Ranger 3000', 11),
(33, 'RC31P30', NULL),
(34, 'XE-6000', NULL),
(35, 'SOLAIR 3100+', 17),
(36, 'S3100', 17),
(37, 'CORIO CD-201F', 19),
(38, '1235D46', NULL),
(39, '365510', NULL),
(40, '5017', NULL),
(41, '5008', NULL),
(42, 'SW888L', NULL),
(43, 'Fingertip', NULL),
(44, 'Compact 2', NULL),
(45, 'TD-12', NULL),
(46, 'MHT-100', NULL),
(47, 'ETB-2PRL', NULL),
(48, 'PHT-500P', NULL),
(49, 'Halo HI11102', 28),
(50, 'perfectION comb F- Lemo', NULL),
(51, 'Research plus 100-1000 μL', 66),
(52, '0.5-5 mL', 66),
(53, '100-1000 μL', 66),
(54, 'Pocket Pro+Multi2', NULL),
(55, '76mm', NULL),
(56, 'Traceable', 20),
(57, 'AW-20', NULL),
(58, 'AFR 2000', NULL),
(59, 'MS9-LWS-G-U-V', NULL),
(60, 'MS6-LF-1/2-CRV', NULL)
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);

-- =============================================================================
-- PERMISOS BASE DEL SISTEMA
-- =============================================================================
INSERT INTO permisos (id, nombre, descripcion, activo) VALUES
(1, 'admin_usuarios', 'Administrar usuarios del sistema', 1),
(2, 'admin_roles', 'Administrar roles y permisos', 1),
(3, 'admin_empresas', 'Administrar empresas', 1),
(4, 'admin_instrumentos', 'Administrar instrumentos', 1),
(5, 'admin_calibraciones', 'Administrar calibraciones', 1),
(6, 'admin_certificados', 'Administrar certificados', 1),
(7, 'admin_reportes', 'Administrar reportes', 1),
(8, 'admin_configuracion', 'Administrar configuración del sistema', 1),
(9, 'ver_dashboard', 'Ver panel de control', 1),
(10, 'ver_instrumentos', 'Ver listado de instrumentos', 1),
(11, 'ver_calibraciones', 'Ver listado de calibraciones', 1),
(12, 'ver_certificados', 'Ver certificados', 1),
(13, 'ver_reportes', 'Ver reportes', 1),
(14, 'crear_instrumentos', 'Crear nuevos instrumentos', 1),
(15, 'editar_instrumentos', 'Editar instrumentos existentes', 1),
(16, 'eliminar_instrumentos', 'Eliminar instrumentos', 1),
(17, 'crear_calibraciones', 'Crear nuevas calibraciones', 1),
(18, 'editar_calibraciones', 'Editar calibraciones', 1),
(19, 'eliminar_calibraciones', 'Eliminar calibraciones', 1),
(20, 'exportar_datos', 'Exportar datos del sistema', 1)
ON DUPLICATE KEY UPDATE nombre = VALUES(nombre);

-- =============================================================================
-- ASIGNACIÓN DE PERMISOS A ROLES GLOBALES
-- =============================================================================
-- Superadministrador - Todos los permisos
INSERT INTO role_permissions (role_id, permission_id) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (1, 10),
(1, 11), (1, 12), (1, 13), (1, 14), (1, 15), (1, 16), (1, 17), (1, 18), (1, 19), (1, 20)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Administrador - Permisos operativos completos
INSERT INTO role_permissions (role_id, permission_id) VALUES
(2, 1), (2, 4), (2, 5), (2, 6), (2, 7), (2, 9), (2, 10), (2, 11), (2, 12), (2, 13),
(2, 14), (2, 15), (2, 16), (2, 17), (2, 18), (2, 19), (2, 20)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Supervisor - Permisos de supervisión
INSERT INTO role_permissions (role_id, permission_id) VALUES
(3, 5), (3, 6), (3, 7), (3, 9), (3, 10), (3, 11), (3, 12), (3, 13),
(3, 15), (3, 18), (3, 20)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Analista - Permisos operativos básicos
INSERT INTO role_permissions (role_id, permission_id) VALUES
(4, 9), (4, 10), (4, 11), (4, 12), (4, 13), (4, 14), (4, 17)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

-- Lector - Solo permisos de lectura
INSERT INTO role_permissions (role_id, permission_id) VALUES
(5, 9), (5, 10), (5, 11), (5, 12), (5, 13)
ON DUPLICATE KEY UPDATE role_id = VALUES(role_id);

COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
