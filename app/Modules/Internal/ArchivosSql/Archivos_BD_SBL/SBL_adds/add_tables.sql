CREATE DATABASE IF NOT EXISTS iso17025 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE iso17025;

-- Este script puede ejecutarse tanto en instalaciones nuevas como en bases existentes.
-- Crea todas las tablas necesarias y alinea esquemas legados con la estructura multiempresa.
-- Script incremental para alinear bases existentes con la estructura multiempresa
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE IF NOT EXISTS portals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    slug VARCHAR(60) NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE portals
    ADD COLUMN IF NOT EXISTS slug VARCHAR(60) NOT NULL AFTER id,
    ADD COLUMN IF NOT EXISTS nombre VARCHAR(100) NOT NULL AFTER slug,
    ADD COLUMN IF NOT EXISTS descripcion VARCHAR(255) DEFAULT NULL AFTER nombre,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER descripcion;

ALTER TABLE portals
    MODIFY COLUMN slug VARCHAR(60) NOT NULL,
    MODIFY COLUMN nombre VARCHAR(100) NOT NULL;

SELECT COUNT(*) INTO @idx_portals_slug
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'portals'
  AND INDEX_NAME = 'uniq_portals_slug';

SET @sql := IF(@idx_portals_slug = 0,
    'ALTER TABLE portals ADD UNIQUE KEY uniq_portals_slug (slug)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS portal_domains (
    id INT AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    portal_id INT NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    is_primary TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_portal_domains_domain (domain),
    KEY idx_portal_domains_portal (portal_id, is_active),
    CONSTRAINT fk_portal_domains_portal FOREIGN KEY (portal_id) REFERENCES portals(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

UPDATE portal_domains
SET domain = LOWER(TRIM(domain))
WHERE domain <> LOWER(TRIM(domain));

ALTER TABLE portal_domains
    ADD COLUMN IF NOT EXISTS portal_id INT NOT NULL AFTER domain,
    ADD COLUMN IF NOT EXISTS is_active TINYINT(1) NOT NULL DEFAULT 1 AFTER portal_id,
    ADD COLUMN IF NOT EXISTS is_primary TINYINT(1) NOT NULL DEFAULT 0 AFTER is_active,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER is_primary,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;

ALTER TABLE portal_domains
    MODIFY COLUMN portal_id INT NOT NULL,
    MODIFY COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1,
    MODIFY COLUMN is_primary TINYINT(1) NOT NULL DEFAULT 0;

SELECT COUNT(*) INTO @idx_portal_domains_domain
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'portal_domains'
  AND INDEX_NAME = 'uniq_portal_domains_domain';

SET @sql := IF(@idx_portal_domains_domain = 0,
    'ALTER TABLE portal_domains ADD UNIQUE KEY uniq_portal_domains_domain (domain)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_portal_domains_portal
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'portal_domains'
  AND INDEX_NAME = 'idx_portal_domains_portal';

SET @sql := IF(@idx_portal_domains_portal = 0,
    'CREATE INDEX idx_portal_domains_portal ON portal_domains(portal_id, is_active)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @fk_portal_domains
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND TABLE_NAME = 'portal_domains'
  AND REFERENCED_TABLE_NAME = 'portals'
LIMIT 1;

SET @sql := IF(@fk_portal_domains IS NULL,
    'ALTER TABLE portal_domains ADD CONSTRAINT fk_portal_domains_portal FOREIGN KEY (portal_id) REFERENCES portals(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

INSERT INTO portals (id, slug, nombre, descripcion)
VALUES
    (1, 'internal', 'Portal interno', 'Panel operativo para personal interno'),
    (2, 'tenant', 'Portal de clientes', 'Acceso para empresas cliente'),
    (3, 'service', 'Portal de servicio', 'Módulo de atención y soporte')
ON DUPLICATE KEY UPDATE
    nombre = VALUES(nombre),
    descripcion = VALUES(descripcion);

CREATE TABLE IF NOT EXISTS roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    empresa_id INT DEFAULT NULL,
    delegated TINYINT(1) NOT NULL DEFAULT 0,
    portal_id INT DEFAULT NULL,
    UNIQUE KEY uniq_roles_empresa_nombre (empresa_id, nombre),
    KEY idx_roles_portal (portal_id),
    FOREIGN KEY (portal_id) REFERENCES portals(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE roles
    ADD COLUMN IF NOT EXISTS empresa_id INT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS delegated TINYINT(1) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS portal_id INT DEFAULT NULL AFTER delegated;

SELECT COUNT(*) INTO @roles_portal_idx
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'roles'
  AND INDEX_NAME = 'idx_roles_portal';

SET @sql := IF(@roles_portal_idx = 0,
    'ALTER TABLE roles ADD INDEX idx_roles_portal (portal_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @roles_portal_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'roles'
  AND COLUMN_NAME = 'portal_id'
  AND REFERENCED_TABLE_NAME = 'portals';

SET @sql := IF(@roles_portal_fk = 0,
    'ALTER TABLE roles ADD CONSTRAINT fk_roles_portal FOREIGN KEY (portal_id) REFERENCES portals(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT INDEX_NAME INTO @old_roles_idx
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'roles'
  AND COLUMN_NAME = 'nombre'
  AND NON_UNIQUE = 0
  AND INDEX_NAME NOT IN ('PRIMARY', 'uniq_roles_empresa_nombre')
LIMIT 1;

SET @sql := IF(@old_roles_idx IS NOT NULL,
    CONCAT('ALTER TABLE roles DROP INDEX `', @old_roles_idx, '`'),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @roles_unique_exists
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'roles'
  AND INDEX_NAME = 'uniq_roles_empresa_nombre';

SET @sql := IF(@roles_unique_exists = 0,
    'ALTER TABLE roles ADD UNIQUE KEY uniq_roles_empresa_nombre (empresa_id, nombre)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @fk_roles_portal
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND TABLE_NAME = 'roles'
  AND REFERENCED_TABLE_NAME = 'portals'
LIMIT 1;

SET @sql := IF(@fk_roles_portal IS NULL,
    'ALTER TABLE roles ADD CONSTRAINT fk_roles_portal FOREIGN KEY (portal_id) REFERENCES portals(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS permissions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    descripcion VARCHAR(250)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id INT,
    permission_id INT,
    PRIMARY KEY (role_id, permission_id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Asegurar estructura completa de empresas
CREATE TABLE IF NOT EXISTS empresas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    contacto VARCHAR(100),
    direccion VARCHAR(250),
    telefono VARCHAR(50),
    email VARCHAR(150),
    responsable_calidad_id INT NULL,
    activo TINYINT(1) DEFAULT 1,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE empresas
    ADD COLUMN IF NOT EXISTS contacto VARCHAR(100) AFTER nombre,
    ADD COLUMN IF NOT EXISTS direccion VARCHAR(250) AFTER contacto,
    ADD COLUMN IF NOT EXISTS telefono VARCHAR(50) AFTER direccion,
    ADD COLUMN IF NOT EXISTS email VARCHAR(150) AFTER telefono,
    ADD COLUMN IF NOT EXISTS responsable_calidad_id INT NULL AFTER email,
    ADD COLUMN IF NOT EXISTS activo TINYINT(1) DEFAULT 1 AFTER email,
    ADD COLUMN IF NOT EXISTS fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP AFTER activo;

SELECT CONSTRAINT_NAME INTO @roles_empresas_fk
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND TABLE_NAME = 'roles'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@roles_empresas_fk IS NULL,
    'ALTER TABLE roles ADD CONSTRAINT fk_roles_empresas FOREIGN KEY (empresa_id) REFERENCES empresas(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Inserta aquí las empresas y roles reales si necesitas precargarlos en tu instalación.
-- Este bloque se deja intencionalmente vacío para evitar datos ficticios.
    
-- Alinear los roles existentes con su portal
UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL
  AND delegated = 0
  AND (empresa_id IS NULL OR empresa_id = 0);

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'tenant')
WHERE portal_id IS NULL
  AND (delegated = 1 OR nombre = 'Cliente');

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'service')
WHERE portal_id IS NULL
  AND nombre = 'Sistemas';

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL;

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'tenant')
WHERE delegated = 1 OR nombre = 'Cliente';

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'service')
WHERE nombre = 'Sistemas';

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL
  AND delegated = 0
  AND (empresa_id IS NULL OR empresa_id = 0)
  AND nombre NOT IN ('Cliente', 'Sistemas');

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL;

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'tenant')
WHERE delegated = 1 OR nombre = 'Cliente';

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'service')
WHERE nombre = 'Sistemas';

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL
  AND delegated = 0
  AND (empresa_id IS NULL OR empresa_id = 0)
  AND nombre NOT IN ('Cliente', 'Sistemas');

UPDATE roles
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL;

CREATE TABLE IF NOT EXISTS tenant_roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_tenant_roles_empresa_nombre (empresa_id, nombre),
    INDEX idx_tenant_roles_empresa (empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tenant_role_permissions (
    tenant_role_id INT NOT NULL,
    permission_id INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_role_id, permission_id),
    FOREIGN KEY (tenant_role_id) REFERENCES tenant_roles(id) ON DELETE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla de usuarios
CREATE TABLE IF NOT EXISTS usuarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario VARCHAR(100) NOT NULL,
    correo VARCHAR(100) NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100),
    puesto VARCHAR(150) NOT NULL DEFAULT 'Sin especificar',
    contrasena VARCHAR(255) NOT NULL,
    empresa_id INT NOT NULL,
    role_id INT NOT NULL,
    portal_id INT DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    sso TINYINT(1) DEFAULT 0,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_usuarios_usuario (usuario),
    UNIQUE KEY uq_usuarios_correo (correo),
    KEY idx_usuarios_portal (portal_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (portal_id) REFERENCES portals(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Normalizar usuarios
SELECT CONSTRAINT_NAME INTO @usuarios_cliente_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'cliente_id'
  AND REFERENCED_TABLE_NAME IS NOT NULL
LIMIT 1;

SET @sql := IF(@usuarios_cliente_fk IS NOT NULL,
    CONCAT('ALTER TABLE usuarios DROP FOREIGN KEY ', @usuarios_cliente_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @cliente_col
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'cliente_id';

SELECT COUNT(*) INTO @empresa_col
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'empresa_id';

SET @sql := IF(@cliente_col > 0 AND @empresa_col = 0,
    'ALTER TABLE usuarios CHANGE COLUMN cliente_id empresa_id INT',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE usuarios
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS portal_id INT NULL AFTER role_id,
    ADD COLUMN IF NOT EXISTS puesto VARCHAR(150) DEFAULT '' AFTER apellidos,
    ADD COLUMN IF NOT EXISTS telefono VARCHAR(50) DEFAULT '' AFTER puesto;

-- Reasignar portal_id de usuarios existentes usando el portal configurado en roles
UPDATE usuarios u
JOIN roles r ON r.id = u.role_id
SET u.portal_id = r.portal_id
WHERE u.portal_id IS NULL
  AND r.portal_id IS NOT NULL;

UPDATE usuarios
SET portal_id = @internal_portal_id
WHERE @internal_portal_id IS NOT NULL
  AND portal_id IS NULL;

UPDATE usuarios
SET empresa_id = 1
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE usuarios
    MODIFY COLUMN empresa_id INT NOT NULL;

ALTER TABLE usuarios
    ADD COLUMN IF NOT EXISTS sso TINYINT(1) DEFAULT 0 AFTER activo,
    ADD COLUMN IF NOT EXISTS fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP AFTER sso;

UPDATE usuarios
SET puesto = CASE WHEN puesto IS NULL OR puesto = '' THEN 'Sin especificar' ELSE puesto END;

ALTER TABLE usuarios
    MODIFY COLUMN puesto VARCHAR(150) NOT NULL,
    MODIFY COLUMN telefono VARCHAR(50) NULL;

SELECT COUNT(*) INTO @usuarios_portal_idx
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND INDEX_NAME = 'idx_usuarios_portal';

SET @sql := IF(@usuarios_portal_idx = 0,
    'ALTER TABLE usuarios ADD INDEX idx_usuarios_portal (portal_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @usuarios_portal_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'portal_id'
  AND REFERENCED_TABLE_NAME = 'portals';

SET @sql := IF(@usuarios_portal_fk = 0,
    'ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_portal FOREIGN KEY (portal_id) REFERENCES portals(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE usuarios u
JOIN roles r ON r.id = u.role_id
SET u.portal_id = r.portal_id
WHERE u.portal_id IS NULL
  AND r.portal_id IS NOT NULL;

UPDATE usuarios
SET portal_id = (SELECT id FROM portals WHERE slug = 'internal')
WHERE portal_id IS NULL;

SELECT COUNT(*) INTO @usuarios_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas';

SET @sql := IF(@usuarios_empresa_fk = 0,
    'ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @empresas_responsable_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'empresas'
  AND COLUMN_NAME = 'responsable_calidad_id'
  AND REFERENCED_TABLE_NAME = 'usuarios';

SET @sql := IF(@empresas_responsable_fk = 0,
    'ALTER TABLE empresas ADD CONSTRAINT fk_empresas_responsable FOREIGN KEY (responsable_calidad_id) REFERENCES usuarios(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @usuarios_role_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'role_id'
  AND REFERENCED_TABLE_NAME = 'roles';

SET @sql := IF(@usuarios_role_fk = 0,
    'ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_role FOREIGN KEY (role_id) REFERENCES roles(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @usuarios_portal_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuarios'
  AND COLUMN_NAME = 'portal_id'
  AND REFERENCED_TABLE_NAME = 'portals';

SET @sql := IF(@usuarios_portal_fk = 0,
    'ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_portal FOREIGN KEY (portal_id) REFERENCES portals(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS tenant_user_roles (
    usuario_id INT NOT NULL,
    tenant_role_id INT NOT NULL,
    assigned_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuario_id, tenant_role_id),
    INDEX idx_tenant_user_roles_usuario (usuario_id),
    INDEX idx_tenant_user_roles_role (tenant_role_id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_role_id) REFERENCES tenant_roles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tokens de restablecimiento de contraseña
CREATE TABLE IF NOT EXISTS password_resets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario_id INT NOT NULL,
    token_hash CHAR(64) NOT NULL,
    expira_en DATETIME NOT NULL,
    creado_en DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_password_resets_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE password_resets
    MODIFY COLUMN token_hash CHAR(64) NOT NULL,
    MODIFY COLUMN expira_en DATETIME NOT NULL,
    MODIFY COLUMN usuario_id INT NOT NULL,
    ADD COLUMN IF NOT EXISTS creado_en DATETIME DEFAULT CURRENT_TIMESTAMP;

SELECT COUNT(*) INTO @fk_password_resets_usuario
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'password_resets'
  AND COLUMN_NAME = 'usuario_id'
  AND REFERENCED_TABLE_NAME = 'usuarios';

SET @sql := IF(@fk_password_resets_usuario = 0,
    'ALTER TABLE password_resets ADD CONSTRAINT fk_password_resets_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_password_resets_token
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'password_resets'
  AND INDEX_NAME = 'uq_password_resets_token';

SET @sql := IF(@idx_password_resets_token = 0,
    'ALTER TABLE password_resets ADD UNIQUE KEY uq_password_resets_token (token_hash)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_password_resets_usuario
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'password_resets'
  AND INDEX_NAME = 'idx_password_resets_usuario';

SET @sql := IF(@idx_password_resets_usuario = 0,
    'ALTER TABLE password_resets ADD INDEX idx_password_resets_usuario (usuario_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_password_resets_expira
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'password_resets'
  AND INDEX_NAME = 'idx_password_resets_expira';

SET @sql := IF(@idx_password_resets_expira = 0,
    'ALTER TABLE password_resets ADD INDEX idx_password_resets_expira (expira_en)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Gestión de clientes y tokens para la API
CREATE TABLE IF NOT EXISTS api_clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    client_secret_hash CHAR(64) NOT NULL,
    scopes TEXT NULL,
    rate_limit_per_minute INT NOT NULL DEFAULT 60,
    rate_limit_per_day INT NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_api_clients_client_id (client_id),
    INDEX idx_api_clients_empresa (empresa_id),
    CONSTRAINT fk_api_clients_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE api_clients
    ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL AFTER id,
    ADD COLUMN IF NOT EXISTS nombre VARCHAR(150) NOT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS client_id VARCHAR(100) NOT NULL AFTER nombre,
    ADD COLUMN IF NOT EXISTS client_secret_hash CHAR(64) NOT NULL AFTER client_id,
    ADD COLUMN IF NOT EXISTS scopes TEXT NULL AFTER client_secret_hash,
    ADD COLUMN IF NOT EXISTS rate_limit_per_minute INT NOT NULL DEFAULT 60 AFTER scopes,
    ADD COLUMN IF NOT EXISTS rate_limit_per_day INT NOT NULL DEFAULT 0 AFTER rate_limit_per_minute,
    ADD COLUMN IF NOT EXISTS created_at DATETIME DEFAULT CURRENT_TIMESTAMP AFTER rate_limit_per_day,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;

ALTER TABLE api_clients
    MODIFY COLUMN empresa_id INT NOT NULL,
    MODIFY COLUMN nombre VARCHAR(150) NOT NULL,
    MODIFY COLUMN client_id VARCHAR(100) NOT NULL,
    MODIFY COLUMN client_secret_hash CHAR(64) NOT NULL,
    MODIFY COLUMN rate_limit_per_minute INT NOT NULL DEFAULT 60,
    MODIFY COLUMN rate_limit_per_day INT NOT NULL DEFAULT 0;

SELECT COUNT(*) INTO @api_clients_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_clients'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas';

SET @sql := IF(@api_clients_empresa_fk = 0,
    'ALTER TABLE api_clients ADD CONSTRAINT fk_api_clients_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_api_clients_client_id
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_clients'
  AND INDEX_NAME = 'uq_api_clients_client_id';

SET @sql := IF(@idx_api_clients_client_id = 0,
    'ALTER TABLE api_clients ADD UNIQUE KEY uq_api_clients_client_id (client_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_api_clients_empresa
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_clients'
  AND INDEX_NAME = 'idx_api_clients_empresa';

SET @sql := IF(@idx_api_clients_empresa = 0,
    'ALTER TABLE api_clients ADD INDEX idx_api_clients_empresa (empresa_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS api_tokens (
    id INT AUTO_INCREMENT PRIMARY KEY,
    api_client_id INT NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    empresa_id INT NOT NULL,
    token_hash CHAR(64) NOT NULL,
    scopes TEXT NULL,
    expires_at DATETIME NOT NULL,
    rate_limit_per_minute INT NOT NULL DEFAULT 60,
    rate_limit_per_day INT NOT NULL DEFAULT 0,
    last_used_at DATETIME NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_api_tokens_hash (token_hash),
    INDEX idx_api_tokens_client_id (client_id),
    INDEX idx_api_tokens_empresa (empresa_id),
    CONSTRAINT fk_api_tokens_client FOREIGN KEY (api_client_id) REFERENCES api_clients(id) ON DELETE CASCADE,
    CONSTRAINT fk_api_tokens_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE api_tokens
    ADD COLUMN IF NOT EXISTS api_client_id INT NOT NULL AFTER id,
    ADD COLUMN IF NOT EXISTS client_id VARCHAR(100) NOT NULL AFTER api_client_id,
    ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL AFTER client_id,
    ADD COLUMN IF NOT EXISTS token_hash CHAR(64) NOT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS scopes TEXT NULL AFTER token_hash,
    ADD COLUMN IF NOT EXISTS expires_at DATETIME NOT NULL AFTER scopes,
    ADD COLUMN IF NOT EXISTS rate_limit_per_minute INT NOT NULL DEFAULT 60 AFTER expires_at,
    ADD COLUMN IF NOT EXISTS rate_limit_per_day INT NOT NULL DEFAULT 0 AFTER rate_limit_per_minute,
    ADD COLUMN IF NOT EXISTS last_used_at DATETIME NULL AFTER rate_limit_per_day,
    ADD COLUMN IF NOT EXISTS created_at DATETIME DEFAULT CURRENT_TIMESTAMP AFTER last_used_at,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;

ALTER TABLE api_tokens
    MODIFY COLUMN api_client_id INT NOT NULL,
    MODIFY COLUMN client_id VARCHAR(100) NOT NULL,
    MODIFY COLUMN empresa_id INT NOT NULL,
    MODIFY COLUMN token_hash CHAR(64) NOT NULL,
    MODIFY COLUMN expires_at DATETIME NOT NULL,
    MODIFY COLUMN rate_limit_per_minute INT NOT NULL DEFAULT 60,
    MODIFY COLUMN rate_limit_per_day INT NOT NULL DEFAULT 0;

SELECT COUNT(*) INTO @api_tokens_client_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_tokens'
  AND COLUMN_NAME = 'api_client_id'
  AND REFERENCED_TABLE_NAME = 'api_clients';

SET @sql := IF(@api_tokens_client_fk = 0,
    'ALTER TABLE api_tokens ADD CONSTRAINT fk_api_tokens_client FOREIGN KEY (api_client_id) REFERENCES api_clients(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @api_tokens_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_tokens'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas';

SET @sql := IF(@api_tokens_empresa_fk = 0,
    'ALTER TABLE api_tokens ADD CONSTRAINT fk_api_tokens_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_api_tokens_hash
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_tokens'
  AND INDEX_NAME = 'uq_api_tokens_hash';

SET @sql := IF(@idx_api_tokens_hash = 0,
    'ALTER TABLE api_tokens ADD UNIQUE KEY uq_api_tokens_hash (token_hash)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_api_tokens_client
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_tokens'
  AND INDEX_NAME = 'idx_api_tokens_client_id';

SET @sql := IF(@idx_api_tokens_client = 0,
    'ALTER TABLE api_tokens ADD INDEX idx_api_tokens_client_id (client_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_api_tokens_empresa
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_tokens'
  AND INDEX_NAME = 'idx_api_tokens_empresa';

SET @sql := IF(@idx_api_tokens_empresa = 0,
    'ALTER TABLE api_tokens ADD INDEX idx_api_tokens_empresa (empresa_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @idx_api_tokens_expires
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'api_tokens'
  AND INDEX_NAME = 'idx_api_tokens_expires_at';

SET @sql := IF(@idx_api_tokens_expires = 0,
    'ALTER TABLE api_tokens ADD INDEX idx_api_tokens_expires_at (expires_at)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Departamentos por empresa
CREATE TABLE IF NOT EXISTS departamentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    empresa_id INT NOT NULL,
    UNIQUE KEY uq_departamentos_empresa (nombre, empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE departamentos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE departamentos
SET empresa_id = 1
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE departamentos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @departamentos_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'departamentos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@departamentos_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE departamentos DROP FOREIGN KEY ', @departamentos_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE departamentos
    ADD CONSTRAINT fk_departamentos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Marcas por empresa
CREATE TABLE IF NOT EXISTS marcas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    empresa_id INT NOT NULL,
    UNIQUE KEY uq_marcas_empresa (nombre, empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE marcas
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE marcas
SET empresa_id = 1
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE marcas
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @marcas_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'marcas'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@marcas_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE marcas DROP FOREIGN KEY ', @marcas_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE marcas
    ADD CONSTRAINT fk_marcas_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Modelos por empresa
CREATE TABLE IF NOT EXISTS modelos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    marca_id INT,
    empresa_id INT NOT NULL,
    UNIQUE KEY uq_modelos_marca (nombre, marca_id, empresa_id),
    FOREIGN KEY (marca_id) REFERENCES marcas(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE modelos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE modelos mo
LEFT JOIN marcas ma ON ma.id = mo.marca_id
SET mo.empresa_id = COALESCE(ma.empresa_id, 1)
WHERE mo.empresa_id IS NULL OR mo.empresa_id = 0;

ALTER TABLE modelos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @modelos_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'modelos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@modelos_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE modelos DROP FOREIGN KEY ', @modelos_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE modelos
    ADD CONSTRAINT fk_modelos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Catálogo de instrumentos por empresa
CREATE TABLE IF NOT EXISTS catalogo_instrumentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    empresa_id INT NOT NULL,
    UNIQUE KEY uq_catalogo_empresa (nombre, empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE catalogo_instrumentos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE catalogo_instrumentos
SET empresa_id = 1
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE catalogo_instrumentos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @catalogo_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'catalogo_instrumentos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@catalogo_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE catalogo_instrumentos DROP FOREIGN KEY ', @catalogo_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE catalogo_instrumentos
    ADD CONSTRAINT fk_catalogo_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Instrumentos multiempresa
CREATE TABLE IF NOT EXISTS instrumentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    catalogo_id INT,
    marca_id INT,
    modelo_id INT,
    serie VARCHAR(100),
    codigo VARCHAR(100),
    departamento_id INT,
    ubicacion VARCHAR(150),
    fecha_alta DATE,
    fecha_baja DATE,
    proxima_calibracion DATE,
    estado VARCHAR(20),
    programado TINYINT(1) DEFAULT 0,
    empresa_id INT NOT NULL,
    FOREIGN KEY (catalogo_id) REFERENCES catalogo_instrumentos(id),
    FOREIGN KEY (marca_id) REFERENCES marcas(id),
    FOREIGN KEY (modelo_id) REFERENCES modelos(id),
    FOREIGN KEY (departamento_id) REFERENCES departamentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    UNIQUE KEY uq_instrumentos_codigo_empresa (codigo, empresa_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE instrumentos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE instrumentos
SET empresa_id = 1
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE instrumentos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @instrumentos_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'instrumentos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@instrumentos_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE instrumentos DROP FOREIGN KEY ', @instrumentos_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE instrumentos
    ADD CONSTRAINT fk_instrumentos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Catálogo de calibradores conectados
CREATE TABLE IF NOT EXISTS calibradores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    numero_serie VARCHAR(150) DEFAULT NULL,
    fabricante VARCHAR(150) DEFAULT NULL,
    modelo VARCHAR(150) DEFAULT NULL,
    tipo VARCHAR(100) DEFAULT NULL,
    descripcion TEXT,
    instrumento_id_default INT NULL,
    token_firma VARCHAR(191) NOT NULL,
    activo TINYINT(1) DEFAULT 1,
    ultimo_contacto DATETIME DEFAULT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_calibradores_empresa_nombre (empresa_id, nombre),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (instrumento_id_default) REFERENCES instrumentos(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE calibradores
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS nombre VARCHAR(150) NOT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS numero_serie VARCHAR(150) NULL AFTER nombre,
    ADD COLUMN IF NOT EXISTS fabricante VARCHAR(150) NULL AFTER numero_serie,
    ADD COLUMN IF NOT EXISTS modelo VARCHAR(150) NULL AFTER fabricante,
    ADD COLUMN IF NOT EXISTS tipo VARCHAR(100) NULL AFTER modelo,
    ADD COLUMN IF NOT EXISTS descripcion TEXT AFTER tipo,
    ADD COLUMN IF NOT EXISTS instrumento_id_default INT NULL AFTER descripcion,
    ADD COLUMN IF NOT EXISTS token_firma VARCHAR(191) NOT NULL AFTER instrumento_id_default,
    ADD COLUMN IF NOT EXISTS activo TINYINT(1) DEFAULT 1 AFTER token_firma,
    ADD COLUMN IF NOT EXISTS ultimo_contacto DATETIME DEFAULT NULL AFTER activo,
    ADD COLUMN IF NOT EXISTS creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER ultimo_contacto,
    ADD COLUMN IF NOT EXISTS actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER creado_en;

UPDATE calibradores
SET empresa_id = COALESCE(empresa_id, 1), activo = COALESCE(activo, 1)
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE calibradores
    MODIFY COLUMN empresa_id INT NOT NULL,
    MODIFY COLUMN nombre VARCHAR(150) NOT NULL,
    MODIFY COLUMN instrumento_id_default INT NULL,
    MODIFY COLUMN token_firma VARCHAR(191) NOT NULL;

SELECT CONSTRAINT_NAME INTO @calibradores_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibradores'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@calibradores_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE calibradores DROP FOREIGN KEY ', @calibradores_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE calibradores
    ADD CONSTRAINT fk_calibradores_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

SELECT CONSTRAINT_NAME INTO @calibradores_instrumento_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibradores'
  AND COLUMN_NAME = 'instrumento_id_default'
  AND REFERENCED_TABLE_NAME = 'instrumentos'
LIMIT 1;

SET @sql := IF(@calibradores_instrumento_fk IS NOT NULL,
    'SELECT 1',
    'ALTER TABLE calibradores ADD CONSTRAINT fk_calibradores_instrumento FOREIGN KEY (instrumento_id_default) REFERENCES instrumentos(id) ON DELETE SET NULL');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Historiales con empresa
CREATE TABLE IF NOT EXISTS historial_departamentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    departamento_id INT,
    empresa_id INT NOT NULL,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (departamento_id) REFERENCES departamentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE historial_departamentos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP AFTER fecha;

UPDATE historial_departamentos hd
JOIN instrumentos i ON i.id = hd.instrumento_id
SET hd.empresa_id = i.empresa_id
WHERE hd.empresa_id IS NULL OR hd.empresa_id = 0;

ALTER TABLE historial_departamentos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @hist_dep_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'historial_departamentos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@hist_dep_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE historial_departamentos DROP FOREIGN KEY ', @hist_dep_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE historial_departamentos
    ADD CONSTRAINT fk_historial_departamentos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS historial_ubicaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    ubicacion VARCHAR(150),
    empresa_id INT NOT NULL,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE historial_ubicaciones
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP AFTER fecha;

UPDATE historial_ubicaciones hu
JOIN instrumentos i ON i.id = hu.instrumento_id
SET hu.empresa_id = i.empresa_id
WHERE hu.empresa_id IS NULL OR hu.empresa_id = 0;

ALTER TABLE historial_ubicaciones
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @hist_ubi_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'historial_ubicaciones'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@hist_ubi_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE historial_ubicaciones DROP FOREIGN KEY ', @hist_ubi_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE historial_ubicaciones
    ADD CONSTRAINT fk_historial_ubicaciones_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS historial_tipos_instrumento (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    estado VARCHAR(20),
    empresa_id INT NOT NULL,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE historial_tipos_instrumento
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP AFTER fecha;

UPDATE historial_tipos_instrumento ht
JOIN instrumentos i ON i.id = ht.instrumento_id
SET ht.empresa_id = i.empresa_id
WHERE ht.empresa_id IS NULL OR ht.empresa_id = 0;

ALTER TABLE historial_tipos_instrumento
    MODIFY COLUMN empresa_id INT NOT NULL;

ALTER TABLE historial_tipos_instrumento
    MODIFY COLUMN `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP;

SELECT CONSTRAINT_NAME INTO @hist_tipos_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'historial_tipos_instrumento'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@hist_tipos_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE historial_tipos_instrumento DROP FOREIGN KEY ', @hist_tipos_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE historial_tipos_instrumento
    ADD CONSTRAINT fk_historial_tipos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS historial_fecha_alta (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha DATE,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE historial_fecha_alta
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP AFTER fecha;

UPDATE historial_fecha_alta hfa
JOIN instrumentos i ON i.id = hfa.instrumento_id
SET hfa.empresa_id = i.empresa_id
WHERE hfa.empresa_id IS NULL OR hfa.empresa_id = 0;

ALTER TABLE historial_fecha_alta
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @hist_alta_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'historial_fecha_alta'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@hist_alta_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE historial_fecha_alta DROP FOREIGN KEY ', @hist_alta_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE historial_fecha_alta
    ADD CONSTRAINT fk_historial_fecha_alta_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS historial_fecha_baja (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha DATE,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE historial_fecha_baja
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP AFTER fecha;

UPDATE historial_fecha_baja hfb
JOIN instrumentos i ON i.id = hfb.instrumento_id
SET hfb.empresa_id = i.empresa_id
WHERE hfb.empresa_id IS NULL OR hfb.empresa_id = 0;

ALTER TABLE historial_fecha_baja
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @hist_baja_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'historial_fecha_baja'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@hist_baja_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE historial_fecha_baja DROP FOREIGN KEY ', @hist_baja_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE historial_fecha_baja
    ADD CONSTRAINT fk_historial_fecha_baja_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS historial_calibraciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    fecha_evento DATE NOT NULL,
    tipo_evento VARCHAR(50) NOT NULL,
    descripcion TEXT,
    certificado_codigo VARCHAR(120) DEFAULT NULL,
    usuario_id INT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_hist_calibraciones_instrumento (instrumento_id),
    INDEX idx_hist_calibraciones_fecha (fecha_evento),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS historial_especificaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    fecha_vigencia DATE NOT NULL,
    version VARCHAR(40) NOT NULL,
    descripcion TEXT,
    usuario_id INT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_hist_especificaciones_instrumento (instrumento_id),
    INDEX idx_hist_especificaciones_fecha (fecha_vigencia),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS historial_estado_instrumento (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    fecha_evento DATE NOT NULL,
    estado VARCHAR(40) NOT NULL,
    motivo TEXT,
    usuario_id INT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_hist_estado_instrumento (instrumento_id),
    INDEX idx_hist_estado_fecha (fecha_evento),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Requerimientos y planes de riesgo con empresa
CREATE TABLE IF NOT EXISTS requerimientos_calibracion (
    instrumento_id INT PRIMARY KEY,
    empresa_id INT NOT NULL,
    requerimiento TEXT,
    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE requerimientos_calibracion
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE requerimientos_calibracion rc
JOIN instrumentos i ON i.id = rc.instrumento_id
SET rc.empresa_id = i.empresa_id
WHERE rc.empresa_id IS NULL OR rc.empresa_id = 0;

ALTER TABLE requerimientos_calibracion
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @req_cal_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'requerimientos_calibracion'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@req_cal_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE requerimientos_calibracion DROP FOREIGN KEY ', @req_cal_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE requerimientos_calibracion
    ADD CONSTRAINT fk_requerimientos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS plan_riesgos (
    instrumento_id INT PRIMARY KEY,
    empresa_id INT NOT NULL,
    requerimiento VARCHAR(100) DEFAULT 'NA',
    impacto_falla VARCHAR(100) DEFAULT 'NA',
    consideraciones_falla VARCHAR(100) DEFAULT 'NA',
    clase_riesgo VARCHAR(100) DEFAULT 'NA',
    capacidad_deteccion VARCHAR(100) DEFAULT 'NA',
    frecuencia VARCHAR(100) DEFAULT 'NA',
    observaciones TEXT,
    tipo_calibracion VARCHAR(50) DEFAULT 'NA',
    especificaciones TEXT,
    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE plan_riesgos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE plan_riesgos pr
JOIN instrumentos i ON i.id = pr.instrumento_id
SET pr.empresa_id = i.empresa_id
WHERE pr.empresa_id IS NULL OR pr.empresa_id = 0;

ALTER TABLE plan_riesgos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @plan_riesgos_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'plan_riesgos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@plan_riesgos_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE plan_riesgos DROP FOREIGN KEY ', @plan_riesgos_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE plan_riesgos
    ADD CONSTRAINT fk_plan_riesgos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Proveedores y contactos
CREATE TABLE IF NOT EXISTS proveedores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    direccion VARCHAR(250),
    empresa_id INT NOT NULL,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE proveedores
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE proveedores
SET empresa_id = 1
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE proveedores
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @proveedores_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'proveedores'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@proveedores_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE proveedores DROP FOREIGN KEY ', @proveedores_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE proveedores
    ADD CONSTRAINT fk_proveedores_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS proveedor_contactos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    proveedor_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    puesto VARCHAR(100),
    correo VARCHAR(100),
    empresa_id INT NOT NULL,
    FOREIGN KEY (proveedor_id) REFERENCES proveedores(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE proveedor_contactos
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE proveedor_contactos pc
JOIN proveedores p ON p.id = pc.proveedor_id
SET pc.empresa_id = p.empresa_id
WHERE pc.empresa_id IS NULL OR pc.empresa_id = 0;

ALTER TABLE proveedor_contactos
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @prov_contactos_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'proveedor_contactos'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@prov_contactos_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE proveedor_contactos DROP FOREIGN KEY ', @prov_contactos_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE proveedor_contactos
    ADD CONSTRAINT fk_proveedor_contactos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Solicitudes de calibración por empresa
CREATE TABLE IF NOT EXISTS solicitudes_calibracion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    instrumento_id INT,
    usuario_id INT,
    tipo VARCHAR(20),
    fecha_deseada DATE,
    comentarios TEXT,
    instrucciones_cliente TEXT,
    estado VARCHAR(20) DEFAULT 'Pendiente',
    fecha_solicitud DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE solicitudes_calibracion
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE solicitudes_calibracion sc
LEFT JOIN instrumentos i ON i.id = sc.instrumento_id
LEFT JOIN usuarios u ON u.id = sc.usuario_id
SET sc.empresa_id = COALESCE(sc.empresa_id, i.empresa_id, u.empresa_id, 1)
WHERE sc.empresa_id IS NULL OR sc.empresa_id = 0;

ALTER TABLE solicitudes_calibracion
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @solicitudes_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'solicitudes_calibracion'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@solicitudes_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE solicitudes_calibracion DROP FOREIGN KEY ', @solicitudes_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE solicitudes_calibracion
    ADD CONSTRAINT fk_solicitudes_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

ALTER TABLE solicitudes_calibracion
    ADD COLUMN IF NOT EXISTS instrucciones_cliente TEXT NULL;

-- Catálogo de patrones de referencia
CREATE TABLE IF NOT EXISTS patrones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    codigo VARCHAR(100) DEFAULT NULL,
    descripcion TEXT,
    certificado_numero VARCHAR(150) DEFAULT NULL,
    certificado_archivo VARCHAR(255) DEFAULT NULL,
    fecha_emision DATE DEFAULT NULL,
    fecha_vencimiento DATE DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE patrones
    ADD COLUMN IF NOT EXISTS codigo VARCHAR(100) DEFAULT NULL AFTER nombre,
    ADD COLUMN IF NOT EXISTS descripcion TEXT AFTER codigo,
    ADD COLUMN IF NOT EXISTS certificado_numero VARCHAR(150) DEFAULT NULL AFTER descripcion,
    ADD COLUMN IF NOT EXISTS certificado_archivo VARCHAR(255) DEFAULT NULL AFTER certificado_numero,
    ADD COLUMN IF NOT EXISTS fecha_emision DATE DEFAULT NULL AFTER certificado_archivo,
    ADD COLUMN IF NOT EXISTS fecha_vencimiento DATE DEFAULT NULL AFTER fecha_emision,
    ADD COLUMN IF NOT EXISTS activo TINYINT(1) DEFAULT 1 AFTER fecha_vencimiento,
    ADD COLUMN IF NOT EXISTS fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP AFTER activo;

-- Calibraciones y planes
CREATE TABLE IF NOT EXISTS calibraciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    empresa_id INT NOT NULL,
    patron_id INT NULL,
    patron_certificado VARCHAR(255) NULL,
    tipo VARCHAR(20) NOT NULL DEFAULT 'Interna',
    fecha_calibracion DATE NOT NULL,
    duracion_horas DECIMAL(8,2) NULL,
    costo_total DECIMAL(12,2) NULL,
    periodo ENUM('P1','P2','EXTRA') NOT NULL DEFAULT 'P1',
    fecha_proxima DATE,
    proveedor_id INT,
    usuario_id INT,
    estado ENUM('Pendiente','Aprobado','Rechazado') NOT NULL DEFAULT 'Pendiente',
    resultado_preliminar VARCHAR(50) DEFAULT NULL,
    resultado VARCHAR(50) DEFAULT NULL,
    liberado_por INT,
    fecha_liberacion DATETIME,
    calibrador_id INT NULL,
    origen_datos ENUM('manual','integracion') DEFAULT 'manual',
    payload_json LONGTEXT NULL,
    observaciones TEXT,
    u_value DECIMAL(18,6) DEFAULT NULL,
    u_method VARCHAR(255) DEFAULT NULL,
    u_k DECIMAL(10,4) DEFAULT NULL,
    u_coverage VARCHAR(255) DEFAULT NULL,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (proveedor_id) REFERENCES proveedores(id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
    FOREIGN KEY (liberado_por) REFERENCES usuarios(id),

    FOREIGN KEY (patron_id) REFERENCES patrones(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calibracion_referencias (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT NOT NULL,
    empresa_id INT NOT NULL,
    tipo VARCHAR(40) NOT NULL,
    referencia_id INT DEFAULT NULL,
    etiqueta VARCHAR(190) DEFAULT NULL,
    enlace VARCHAR(500) DEFAULT NULL,
    creado_en DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    INDEX idx_cal_ref_calibracion (calibracion_id),
    INDEX idx_cal_ref_tipo (tipo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calibracion_referencias (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT NOT NULL,
    empresa_id INT NOT NULL,
    tipo VARCHAR(40) NOT NULL,
    referencia_id INT DEFAULT NULL,
    etiqueta VARCHAR(190) DEFAULT NULL,
    enlace VARCHAR(500) DEFAULT NULL,
    creado_en DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    INDEX idx_cal_ref_calibracion (calibracion_id),
    INDEX idx_cal_ref_tipo (tipo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS usuario_id INT NULL AFTER empresa_id;

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS u_value DECIMAL(18,6) DEFAULT NULL AFTER observaciones,
    ADD COLUMN IF NOT EXISTS u_method VARCHAR(255) DEFAULT NULL AFTER u_value,
    ADD COLUMN IF NOT EXISTS u_k DECIMAL(10,4) DEFAULT NULL AFTER u_method,
    ADD COLUMN IF NOT EXISTS u_coverage VARCHAR(255) DEFAULT NULL AFTER u_k;

SELECT CONSTRAINT_NAME INTO @calibraciones_usuario_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'usuario_id'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@calibraciones_usuario_fk IS NOT NULL,
    CONCAT('ALTER TABLE calibraciones DROP FOREIGN KEY ', @calibraciones_usuario_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE calibraciones
    ADD CONSTRAINT fk_calibraciones_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id);

UPDATE calibraciones c
LEFT JOIN instrumentos i ON i.id = c.instrumento_id
SET c.empresa_id = COALESCE(i.empresa_id, 1)
WHERE c.empresa_id IS NULL OR c.empresa_id = 0;

ALTER TABLE calibraciones
    MODIFY COLUMN empresa_id INT NOT NULL;

SELECT CONSTRAINT_NAME INTO @calibraciones_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@calibraciones_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE calibraciones DROP FOREIGN KEY ', @calibraciones_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE calibraciones
    ADD CONSTRAINT fk_calibraciones_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

SELECT CONSTRAINT_NAME INTO @calibraciones_patron_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'patron_id'
  AND REFERENCED_TABLE_NAME = 'patrones'
LIMIT 1;

SET @sql := IF(@calibraciones_patron_fk IS NOT NULL,
    CONCAT('ALTER TABLE calibraciones DROP FOREIGN KEY ', @calibraciones_patron_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE calibraciones
    ADD CONSTRAINT fk_calibraciones_patron FOREIGN KEY (patron_id) REFERENCES patrones(id);

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS periodo ENUM('P1','P2','EXTRA') DEFAULT 'P1' AFTER fecha_calibracion;

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS calibrador_id INT NULL AFTER fecha_liberacion,
    ADD COLUMN IF NOT EXISTS origen_datos ENUM('manual','integracion') DEFAULT 'manual' AFTER calibrador_id,
    ADD COLUMN IF NOT EXISTS payload_json LONGTEXT NULL AFTER origen_datos;

UPDATE calibraciones
SET origen_datos = 'manual'
WHERE origen_datos IS NULL OR origen_datos NOT IN ('manual','integracion');

SELECT CONSTRAINT_NAME INTO @calibraciones_calibrador_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'calibrador_id'
  AND REFERENCED_TABLE_NAME = 'calibradores'
LIMIT 1;

SET @sql := IF(@calibraciones_calibrador_fk IS NOT NULL,
    'SELECT 1',
    'ALTER TABLE calibraciones ADD CONSTRAINT fk_calibraciones_calibrador FOREIGN KEY (calibrador_id) REFERENCES calibradores(id)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Lecturas asociadas a calibraciones
CREATE TABLE IF NOT EXISTS calibraciones_lecturas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT NULL,
    instrumento_id INT NULL,
    calibrador_id INT NOT NULL,
    empresa_id INT NOT NULL,
    measurement_uuid CHAR(36) NOT NULL,
    fecha_lectura DATETIME NOT NULL,
    magnitud VARCHAR(120) NULL,
    valor DECIMAL(18,6) NULL,
    unidad VARCHAR(50) NULL,
    payload_json LONGTEXT NOT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_lecturas_uuid (measurement_uuid),
    KEY idx_lecturas_calibracion (calibracion_id),
    KEY idx_lecturas_instrumento (instrumento_id),
    CONSTRAINT fk_lecturas_calibracion FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE SET NULL,
    CONSTRAINT fk_lecturas_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE SET NULL,
    CONSTRAINT fk_lecturas_calibrador FOREIGN KEY (calibrador_id) REFERENCES calibradores(id) ON DELETE CASCADE,
    CONSTRAINT fk_lecturas_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE calibraciones_lecturas
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL,
    ADD COLUMN IF NOT EXISTS instrumento_id INT NULL AFTER calibracion_id,
    ADD COLUMN IF NOT EXISTS measurement_uuid CHAR(36) NOT NULL,
    ADD COLUMN IF NOT EXISTS fecha_lectura DATETIME NOT NULL,
    ADD COLUMN IF NOT EXISTS magnitud VARCHAR(120) NULL,
    ADD COLUMN IF NOT EXISTS valor DECIMAL(18,6) NULL,
    ADD COLUMN IF NOT EXISTS unidad VARCHAR(50) NULL,
    ADD COLUMN IF NOT EXISTS payload_json LONGTEXT NOT NULL,
    ADD COLUMN IF NOT EXISTS creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;

UPDATE calibraciones_lecturas
SET empresa_id = COALESCE(empresa_id, 1)
WHERE empresa_id IS NULL OR empresa_id = 0;

ALTER TABLE calibraciones_lecturas
    MODIFY COLUMN calibracion_id INT NULL,
    MODIFY COLUMN instrumento_id INT NULL,
    MODIFY COLUMN empresa_id INT NOT NULL,
    MODIFY COLUMN measurement_uuid CHAR(36) NOT NULL,
    MODIFY COLUMN fecha_lectura DATETIME NOT NULL,
    MODIFY COLUMN payload_json LONGTEXT NOT NULL;

SELECT CONSTRAINT_NAME INTO @lecturas_calibracion_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones_lecturas'
  AND COLUMN_NAME = 'calibracion_id'
  AND REFERENCED_TABLE_NAME = 'calibraciones'
LIMIT 1;

SET @sql := IF(@lecturas_calibracion_fk IS NOT NULL,
    'SELECT 1',
    'ALTER TABLE calibraciones_lecturas ADD CONSTRAINT fk_lecturas_calibracion FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE SET NULL');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @lecturas_instrumento_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones_lecturas'
  AND COLUMN_NAME = 'instrumento_id'
  AND REFERENCED_TABLE_NAME = 'instrumentos'
LIMIT 1;

SET @sql := IF(@lecturas_instrumento_fk IS NOT NULL,
    'SELECT 1',
    'ALTER TABLE calibraciones_lecturas ADD CONSTRAINT fk_lecturas_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE SET NULL');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @lecturas_calibrador_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones_lecturas'
  AND COLUMN_NAME = 'calibrador_id'
  AND REFERENCED_TABLE_NAME = 'calibradores'
LIMIT 1;

SET @sql := IF(@lecturas_calibrador_fk IS NOT NULL,
    'SELECT 1',
    'ALTER TABLE calibraciones_lecturas ADD CONSTRAINT fk_lecturas_calibrador FOREIGN KEY (calibrador_id) REFERENCES calibradores(id) ON DELETE CASCADE');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @lecturas_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones_lecturas'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@lecturas_empresa_fk IS NOT NULL,
    'SELECT 1',
    'ALTER TABLE calibraciones_lecturas ADD CONSTRAINT fk_lecturas_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS duracion_horas DECIMAL(8,2) NULL AFTER fecha_calibracion,
    ADD COLUMN IF NOT EXISTS costo_total DECIMAL(12,2) NULL AFTER duracion_horas;

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS estado_ejecucion ENUM('Programada','En proceso','Completada','Reprogramada','Atrasada','Cancelada') DEFAULT 'Completada' AFTER proveedor_id,
    ADD COLUMN IF NOT EXISTS motivo_reprogramacion TEXT AFTER estado_ejecucion,
    ADD COLUMN IF NOT EXISTS fecha_reprogramada DATE NULL AFTER motivo_reprogramacion,
    ADD COLUMN IF NOT EXISTS dias_atraso INT DEFAULT 0 AFTER fecha_reprogramada;

CREATE TABLE IF NOT EXISTS calibration_nonconformities (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT NULL,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    estado ENUM('abierta','cerrada') NOT NULL DEFAULT 'abierta',
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notified_at DATETIME DEFAULT NULL,
    notes TEXT NULL,
    UNIQUE KEY uniq_calibration_nonconformity (calibracion_id),
    INDEX idx_nonconformity_empresa_estado (empresa_id, estado),
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE SET NULL,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE calibration_nonconformities
    ADD COLUMN IF NOT EXISTS estado ENUM('abierta','cerrada') NOT NULL DEFAULT 'abierta' AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER estado,
    ADD COLUMN IF NOT EXISTS notified_at DATETIME NULL AFTER detected_at,
    ADD COLUMN IF NOT EXISTS notes TEXT NULL AFTER notified_at;

ALTER TABLE calibration_nonconformities
    ADD INDEX IF NOT EXISTS idx_nonconformity_empresa_estado (empresa_id, estado);

UPDATE calibraciones
SET estado_ejecucion = CASE
        WHEN estado_ejecucion IS NULL OR TRIM(estado_ejecucion) = '' THEN 'Completada'
        ELSE estado_ejecucion
    END,
    dias_atraso = CASE
        WHEN dias_atraso IS NULL OR dias_atraso < 0 THEN 0
        ELSE dias_atraso
    END;

CREATE TABLE IF NOT EXISTS usuario_competencias (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario_id INT NOT NULL,
    empresa_id INT NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    archivo VARCHAR(255) DEFAULT NULL,
    evidencia_fecha DATE DEFAULT NULL,
    catalogo_id INT DEFAULT NULL,
    vigente_desde DATE DEFAULT NULL,
    vigente_hasta DATE DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_usuario_competencias_usuario (usuario_id),
    INDEX idx_usuario_competencias_empresa (empresa_id),
    INDEX idx_usuario_competencias_catalogo (catalogo_id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    FOREIGN KEY (catalogo_id) REFERENCES catalogo_instrumentos(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE usuario_competencias
    ADD COLUMN IF NOT EXISTS catalogo_id INT DEFAULT NULL AFTER evidencia_fecha,
    ADD COLUMN IF NOT EXISTS vigente_desde DATE DEFAULT NULL AFTER catalogo_id,
    ADD COLUMN IF NOT EXISTS vigente_hasta DATE DEFAULT NULL AFTER vigente_desde;

SELECT COUNT(*) INTO @idx_uc_catalogo
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuario_competencias'
  AND INDEX_NAME = 'idx_usuario_competencias_catalogo';

SET @sql := IF(@idx_uc_catalogo = 0,
    'ALTER TABLE usuario_competencias ADD INDEX idx_usuario_competencias_catalogo (catalogo_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @fk_uc_catalogo
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuario_competencias'
  AND COLUMN_NAME = 'catalogo_id'
  AND REFERENCED_TABLE_NAME = 'catalogo_instrumentos';

SET @sql := IF(@fk_uc_catalogo = 0,
    'ALTER TABLE usuario_competencias ADD CONSTRAINT fk_usuario_competencias_catalogo FOREIGN KEY (catalogo_id) REFERENCES catalogo_instrumentos(id) ON DELETE SET NULL',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE calibraciones
SET duracion_horas = CASE
        WHEN TRIM(CAST(duracion_horas AS CHAR)) = '' THEN NULL
        ELSE duracion_horas
    END,
    costo_total = CASE
        WHEN TRIM(CAST(costo_total AS CHAR)) = '' THEN NULL
        ELSE costo_total
    END,
    tipo = CASE
        WHEN tipo IS NULL OR TRIM(tipo) = '' THEN 'Interna'
        ELSE tipo
    END
WHERE (tipo IS NULL OR TRIM(tipo) = ''
    OR TRIM(CAST(duracion_horas AS CHAR)) = ''
    OR TRIM(CAST(costo_total AS CHAR)) = '');

ALTER TABLE calibraciones
    MODIFY COLUMN tipo VARCHAR(20) NOT NULL DEFAULT 'Interna';

UPDATE calibraciones
SET periodo = COALESCE(NULLIF(periodo, ''), 'P1')
WHERE periodo IS NULL OR periodo = '';

ALTER TABLE calibraciones
    MODIFY COLUMN periodo ENUM('P1','P2','EXTRA') NOT NULL DEFAULT 'P1';

ALTER TABLE calibraciones
    ADD COLUMN IF NOT EXISTS resultado_preliminar VARCHAR(50) DEFAULT NULL AFTER fecha_proxima,
    ADD COLUMN IF NOT EXISTS estado ENUM('Pendiente','Aprobado','Rechazado') NOT NULL DEFAULT 'Pendiente' AFTER resultado_preliminar,
    ADD COLUMN IF NOT EXISTS resultado VARCHAR(50) DEFAULT NULL AFTER estado,
    ADD COLUMN IF NOT EXISTS usuario_id INT NULL AFTER proveedor_id,
    ADD COLUMN IF NOT EXISTS liberado_por INT NULL AFTER usuario_id,
    ADD COLUMN IF NOT EXISTS fecha_liberacion DATETIME NULL AFTER liberado_por,
    ADD COLUMN IF NOT EXISTS aprobado_por INT NULL AFTER fecha_liberacion,
    ADD COLUMN IF NOT EXISTS fecha_aprobacion DATETIME NULL AFTER aprobado_por,
    ADD COLUMN IF NOT EXISTS motivo_rechazo TEXT NULL AFTER fecha_aprobacion;

UPDATE calibraciones
SET estado = COALESCE(NULLIF(estado, ''), 'Pendiente');

UPDATE calibraciones
SET resultado_preliminar = COALESCE(resultado_preliminar, resultado);

ALTER TABLE calibraciones
    MODIFY COLUMN estado ENUM('Pendiente','Aprobado','Rechazado') NOT NULL DEFAULT 'Pendiente';

SELECT CONSTRAINT_NAME INTO @calibraciones_usuario_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'usuario_id'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@calibraciones_usuario_fk IS NULL,
    'ALTER TABLE calibraciones ADD CONSTRAINT fk_calibraciones_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @calibraciones_liberado_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'liberado_por'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@calibraciones_liberado_fk IS NULL,
    'ALTER TABLE calibraciones ADD CONSTRAINT fk_calibraciones_liberado FOREIGN KEY (liberado_por) REFERENCES usuarios(id) ON DELETE SET NULL',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @calibraciones_aprobado_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'calibraciones'
  AND COLUMN_NAME = 'aprobado_por'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@calibraciones_aprobado_fk IS NULL,
    'ALTER TABLE calibraciones ADD CONSTRAINT fk_calibraciones_aprobado FOREIGN KEY (aprobado_por) REFERENCES usuarios(id) ON DELETE SET NULL',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS instrumento_adjuntos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    nombre_visible VARCHAR(255) NOT NULL,
    ruta_archivo VARCHAR(500) NOT NULL,
    tipo VARCHAR(60) DEFAULT NULL,
    descripcion TEXT,
    usuario_id INT DEFAULT NULL,
    mime_type VARCHAR(120) DEFAULT NULL,
    tamano BIGINT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_adjuntos_instrumento (instrumento_id, empresa_id),
    CONSTRAINT fk_adjuntos_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE,
    CONSTRAINT fk_adjuntos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_adjuntos_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE instrumento_adjuntos
    ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL AFTER instrumento_id,
    ADD COLUMN IF NOT EXISTS nombre_visible VARCHAR(255) NOT NULL,
    ADD COLUMN IF NOT EXISTS ruta_archivo VARCHAR(500) NOT NULL,
    ADD COLUMN IF NOT EXISTS tipo VARCHAR(60) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS descripcion TEXT,
    ADD COLUMN IF NOT EXISTS usuario_id INT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS mime_type VARCHAR(120) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS tamano BIGINT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE instrumento_adjuntos
    ADD INDEX IF NOT EXISTS idx_adjuntos_instrumento (instrumento_id, empresa_id);

SELECT COUNT(*) INTO @adjuntos_inst_fk
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND CONSTRAINT_NAME = 'fk_adjuntos_instrumento';

SET @sql := IF(@adjuntos_inst_fk = 0,
    'ALTER TABLE instrumento_adjuntos ADD CONSTRAINT fk_adjuntos_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @adjuntos_emp_fk
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND CONSTRAINT_NAME = 'fk_adjuntos_empresa';

SET @sql := IF(@adjuntos_emp_fk = 0,
    'ALTER TABLE instrumento_adjuntos ADD CONSTRAINT fk_adjuntos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @adjuntos_usr_fk
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE()
  AND CONSTRAINT_NAME = 'fk_adjuntos_usuario';

SET @sql := IF(@adjuntos_usr_fk = 0,
    'ALTER TABLE instrumento_adjuntos ADD CONSTRAINT fk_adjuntos_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS planes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha_programada DATE,
    responsable_id INT,
    estado VARCHAR(20),
    empresa_id INT NOT NULL,
    instrucciones_cliente TEXT,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (responsable_id) REFERENCES usuarios(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE planes
    DROP COLUMN IF EXISTS responsable,
    ADD COLUMN IF NOT EXISTS responsable_id INT;

SELECT CONSTRAINT_NAME INTO @planes_responsable_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'planes'
  AND COLUMN_NAME = 'responsable_id'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@planes_responsable_fk IS NOT NULL,
    CONCAT('ALTER TABLE planes DROP FOREIGN KEY ', @planes_responsable_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE planes
    ADD CONSTRAINT fk_planes_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id);

ALTER TABLE planes
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE planes p
LEFT JOIN instrumentos i ON i.id = p.instrumento_id
SET p.empresa_id = COALESCE(i.empresa_id, 1)
WHERE p.empresa_id IS NULL OR p.empresa_id = 0;

ALTER TABLE planes
    MODIFY COLUMN empresa_id INT NOT NULL;

ALTER TABLE planes
    ADD COLUMN IF NOT EXISTS instrucciones_cliente TEXT NULL;

SELECT CONSTRAINT_NAME INTO @planes_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'planes'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@planes_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE planes DROP FOREIGN KEY ', @planes_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE planes
    ADD CONSTRAINT fk_planes_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

CREATE TABLE IF NOT EXISTS ordenes_calibracion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    plan_id INT NOT NULL,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    tecnico_id INT DEFAULT NULL,
    estado_ejecucion VARCHAR(50) NOT NULL DEFAULT 'Programada',
    fecha_inicio DATE DEFAULT NULL,
    fecha_cierre DATE DEFAULT NULL,
    observaciones TEXT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_orden_plan (plan_id),
    KEY idx_orden_instrumento (instrumento_id),
    KEY idx_orden_empresa (empresa_id),
    KEY idx_orden_tecnico (tecnico_id),
    CONSTRAINT fk_orden_plan FOREIGN KEY (plan_id) REFERENCES planes(id) ON DELETE CASCADE,
    CONSTRAINT fk_orden_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    CONSTRAINT fk_orden_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    CONSTRAINT fk_orden_tecnico FOREIGN KEY (tecnico_id) REFERENCES usuarios(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE ordenes_calibracion
    ADD COLUMN IF NOT EXISTS tecnico_id INT DEFAULT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS estado_ejecucion VARCHAR(50) NOT NULL DEFAULT 'Programada' AFTER tecnico_id,
    ADD COLUMN IF NOT EXISTS fecha_inicio DATE DEFAULT NULL AFTER estado_ejecucion,
    ADD COLUMN IF NOT EXISTS fecha_cierre DATE DEFAULT NULL AFTER fecha_inicio,
    ADD COLUMN IF NOT EXISTS observaciones TEXT DEFAULT NULL AFTER fecha_cierre,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER observaciones,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;

ALTER TABLE ordenes_calibracion
    MODIFY COLUMN estado_ejecucion VARCHAR(50) NOT NULL DEFAULT 'Programada';

SELECT COUNT(*) INTO @orden_plan_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ordenes_calibracion'
  AND CONSTRAINT_NAME = 'fk_orden_plan';

SET @sql := IF(@orden_plan_fk = 0,
    'ALTER TABLE ordenes_calibracion ADD CONSTRAINT fk_orden_plan FOREIGN KEY (plan_id) REFERENCES planes(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @orden_instrumento_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ordenes_calibracion'
  AND CONSTRAINT_NAME = 'fk_orden_instrumento';

SET @sql := IF(@orden_instrumento_fk = 0,
    'ALTER TABLE ordenes_calibracion ADD CONSTRAINT fk_orden_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @orden_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ordenes_calibracion'
  AND CONSTRAINT_NAME = 'fk_orden_empresa';

SET @sql := IF(@orden_empresa_fk = 0,
    'ALTER TABLE ordenes_calibracion ADD CONSTRAINT fk_orden_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @orden_tecnico_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ordenes_calibracion'
  AND CONSTRAINT_NAME = 'fk_orden_tecnico';

SET @sql := IF(@orden_tecnico_fk = 0,
    'ALTER TABLE ordenes_calibracion ADD CONSTRAINT fk_orden_tecnico FOREIGN KEY (tecnico_id) REFERENCES usuarios(id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @orden_plan_unique
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ordenes_calibracion'
  AND INDEX_NAME = 'uq_orden_plan';

SET @sql := IF(@orden_plan_unique = 0,
    'ALTER TABLE ordenes_calibracion ADD UNIQUE KEY uq_orden_plan (plan_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Reportes personalizados con empresa
CREATE TABLE IF NOT EXISTS reportes_personalizados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    instrumento_id INT,
    fecha_creacion DATE NOT NULL,
    usuario_id INT,
    empresa_id INT NOT NULL,
    configuracion JSON NULL,
    formato_preferido VARCHAR(50) DEFAULT 'excel',
    filtros JSON NULL,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE reportes_personalizados
    ADD COLUMN IF NOT EXISTS instrumento_id INT,
    ADD COLUMN IF NOT EXISTS usuario_id INT,
    DROP COLUMN IF EXISTS instrumento,
    DROP COLUMN IF EXISTS creado_por,
    ADD COLUMN IF NOT EXISTS configuracion JSON NULL,
    ADD COLUMN IF NOT EXISTS formato_preferido VARCHAR(50) DEFAULT 'excel',
    ADD COLUMN IF NOT EXISTS filtros JSON NULL;

SELECT CONSTRAINT_NAME INTO @reportes_instr_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'reportes_personalizados'
  AND COLUMN_NAME = 'instrumento_id'
  AND REFERENCED_TABLE_NAME = 'instrumentos'
LIMIT 1;

SET @sql := IF(@reportes_instr_fk IS NOT NULL,
    CONCAT('ALTER TABLE reportes_personalizados DROP FOREIGN KEY ', @reportes_instr_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @reportes_usuario_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'reportes_personalizados'
  AND COLUMN_NAME = 'usuario_id'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@reportes_usuario_fk IS NOT NULL,
    CONCAT('ALTER TABLE reportes_personalizados DROP FOREIGN KEY ', @reportes_usuario_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE reportes_personalizados
    ADD CONSTRAINT fk_reportes_instrumento FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    ADD CONSTRAINT fk_reportes_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id);

ALTER TABLE reportes_personalizados
    ADD COLUMN IF NOT EXISTS empresa_id INT NULL;

UPDATE reportes_personalizados rp
LEFT JOIN instrumentos i ON i.id = rp.instrumento_id
LEFT JOIN usuarios u ON u.id = rp.usuario_id
SET rp.empresa_id = COALESCE(i.empresa_id, u.empresa_id, 1)
WHERE rp.empresa_id IS NULL OR rp.empresa_id = 0;

ALTER TABLE reportes_personalizados
    MODIFY COLUMN empresa_id INT NOT NULL;

UPDATE reportes_personalizados
SET configuracion = COALESCE(configuracion, JSON_OBJECT()),
    formato_preferido = COALESCE(NULLIF(TRIM(formato_preferido), ''), 'excel'),
    filtros = COALESCE(filtros, JSON_OBJECT());

SELECT CONSTRAINT_NAME INTO @reportes_empresa_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'reportes_personalizados'
  AND COLUMN_NAME = 'empresa_id'
  AND REFERENCED_TABLE_NAME = 'empresas'
LIMIT 1;

SET @sql := IF(@reportes_empresa_fk IS NOT NULL,
    CONCAT('ALTER TABLE reportes_personalizados DROP FOREIGN KEY ', @reportes_empresa_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE reportes_personalizados
    ADD CONSTRAINT fk_reportes_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id);

-- Tabla relacional de calibraciones y planes
CREATE TABLE IF NOT EXISTS calibraciones_planes (
    calibracion_id INT,
    plan_id INT,
    PRIMARY KEY (calibracion_id, plan_id),
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id),
    FOREIGN KEY (plan_id) REFERENCES planes(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS certificados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT,
    archivo VARCHAR(255),
    tipo VARCHAR(20),
    hash VARCHAR(64),
    fecha_subida DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS backups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    archivo VARCHAR(255),
    estado VARCHAR(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


SELECT COUNT(*) INTO @audit_trail_exists
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail';

CREATE TABLE IF NOT EXISTS audit_trail (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL DEFAULT 0,
    segmento_actor VARCHAR(50) NOT NULL DEFAULT 'Administradores',
    fecha_evento DATETIME DEFAULT NULL,
    seccion VARCHAR(150) NOT NULL,
    valor_anterior TEXT DEFAULT NULL,
    valor_nuevo TEXT DEFAULT NULL,
    usuario_id INT DEFAULT NULL,
    usuario_correo VARCHAR(150) NOT NULL,
    usuario_nombre VARCHAR(150) NOT NULL DEFAULT 'Administrador general',
    usuario_firma_interna VARCHAR(150) DEFAULT NULL,
    instrumento_codigo VARCHAR(50) DEFAULT NULL,
    columna_excel VARCHAR(10) DEFAULT NULL,
    fila_excel INT DEFAULT NULL,
    INDEX idx_audit_fecha_evento (fecha_evento),
    INDEX idx_audit_seccion (seccion),
    INDEX idx_audit_usuario_correo (usuario_correo),
    INDEX idx_audit_empresa (empresa_id),
    INDEX idx_audit_segmento (segmento_actor),
    INDEX idx_audit_empresa_segmento (empresa_id, segmento_actor),
    INDEX idx_audit_usuario_firma (usuario_firma_interna),
    INDEX idx_audit_instrumento (instrumento_codigo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Recalcular la existencia de la tabla después de crearla para que los
-- ajustes posteriores también se apliquen en instalaciones nuevas.
SELECT COUNT(*) INTO @audit_trail_exists
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail';

SELECT CONSTRAINT_NAME INTO @audit_trail_role_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND REFERENCED_TABLE_NAME = 'roles'
LIMIT 1;

SET @sql := IF(@audit_trail_exists > 0 AND @audit_trail_role_fk IS NOT NULL,
    CONCAT('ALTER TABLE audit_trail DROP FOREIGN KEY ', @audit_trail_role_fk),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @fk_audit_usuario
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND COLUMN_NAME = 'usuario_id'
  AND REFERENCED_TABLE_NAME = 'usuarios'
LIMIT 1;

SET @sql := IF(@audit_trail_exists > 0 AND @fk_audit_usuario IS NOT NULL,
    CONCAT('ALTER TABLE audit_trail DROP FOREIGN KEY ', @fk_audit_usuario),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT CONSTRAINT_NAME INTO @fk_audit_instrumento
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND COLUMN_NAME = 'instrumento_id'
  AND REFERENCED_TABLE_NAME = 'instrumentos'
LIMIT 1;

SET @sql := IF(@audit_trail_exists > 0 AND @fk_audit_instrumento IS NOT NULL,
    CONCAT('ALTER TABLE audit_trail DROP FOREIGN KEY ', @fk_audit_instrumento),
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@audit_trail_exists > 0,
    'ALTER TABLE audit_trail
        DROP COLUMN IF EXISTS registrado_en,
        DROP COLUMN IF EXISTS role_id,
        DROP COLUMN IF EXISTS empresa_id,
        DROP COLUMN IF EXISTS fecha,
        DROP COLUMN IF EXISTS hora,
        DROP COLUMN IF EXISTS actividad,
        DROP COLUMN IF EXISTS relevancia,
        DROP COLUMN IF EXISTS fecha_creacion,
        DROP COLUMN IF EXISTS rango_referencia,
        DROP COLUMN IF EXISTS nombre_usuario,
        DROP COLUMN IF EXISTS correo_usuario,
        DROP COLUMN IF EXISTS entidad,
        DROP COLUMN IF EXISTS entidad_id,
        DROP COLUMN IF EXISTS detalle,
        DROP COLUMN IF EXISTS comentario,
        DROP COLUMN IF EXISTS accion,
        DROP COLUMN IF EXISTS timestamp,
        DROP COLUMN IF EXISTS valores_antes,
        DROP COLUMN IF EXISTS valores_despues,
        DROP COLUMN IF EXISTS ip,
        DROP COLUMN IF EXISTS user_agent',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@audit_trail_exists > 0,
    'ALTER TABLE audit_trail
        ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL DEFAULT 0 AFTER id,
        ADD COLUMN IF NOT EXISTS segmento_actor VARCHAR(50) NOT NULL DEFAULT ''Administradores'' AFTER empresa_id,
        ADD COLUMN IF NOT EXISTS fecha_evento DATETIME DEFAULT NULL,
        ADD COLUMN IF NOT EXISTS seccion VARCHAR(150) NOT NULL,
        ADD COLUMN IF NOT EXISTS valor_anterior TEXT NULL,
        ADD COLUMN IF NOT EXISTS valor_nuevo TEXT NULL,
        ADD COLUMN IF NOT EXISTS usuario_id INT NULL,
        ADD COLUMN IF NOT EXISTS usuario_correo VARCHAR(150) NOT NULL,
        ADD COLUMN IF NOT EXISTS usuario_nombre VARCHAR(150) NOT NULL DEFAULT ''Administrador general'',
        ADD COLUMN IF NOT EXISTS usuario_firma_interna VARCHAR(150) DEFAULT NULL AFTER usuario_nombre,
        ADD COLUMN IF NOT EXISTS instrumento_codigo VARCHAR(50) DEFAULT NULL AFTER usuario_firma_interna,
        ADD COLUMN IF NOT EXISTS columna_excel VARCHAR(10) DEFAULT NULL AFTER instrumento_codigo,
        ADD COLUMN IF NOT EXISTS fila_excel INT DEFAULT NULL AFTER columna_excel',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@audit_trail_exists > 0,
    'ALTER TABLE audit_trail
        MODIFY COLUMN empresa_id INT NOT NULL DEFAULT 0,
        MODIFY COLUMN segmento_actor VARCHAR(50) NOT NULL DEFAULT ''Administradores'',
        MODIFY COLUMN fecha_evento DATETIME DEFAULT NULL,
        MODIFY COLUMN seccion VARCHAR(150) NOT NULL,
        MODIFY COLUMN valor_anterior TEXT NULL,
        MODIFY COLUMN valor_nuevo TEXT NULL,
        MODIFY COLUMN usuario_id INT NULL,
        MODIFY COLUMN usuario_correo VARCHAR(150) NOT NULL,
        MODIFY COLUMN usuario_nombre VARCHAR(150) NOT NULL DEFAULT ''Administrador general'',
        MODIFY COLUMN usuario_firma_interna VARCHAR(150) DEFAULT NULL',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@audit_trail_exists > 0,
    'UPDATE audit_trail
     SET empresa_id = 1
     WHERE empresa_id IS NULL OR empresa_id = 0',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@audit_trail_exists > 0,
    'UPDATE audit_trail
     SET usuario_nombre = ''Administrador general''
     WHERE usuario_nombre IS NULL OR usuario_nombre = ''''',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_registrado
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_registrado_en';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_registrado > 0,
    'ALTER TABLE audit_trail DROP INDEX idx_audit_registrado_en',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_instrumento
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_instrumento';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_instrumento > 0,
    'ALTER TABLE audit_trail DROP INDEX idx_audit_instrumento',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_fecha
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_fecha_evento';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_fecha = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_fecha_evento (fecha_evento)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_seccion
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_seccion';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_seccion = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_seccion (seccion)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_correo
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_usuario_correo';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_correo = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_usuario_correo (usuario_correo)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_empresa
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_empresa';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_empresa = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_empresa (empresa_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_segmento
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_segmento';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_segmento = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_segmento (segmento_actor)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_empresa_segmento
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_empresa_segmento';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_empresa_segmento = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_empresa_segmento (empresa_id, segmento_actor)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @audit_idx_usuario_firma
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'audit_trail'
  AND INDEX_NAME = 'idx_audit_usuario_firma';

SET @sql := IF(@audit_trail_exists > 0 AND @audit_idx_usuario_firma = 0,
    'ALTER TABLE audit_trail ADD INDEX idx_audit_usuario_firma (usuario_firma_interna)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Tabla de firmas internas de usuarios
CREATE TABLE IF NOT EXISTS usuario_firmas_internas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL DEFAULT 0,
    usuario_id INT NOT NULL,
    correo VARCHAR(150) DEFAULT NULL,
    firma_interna VARCHAR(150) NOT NULL,
    vigente_desde DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vigente_hasta DATETIME DEFAULT NULL,
    creado_por INT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_firmas_usuario (usuario_id),
    INDEX idx_firmas_empresa (empresa_id),
    INDEX idx_firmas_correo (correo),
    INDEX idx_firmas_vigencia (vigente_desde, vigente_hasta)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT COUNT(*) INTO @firmas_internas_exists
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuario_firmas_internas';

SET @sql := IF(@firmas_internas_exists > 0,
    'ALTER TABLE usuario_firmas_internas
        MODIFY COLUMN empresa_id INT NOT NULL DEFAULT 0,
        MODIFY COLUMN usuario_id INT NOT NULL,
        MODIFY COLUMN correo VARCHAR(150) DEFAULT NULL,
        MODIFY COLUMN firma_interna VARCHAR(150) NOT NULL,
        MODIFY COLUMN vigente_desde DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        MODIFY COLUMN vigente_hasta DATETIME DEFAULT NULL,
        MODIFY COLUMN creado_por INT DEFAULT NULL,
        MODIFY COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@firmas_internas_exists > 0,
    'ALTER TABLE usuario_firmas_internas
        ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL DEFAULT 0 AFTER id,
        ADD COLUMN IF NOT EXISTS usuario_id INT NOT NULL AFTER empresa_id,
        ADD COLUMN IF NOT EXISTS correo VARCHAR(150) DEFAULT NULL AFTER usuario_id,
        ADD COLUMN IF NOT EXISTS firma_interna VARCHAR(150) NOT NULL AFTER correo,
        ADD COLUMN IF NOT EXISTS vigente_desde DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER firma_interna,
        ADD COLUMN IF NOT EXISTS vigente_hasta DATETIME DEFAULT NULL AFTER vigente_desde,
        ADD COLUMN IF NOT EXISTS creado_por INT DEFAULT NULL AFTER vigente_hasta,
        ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER creado_por,
        ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@firmas_internas_exists > 0,
    'ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_usuario (usuario_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@firmas_internas_exists > 0,
    'ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_empresa (empresa_id)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@firmas_internas_exists > 0,
    'ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_correo (correo)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(@firmas_internas_exists > 0,
    'ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_vigencia (vigente_desde, vigente_hasta)',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SELECT COUNT(*) INTO @firmas_fk
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'usuario_firmas_internas'
  AND REFERENCED_TABLE_NAME = 'usuarios';

SET @sql := IF(@firmas_internas_exists > 0 AND (@firmas_fk IS NULL OR @firmas_fk = 0),
    'ALTER TABLE usuario_firmas_internas ADD CONSTRAINT fk_usuario_firmas_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE',
    'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Actualizar vista de planeación
DROP VIEW IF EXISTS vista_planeacion_instrumentos;
CREATE VIEW vista_planeacion_instrumentos AS
SELECT
    i.id AS instrumento_id,
    i.empresa_id AS empresa_id,
    ci.nombre AS instrumento,
    i.codigo,
    m.nombre AS marca,
    mo.nombre AS modelo,
    i.serie,
    d.nombre AS departamento,
    i.ubicacion,
    i.proxima_calibracion AS fecha_proxima,
    ult.fecha_programada,
    ult.responsable_id,
    u.usuario AS responsable,
    CASE WHEN ult.instrumento_id IS NULL THEN 0 ELSE 1 END AS tiene_plan,
    ult.estado_etiqueta AS estado_plan,
    ult.estado_etiqueta AS estado_descriptivo,
    ult.estado_clave,
    DATEDIFF(i.proxima_calibracion, CURDATE()) AS dias_restantes,
    i.estado AS estado_instrumento,
    ult.instrucciones_cliente
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN marcas m ON i.marca_id = m.id
LEFT JOIN modelos mo ON i.modelo_id = mo.id
LEFT JOIN departamentos d ON i.departamento_id = d.id
LEFT JOIN (
    SELECT
        datos.instrumento_id,
        datos.empresa_id,
        datos.fecha_programada,
        datos.responsable_id,
        datos.estado_clave,
        datos.estado_etiqueta,
        datos.instrucciones_cliente
    FROM (
        SELECT
            sub.instrumento_id,
            sub.empresa_id,
            sub.fecha_programada,
            sub.responsable_id,
            CASE
                WHEN sub.estado_normalizado IS NULL OR sub.estado_normalizado = '' THEN 'programada'
                WHEN sub.estado_normalizado IN ('en curso', 'en_curso', 'encurso', 'en ejecucion', 'enejecucion', 'ejecucion', 'en proceso', 'enproceso', 'proceso', 'curso') THEN 'en_curso'
                WHEN sub.estado_normalizado IN ('completada', 'completado', 'finalizada', 'finalizado', 'terminada', 'terminado', 'cerrada', 'cerrado', 'concluida', 'concluido', 'ejecutada', 'ejecutado') THEN 'completada'
                WHEN sub.estado_normalizado IN ('cancelada', 'cancelado', 'anulada', 'anulado', 'suspendida', 'suspendido', 'detenida', 'detenido') THEN 'cancelada'
                ELSE 'programada'
            END AS estado_clave,
            CASE
                WHEN sub.estado_normalizado IS NULL OR sub.estado_normalizado = '' THEN 'Programada'
                WHEN sub.estado_normalizado IN ('en curso', 'en_curso', 'encurso', 'en ejecucion', 'enejecucion', 'ejecucion', 'en proceso', 'enproceso', 'proceso', 'curso') THEN 'En curso'
                WHEN sub.estado_normalizado IN ('completada', 'completado', 'finalizada', 'finalizado', 'terminada', 'terminado', 'cerrada', 'cerrado', 'concluida', 'concluido', 'ejecutada', 'ejecutado') THEN 'Completada'
                WHEN sub.estado_normalizado IN ('cancelada', 'cancelado', 'anulada', 'anulado', 'suspendida', 'suspendido', 'detenida', 'detenido') THEN 'Cancelada'
                ELSE 'Programada'
            END AS estado_etiqueta,
            sub.instrucciones_cliente
        FROM (
            SELECT
                p.instrumento_id,
                p.empresa_id,
                p.fecha_programada,
                COALESCE(oc.tecnico_id, p.responsable_id) AS responsable_id,
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        LOWER(
                                            TRIM(
                                                REPLACE(
                                                    REPLACE(
                                                        REPLACE(COALESCE(oc.estado_ejecucion, p.estado, 'Programada'), CHAR(10), ' '),
                                                        CHAR(13), ' '
                                                    ),
                                                    CHAR(9), ' '
                                                )
                                            )
                                        ),
                                        '-', ' '
                                    ),
                                    'á', 'a'
                                ),
                                'é', 'e'
                            ),
                            'í', 'i'
                        ),
                        'ó', 'o'
                    ),
                    'ú', 'u'
                ) AS estado_normalizado,
                p.instrucciones_cliente,
                ROW_NUMBER() OVER (PARTITION BY p.instrumento_id ORDER BY p.fecha_programada DESC, p.id DESC) AS rn
            FROM planes p
            LEFT JOIN ordenes_calibracion oc ON oc.plan_id = p.id AND oc.empresa_id = p.empresa_id
        ) AS sub
        WHERE sub.rn = 1
    ) AS datos
) AS ult ON ult.instrumento_id = i.id AND ult.empresa_id = i.empresa_id
LEFT JOIN usuarios u ON ult.responsable_id = u.id;

-- Registro de alertas de calibraciones enviadas
CREATE TABLE IF NOT EXISTS calibration_alert_notifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    due_date DATE NOT NULL,
    alert_type VARCHAR(20) NOT NULL DEFAULT 'upcoming',
    notified_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_alert (instrumento_id, empresa_id, due_date, alert_type),
    INDEX idx_alert_due_date (due_date),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS report_templates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT DEFAULT NULL,
    template_key VARCHAR(100) NOT NULL,
    spreadsheet_id VARCHAR(120) NOT NULL,
    sheet_id INT NOT NULL,
    title_pattern VARCHAR(255) DEFAULT NULL,
    sheet_title_pattern VARCHAR(255) DEFAULT NULL,
    config_json TEXT DEFAULT NULL,
    drive_folder_id VARCHAR(120) DEFAULT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_template_empresa (template_key, empresa_id),
    INDEX idx_template_empresa (empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS catalogo_estados_calidad (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    tipo ENUM('documento','version_documento','capacitacion','no_conformidad','accion_correctiva') NOT NULL,
    clave VARCHAR(50) NOT NULL,
    nombre VARCHAR(120) NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    es_final TINYINT(1) DEFAULT 0,
    UNIQUE KEY uq_estados_tipo_empresa (empresa_id, tipo, clave),
    INDEX idx_estados_tipo (tipo),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS catalogo_tipos_documento (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    UNIQUE KEY uq_tipos_documento (empresa_id, nombre),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS catalogo_categorias_capacitacion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    UNIQUE KEY uq_categorias_capacitacion (empresa_id, nombre),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS catalogo_clasificaciones_nc (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    clave VARCHAR(50) NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    UNIQUE KEY uq_clasificacion_nc (empresa_id, clave),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_documentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    titulo VARCHAR(255) NOT NULL,
    descripcion TEXT NULL,
    contenido LONGTEXT NULL,
    estado ENUM('borrador', 'publicado') NOT NULL DEFAULT 'borrador',
    publicado_en DATETIME NULL,
    publicado_por INT NULL,
    creado_por INT NULL,
    responsable_id INT NULL,
    responsable VARCHAR(150) NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_calidad_documentos_empresa_estado (empresa_id, estado),
    INDEX idx_calidad_documentos_publicado_por (publicado_por),
    INDEX idx_calidad_documentos_creado_por (creado_por),
    INDEX idx_calidad_documentos_responsable (responsable_id),
    CONSTRAINT fk_calidad_documentos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_documentos_publicado_por FOREIGN KEY (publicado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_documentos_creado_por FOREIGN KEY (creado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_documentos_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_documentos_revision (
    documento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    revisor_id INT NOT NULL,
    decision ENUM('en_revision', 'aprobado', 'rechazado') NOT NULL DEFAULT 'en_revision',
    comentarios TEXT NULL,
    revisado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (documento_id, empresa_id),
    KEY idx_calidad_doc_revision_revisor (revisor_id),
    CONSTRAINT fk_calidad_doc_revision_documento FOREIGN KEY (documento_id) REFERENCES calidad_documentos(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_doc_revision_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_doc_revision_revisor FOREIGN KEY (revisor_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_capacitaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    tema VARCHAR(255) NOT NULL,
    descripcion TEXT NULL,
    fecha_programada DATE NULL,
    fecha_ejecucion DATE NULL,
    duracion_horas DECIMAL(5,2) NULL,
    evidencias TEXT NULL,
    responsable_id INT NULL,
    responsable VARCHAR(150) NULL,
    estado ENUM('borrador', 'publicado') NOT NULL DEFAULT 'borrador',
    publicado_en DATETIME NULL,
    publicado_por INT NULL,
    creado_por INT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_calidad_capacitaciones_empresa_estado (empresa_id, estado),
    INDEX idx_calidad_capacitaciones_publicado_por (publicado_por),
    INDEX idx_calidad_capacitaciones_creado_por (creado_por),
    INDEX idx_calidad_capacitaciones_responsable (responsable_id),
    CONSTRAINT fk_calidad_capacitaciones_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_capacitaciones_publicado_por FOREIGN KEY (publicado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_capacitaciones_creado_por FOREIGN KEY (creado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_capacitaciones_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_capacitaciones_participantes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    capacitacion_id INT NOT NULL,
    participante_id INT NOT NULL,
    asistencia TINYINT(1) NOT NULL DEFAULT 0,
    calificacion DECIMAL(5,2) DEFAULT NULL,
    comentarios VARCHAR(255) DEFAULT NULL,
    registrado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_calidad_participante_capacitacion (capacitacion_id, participante_id),
    INDEX idx_calidad_participantes_participante (participante_id),
    CONSTRAINT fk_calidad_participantes_capacitacion FOREIGN KEY (capacitacion_id) REFERENCES calidad_capacitaciones(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_participantes_usuario FOREIGN KEY (participante_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_no_conformidades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    codigo VARCHAR(100) DEFAULT NULL,
    clasificacion_id INT DEFAULT NULL,
    titulo VARCHAR(255) NOT NULL,
    descripcion TEXT NULL,
    causa_raiz TEXT NULL,
    acciones TEXT NULL,
    responsable_id INT NULL,
    responsable VARCHAR(150) NULL,
    estado ENUM('abierta', 'en_proceso', 'cerrada') NOT NULL DEFAULT 'abierta',
    reportado_por INT NULL,
    publicado_en DATETIME NULL,
    publicado_por INT NULL,
    creado_por INT NULL,
    detectado_en DATE NULL,
    cerrado_en DATETIME NULL,
    cerrado_por INT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_calidad_nc_codigo_empresa (empresa_id, codigo),
    INDEX idx_calidad_nc_empresa_estado (empresa_id, estado),
    INDEX idx_calidad_nc_reportado_por (reportado_por),
    INDEX idx_calidad_nc_responsable (responsable_id),
    CONSTRAINT fk_calidad_nc_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_nc_clasificacion FOREIGN KEY (clasificacion_id) REFERENCES catalogo_clasificaciones_nc(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_nc_reportado_por FOREIGN KEY (reportado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_nc_publicado_por FOREIGN KEY (publicado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_nc_creado_por FOREIGN KEY (creado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_nc_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_nc_cerrado_por FOREIGN KEY (cerrado_por) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_acciones_correctivas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    no_conformidad_id INT NOT NULL,
    descripcion TEXT NOT NULL,
    estado ENUM('pendiente', 'en_proceso', 'cerrada') NOT NULL DEFAULT 'pendiente',
    responsable_id INT NULL,
    responsable VARCHAR(150) NULL,
    fecha_compromiso DATE DEFAULT NULL,
    fecha_cierre DATE DEFAULT NULL,
    resultado TEXT NULL,
    seguimiento TEXT NULL,
    publicado_en DATETIME NULL,
    publicado_por INT NULL,
    creado_por INT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_calidad_acciones_nc (no_conformidad_id),
    INDEX idx_calidad_acciones_estado (estado),
    INDEX idx_calidad_acciones_responsable (responsable_id),
    CONSTRAINT fk_calidad_acciones_nc FOREIGN KEY (no_conformidad_id) REFERENCES calidad_no_conformidades(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_acciones_publicado_por FOREIGN KEY (publicado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_acciones_creado_por FOREIGN KEY (creado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_acciones_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS report_template_exports (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario_id INT NOT NULL,
    empresa_id INT DEFAULT NULL,
    template_id INT NOT NULL,
    template_key VARCHAR(100) NOT NULL,
    year_value INT NOT NULL,
    revision_number INT NOT NULL,
    firmas_json TEXT,
    parametros_json TEXT,
    drive_file_id VARCHAR(120) NOT NULL,
    drive_file_url VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rte_usuario (usuario_id),
    INDEX idx_rte_empresa (empresa_id),
    INDEX idx_rte_template (template_key),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (template_id) REFERENCES report_templates(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS feedback_reports (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT DEFAULT NULL,
    reporter_id INT NOT NULL,
    origen ENUM('internal','tenant') NOT NULL DEFAULT 'internal',
    tipo ENUM('error','sugerencia') NOT NULL,
    resumen VARCHAR(255) NOT NULL,
    descripcion TEXT,
    estado ENUM('abierto','en_progreso','cerrado') NOT NULL DEFAULT 'abierto',
    prioridad ENUM('baja','media','alta','critica') NOT NULL DEFAULT 'media',
    asignado_a INT DEFAULT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_feedback_empresa (empresa_id),
    INDEX idx_feedback_estado (estado),
    INDEX idx_feedback_prioridad (prioridad),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE SET NULL,
    FOREIGN KEY (reporter_id) REFERENCES usuarios(id) ON DELETE CASCADE,
    FOREIGN KEY (asignado_a) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_notifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    titulo VARCHAR(255) NOT NULL,
    mensaje TEXT NOT NULL,
    tipo VARCHAR(50) DEFAULT 'general',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    read_at DATETIME DEFAULT NULL,
    INDEX idx_user_notifications_user (user_id),
    INDEX idx_user_notifications_read (read_at),
    FOREIGN KEY (user_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS feedback_attachments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    feedback_id INT NOT NULL,
    nombre_original VARCHAR(255) NOT NULL,
    archivo VARCHAR(255) NOT NULL,
    mime_type VARCHAR(120) DEFAULT NULL,
    tamano BIGINT DEFAULT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_feedback_attachment_feedback (feedback_id),
    FOREIGN KEY (feedback_id) REFERENCES feedback_reports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
