<?php

require_once dirname(__DIR__, 2) . '/Core/db_config.php';
require_once dirname(__DIR__, 2) . '/Core/helpers/company_provisioning.php';

// Permite ejecutar este script tanto de forma independiente como incluida
if (!isset($conn)) {
    require_once dirname(__DIR__, 2) . '/Core/db.php';
    $standalone = true;
} else {
    $standalone = false;
}

// ------------------------------------------------------------
// Tablas básicas
// ------------------------------------------------------------

// Tabla de portales
$conn->query("CREATE TABLE IF NOT EXISTS portals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    slug VARCHAR(60) NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(255) DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS portal_domains (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("UPDATE portal_domains SET domain = LOWER(TRIM(domain)) WHERE domain <> LOWER(TRIM(domain))");

$columnCheck = $conn->query("SHOW COLUMNS FROM portal_domains LIKE 'is_active'");
if ($columnCheck && $columnCheck->num_rows === 0) {
    $conn->query("ALTER TABLE portal_domains ADD COLUMN is_active TINYINT(1) NOT NULL DEFAULT 1 AFTER portal_id");
}
if ($columnCheck instanceof mysqli_result) {
    $columnCheck->close();
}

$columnCheck = $conn->query("SHOW COLUMNS FROM portal_domains LIKE 'is_primary'");
if ($columnCheck && $columnCheck->num_rows === 0) {
    $conn->query("ALTER TABLE portal_domains ADD COLUMN is_primary TINYINT(1) NOT NULL DEFAULT 0 AFTER is_active");
}
if ($columnCheck instanceof mysqli_result) {
    $columnCheck->close();
}

$indexResult = $conn->query("SHOW INDEX FROM portal_domains WHERE Key_name = 'uniq_portal_domains_domain'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("ALTER TABLE portal_domains ADD UNIQUE KEY uniq_portal_domains_domain (domain)");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$indexResult = $conn->query("SHOW INDEX FROM portal_domains WHERE Key_name = 'idx_portal_domains_portal'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("CREATE INDEX idx_portal_domains_portal ON portal_domains(portal_id, is_active)");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$fkResult = $conn->query(
    "SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS "
    . "WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'portal_domains' "
    . "AND REFERENCED_TABLE_NAME = 'portals'"
);
if ($fkResult && $fkResult->num_rows === 0) {
    $conn->query("ALTER TABLE portal_domains ADD CONSTRAINT fk_portal_domains_portal FOREIGN KEY (portal_id) REFERENCES portals(id) ON DELETE CASCADE");
}
if ($fkResult instanceof mysqli_result) {
    $fkResult->close();
}

$portalesIniciales = [
    ['id' => 1, 'slug' => 'internal', 'nombre' => 'Portal interno', 'descripcion' => 'Panel operativo para personal interno'],
    ['id' => 2, 'slug' => 'tenant', 'nombre' => 'Portal de clientes', 'descripcion' => 'Acceso para empresas cliente'],
    ['id' => 3, 'slug' => 'service', 'nombre' => 'Portal de servicio', 'descripcion' => 'Módulo de atención y soporte'],
];

foreach ($portalesIniciales as $portalData) {
    $stmt = $conn->prepare("INSERT INTO portals (id, slug, nombre, descripcion) VALUES (?, ?, ?, ?)"
        . " ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), descripcion = VALUES(descripcion)");
    if ($stmt) {
        $stmt->bind_param(
            'isss',
            $portalData['id'],
            $portalData['slug'],
            $portalData['nombre'],
            $portalData['descripcion']
        );
        $stmt->execute();
        $stmt->close();
    }
}

// Tabla de roles
$conn->query("CREATE TABLE IF NOT EXISTS roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    empresa_id INT DEFAULT NULL,
    delegated TINYINT(1) NOT NULL DEFAULT 0,
    portal_id INT DEFAULT NULL,
    UNIQUE KEY uniq_roles_empresa_nombre (empresa_id, nombre),
    KEY idx_roles_portal (portal_id),
    CONSTRAINT fk_roles_portal FOREIGN KEY (portal_id) REFERENCES portals(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$result = $conn->query("SHOW COLUMNS FROM roles LIKE 'empresa_id'");
if ($result && $result->num_rows === 0) {
    $conn->query("ALTER TABLE roles ADD COLUMN empresa_id INT DEFAULT NULL");
}
if ($result instanceof mysqli_result) {
    $result->close();
}

$result = $conn->query("SHOW COLUMNS FROM roles LIKE 'portal_id'");
if ($result && $result->num_rows === 0) {
    $conn->query("ALTER TABLE roles ADD COLUMN portal_id INT DEFAULT NULL AFTER delegated");
}
if ($result instanceof mysqli_result) {
    $result->close();
}

$indexResult = $conn->query("SHOW INDEX FROM roles WHERE Key_name = 'idx_roles_portal'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("CREATE INDEX idx_roles_portal ON roles(portal_id)");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$fkPortal = $conn->query(
    "SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS "
    . "WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'roles' "
    . "AND REFERENCED_TABLE_NAME = 'portals'"
);
if ($fkPortal && $fkPortal->num_rows === 0) {
    $conn->query("ALTER TABLE roles ADD CONSTRAINT fk_roles_portal FOREIGN KEY (portal_id) REFERENCES portals(id)");
}
if ($fkPortal instanceof mysqli_result) {
    $fkPortal->close();
}

$result = $conn->query("SHOW COLUMNS FROM roles LIKE 'delegated'");
if ($result && $result->num_rows === 0) {
    $conn->query("ALTER TABLE roles ADD COLUMN delegated TINYINT(1) NOT NULL DEFAULT 0");
}
if ($result instanceof mysqli_result) {
    $result->close();
}

$result = $conn->query("SHOW INDEX FROM roles WHERE Column_name = 'nombre' AND Key_name NOT IN ('PRIMARY','uniq_roles_empresa_nombre')");
if ($result) {
    while ($row = $result->fetch_assoc()) {
        $keyName = $row['Key_name'] ?? '';
        if ($keyName !== '' && $keyName !== 'uniq_roles_empresa_nombre') {
            $conn->query("ALTER TABLE roles DROP INDEX `{$keyName}`");
        }
    }
    $result->close();
}

$result = $conn->query("SHOW INDEX FROM roles WHERE Key_name = 'uniq_roles_empresa_nombre'");
if ($result && $result->num_rows === 0) {
    $conn->query("ALTER TABLE roles ADD UNIQUE KEY uniq_roles_empresa_nombre (empresa_id, nombre)");
}
if ($result instanceof mysqli_result) {
    $result->close();
}

$portalMap = [];
$portalResult = $conn->query('SELECT id, slug FROM portals');
if ($portalResult instanceof mysqli_result) {
    while ($row = $portalResult->fetch_assoc()) {
        if (!isset($row['id'], $row['slug'])) {
            continue;
        }
        $portalMap[$row['slug']] = (int) $row['id'];
    }
    $portalResult->close();
}

$internalPortalId = $portalMap['internal'] ?? null;
$tenantPortalId   = $portalMap['tenant'] ?? null;
$servicePortalId  = $portalMap['service'] ?? null;

if ($internalPortalId !== null) {
    $conn->query("UPDATE roles SET portal_id = {$internalPortalId} WHERE portal_id IS NULL AND delegated = 0 AND (empresa_id IS NULL OR empresa_id = 0)");
}
if ($tenantPortalId !== null) {
    $conn->query("UPDATE roles SET portal_id = {$tenantPortalId} WHERE portal_id IS NULL AND (delegated = 1 OR nombre = 'Cliente')");
}
if ($servicePortalId !== null) {
    $conn->query("UPDATE roles SET portal_id = {$servicePortalId} WHERE portal_id IS NULL AND nombre = 'Sistemas'");
}
if ($internalPortalId !== null) {
    $conn->query("UPDATE roles SET portal_id = {$internalPortalId} WHERE portal_id IS NULL");
}

if (!empty($portalMap)) {
    $conn->query("UPDATE usuarios u JOIN roles r ON u.role_id = r.id SET u.portal_id = r.portal_id WHERE u.portal_id IS NULL AND r.portal_id IS NOT NULL");
}

if (!empty($portalMap)) {
    $defaultPortalDomains = [
        ['domain' => 'internal.local', 'slug' => 'internal', 'is_primary' => 1],
        ['domain' => 'clientes.local', 'slug' => 'tenant', 'is_primary' => 1],
        ['domain' => 'service.local', 'slug' => 'service', 'is_primary' => 0],
    ];

    foreach ($defaultPortalDomains as $domainSeed) {
        $slug = $domainSeed['slug'];
        $domain = strtolower(trim($domainSeed['domain']));
        if ($domain === '' || !isset($portalMap[$slug])) {
            continue;
        }

        $portalId = (int) $portalMap[$slug];
        $isPrimary = (int) ($domainSeed['is_primary'] ?? 0);

        $stmt = $conn->prepare(
            "INSERT INTO portal_domains (domain, portal_id, is_primary, is_active) "
            . "VALUES (?, ?, ?, 1) ON DUPLICATE KEY UPDATE portal_id = VALUES(portal_id), "
            . "is_primary = VALUES(is_primary), is_active = VALUES(is_active)"
        );

        if (!$stmt) {
            continue;
        }

        $stmt->bind_param('sii', $domain, $portalId, $isPrimary);
        $stmt->execute();
        $stmt->close();
    }
}

// Tabla de permisos
$conn->query("CREATE TABLE IF NOT EXISTS permissions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    descripcion VARCHAR(250)
);");

// Relación rol-permiso
$conn->query("CREATE TABLE IF NOT EXISTS role_permissions (
    role_id INT,
    permission_id INT,
    PRIMARY KEY (role_id, permission_id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id)
);");

// Roles delegados por empresa
$conn->query("CREATE TABLE IF NOT EXISTS tenant_roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_tenant_roles_empresa_nombre (empresa_id, nombre),
    INDEX idx_tenant_roles_empresa (empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE
);");

// Permisos asociados a los roles delegados
$conn->query("CREATE TABLE IF NOT EXISTS tenant_role_permissions (
    tenant_role_id INT NOT NULL,
    permission_id INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_role_id, permission_id),
    FOREIGN KEY (tenant_role_id) REFERENCES tenant_roles(id) ON DELETE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE
);");

// Asignación de roles delegados a usuarios
$conn->query("CREATE TABLE IF NOT EXISTS tenant_user_roles (
    usuario_id INT NOT NULL,
    tenant_role_id INT NOT NULL,
    assigned_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (usuario_id, tenant_role_id),
    INDEX idx_tenant_user_roles_usuario (usuario_id),
    INDEX idx_tenant_user_roles_role (tenant_role_id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_role_id) REFERENCES tenant_roles(id) ON DELETE CASCADE
);");

// Tabla de empresas
$conn->query("CREATE TABLE IF NOT EXISTS empresas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(150) NOT NULL,
    contacto VARCHAR(100),
    direccion VARCHAR(250),
    telefono VARCHAR(50),
    email VARCHAR(150),
    responsable_calidad_id INT DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
);");

$result = $conn->query(
    "SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS "
    . "WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'roles' "
    . "AND REFERENCED_TABLE_NAME = 'empresas'"
);
if ($result && $result->num_rows === 0) {
    $conn->query("ALTER TABLE roles ADD CONSTRAINT fk_roles_empresas FOREIGN KEY (empresa_id) REFERENCES empresas(id)");
}
if ($result instanceof mysqli_result) {
    $result->close();
}

// Tabla de usuarios con relación a empresas y roles
$conn->query("CREATE TABLE IF NOT EXISTS usuarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario VARCHAR(100) UNIQUE NOT NULL,
    correo VARCHAR(100) UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100),
    puesto VARCHAR(150) NOT NULL DEFAULT 'Sin especificar',
    telefono VARCHAR(50) DEFAULT NULL,
    contrasena VARCHAR(255) NOT NULL,
    empresa_id INT NOT NULL,
    role_id INT NOT NULL,
    portal_id INT DEFAULT NULL,
    activo TINYINT(1) DEFAULT 1,
    sso TINYINT(1) DEFAULT 0,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (role_id) REFERENCES roles(id),
    FOREIGN KEY (portal_id) REFERENCES portals(id)
);");

$conn->query("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS responsable_calidad_id INT DEFAULT NULL AFTER email;");

$result = $conn->query("SHOW COLUMNS FROM usuarios LIKE 'portal_id'");
if ($result && $result->num_rows === 0) {
    $conn->query("ALTER TABLE usuarios ADD COLUMN portal_id INT DEFAULT NULL AFTER role_id");
}
if ($result instanceof mysqli_result) {
    $result->close();
}

$indexResult = $conn->query("SHOW INDEX FROM usuarios WHERE Key_name = 'idx_usuarios_portal'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("CREATE INDEX idx_usuarios_portal ON usuarios(portal_id)");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$fkUsuarioPortal = $conn->query(
    "SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS "
    . "WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'usuarios' "
    . "AND REFERENCED_TABLE_NAME = 'portals'"
);
if ($fkUsuarioPortal && $fkUsuarioPortal->num_rows === 0) {
    $conn->query("ALTER TABLE usuarios ADD CONSTRAINT fk_usuarios_portal FOREIGN KEY (portal_id) REFERENCES portals(id)");
}
if ($fkUsuarioPortal instanceof mysqli_result) {
    $fkUsuarioPortal->close();
}

$fkQuality = $conn->query("SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'empresas' AND COLUMN_NAME = 'responsable_calidad_id' AND REFERENCED_TABLE_NAME = 'usuarios'");
if ($fkQuality && $fkQuality->num_rows === 0) {
    $conn->query("ALTER TABLE empresas ADD CONSTRAINT fk_empresas_responsable FOREIGN KEY (responsable_calidad_id) REFERENCES usuarios(id)");
}
if ($fkQuality instanceof mysqli_result) {
    $fkQuality->close();
}

// Conversaciones de mensajería entre empresas y soporte
$conn->query("CREATE TABLE IF NOT EXISTS tenant_conversations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    asunto VARCHAR(180) NOT NULL,
    estado VARCHAR(30) NOT NULL DEFAULT 'abierto',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Mensajes asociados a las conversaciones
$conn->query("CREATE TABLE IF NOT EXISTS tenant_messages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    conversation_id INT NOT NULL,
    autor_id INT,
    autor_tipo VARCHAR(20) NOT NULL,
    mensaje TEXT NOT NULL,
    adjuntos LONGTEXT,
    leido_empresa TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES tenant_conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (autor_id) REFERENCES usuarios(id)
);");

$indexResult = $conn->query("SHOW INDEX FROM tenant_messages WHERE Key_name = 'idx_tenant_messages_conversation'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("CREATE INDEX idx_tenant_messages_conversation ON tenant_messages(conversation_id);");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$indexResult = $conn->query("SHOW INDEX FROM tenant_messages WHERE Key_name = 'idx_tenant_messages_unread'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("CREATE INDEX idx_tenant_messages_unread ON tenant_messages(conversation_id, leido_empresa);");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$conn->query("CREATE TABLE IF NOT EXISTS tenant_roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    nombre_visible VARCHAR(150) NOT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_tenant_roles_empresa_nombre (empresa_id, nombre),
    INDEX idx_tenant_roles_empresa (empresa_id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS tenant_role_permissions (
    tenant_role_id INT NOT NULL,
    permission_id INT NOT NULL,
    PRIMARY KEY (tenant_role_id, permission_id),
    INDEX idx_tenant_role_permissions_permission (permission_id),
    FOREIGN KEY (tenant_role_id) REFERENCES tenant_roles(id) ON DELETE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES permissions(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Los roles delegados y roles tenant base por empresa se gestionan mediante helper reutilizable.

// ------------------------------------------------------------
// Datos iniciales
// ------------------------------------------------------------

// Inserta roles iniciales solo si la tabla está vacía
$result = $conn->query("SELECT COUNT(*) AS count FROM roles");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO roles (id, nombre) VALUES
        (1, 'Superadministrador'),
        (2, 'Administrador'),
        (3, 'Supervisor'),
        (4, 'Operador'),
        (5, 'Lector'),
        (6, 'Cliente'),
        (7, 'Sistemas'),
        (8, 'Developer')");
}

// Inserta empresa por defecto si no existe
$result = $conn->query("SELECT COUNT(*) AS count FROM empresas");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO empresas (nombre) VALUES ('Empresa Demo')");
}
$conn->query("INSERT INTO empresas (id, nombre) VALUES (1, 'Empresa Demo') ON DUPLICATE KEY UPDATE nombre = VALUES(nombre)");
$conn->query("INSERT INTO empresas (id, nombre) VALUES (2, 'Laboratorio Norte') ON DUPLICATE KEY UPDATE nombre = VALUES(nombre)");

if ($internalPortalId !== null) {
    $empresasDeveloper = $conn->query('SELECT id FROM empresas');
    if ($empresasDeveloper instanceof mysqli_result) {
        while ($empresa = $empresasDeveloper->fetch_assoc()) {
            if (!isset($empresa['id'])) {
                continue;
            }

            $empresaId = (int) $empresa['id'];
            if ($empresaId <= 0) {
                continue;
            }

            $stmtDeveloper = $conn->prepare(
                "INSERT INTO roles (nombre, empresa_id, delegated, portal_id) VALUES ('Developer', ?, 0, ?) "
                . "ON DUPLICATE KEY UPDATE portal_id = VALUES(portal_id), delegated = VALUES(delegated)"
            );

            if ($stmtDeveloper) {
                $stmtDeveloper->bind_param('ii', $empresaId, $internalPortalId);
                $stmtDeveloper->execute();
                $stmtDeveloper->close();
            }
        }
        $empresasDeveloper->close();
    }
}

// Inserta usuarios iniciales si la tabla está vacía
$result = $conn->query("SELECT COUNT(*) AS count FROM usuarios");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query(<<<'SQL'
INSERT INTO usuarios (usuario, correo, nombre, apellidos, contrasena, role_id, empresa_id, portal_id) VALUES
    (
        'superadmin',
        'superadmin@ejemplo.com',
        'Super',
        'Admin',
        '$2y$12$fY.IW9CErKI.NZup05GmnOG70kRaY0gAt9CbAnyUCWl9e3HhLulna',
        (SELECT id FROM roles WHERE nombre='Superadministrador'),
        1,
        (SELECT id FROM portals WHERE slug='internal')
    ),
    (
        'admin',
        'admin@ejemplo.com',
        'Admin',
        'General',
        '$2y$12$/zbgQm3d8OkHaWfIjZifMuwcAoPHjM9Z5EXVFKmMGrzklTDwfhJFm',
        (SELECT id FROM roles WHERE nombre='Administrador'),
        1,
        (SELECT id FROM portals WHERE slug='internal')
    ),
    (
        'supervisor',
        'supervisor@ejemplo.com',
        'Super',
        'Visor',
        '$2y$12$mMSpZxB76p6uj7vvLoGsUOHNjZPPNEVMOuhIfAyA1/5mni/nYX1cC',
        (SELECT id FROM roles WHERE nombre='Supervisor'),
        1,
        (SELECT id FROM portals WHERE slug='internal')
    ),
    (
        'operador',
        'operador@ejemplo.com',
        'Ope',
        'Rador',
        '$2y$12$1oUy4C68eqwWicqyhhEwL.xiNGlHCT2GfNBgOluIJY6IEmdenSc.q',
        (SELECT id FROM roles WHERE nombre='Operador'),
        1,
        (SELECT id FROM portals WHERE slug='internal')
    ),
    (
        'lector',
        'lector@ejemplo.com',
        'Le',
        'Ctor',
        '$2y$12$lRI71LrYR.d7Bg8tIYrmy.hNgCr7lqhXTJHwESxTquIGdBYuovqqK',
        (SELECT id FROM roles WHERE nombre='Lector'),
        1,
        (SELECT id FROM portals WHERE slug='internal')
    ),
    (
        'cliente',
        'cliente@ejemplo.com',
        'Cli',
        'Ente',
        '$2y$12$dN6RpdMIyhsz5XjpkffcAeSPQwDHpBCEPflw86fjUqjF9/boKa1Di',
        (SELECT id FROM roles WHERE nombre='Cliente'),
        1,
        (SELECT id FROM portals WHERE slug='tenant')
    ),
    (
        'sistemas',
        'sistemas@ejemplo.com',
        'Si',
        'Stemas',
        '$2y$12$12IvTPzaMGon1W5rfDQB/.Y82T2vU/E2QHM7kTocp7Y/Gyl5tws7K',
        (SELECT id FROM roles WHERE nombre='Sistemas'),
        1,
        (SELECT id FROM portals WHERE slug='service')
    ),
    (
        'developer',
        'developer@ejemplo.com',
        'Dev',
        'Eloper',
        '$2y$12$wB.EkHW7s.vqXpZgabkt/.Xd1LP3/oGHbY7wavSas89CTx8LnhIES',
        (
            SELECT id
            FROM roles
            WHERE nombre = 'Developer'
              AND (empresa_id = 1 OR empresa_id IS NULL)
            ORDER BY empresa_id = 1 DESC, empresa_id IS NULL DESC
            LIMIT 1
        ),
        1,
        (SELECT id FROM portals WHERE slug='internal')
    )
SQL
    );
}

// Inserta permisos iniciales si la tabla está vacía
$result = $conn->query("SELECT COUNT(*) AS count FROM permissions");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO permissions (nombre, descripcion) VALUES
        ('instrumentos_crear', 'Crear instrumentos'),
        ('instrumentos_leer', 'Leer instrumentos'),
        ('instrumentos_actualizar', 'Actualizar instrumentos'),
        ('instrumentos_eliminar', 'Eliminar instrumentos'),
        ('calibraciones_crear', 'Crear calibraciones'),
        ('calibraciones_leer', 'Leer calibraciones'),
        ('calibraciones_actualizar', 'Actualizar calibraciones'),
        ('calibraciones_aprobar', 'Aprobar calibraciones'),
        ('calibraciones_eliminar', 'Eliminar calibraciones'),
        ('reportes_crear', 'Crear reportes'),
        ('reportes_leer', 'Leer reportes'),
        ('reportes_actualizar', 'Actualizar reportes'),
        ('reportes_eliminar', 'Eliminar reportes'),
        ('planeacion_crear', 'Crear planeaciones'),
        ('planeacion_leer', 'Leer planeaciones'),
        ('planeacion_actualizar', 'Actualizar planeaciones'),
        ('planeacion_eliminar', 'Eliminar planeaciones'),
        ('adjuntos_crear', 'Crear adjuntos'),
        ('adjuntos_leer', 'Leer adjuntos'),
        ('adjuntos_actualizar', 'Actualizar adjuntos'),
        ('adjuntos_eliminar', 'Eliminar adjuntos'),
        ('proveedores_crear', 'Crear proveedores'),
        ('proveedores_leer', 'Leer proveedores'),
        ('proveedores_actualizar', 'Actualizar proveedores'),
        ('proveedores_eliminar', 'Eliminar proveedores'),
        ('clientes_crear', 'Crear clientes'),
        ('clientes_leer', 'Leer clientes'),
        ('clientes_actualizar', 'Actualizar clientes'),
        ('clientes_eliminar', 'Eliminar clientes'),
        ('calidad_documentos_crear', 'Crear documentos de calidad'),
        ('calidad_documentos_leer', 'Leer documentos de calidad'),
        ('calidad_documentos_actualizar', 'Actualizar documentos de calidad'),
        ('calidad_documentos_eliminar', 'Eliminar documentos de calidad'),
        ('calidad_capacitaciones_crear', 'Crear capacitaciones de calidad'),
        ('calidad_capacitaciones_leer', 'Leer capacitaciones de calidad'),
        ('calidad_capacitaciones_actualizar', 'Actualizar capacitaciones de calidad'),
        ('calidad_capacitaciones_eliminar', 'Eliminar capacitaciones de calidad'),
        ('calidad_nc_crear', 'Registrar no conformidades'),
        ('calidad_nc_leer', 'Consultar no conformidades'),
        ('calidad_nc_actualizar', 'Actualizar no conformidades'),
        ('calidad_nc_eliminar', 'Eliminar no conformidades'),
        ('auditoria_crear', 'Registrar auditorías'),
        ('auditoria_leer', 'Consultar auditorías'),
        ('auditoria_actualizar', 'Actualizar auditorías'),
        ('auditoria_eliminar', 'Eliminar auditorías'),
        ('configuracion_leer', 'Ver configuración'),
        ('configuracion_actualizar', 'Actualizar configuración'),
        ('backups_view', 'Ver respaldos'),
        ('backups_create', 'Crear respaldos'),
        ('backups_restore', 'Restaurar respaldos'),
        ('mensajeria_leer', 'Consultar conversaciones de mensajería'),
        ('mensajeria_responder', 'Responder en mensajería'),
        ('usuarios_view', 'Ver usuarios'),
        ('usuarios_add', 'Agregar usuarios'),
        ('usuarios_edit', 'Editar usuarios'),
        ('usuarios_delete', 'Eliminar usuarios')
    ");
}

$calidadPermissions = [
    ['calidad_documentos_crear', 'Crear y actualizar documentos de calidad'],
    ['calidad_documentos_leer', 'Consultar documentos de calidad'],
    ['calidad_documentos_publicar', 'Publicar documentos de calidad'],
    ['calidad_capacitaciones_crear', 'Crear y actualizar capacitaciones'],
    ['calidad_capacitaciones_leer', 'Consultar capacitaciones de calidad'],
    ['calidad_capacitaciones_publicar', 'Publicar capacitaciones de calidad'],
    ['calidad_no_conformidades_crear', 'Registrar y actualizar no conformidades'],
    ['calidad_no_conformidades_leer', 'Consultar no conformidades'],
    ['calidad_no_conformidades_cerrar', 'Cerrar no conformidades'],
];

foreach ($calidadPermissions as [$permName, $permDesc]) {
    $stmt = $conn->prepare("INSERT INTO permissions (nombre, descripcion)
        SELECT ?, ? FROM DUAL WHERE NOT EXISTS (
            SELECT 1 FROM permissions WHERE nombre = ?
        )");
    if ($stmt) {
        $stmt->bind_param('sss', $permName, $permDesc, $permName);
        $stmt->execute();
        $stmt->close();
    }
}


// Superadministrador: todos los permisos
$conn->query("INSERT INTO role_permissions (role_id, permission_id)
    SELECT r.id, p.id
    FROM roles r CROSS JOIN permissions p
    WHERE r.nombre = 'Superadministrador'
      AND NOT EXISTS (
            SELECT 1 FROM role_permissions rp
            WHERE rp.role_id = r.id AND rp.permission_id = p.id
        )");

$calidadRoleAssignments = [
    'calidad_documentos_leer' => ['Administrador', 'Supervisor', 'Operador'],
    'calidad_documentos_crear' => ['Administrador', 'Supervisor'],
    'calidad_documentos_publicar' => ['Administrador', 'Supervisor'],
    'calidad_capacitaciones_leer' => ['Administrador', 'Supervisor', 'Operador'],
    'calidad_capacitaciones_crear' => ['Administrador', 'Supervisor'],
    'calidad_capacitaciones_publicar' => ['Administrador', 'Supervisor'],
    'calidad_no_conformidades_leer' => ['Administrador', 'Supervisor'],
    'calidad_no_conformidades_crear' => ['Administrador', 'Supervisor'],
    'calidad_no_conformidades_cerrar' => ['Administrador', 'Supervisor'],
];

foreach ($calidadRoleAssignments as $permName => $roles) {
    foreach ($roles as $roleName) {
        $stmt = $conn->prepare("INSERT INTO role_permissions (role_id, permission_id)
            SELECT r.id, p.id
            FROM roles r
            JOIN permissions p ON p.nombre = ?
            WHERE r.nombre = ?
              AND NOT EXISTS (
                    SELECT 1 FROM role_permissions rp
                    WHERE rp.role_id = r.id AND rp.permission_id = p.id
                )");
        if ($stmt) {
            $stmt->bind_param('ss', $permName, $roleName);
            $stmt->execute();
            $stmt->close();
        }
    }
}

// Sistemas: sólo permisos de usuarios
$conn->query("INSERT INTO role_permissions (role_id, permission_id)
    SELECT r.id, p.id
    FROM roles r
    JOIN permissions p ON p.nombre IN ('usuarios_view','usuarios_add','usuarios_edit','usuarios_delete','reportes_leer','mensajeria_leer','mensajeria_responder')
    WHERE r.nombre = 'Sistemas'
      AND NOT EXISTS (
            SELECT 1 FROM role_permissions rp
            WHERE rp.role_id = r.id AND rp.permission_id = p.id
        )");

// Developer: seguimiento técnico limitado a bitácoras
$conn->query("INSERT INTO role_permissions (role_id, permission_id)
    SELECT r.id, p.id
    FROM roles r
    JOIN permissions p ON p.nombre IN ('auditoria_leer','auditoria_crear')
    WHERE r.nombre = 'Developer'
      AND NOT EXISTS (
            SELECT 1 FROM role_permissions rp
            WHERE rp.role_id = r.id AND rp.permission_id = p.id
        )");

// Mensajería disponible para roles operativos y clientes
$conn->query("INSERT INTO role_permissions (role_id, permission_id)
    SELECT r.id, p.id
    FROM roles r
    JOIN permissions p ON p.nombre IN ('mensajeria_leer','mensajeria_responder')
    WHERE r.nombre IN ('Administrador','Supervisor','Operador','Lector','Cliente')
      AND NOT EXISTS (
            SELECT 1 FROM role_permissions rp
            WHERE rp.role_id = r.id AND rp.permission_id = p.id
        )");

// Provisionar roles delegados y tenant por empresa existente
$empresasProvision = $conn->query('SELECT id FROM empresas');
if ($empresasProvision instanceof mysqli_result) {
    while ($empresaRow = $empresasProvision->fetch_assoc()) {
        if (!isset($empresaRow['id'])) {
            continue;
        }
        $empresaId = (int) $empresaRow['id'];
        if ($empresaId <= 0) {
            continue;
        }
        company_provision_roles($conn, $empresaId);
    }
    $empresasProvision->close();
}

// ------------------------------------------------------------
// Tablas de instrumentos y catálogos
// ------------------------------------------------------------

// Tabla de departamentos
$conn->query("CREATE TABLE IF NOT EXISTS departamentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);");

// Tabla de marcas
$conn->query("CREATE TABLE IF NOT EXISTS marcas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);");

// Tabla de modelos
$conn->query("CREATE TABLE IF NOT EXISTS modelos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    marca_id INT,
    FOREIGN KEY (marca_id) REFERENCES marcas(id)
);");

// Catálogo de instrumentos (opciones para registro)
$conn->query("CREATE TABLE IF NOT EXISTS catalogo_instrumentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);");

// Tabla de instrumentos
$conn->query("CREATE TABLE IF NOT EXISTS instrumentos (
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
    estado VARCHAR(20), -- activo, stock, inactivo
    programado TINYINT(1) DEFAULT 0,
    empresa_id INT NOT NULL DEFAULT 1,
    FOREIGN KEY (catalogo_id) REFERENCES catalogo_instrumentos(id),
    FOREIGN KEY (marca_id) REFERENCES marcas(id),
    FOREIGN KEY (modelo_id) REFERENCES modelos(id),
    FOREIGN KEY (departamento_id) REFERENCES departamentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

$conn->query("CREATE TABLE IF NOT EXISTS bulk_import_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    usuario_id INT NULL,
    archivo_nombre VARCHAR(255) NOT NULL,
    total_registros INT NOT NULL DEFAULT 0,
    procesados INT NOT NULL DEFAULT 0,
    exitosos INT NOT NULL DEFAULT 0,
    fallidos INT NOT NULL DEFAULT 0,
    errores_json LONGTEXT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
);");

// Historial de departamentos responsables
$conn->query("CREATE TABLE IF NOT EXISTS historial_departamentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    departamento_id INT,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (departamento_id) REFERENCES departamentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Historial de ubicaciones
$conn->query("CREATE TABLE IF NOT EXISTS historial_ubicaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    ubicacion VARCHAR(150),
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Historial de fechas de alta de instrumentos
$conn->query("CREATE TABLE IF NOT EXISTS historial_fecha_alta (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha DATE,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Historial de fechas de baja de instrumentos
$conn->query("CREATE TABLE IF NOT EXISTS historial_fecha_baja (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha DATE,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Historial de fechas de alta de instrumentos
$conn->query("CREATE TABLE IF NOT EXISTS historial_fecha_alta (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha DATE,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Historial de fechas de baja de instrumentos
$conn->query("CREATE TABLE IF NOT EXISTS historial_fecha_baja (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha DATE,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
);");

// Historial de cambios de tipo/estado del instrumento
$conn->query("CREATE TABLE IF NOT EXISTS historial_tipos_instrumento (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    estado VARCHAR(50) NOT NULL,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    empresa_id INT NOT NULL,
    `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS historial_calibraciones (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS historial_especificaciones (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS historial_estado_instrumento (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Requerimientos de calibración por instrumento
$conn->query("CREATE TABLE IF NOT EXISTS requerimientos_calibracion (
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    requerimiento TEXT,
    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrumento_id, empresa_id),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Plan basado en riesgos asociado a cada instrumento
$conn->query("CREATE TABLE IF NOT EXISTS plan_riesgos (
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    requerimiento VARCHAR(20) DEFAULT 'NA',
    impacto_falla INT DEFAULT 0,
    consideraciones_falla INT DEFAULT 0,
    clase_riesgo VARCHAR(10) DEFAULT 'NA',
    capacidad_deteccion INT DEFAULT 0,
    frecuencia INT DEFAULT 0,
    observaciones TEXT,
    tipo_calibracion VARCHAR(20) DEFAULT 'NA',
    especificaciones TEXT,
    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrumento_id),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Documentos y adjuntos asociados a cada instrumento
$conn->query("CREATE TABLE IF NOT EXISTS instrumento_adjuntos (
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
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id) ON DELETE CASCADE,
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("ALTER TABLE instrumento_adjuntos
    ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL AFTER instrumento_id,
    ADD COLUMN IF NOT EXISTS nombre_visible VARCHAR(255) NOT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS ruta_archivo VARCHAR(500) NOT NULL AFTER nombre_visible,
    ADD COLUMN IF NOT EXISTS tipo VARCHAR(60) DEFAULT NULL AFTER ruta_archivo,
    ADD COLUMN IF NOT EXISTS descripcion TEXT AFTER tipo,
    ADD COLUMN IF NOT EXISTS usuario_id INT DEFAULT NULL AFTER descripcion,
    ADD COLUMN IF NOT EXISTS mime_type VARCHAR(120) DEFAULT NULL AFTER usuario_id,
    ADD COLUMN IF NOT EXISTS tamano BIGINT DEFAULT NULL AFTER mime_type,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER tamano,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;");

$conn->query("ALTER TABLE instrumento_adjuntos
    ADD INDEX IF NOT EXISTS idx_adjuntos_instrumento (instrumento_id, empresa_id);");

// Tabla de calibraciones
$conn->query("CREATE TABLE IF NOT EXISTS calibraciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    empresa_id INT NOT NULL DEFAULT 1,
    usuario_id INT DEFAULT NULL,
    tipo VARCHAR(20) NOT NULL DEFAULT 'Interna',
    fecha_calibracion DATE,
    duracion_horas DECIMAL(8,2) DEFAULT NULL,
    costo_total DECIMAL(12,2) DEFAULT NULL,
    fecha_proxima DATE,
    resultado_preliminar VARCHAR(50) DEFAULT NULL,
    resultado VARCHAR(50),
    observaciones TEXT,
    u_value DECIMAL(18,6) DEFAULT NULL,
    u_method VARCHAR(255) DEFAULT NULL,
    u_k DECIMAL(10,4) DEFAULT NULL,
    u_coverage VARCHAR(255) DEFAULT NULL,
    proveedor_id INT,
    estado ENUM('Pendiente','Aprobado','Rechazado') NOT NULL DEFAULT 'Pendiente',
    usuario_id INT DEFAULT NULL,
    liberado_por INT DEFAULT NULL,
    fecha_liberacion DATETIME DEFAULT NULL,
    aprobado_por INT DEFAULT NULL,
    fecha_aprobacion DATETIME DEFAULT NULL,
    motivo_rechazo TEXT DEFAULT NULL,
    estado_ejecucion ENUM('Programada','En proceso','Completada','Reprogramada','Atrasada','Cancelada') DEFAULT 'Completada',
    motivo_reprogramacion TEXT,
    fecha_reprogramada DATE DEFAULT NULL,
    dias_atraso INT DEFAULT 0,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS usuario_competencias (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$columnCheck = $conn->query("SHOW COLUMNS FROM usuario_competencias LIKE 'catalogo_id'");
if ($columnCheck && $columnCheck->num_rows === 0) {
    $conn->query("ALTER TABLE usuario_competencias ADD COLUMN catalogo_id INT DEFAULT NULL AFTER evidencia_fecha");
}
if ($columnCheck instanceof mysqli_result) {
    $columnCheck->close();
}

$columnCheck = $conn->query("SHOW COLUMNS FROM usuario_competencias LIKE 'vigente_desde'");
if ($columnCheck && $columnCheck->num_rows === 0) {
    $conn->query("ALTER TABLE usuario_competencias ADD COLUMN vigente_desde DATE DEFAULT NULL AFTER catalogo_id");
}
if ($columnCheck instanceof mysqli_result) {
    $columnCheck->close();
}

$columnCheck = $conn->query("SHOW COLUMNS FROM usuario_competencias LIKE 'vigente_hasta'");
if ($columnCheck && $columnCheck->num_rows === 0) {
    $conn->query("ALTER TABLE usuario_competencias ADD COLUMN vigente_hasta DATE DEFAULT NULL AFTER vigente_desde");
}
if ($columnCheck instanceof mysqli_result) {
    $columnCheck->close();
}

$indexResult = $conn->query("SHOW INDEX FROM usuario_competencias WHERE Key_name = 'idx_usuario_competencias_catalogo'");
if ($indexResult && $indexResult->num_rows === 0) {
    $conn->query("ALTER TABLE usuario_competencias ADD INDEX idx_usuario_competencias_catalogo (catalogo_id)");
}
if ($indexResult instanceof mysqli_result) {
    $indexResult->close();
}

$fkResult = $conn->query(
    "SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
    . "WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'usuario_competencias' "
    . "AND COLUMN_NAME = 'catalogo_id' AND REFERENCED_TABLE_NAME = 'catalogo_instrumentos'"
);
if ($fkResult && $fkResult->num_rows === 0) {
    $conn->query("ALTER TABLE usuario_competencias ADD CONSTRAINT fk_usuario_competencias_catalogo FOREIGN KEY (catalogo_id) REFERENCES catalogo_instrumentos(id) ON DELETE SET NULL");
}
if ($fkResult instanceof mysqli_result) {
    $fkResult->close();
}

$conn->query("CREATE TABLE IF NOT EXISTS calibracion_referencias (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("CREATE TABLE IF NOT EXISTS calibracion_referencias (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Control logístico de calibraciones externas
$conn->query("CREATE TABLE IF NOT EXISTS logistica_calibraciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT NOT NULL,
    estado VARCHAR(20) NOT NULL DEFAULT 'Pendiente',
    proveedor_externo VARCHAR(150) DEFAULT NULL,
    transportista VARCHAR(150) DEFAULT NULL,
    numero_guia VARCHAR(100) DEFAULT NULL,
    orden_servicio VARCHAR(100) DEFAULT NULL,
    fecha_envio DATE DEFAULT NULL,
    fecha_en_transito DATE DEFAULT NULL,
    fecha_recibido DATE DEFAULT NULL,
    fecha_retorno DATE DEFAULT NULL,
    comentarios TEXT DEFAULT NULL,
    creado_en DATETIME DEFAULT CURRENT_TIMESTAMP,
    actualizado_en DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_calibracion (calibracion_id),
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Registro de alertas enviadas de calibración
$conn->query("CREATE TABLE IF NOT EXISTS calibration_alert_notifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    due_date DATE NOT NULL,
    alert_type VARCHAR(20) NOT NULL DEFAULT 'upcoming',
    attention_status VARCHAR(20) NOT NULL DEFAULT 'pendiente',
    attention_notes TEXT DEFAULT NULL,
    attention_user_id INT DEFAULT NULL,
    attention_at DATETIME DEFAULT NULL,
    notified_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_alert (instrumento_id, empresa_id, due_date),

    INDEX idx_alert_due_date (due_date),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (attention_user_id) REFERENCES usuarios(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$schemaResult = $conn->query('SELECT DATABASE() AS db');
if ($schemaResult) {
    $schemaRow = $schemaResult->fetch_assoc();
    $schemaResult->close();
    $schemaName = $schemaRow['db'] ?? null;

    if ($schemaName) {
        $alertColumns = [
            'alert_type' => "ALTER TABLE calibration_alert_notifications ADD COLUMN alert_type VARCHAR(20) NOT NULL DEFAULT 'upcoming' AFTER due_date",
            'attention_status' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_status VARCHAR(20) NOT NULL DEFAULT 'pendiente' AFTER alert_type",
            'attention_notes' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_notes TEXT DEFAULT NULL AFTER attention_status",
            'attention_user_id' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_user_id INT DEFAULT NULL AFTER attention_notes",
            'attention_at' => "ALTER TABLE calibration_alert_notifications ADD COLUMN attention_at DATETIME DEFAULT NULL AFTER attention_user_id",
            'updated_at' => "ALTER TABLE calibration_alert_notifications ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER notified_at",
        ];

        foreach ($alertColumns as $column => $alterSql) {
            $checkStmt = $conn->prepare('SELECT COUNT(*) AS total FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = "calibration_alert_notifications" AND COLUMN_NAME = ?');
            if ($checkStmt) {
                $checkStmt->bind_param('ss', $schemaName, $column);
                if ($checkStmt->execute()) {
                    $checkResult = $checkStmt->get_result();
                    $exists = $checkResult && ($row = $checkResult->fetch_assoc()) ? ((int) ($row['total'] ?? 0) > 0) : false;
                    if ($checkResult instanceof mysqli_result) {
                        $checkResult->close();
                    }
                    if (!$exists) {
                        $conn->query($alterSql);
                        if ($column === 'attention_user_id') {
                            $conn->query('ALTER TABLE calibration_alert_notifications ADD FOREIGN KEY (attention_user_id) REFERENCES usuarios(id)');
                        }
                    }
                }
                $checkStmt->close();
            }
        }

        $conn->query("UPDATE calibration_alert_notifications SET alert_type = CASE WHEN due_date < CURDATE() THEN 'overdue' ELSE 'upcoming' END");
    }
}

// Registro de alertas específicas de certificados para evitar duplicados
$conn->query("CREATE TABLE IF NOT EXISTS certificate_alert_notifications (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT NOT NULL,
    empresa_id INT NOT NULL,
    reference_date DATE NOT NULL,
    calibration_date DATE DEFAULT NULL,
    certificate_path VARCHAR(255) DEFAULT NULL,
    notified_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_certificate_alert (instrumento_id, empresa_id, reference_date),
    INDEX idx_certificate_alert_reference (reference_date),
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Tabla para registrar la última ejecución de procesos automáticos
$conn->query("CREATE TABLE IF NOT EXISTS system_jobs (
    job_name VARCHAR(150) NOT NULL PRIMARY KEY,
    last_run_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_status VARCHAR(30) NOT NULL DEFAULT 'success',
    last_message TEXT DEFAULT NULL,
    last_result TEXT DEFAULT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Notificaciones para usuarios
$conn->query("CREATE TABLE IF NOT EXISTS user_notifications (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

// Solicitudes de calibración generadas por clientes
$conn->query("CREATE TABLE IF NOT EXISTS solicitudes_calibracion (
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
);");

// Tabla de planes de calibración
$conn->query("CREATE TABLE IF NOT EXISTS planes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    instrumento_id INT,
    fecha_programada DATE,
    responsable_id INT,
    estado VARCHAR(20),
    empresa_id INT NOT NULL DEFAULT 1,
    instrucciones_cliente TEXT,
    FOREIGN KEY (instrumento_id) REFERENCES instrumentos(id),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id),
    FOREIGN KEY (responsable_id) REFERENCES usuarios(id)
);");

// Órdenes de trabajo asociadas a cada plan de calibración
$conn->query("CREATE TABLE IF NOT EXISTS ordenes_calibracion (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

$conn->query("ALTER TABLE ordenes_calibracion
    ADD COLUMN IF NOT EXISTS tecnico_id INT DEFAULT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS estado_ejecucion VARCHAR(50) NOT NULL DEFAULT 'Programada' AFTER tecnico_id,
    ADD COLUMN IF NOT EXISTS fecha_inicio DATE DEFAULT NULL AFTER estado_ejecucion,
    ADD COLUMN IF NOT EXISTS fecha_cierre DATE DEFAULT NULL AFTER fecha_inicio,
    ADD COLUMN IF NOT EXISTS observaciones TEXT DEFAULT NULL AFTER fecha_cierre,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER observaciones,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at;");

$conn->query("ALTER TABLE ordenes_calibracion
    MODIFY COLUMN estado_ejecucion VARCHAR(50) NOT NULL DEFAULT 'Programada';");

// Tabla de certificados
$conn->query("CREATE TABLE IF NOT EXISTS certificados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    calibracion_id INT,
    archivo VARCHAR(255),
    fecha_subida DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (calibracion_id) REFERENCES calibraciones(id)
);");

// Tabla de auditoría
$conn->query("CREATE TABLE IF NOT EXISTS audit_trail (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    segmento_actor VARCHAR(50) NOT NULL DEFAULT 'Administradores',
    fecha_evento DATETIME DEFAULT NULL,
    seccion VARCHAR(150) NOT NULL,
    rango_referencia VARCHAR(150) DEFAULT NULL,
    instrumento_id INT DEFAULT NULL,
    valor_anterior TEXT DEFAULT NULL,
    valor_nuevo TEXT DEFAULT NULL,
    usuario_id INT DEFAULT NULL,
    usuario_correo VARCHAR(150) NOT NULL,
    usuario_nombre VARCHAR(150) NOT NULL DEFAULT 'Administrador general',
    usuario_firma_interna VARCHAR(150) DEFAULT NULL,
    instrumento_codigo VARCHAR(50) DEFAULT NULL,
    columna_excel VARCHAR(10) DEFAULT NULL,
    fila_excel INT DEFAULT NULL,
    INDEX idx_audit_empresa (empresa_id),
    INDEX idx_audit_segmento (segmento_actor),
    INDEX idx_audit_empresa_segmento (empresa_id, segmento_actor),
    INDEX idx_audit_fecha_evento (fecha_evento),
    INDEX idx_audit_seccion (seccion),
    INDEX idx_audit_usuario_correo (usuario_correo),
    INDEX idx_audit_usuario_firma (usuario_firma_interna),
    INDEX idx_audit_instrumento (instrumento_codigo)
);");


$result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail'");
$auditExists = $result && ($row = $result->fetch_assoc()) ? ((int)$row['total'] > 0) : false;
if ($result) {
    $result->free();
}

if ($auditExists) {
    $result = $conn->query("SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND REFERENCED_TABLE_NAME = 'roles' LIMIT 1");
    if ($result && ($row = $result->fetch_assoc()) && !empty($row['CONSTRAINT_NAME'])) {
        $constraint = $row['CONSTRAINT_NAME'];
        $conn->query("ALTER TABLE audit_trail DROP FOREIGN KEY `{$constraint}`");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND COLUMN_NAME = 'usuario_id' AND REFERENCED_TABLE_NAME = 'usuarios' LIMIT 1");
    if ($result && ($row = $result->fetch_assoc()) && !empty($row['CONSTRAINT_NAME'])) {
        $constraint = $row['CONSTRAINT_NAME'];
        $conn->query("ALTER TABLE audit_trail DROP FOREIGN KEY `{$constraint}`");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND COLUMN_NAME = 'instrumento_id' AND REFERENCED_TABLE_NAME = 'instrumentos' LIMIT 1");
    if ($result && ($row = $result->fetch_assoc()) && !empty($row['CONSTRAINT_NAME'])) {
        $constraint = $row['CONSTRAINT_NAME'];
        $conn->query("ALTER TABLE audit_trail DROP FOREIGN KEY `{$constraint}`");
    }
    if ($result) {
        $result->free();
    }

    $conn->query("ALTER TABLE audit_trail
        DROP COLUMN IF EXISTS registrado_en,
        DROP COLUMN IF EXISTS role_id,
        DROP COLUMN IF EXISTS fecha,
        DROP COLUMN IF EXISTS hora,
        DROP COLUMN IF EXISTS actividad,
        DROP COLUMN IF EXISTS relevancia,
        DROP COLUMN IF EXISTS fecha_creacion,
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
        DROP COLUMN IF EXISTS user_agent");

    $conn->query("ALTER TABLE audit_trail
        ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL DEFAULT 0 AFTER id,
        ADD COLUMN IF NOT EXISTS segmento_actor VARCHAR(50) NOT NULL DEFAULT 'Administradores' AFTER empresa_id,
        ADD COLUMN IF NOT EXISTS fecha_evento DATETIME DEFAULT NULL,
        ADD COLUMN IF NOT EXISTS seccion VARCHAR(150) NOT NULL,
        ADD COLUMN IF NOT EXISTS rango_referencia VARCHAR(150) NULL,
        ADD COLUMN IF NOT EXISTS instrumento_id INT NULL,
        ADD COLUMN IF NOT EXISTS valor_anterior TEXT NULL,
        ADD COLUMN IF NOT EXISTS valor_nuevo TEXT NULL,
        ADD COLUMN IF NOT EXISTS usuario_id INT NULL,
        ADD COLUMN IF NOT EXISTS usuario_correo VARCHAR(150) NOT NULL,
        ADD COLUMN IF NOT EXISTS usuario_nombre VARCHAR(150) NOT NULL DEFAULT 'Administrador general',
        ADD COLUMN IF NOT EXISTS usuario_firma_interna VARCHAR(150) DEFAULT NULL AFTER usuario_nombre,
        ADD COLUMN IF NOT EXISTS instrumento_codigo VARCHAR(50) DEFAULT NULL AFTER usuario_firma_interna,
        ADD COLUMN IF NOT EXISTS columna_excel VARCHAR(10) DEFAULT NULL AFTER instrumento_codigo,
        ADD COLUMN IF NOT EXISTS fila_excel INT DEFAULT NULL AFTER columna_excel");

    $conn->query("ALTER TABLE audit_trail
        MODIFY COLUMN empresa_id INT NOT NULL DEFAULT 0,
        MODIFY COLUMN segmento_actor VARCHAR(50) NOT NULL DEFAULT 'Administradores',
        MODIFY COLUMN fecha_evento DATETIME DEFAULT NULL,
        MODIFY COLUMN seccion VARCHAR(150) NOT NULL,
        MODIFY COLUMN rango_referencia VARCHAR(150) NULL,
        MODIFY COLUMN instrumento_id INT NULL,
        MODIFY COLUMN valor_anterior TEXT NULL,
        MODIFY COLUMN valor_nuevo TEXT NULL,
        MODIFY COLUMN usuario_id INT NULL,
        MODIFY COLUMN usuario_correo VARCHAR(150) NOT NULL,
        MODIFY COLUMN usuario_nombre VARCHAR(150) NOT NULL DEFAULT 'Administrador general',
        MODIFY COLUMN usuario_firma_interna VARCHAR(150) DEFAULT NULL");

    $conn->query("UPDATE audit_trail SET usuario_nombre = 'Administrador general' WHERE usuario_nombre IS NULL OR usuario_nombre = ''");
    $conn->query("UPDATE audit_trail SET segmento_actor = 'Administradores' WHERE segmento_actor IS NULL OR segmento_actor = ''");
    $conn->query("UPDATE audit_trail SET rango_referencia = NULL WHERE rango_referencia REGEXP '^[A-Z]+[0-9]+(:[A-Z]+[0-9]+)?$'");

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_registrado_en'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] > 0) {
        $conn->query("ALTER TABLE audit_trail DROP INDEX idx_audit_registrado_en");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_instrumento'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] > 0) {
        $conn->query("ALTER TABLE audit_trail DROP INDEX idx_audit_instrumento");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_segmento'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_segmento (segmento_actor)");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_empresa_segmento'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_empresa_segmento (empresa_id, segmento_actor)");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_fecha_evento'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_fecha_evento (fecha_evento)");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_seccion'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_seccion (seccion)");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_usuario_correo'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_usuario_correo (usuario_correo)");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_usuario_firma'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_usuario_firma (usuario_firma_interna)");
    }
    if ($result) {
        $result->free();
    }

    $result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'audit_trail' AND INDEX_NAME = 'idx_audit_empresa'");
    if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
        $conn->query("ALTER TABLE audit_trail ADD INDEX idx_audit_empresa (empresa_id)");
    }
    if ($result) {
        $result->free();
    }
}
// ------------------------------------------------------------
// Tabla de firmas internas de usuarios
// ------------------------------------------------------------

$conn->query("CREATE TABLE IF NOT EXISTS usuario_firmas_internas (
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
    INDEX idx_firmas_vigencia (vigente_desde, vigente_hasta),
    CONSTRAINT fk_usuario_firmas_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
);");

$conn->query("ALTER TABLE usuario_firmas_internas
    ADD COLUMN IF NOT EXISTS empresa_id INT NOT NULL DEFAULT 0 AFTER id,
    ADD COLUMN IF NOT EXISTS usuario_id INT NOT NULL AFTER empresa_id,
    ADD COLUMN IF NOT EXISTS correo VARCHAR(150) DEFAULT NULL AFTER usuario_id,
    ADD COLUMN IF NOT EXISTS firma_interna VARCHAR(150) NOT NULL AFTER correo,
    ADD COLUMN IF NOT EXISTS vigente_desde DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER firma_interna,
    ADD COLUMN IF NOT EXISTS vigente_hasta DATETIME DEFAULT NULL AFTER vigente_desde,
    ADD COLUMN IF NOT EXISTS creado_por INT DEFAULT NULL AFTER vigente_hasta,
    ADD COLUMN IF NOT EXISTS created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER creado_por,
    ADD COLUMN IF NOT EXISTS updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at
;");

$conn->query("ALTER TABLE usuario_firmas_internas
    MODIFY COLUMN empresa_id INT NOT NULL DEFAULT 0,
    MODIFY COLUMN usuario_id INT NOT NULL,
    MODIFY COLUMN correo VARCHAR(150) DEFAULT NULL,
    MODIFY COLUMN firma_interna VARCHAR(150) NOT NULL,
    MODIFY COLUMN vigente_desde DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    MODIFY COLUMN vigente_hasta DATETIME DEFAULT NULL,
    MODIFY COLUMN creado_por INT DEFAULT NULL,
    MODIFY COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
;");

$result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'usuario_firmas_internas' AND INDEX_NAME = 'idx_firmas_usuario'");
if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
    $conn->query("ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_usuario (usuario_id)");
}
if ($result) {
    $result->free();
}

$result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'usuario_firmas_internas' AND INDEX_NAME = 'idx_firmas_empresa'");
if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
    $conn->query("ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_empresa (empresa_id)");
}
if ($result) {
    $result->free();
}

$result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'usuario_firmas_internas' AND INDEX_NAME = 'idx_firmas_correo'");
if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
    $conn->query("ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_correo (correo)");
}
if ($result) {
    $result->free();
}

$result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'usuario_firmas_internas' AND INDEX_NAME = 'idx_firmas_vigencia'");
if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
    $conn->query("ALTER TABLE usuario_firmas_internas ADD INDEX idx_firmas_vigencia (vigente_desde, vigente_hasta)");
}
if ($result) {
    $result->free();
}

$result = $conn->query("SELECT COUNT(*) AS total FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'usuario_firmas_internas' AND REFERENCED_TABLE_NAME = 'usuarios'");
if ($result && ($row = $result->fetch_assoc()) && (int)$row['total'] === 0) {
    $conn->query("ALTER TABLE usuario_firmas_internas ADD CONSTRAINT fk_usuario_firmas_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE");
}
if ($result) {
    $result->free();
}

// Datos de ejemplo para instrumentos (demo)
// ------------------------------------------------------------

// Inserta departamentos de ejemplo si no existen
$result = $conn->query("SELECT COUNT(*) AS count FROM departamentos");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO departamentos (nombre) VALUES
        ('Producción'),
        ('Laboratorio QC'),
        ('Laboratorio Microbiología'),
        ('Almacén'),
        ('Mantenimiento')");
}

// Inserta marcas de ejemplo si no existen
$result = $conn->query("SELECT COUNT(*) AS count FROM marcas");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO marcas (nombre) VALUES
        ('Mettler Toledo'),
        ('Sartorius'),
        ('Thermo Fisher'),
        ('Agilent'),
        ('Waters')");
}

// Inserta modelos de ejemplo si no existen
$result = $conn->query("SELECT COUNT(*) AS count FROM modelos");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO modelos (nombre, marca_id) VALUES
        ('XS204', 1),
        ('XP205', 1),
        ('Entris224-1S', 2),
        ('Cubis MSA', 2),
        ('Evolution 201', 3)");
}

// Inserta catálogo de instrumentos si no existe
$result = $conn->query("SELECT COUNT(*) AS count FROM catalogo_instrumentos");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO catalogo_instrumentos (nombre) VALUES
        ('Balanza Analítica'),
        ('Balanza Semi-micro'),
        ('pH Metro'),
        ('Espectrofotómetro'),
        ('HPLC'),
        ('Termómetro'),
        ('Pipeta'),
        ('Micropipeta'),
        ('Cronómetro'),
        ('Báscula Industrial')");
}

// Inserta instrumentos de ejemplo si no existen
$result = $conn->query("SELECT COUNT(*) AS count FROM instrumentos");
$row = $result->fetch_assoc();
if ((int)$row['count'] === 0) {
    $conn->query("INSERT INTO instrumentos
        (catalogo_id, marca_id, modelo_id, serie, codigo, departamento_id, ubicacion, fecha_alta, estado, proxima_calibracion, programado, empresa_id)
        VALUES
        (1, 1, 1, 'MT001', 'BAL-001', 2, 'Mesa 1 - Lab QC', '2023-01-15', 'activo', '2024-01-15', 1, 1),
        (1, 2, 3, 'SAR002', 'BAL-002', 2, 'Mesa 2 - Lab QC', '2023-02-10', 'activo', '2024-02-10', 1, 1),
        (3, 3, 5, 'TF003', 'PH-001', 2, 'Mesa 3 - Lab QC', '2023-03-05', 'activo', '2024-03-05', 0, 1),
        (5, 4, NULL, 'AG004', 'HPLC-001', 2, 'Instrumental - Lab QC', '2023-01-20', 'activo', '2024-01-20', 1, 1),
        (10, 1, 2, 'MT005', 'BAS-001', 1, 'Área Pesaje - Producción', '2023-04-12', 'activo', '2024-04-12', 0, 1)");
}

// Inserta planes de riesgo por defecto para los instrumentos existentes
$conn->query("INSERT IGNORE INTO plan_riesgos (instrumento_id, empresa_id)
    SELECT id, empresa_id FROM instrumentos;");

// Tabla de reportes de retroalimentación
$conn->query("CREATE TABLE IF NOT EXISTS feedback_reports (
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
);");

// Tabla de adjuntos para los reportes de retroalimentación
$conn->query("CREATE TABLE IF NOT EXISTS feedback_attachments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    feedback_id INT NOT NULL,
    nombre_original VARCHAR(255) NOT NULL,
    archivo VARCHAR(255) NOT NULL,
    mime_type VARCHAR(120) DEFAULT NULL,
    tamano BIGINT DEFAULT NULL,
    creado_en DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_feedback_attachment_feedback (feedback_id),
    FOREIGN KEY (feedback_id) REFERENCES feedback_reports(id) ON DELETE CASCADE
);");

// Vista consolidada de planeación
$viewSql = <<<'SQL'
CREATE OR REPLACE VIEW vista_planeacion_instrumentos AS
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
LEFT JOIN usuarios u ON ult.responsable_id = u.id
SQL;

if (!$conn->query($viewSql)) {
    error_log('No se pudo crear la vista vista_planeacion_instrumentos: ' . $conn->error);
}

echo 'Tablas principales y de instrumentos creadas/verificadas con datos de ejemplo.';

if ($standalone) {
    echo 'Tablas principales creadas/verificadas.';
}


