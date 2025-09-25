USE iso17025;

-- Limpia las estructuras actuales en español antes de recrearlas
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS calidad_acciones_correctivas;
DROP TABLE IF EXISTS calidad_no_conformidad_acciones;
DROP TABLE IF EXISTS calidad_no_conformidades;
DROP TABLE IF EXISTS calidad_capacitaciones_participantes;
DROP TABLE IF EXISTS calidad_capacitaciones_asistentes;
DROP TABLE IF EXISTS calidad_capacitaciones;
DROP TABLE IF EXISTS calidad_documentos_revision;
DROP TABLE IF EXISTS calidad_documentos_historial;
DROP TABLE IF EXISTS calidad_documentos;
SET FOREIGN_KEY_CHECKS = 1;

-- Documentos controlados del sistema de gestión
CREATE TABLE IF NOT EXISTS calidad_documentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    codigo VARCHAR(50) NOT NULL,
    titulo VARCHAR(150) NOT NULL,
    version VARCHAR(20) NOT NULL DEFAULT '1.0',
    estado ENUM('borrador','en_revision','publicado','obsoleto') NOT NULL DEFAULT 'borrador',
    autor_id INT NOT NULL,
    revisor_id INT DEFAULT NULL,
    publicado_por_id INT DEFAULT NULL,
    fecha_creacion DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_revision DATETIME DEFAULT NULL,
    fecha_publicacion DATETIME DEFAULT NULL,
    contenido TEXT,
    UNIQUE KEY uq_calidad_documentos_codigo (empresa_id, codigo),
    KEY idx_calidad_documentos_estado (empresa_id, estado),
    CONSTRAINT fk_calidad_documentos_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_documentos_autor FOREIGN KEY (autor_id) REFERENCES usuarios(id) ON DELETE RESTRICT,
    CONSTRAINT fk_calidad_documentos_revisor FOREIGN KEY (revisor_id) REFERENCES usuarios(id) ON DELETE SET NULL,
    CONSTRAINT fk_calidad_documentos_publicado FOREIGN KEY (publicado_por_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_documentos_historial (
    id INT AUTO_INCREMENT PRIMARY KEY,
    documento_id INT NOT NULL,
    estado VARCHAR(30) NOT NULL,
    usuario_id INT NOT NULL,
    comentario VARCHAR(255),
    fecha_evento DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_calidad_historial_evento (documento_id, estado),
    KEY idx_calidad_historial_documento (documento_id),
    CONSTRAINT fk_calidad_historial_documento FOREIGN KEY (documento_id) REFERENCES calidad_documentos(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_historial_usuario FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Programación y control de capacitaciones
CREATE TABLE IF NOT EXISTS calidad_capacitaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    tema VARCHAR(150) NOT NULL,
    fecha_programada DATETIME NOT NULL,
    responsable_id INT NOT NULL,
    modalidad VARCHAR(50) NOT NULL DEFAULT 'presencial',
    estado ENUM('programada','reprogramada','impartida','cancelada') NOT NULL DEFAULT 'programada',
    fecha_creacion DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_calidad_capacitaciones_tema (empresa_id, tema, fecha_programada),
    KEY idx_calidad_capacitaciones_estado (empresa_id, estado),
    CONSTRAINT fk_calidad_capacitaciones_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_capacitaciones_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_capacitaciones_asistentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    capacitacion_id INT NOT NULL,
    participante_id INT NOT NULL,
    estatus ENUM('pendiente','asistio','falta','justificado') NOT NULL DEFAULT 'pendiente',
    comentarios VARCHAR(255),
    fecha_registro DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_calidad_capacitaciones_asistente (capacitacion_id, participante_id),
    KEY idx_calidad_capacitaciones_asistente (participante_id),
    CONSTRAINT fk_calidad_capacitaciones_asistencia FOREIGN KEY (capacitacion_id) REFERENCES calidad_capacitaciones(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_capacitaciones_participante FOREIGN KEY (participante_id) REFERENCES usuarios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Gestión de no conformidades
CREATE TABLE IF NOT EXISTS calidad_no_conformidades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    codigo VARCHAR(50) NOT NULL,
    descripcion TEXT NOT NULL,
    estado ENUM('abierta','en_investigacion','implementando','cerrada') NOT NULL DEFAULT 'abierta',
    origen VARCHAR(100) NOT NULL,
    responsable_id INT NOT NULL,
    verificado_por_id INT DEFAULT NULL,
    fecha_apertura DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_cierre DATETIME DEFAULT NULL,
    evidencia_cierre TEXT,
    UNIQUE KEY uq_calidad_no_conformidades_codigo (empresa_id, codigo),
    KEY idx_calidad_no_conformidades_estado (empresa_id, estado),
    CONSTRAINT fk_calidad_no_conformidades_empresa FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_no_conformidades_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE RESTRICT,
    CONSTRAINT fk_calidad_no_conformidades_verificador FOREIGN KEY (verificado_por_id) REFERENCES usuarios(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calidad_no_conformidad_acciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    no_conformidad_id INT NOT NULL,
    descripcion VARCHAR(250) NOT NULL,
    responsable_id INT NOT NULL,
    estado ENUM('pendiente','en_proceso','completada') NOT NULL DEFAULT 'pendiente',
    evidencia TEXT,
    fecha_cierre DATETIME DEFAULT NULL,
    UNIQUE KEY uq_calidad_accion_nc (no_conformidad_id, descripcion),
    CONSTRAINT fk_calidad_acciones_nc FOREIGN KEY (no_conformidad_id) REFERENCES calidad_no_conformidades(id) ON DELETE CASCADE,
    CONSTRAINT fk_calidad_acciones_responsable FOREIGN KEY (responsable_id) REFERENCES usuarios(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
