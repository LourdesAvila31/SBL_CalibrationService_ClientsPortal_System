-- Archivo generado automáticamente por convert_historiales_csv.py

START TRANSACTION;
SET @now_historial := NOW();

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT
    datos.instrumento_id,
    datos.departamento_id,
    datos.empresa_id,
    COALESCE(datos.fecha_evento, @now_historial) AS fecha,
    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`
FROM (
    SELECT
        base.empresa_id,
        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,
        (SELECT id FROM departamentos WHERE empresa_id = base.empresa_id AND LOWER(nombre) = LOWER(base.valor_texto) LIMIT 1) AS departamento_id,
        base.valor_texto,
        base.fecha_evento
    FROM (
        SELECT 1 AS empresa_id, 'VIS-DA-02' AS instrumento_codigo, 'Control de calidad' AS valor_texto, '2024-04-19 12:56:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-MA-01' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-04-29 13:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-04-29 14:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAE-MI-01' AS instrumento_codigo, 'Microbiología' AS valor_texto, '2024-05-08 16:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-FR-23' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2024-05-31 14:41:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-09' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2024-06-21 16:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-DA-02' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2024-06-25 13:25:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-DF-01' AS instrumento_codigo, 'Desarrollo Farmaceutico' AS valor_texto, '2024-07-03 12:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-54' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-07-11 10:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-22' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-07-19 11:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-23' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-07-19 11:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-24' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-07-19 11:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-CP-05' AS instrumento_codigo, 'Control en proceso' AS valor_texto, '2024-07-26 12:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-LC-03' AS instrumento_codigo, 'Control de calidad' AS valor_texto, '2024-08-06 11:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-02' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-08-08 16:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-03' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-08-08 16:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-06' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-08-29 09:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-07' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-08-29 09:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'HUM-MA-01' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-08-29 09:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-12' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-08-29 09:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-13' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-08-29 09:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'LUX-VA-01' AS instrumento_codigo, 'Validación' AS valor_texto, '2024-09-26 08:39:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-AL-01' AS instrumento_codigo, 'Almacen' AS valor_texto, '2024-10-04 11:24:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CRO-DF-01' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2024-10-09 07:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-03' AS instrumento_codigo, 'Almacén' AS valor_texto, '2024-11-08 15:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-04' AS instrumento_codigo, 'Almacén' AS valor_texto, '2024-11-08 15:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-05' AS instrumento_codigo, 'Almacén' AS valor_texto, '2024-11-08 15:54:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-39' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2024-11-19 08:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-40' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2024-11-19 08:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-41' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2024-11-19 08:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-42' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2024-11-19 08:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, 'Validación' AS valor_texto, '2024-11-22 14:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, 'Validación' AS valor_texto, '2024-11-22 14:48:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BSM-LC-02' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2024-11-27 09:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-MA-38' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-12-03 14:33:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-DF-01' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2024-12-05 14:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TMB-GC-01' AS instrumento_codigo, 'Control en proceso' AS valor_texto, '2024-12-11 12:51:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-GC-02' AS instrumento_codigo, 'Gestión de la Calidad' AS valor_texto, '2024-12-11 13:00:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-GC-02' AS instrumento_codigo, 'Gestión de Calidad' AS valor_texto, '2024-12-11 13:00:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TMB-GC-01' AS instrumento_codigo, 'Gestión de la Calidad' AS valor_texto, '2024-12-11 13:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-MA-01' AS instrumento_codigo, 'Validación' AS valor_texto, '2024-12-14 15:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-21' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2025-01-20 15:34:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-22' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2025-01-20 15:34:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-11' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-01-22 09:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-12' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-01-22 09:21:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-03' AS instrumento_codigo, 'Microbiología' AS valor_texto, '2025-01-30 14:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-MI-02' AS instrumento_codigo, 'Microbiología' AS valor_texto, '2025-01-30 14:27:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-08' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-02-11 10:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-23' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2025-04-01 15:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-LC-13' AS instrumento_codigo, 'Control de calidad' AS valor_texto, '2025-04-02 15:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-02' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-04-03 07:46:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-03' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-04-03 08:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-CP-05' AS instrumento_codigo, 'Control en Proceso' AS valor_texto, '2025-04-03 13:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-12' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 13:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-13' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 13:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-MI-02' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 13:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'HUM-MA-01' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 13:24:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-01' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 13:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-DA-02' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 13:57:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-04' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 14:01:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-05' AS instrumento_codigo, 'Control en Proceso' AS valor_texto, '2025-04-03 14:01:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-64' AS instrumento_codigo, 'Control en Proceso' AS valor_texto, '2025-04-03 14:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-75' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 14:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-78' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 14:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-79' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 14:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-81' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 14:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-VA-29' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-04-03 14:20:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-AL-01' AS instrumento_codigo, 'Almacén' AS valor_texto, '2025-04-03 14:48:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-77' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2025-04-03 14:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-04' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-05-21 09:53:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-05' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-07-03 11:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-06' AS instrumento_codigo, 'Desarrollo Analítico' AS valor_texto, '2025-07-03 11:19:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-01' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-02' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-03' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-04' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-05' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-06' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-07' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-08' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2025-07-07 13:11:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-VA-29' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-07-07 13:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LO-02' AS instrumento_codigo, 'Logística' AS valor_texto, '2025-07-07 13:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-52' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-07-21 11:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-26' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-07-29 09:49:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'SMI-FR-01' AS instrumento_codigo, 'Fabricación' AS valor_texto, '2025-08-12 15:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEM-DF-02' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2025-08-13 14:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-DF-02' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2025-08-14 10:58:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-34' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-08-26 10:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-36' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-08-26 10:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-08-26 10:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-20' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-08-26 10:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, 'Validación' AS valor_texto, '2025-08-26 10:40:00' AS fecha_evento
    ) AS base
) AS datos
WHERE datos.instrumento_id IS NOT NULL
  AND datos.departamento_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = datos.instrumento_id
        AND hd.departamento_id = datos.departamento_id
        AND hd.empresa_id = datos.empresa_id
        AND hd.fecha <=> COALESCE(datos.fecha_evento, @now_historial)
  );

COMMIT;
