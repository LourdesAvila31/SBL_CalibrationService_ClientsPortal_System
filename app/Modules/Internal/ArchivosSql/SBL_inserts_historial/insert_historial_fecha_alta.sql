-- Archivo generado automáticamente por convert_historiales_csv.py

START TRANSACTION;
SET @now_historial := NOW();

INSERT INTO historial_fecha_alta (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT
    datos.instrumento_id,
    datos.valor_fecha,
    datos.empresa_id,
    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`
FROM (
    SELECT
        base.empresa_id,
        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,
        base.valor_fecha,
        base.fecha_evento
    FROM (
        SELECT 1 AS empresa_id, 'VIS-DA-02' AS instrumento_codigo, '2024-04-19' AS valor_fecha, '2024-04-19 12:56:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-MA-01' AS instrumento_codigo, '2024-04-26' AS valor_fecha, '2024-04-29 13:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, '2024-04-29' AS valor_fecha, '2024-04-29 14:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-20' AS instrumento_codigo, '2024-04-29' AS valor_fecha, '2024-04-29 14:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, '2024-04-29' AS valor_fecha, '2024-04-29 14:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-27' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:39:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-28' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:39:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-29' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-30' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:41:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-31' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-32' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-33' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-34' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-35' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:47:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-36' AS instrumento_codigo, '2024-05-01' AS valor_fecha, '2024-05-02 07:48:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAE-MI-01' AS instrumento_codigo, '2024-05-08' AS valor_fecha, '2024-05-08 16:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-FR-23' AS instrumento_codigo, '2024-05-31' AS valor_fecha, '2024-05-31 14:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAD-MA-09' AS instrumento_codigo, '2024-06-07' AS valor_fecha, '2024-06-20 15:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-09' AS instrumento_codigo, '2024-06-21' AS valor_fecha, '2024-06-21 16:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-10' AS instrumento_codigo, '2024-06-21' AS valor_fecha, '2024-06-21 16:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-DA-02' AS instrumento_codigo, '2024-06-25' AS valor_fecha, '2024-06-25 13:25:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-DF-01' AS instrumento_codigo, '2024-07-03' AS valor_fecha, '2024-07-03 12:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-20' AS instrumento_codigo, '2024-07-09' AS valor_fecha, '2024-07-09 13:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-22' AS instrumento_codigo, '2024-07-18' AS valor_fecha, '2024-07-19 11:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-23' AS instrumento_codigo, '2024-07-18' AS valor_fecha, '2024-07-19 11:15:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-24' AS instrumento_codigo, '2024-07-18' AS valor_fecha, '2024-07-19 11:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-25' AS instrumento_codigo, '2024-07-18' AS valor_fecha, '2024-07-19 11:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-CP-05' AS instrumento_codigo, '2024-07-26' AS valor_fecha, '2024-07-26 12:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-37' AS instrumento_codigo, '2024-07-29' AS valor_fecha, '2024-07-29 15:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-LC-03' AS instrumento_codigo, '2024-08-05' AS valor_fecha, '2024-08-06 11:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-02' AS instrumento_codigo, '2024-08-08' AS valor_fecha, '2024-08-08 16:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-03' AS instrumento_codigo, '2024-08-08' AS valor_fecha, '2024-08-08 16:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-06' AS instrumento_codigo, '2024-06-07' AS valor_fecha, '2024-08-29 09:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-07' AS instrumento_codigo, '2024-06-07' AS valor_fecha, '2024-08-29 09:24:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'HUM-MA-01' AS instrumento_codigo, '2024-06-07' AS valor_fecha, '2024-08-29 09:33:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-12' AS instrumento_codigo, '2024-06-07' AS valor_fecha, '2024-08-29 09:41:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-13' AS instrumento_codigo, '2024-06-07' AS valor_fecha, '2024-08-29 09:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'LUX-VA-01' AS instrumento_codigo, '2024-09-26' AS valor_fecha, '2024-09-26 08:39:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-AL-01' AS instrumento_codigo, '2024-10-03' AS valor_fecha, '2024-10-04 11:24:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CRO-DF-01' AS instrumento_codigo, '2024-10-04' AS valor_fecha, '2024-10-09 07:51:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-04' AS instrumento_codigo, '2024-11-01' AS valor_fecha, '2024-11-08 15:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-03' AS instrumento_codigo, '2024-11-01' AS valor_fecha, '2024-11-08 15:53:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-05' AS instrumento_codigo, '2024-11-04' AS valor_fecha, '2024-11-08 15:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-06' AS instrumento_codigo, '2024-11-04' AS valor_fecha, '2024-11-08 15:57:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-39' AS instrumento_codigo, '2024-11-19' AS valor_fecha, '2024-11-19 08:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-40' AS instrumento_codigo, '2024-11-19' AS valor_fecha, '2024-11-19 08:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-41' AS instrumento_codigo, '2024-11-19' AS valor_fecha, '2024-11-19 08:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-42' AS instrumento_codigo, '2024-11-19' AS valor_fecha, '2024-11-19 08:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BSM-LC-02' AS instrumento_codigo, '2024-10-23' AS valor_fecha, '2024-11-27 09:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-MA-38' AS instrumento_codigo, '2024-12-03' AS valor_fecha, '2024-12-03 14:36:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-DF-01' AS instrumento_codigo, '2023-11-06' AS valor_fecha, '2024-12-05 14:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-21' AS instrumento_codigo, '2025-01-20' AS valor_fecha, '2025-01-20 15:34:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-22' AS instrumento_codigo, '2025-01-20' AS valor_fecha, '2025-01-20 15:34:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-11' AS instrumento_codigo, '2025-01-20' AS valor_fecha, '2025-01-22 09:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-12' AS instrumento_codigo, '2025-01-20' AS valor_fecha, '2025-01-22 09:21:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-MI-02' AS instrumento_codigo, '2025-01-29' AS valor_fecha, '2025-01-30 14:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-08' AS instrumento_codigo, '2025-02-10' AS valor_fecha, '2025-02-11 10:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-23' AS instrumento_codigo, '2025-04-01' AS valor_fecha, '2025-04-01 15:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-02' AS instrumento_codigo, '2025-04-03' AS valor_fecha, '2025-04-03 07:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-03' AS instrumento_codigo, '2025-04-03' AS valor_fecha, '2025-04-03 08:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-04' AS instrumento_codigo, '2025-05-21' AS valor_fecha, '2025-05-21 09:54:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-05' AS instrumento_codigo, '2025-07-02' AS valor_fecha, '2025-07-03 11:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-06' AS instrumento_codigo, '2025-07-02' AS valor_fecha, '2025-07-03 11:19:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-01' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-02' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-03' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-04' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-05' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-06' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-07' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-08' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:11:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-VA-29' AS instrumento_codigo, '2025-07-04' AS valor_fecha, '2025-07-07 13:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LO-02' AS instrumento_codigo, '2025-07-07' AS valor_fecha, '2025-07-07 13:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-52' AS instrumento_codigo, '2025-07-21' AS valor_fecha, '2025-07-21 11:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-26' AS instrumento_codigo, '2025-07-17' AS valor_fecha, '2025-07-29 09:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'SMI-FR-01' AS instrumento_codigo, '2025-08-12' AS valor_fecha, '2025-08-12 15:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEM-DF-02' AS instrumento_codigo, '2025-08-11' AS valor_fecha, '2025-08-13 14:30:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-DF-02' AS instrumento_codigo, '2025-08-11' AS valor_fecha, '2025-08-14 10:59:00' AS fecha_evento
    ) AS base
) AS datos
WHERE datos.instrumento_id IS NOT NULL
  AND datos.valor_fecha IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM historial_fecha_alta ht
      WHERE ht.instrumento_id = datos.instrumento_id
        AND ht.fecha = datos.valor_fecha
        AND ht.empresa_id = datos.empresa_id
  );

COMMIT;
