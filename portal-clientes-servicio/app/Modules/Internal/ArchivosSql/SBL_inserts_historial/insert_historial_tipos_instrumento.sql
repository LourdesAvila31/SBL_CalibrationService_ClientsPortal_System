-- Archivo generado automáticamente por convert_historiales_csv.py
-- Prefijo de estado detectado: D

START TRANSACTION;
SET @now_historial := NOW();

INSERT INTO historial_tipos_instrumento (instrumento_id, estado, empresa_id, fecha, `timestamp`)
SELECT
    datos.instrumento_id,
    datos.valor_texto,
    datos.empresa_id,
    COALESCE(datos.fecha_evento, @now_historial) AS fecha,
    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`
FROM (
    SELECT
        base.empresa_id,
        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,
        base.valor_texto,
        base.fecha_evento
    FROM (
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, 'V210002' AS valor_texto, '2024-04-19 12:56:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-MA-01' AS instrumento_codigo, 'A21151470' AS valor_texto, '2024-04-29 13:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-27' AS instrumento_codigo, '13-jun-01' AS valor_texto, '2024-05-02 07:38:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-28' AS instrumento_codigo, '19-feb-08' AS valor_texto, '2024-05-02 07:39:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-29' AS instrumento_codigo, '16-ago-01' AS valor_texto, '2024-05-02 07:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-30' AS instrumento_codigo, '12-sep-02' AS valor_texto, '2024-05-02 07:41:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-31' AS instrumento_codigo, '20-may-06' AS valor_texto, '2024-05-02 07:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-32' AS instrumento_codigo, '04-dic-01' AS valor_texto, '2024-05-02 07:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-33' AS instrumento_codigo, '07-may-01' AS valor_texto, '2024-05-02 07:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-34' AS instrumento_codigo, '02-may-01' AS valor_texto, '2024-05-02 07:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-35' AS instrumento_codigo, '24-jun-05' AS valor_texto, '2024-05-02 07:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-36' AS instrumento_codigo, '22-abr-03' AS valor_texto, '2024-05-02 07:47:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAE-MI-01' AS instrumento_codigo, 'C204465423' AS valor_texto, '2024-05-08 16:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-FR-23' AS instrumento_codigo, '3220100758' AS valor_texto, '2024-05-31 14:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-FR-23' AS instrumento_codigo, '3220100758' AS valor_texto, '2024-05-31 14:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-05' AS instrumento_codigo, 'P17135J' AS valor_texto, '2024-06-21 16:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-06' AS instrumento_codigo, 'P17139J' AS valor_texto, '2024-06-21 16:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-DA-02' AS instrumento_codigo, 'NW23BAA0002429' AS valor_texto, '2024-06-25 13:25:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-20' AS instrumento_codigo, '231150291' AS valor_texto, '2024-07-09 13:00:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-CP-05' AS instrumento_codigo, 'C3025547933' AS valor_texto, '2024-07-26 12:58:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-37' AS instrumento_codigo, '19-oct-15' AS valor_texto, '2024-07-29 15:12:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-LC-13' AS instrumento_codigo, 'D42741588' AS valor_texto, '2024-08-06 11:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-02' AS instrumento_codigo, 'NW23AAA0000620' AS valor_texto, '2024-08-08 16:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-03' AS instrumento_codigo, 'NW23AAA0000607' AS valor_texto, '2024-08-08 16:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-06' AS instrumento_codigo, '79944011636992' AS valor_texto, '2024-08-29 09:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-07' AS instrumento_codigo, '2412JJB02456' AS valor_texto, '2024-08-29 09:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'HUM-MA-01' AS instrumento_codigo, '2409' AS valor_texto, '2024-08-29 09:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'LUX-VA-01' AS instrumento_codigo, '230734195' AS valor_texto, '2024-09-26 08:38:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-AL-01' AS instrumento_codigo, 'D604100010' AS valor_texto, '2024-10-04 11:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BSM-LC-02' AS instrumento_codigo, 'C412794367' AS valor_texto, '2024-11-27 09:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-21' AS instrumento_codigo, '240755468' AS valor_texto, '2025-01-22 08:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-22' AS instrumento_codigo, '240755469' AS valor_texto, '2025-01-22 08:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-11' AS instrumento_codigo, 'E42762007' AS valor_texto, '2025-01-22 09:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-12' AS instrumento_codigo, 'E42762017' AS valor_texto, '2025-01-22 09:20:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-MI-02' AS instrumento_codigo, 'EQ240814-27' AS valor_texto, '2025-01-30 14:26:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-08' AS instrumento_codigo, '1827JB605273' AS valor_texto, '2025-02-11 10:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-23' AS instrumento_codigo, '240755485' AS valor_texto, '2025-04-01 15:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-02' AS instrumento_codigo, '51344815' AS valor_texto, '2025-04-03 07:46:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-02' AS instrumento_codigo, 'BU1-11977' AS valor_texto, '2025-04-03 07:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-03' AS instrumento_codigo, 'C442896093' AS valor_texto, '2025-04-03 08:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-37' AS instrumento_codigo, '65042582' AS valor_texto, '2025-04-03 08:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, '5776' AS valor_texto, '2025-04-03 08:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-20' AS instrumento_codigo, '2568' AS valor_texto, '2025-04-03 08:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, '5763' AS valor_texto, '2025-04-03 08:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-DF-01' AS instrumento_codigo, '620' AS valor_texto, '2025-04-03 08:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-04' AS instrumento_codigo, 'K81N04E6T' AS valor_texto, '2025-05-21 09:53:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-05' AS instrumento_codigo, 'E0FFF144DD8E' AS valor_texto, '2025-07-03 11:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-06' AS instrumento_codigo, 'E0FFF1450C90' AS valor_texto, '2025-07-03 11:19:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-01' AS instrumento_codigo, '06PRBV' AS valor_texto, '2025-07-07 13:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-02' AS instrumento_codigo, '06PRBS' AS valor_texto, '2025-07-07 13:05:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-03' AS instrumento_codigo, '06PRC5' AS valor_texto, '2025-07-07 13:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-04' AS instrumento_codigo, '06PRBW' AS valor_texto, '2025-07-07 13:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-09' AS instrumento_codigo, '06PRBX' AS valor_texto, '2025-07-07 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-10' AS instrumento_codigo, '06PRC2' AS valor_texto, '2025-07-07 13:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-07' AS instrumento_codigo, '06PRC0' AS valor_texto, '2025-07-07 13:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-08' AS instrumento_codigo, '06PRBT' AS valor_texto, '2025-07-07 13:11:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LO-02' AS instrumento_codigo, 'H2000JA095' AS valor_texto, '2025-07-07 13:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-52' AS instrumento_codigo, '14757' AS valor_texto, '2025-07-21 11:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-53' AS instrumento_codigo, '14779' AS valor_texto, '2025-07-21 11:20:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-54' AS instrumento_codigo, '14869' AS valor_texto, '2025-07-21 11:35:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TMB-GC-01' AS instrumento_codigo, '14786' AS valor_texto, '2025-07-21 12:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-56' AS instrumento_codigo, '14827' AS valor_texto, '2025-07-21 12:47:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-03' AS instrumento_codigo, '14854' AS valor_texto, '2025-07-21 12:48:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-04' AS instrumento_codigo, '14793' AS valor_texto, '2025-07-21 12:49:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-05' AS instrumento_codigo, '14767' AS valor_texto, '2025-07-21 12:51:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-06' AS instrumento_codigo, '14870' AS valor_texto, '2025-07-21 12:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-25' AS instrumento_codigo, '14801' AS valor_texto, '2025-07-21 12:53:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-62' AS instrumento_codigo, '14799' AS valor_texto, '2025-07-21 12:54:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-63' AS instrumento_codigo, '14761' AS valor_texto, '2025-07-21 12:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-64' AS instrumento_codigo, '14858' AS valor_texto, '2025-07-21 12:56:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-65' AS instrumento_codigo, '6115' AS valor_texto, '2025-07-21 12:58:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-66' AS instrumento_codigo, '14760' AS valor_texto, '2025-07-21 13:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-67' AS instrumento_codigo, '14829' AS valor_texto, '2025-07-21 13:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-68' AS instrumento_codigo, '5806' AS valor_texto, '2025-07-21 13:05:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-69' AS instrumento_codigo, '5809' AS valor_texto, '2025-07-21 13:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-70' AS instrumento_codigo, '14796' AS valor_texto, '2025-07-21 13:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-71' AS instrumento_codigo, '5801' AS valor_texto, '2025-07-21 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-72' AS instrumento_codigo, '14874' AS valor_texto, '2025-07-21 13:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-73' AS instrumento_codigo, '14856' AS valor_texto, '2025-07-21 13:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-74' AS instrumento_codigo, '6508' AS valor_texto, '2025-07-21 13:12:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-75' AS instrumento_codigo, '6125' AS valor_texto, '2025-07-21 13:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-76' AS instrumento_codigo, '6123' AS valor_texto, '2025-07-21 13:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-77' AS instrumento_codigo, '6114' AS valor_texto, '2025-07-21 13:26:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-78' AS instrumento_codigo, '6490' AS valor_texto, '2025-07-21 13:27:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-79' AS instrumento_codigo, '5818' AS valor_texto, '2025-07-21 13:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-80' AS instrumento_codigo, '5798' AS valor_texto, '2025-07-21 13:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-81' AS instrumento_codigo, '6105' AS valor_texto, '2025-07-21 13:31:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'SMI-FR-01' AS instrumento_codigo, 'AX502204060176' AS valor_texto, '2025-08-12 15:09:00' AS fecha_evento
    ) AS base
) AS datos
WHERE datos.instrumento_id IS NOT NULL
  AND datos.valor_texto IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM historial_tipos_instrumento hti
      WHERE hti.instrumento_id = datos.instrumento_id
        AND hti.estado = datos.valor_texto
        AND hti.empresa_id = datos.empresa_id
        AND hti.fecha <=> COALESCE(datos.fecha_evento, @now_historial)
  );

COMMIT;
