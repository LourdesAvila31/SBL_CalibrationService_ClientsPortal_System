-- Archivo generado automáticamente por convert_historiales_csv.py

START TRANSACTION;
SET @now_historial := NOW();

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
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
        SELECT 1 AS empresa_id, 'MIP-DA-12' AS instrumento_codigo, '2024-05-07' AS valor_fecha, '2024-05-07 14:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-03' AS instrumento_codigo, '2024-06-25' AS valor_fecha, '2024-06-25 13:27:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-66' AS instrumento_codigo, '2024-07-03' AS valor_fecha, '2024-07-03 12:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-54' AS instrumento_codigo, '2024-07-10' AS valor_fecha, '2024-07-11 10:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-03' AS instrumento_codigo, '2024-11-01' AS valor_fecha, '2024-11-08 15:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-03' AS instrumento_codigo, '2024-11-01' AS valor_fecha, '2024-11-08 15:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, '2024-11-22' AS valor_fecha, '2024-11-22 14:48:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-03' AS instrumento_codigo, '2025-01-29' AS valor_fecha, '2025-01-30 14:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-34' AS instrumento_codigo, '2025-08-26' AS valor_fecha, '2025-08-26 10:31:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-36' AS instrumento_codigo, '2025-08-26' AS valor_fecha, '2025-08-26 10:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, '2025-08-26' AS valor_fecha, '2025-08-26 10:36:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-20' AS instrumento_codigo, '2025-08-26' AS valor_fecha, '2025-08-26 10:36:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, '2025-08-26' AS valor_fecha, '2025-08-26 10:36:00' AS fecha_evento
    ) AS base
) AS datos
WHERE datos.instrumento_id IS NOT NULL
  AND datos.valor_fecha IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM historial_fecha_baja ht
      WHERE ht.instrumento_id = datos.instrumento_id
        AND ht.fecha = datos.valor_fecha
        AND ht.empresa_id = datos.empresa_id
  );

COMMIT;
