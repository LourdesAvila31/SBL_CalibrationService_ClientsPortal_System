-- Archivo generado automáticamente por generate_historial_inserts.py
-- Empresa destino: 1
-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:
-- SET @empresa_id = 1;

SET @empresa_id := IFNULL(@empresa_id, 1);

START TRANSACTION;

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-04-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-04-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-04-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-04-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-04-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-04-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-04-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-04-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-04-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-04-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-08-24', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-08-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-08-24', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-06-24', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-06-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-06-24', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-06-19', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-06-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-06-19', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-04-10', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-04-10', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-11-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-11-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-08-23', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-08-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-08-23', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-03-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-03-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-03-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-04-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-04-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-04-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-02-21', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-02-21', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-08-30', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-08-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-08-30', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-02-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-02-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-02-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-02-21', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-02-21', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-01-19', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-01-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-01-19', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-02-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-02-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-02-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-04-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-04-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-06-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-06-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-08-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-08-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-08-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-10-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-10-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CON-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-10-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-12-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'COP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-12-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-04-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-04-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-04-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-04-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-04-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-04-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-10-13', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-10-13', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-10-13', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-10-13', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-09-23', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-09-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-09-23', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-03-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-03-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-03-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-07-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-07-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-12-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-12-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-09-18', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-09-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-09-18', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-05-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-05-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-05-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-03-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-04-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-04-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-04-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-04-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-04-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-04-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-05-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-05-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-12-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-12-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-07-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-07-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-07-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-12-23', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-12-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-12-23', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-12-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-12-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-12-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-12-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-01-29', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-01-29', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-08-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-08-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-03-26', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-03-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-03-26', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-27', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-27', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-27', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-27', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-27', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-27', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-09-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-09-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAB-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-09-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-09-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAB-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-09-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-09-28', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-09-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-09-28', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-09-28', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-09-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-09-28', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-04-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-04-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-07-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-07-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-04-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-04-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-04-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-04-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-03-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-03-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-03-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-04-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-04-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-04-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-04-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-09-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-09-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-09-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-09-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-03-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-04-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-04-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-04-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-04-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-03-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-04-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-04-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-11-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-11-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAN-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-11-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-07-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-07-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-07-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-11-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-11-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-08-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-08-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-08-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-11-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-11-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-05-31', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-05-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-05-31', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-07-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-07-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-07-25', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-07-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-07-25', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-07-25', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-07-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-07-25', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-07-24', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-07-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-07-24', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-11-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-11-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-11-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-11-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-11-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-11-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-10-19', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-10-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-10-19', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-11-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-11-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-12-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-12-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-11-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-11-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-11-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-11-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-12-06', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-12-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-12-06', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-05-10', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-05-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-05-10', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-12-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-12-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-08-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-08-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-08-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-07-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-07-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-11-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-11-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-01-18', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-01-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-01-18', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-12-06', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-12-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-12-06', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-03-09', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-03-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-03-09', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-01-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-01-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-10-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-10-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-10-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-06-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-06-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-02-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-02-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-02-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-09-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-09-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-10-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-10-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-10-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-07-21', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-07-21', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-08-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-08-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-11-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-11-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-11-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-12-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-12-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-12-23', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-12-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-12-23', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-07-02', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-07-02', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-06-25', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-06-25', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-09-18', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-09-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-09-18', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-01-27', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-01-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-01-27', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-08-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-08-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-05-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-01-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-01-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-05-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-05-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-05-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-09-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-09-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-09-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-05-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-05-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-06-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-06-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2020-09-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2020-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2020-09-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-06-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-06-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-04-12', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-04-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-04-12', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-10-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-10-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-10-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-12-13', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-12-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-12-13', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-12-13', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-12-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-12-13', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PIR-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-04-09', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SON-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-04-09', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-08-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-08-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SON-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-08-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-08-18', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SON-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-08-18', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-08-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-08-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-09-21', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-09-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-09-21', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-02-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-02-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-02-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-03-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-03-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-03-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-08-06', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-08-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-08-06', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-02-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-02-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-09-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-09-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-02-10', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-02-10', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-09-30', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-09-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-09-30', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-08-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-08-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-04-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-04-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-07-10', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-07-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-07-10', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-02-08', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-02-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-02-08', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-01-09', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-01-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-01-09', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-09-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-09-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-04-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-04-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-04-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-04-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2021-10-18', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2021-10-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2021-10-18', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-05-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-07-03', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-07-03', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-03-28', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-03-28', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2025-07-04', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2025-07-04', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-12-06', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-12-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-12-06', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-09-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-09-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-11-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-11-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2018-09-07', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2018-09-07', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-11', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-11', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-11-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-11-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-11-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-11-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-08-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-08-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-08-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-08-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-08-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-08-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2019-08-01', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2019-08-01', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-12-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-12-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-11-22', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-11-22', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-05', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-05', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-01-20', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-01-20', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-01-10', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-01-10', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-06-15', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-06-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-06-15', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-02-09', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-02-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-02-09', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2024-08-30', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2024-08-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2024-08-30', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-01-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-01-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-01-17', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-01-17', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2023-02-09', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2023-02-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2023-02-09', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

INSERT INTO historial_fecha_baja (instrumento_id, fecha, empresa_id, `timestamp`)
SELECT i.id, STR_TO_DATE('2022-12-16', '%Y-%m-%d'), @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRT-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_fecha_baja hfb
      WHERE hfb.instrumento_id = i.id
        AND hfb.fecha = STR_TO_DATE('2022-12-16', '%Y-%m-%d')
        AND hfb.empresa_id = @empresa_id
  );

COMMIT;
