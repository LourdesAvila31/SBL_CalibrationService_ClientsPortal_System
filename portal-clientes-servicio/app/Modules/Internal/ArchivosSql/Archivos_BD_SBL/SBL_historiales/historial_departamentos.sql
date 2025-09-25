-- Archivo generado automáticamente por generate_historial_inserts.py
-- Empresa destino: 1
-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:
-- SET @empresa_id = 1;

SET @empresa_id := IFNULL(@empresa_id, 1);

START TRANSACTION;

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2021-08-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-08-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'AGM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-03-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-03-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-02-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGP-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-03-06', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2024-10-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-10-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2019-01-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-05-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-05-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2021-08-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-08-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2024-07-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2024-05-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2020-06-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-06-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 2, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 2
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-04-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2021-10-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2022-02-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-02-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-08-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-08-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-04-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2025-04-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-04-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2017-11-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2017-12-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 2, @empresa_id, COALESCE(STR_TO_DATE('2018-05-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 2
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-09-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-09-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-01-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-03-06', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2021-01-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-01-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-10-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-10-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2022-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'CON-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'COP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2021-10-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRI-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2022-01-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-01-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2024-10-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-10-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-01-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-01-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-02-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-02-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-02-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-02-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'CRO-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-01-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CTF-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CTS-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-05-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-05-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-05-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-12-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-53'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-54'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-55'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-56'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-57'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-58'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-59'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-60'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-61'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-62'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-63'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-64'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-65'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-66'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-67'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-68'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-69'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-70'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-71'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-72'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-73'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-74'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-75'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-76'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-77'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-78'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-79'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-80'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-81'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DEN-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2022-11-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-05-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-05-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-03-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-10-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'FUS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'HUM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'LAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUV-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-07-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUX-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2024-09-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-09-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUX-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAB-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAB-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2019-08-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-08-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-04-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-04-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-09-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAN-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAN-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-02-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-05-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-09-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-05-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-05-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-06-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-06-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-06-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-06-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-04-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-04-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2023-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-12-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-12-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2022-04-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2022-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2019-01-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2022-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2018-01-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2020-10-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-10-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-10-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-09-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-09-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2022-10-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-10-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIC-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIC-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2023-07-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIM-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2021-05-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-05-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-06-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-06-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2020-08-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-08-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2021-09-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-09-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2020-08-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-08-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-10-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-10-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2021-10-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUT-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-09-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-09-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PIR-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'REG-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'REG-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2020-07-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-07-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'REG-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2023-06-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2021-12-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-12-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2025-08-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-08-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SMI-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'SON-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'SON-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SON-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2021-07-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SON-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2019-10-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-10-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TAC-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TED-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-01-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2019-01-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2021-03-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2025-09-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 1, @empresa_id, COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 1
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2024-01-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, COALESCE(STR_TO_DATE('2025-07-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 8, @empresa_id, COALESCE(STR_TO_DATE('2025-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 8
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 8, @empresa_id, COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 8
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 11, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 11
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2022-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-05-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 10, @empresa_id, COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 10
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-02-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-02-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-02-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-02-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2021-02-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-02-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 12, @empresa_id, COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-03-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRT-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 12
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 6, @empresa_id, COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TTH-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 6
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 7, @empresa_id, COALESCE(STR_TO_DATE('2018-04-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 7
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 5, @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 5
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 3, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 3
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, COALESCE(STR_TO_DATE('2024-04-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 9, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 9
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VIS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

INSERT INTO historial_departamentos (instrumento_id, departamento_id, empresa_id, fecha, `timestamp`)
SELECT i.id, 4, @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'VOR-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_departamentos hd
      WHERE hd.instrumento_id = i.id
        AND hd.departamento_id = 4
        AND hd.empresa_id = @empresa_id
  );

COMMIT;
